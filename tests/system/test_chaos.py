"""Random schedules of transient faults: nothing may be lost or stored twice.

Each example is a few hourly runs. Before each run the account posts a few Tweets and
some faults are armed for that run's hour: X API 5xx and 429s, SQS entries failing,
the checkpoint write failing, CDN 503s, the process runtime crashing, tweets-table
writes failing, and SQS duplicating deliveries. Every fault is one the pipeline is
documented to survive, and they are bounded so that no message fails more often than
``maxReceiveCount`` allows and no backlog outgrows a run's page budget (those cases
belong to #12 and #11). After a final clean run the end state must be exact.
"""

from dataclasses import dataclass, field

import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from moto import mock_aws

from system.checks import assert_quiet, assert_stored_once
from system.fake_x import ApiCall, FakeMedia, FakeTweet
from system.harness import Simulation
from system.runtime import (
    AwsCall,
    all_package_modules,
    duplicating_send,
    reset_module_state,
    sqs_partial_failure,
)

PARAMETERS = {"MaxPages": 5, "MaxResults": 5}  # 25 Tweets a run; at most 4 x 5 posted


@dataclass(frozen=True)
class RunPlan:
    posts: tuple[str, ...]
    """One entry per Tweet: "text", "photo" or "video"."""
    x_5xx: tuple[int, int] | None = None
    """(page, attempts) answered 503."""
    rate_limit: float | None = None
    """Seconds until the 429 window on page 1 resets (near: waited out; far: not)."""
    sqs_entry_fails: str | None = None
    """"once" (retried within the run) or "run" (every attempt this run)."""
    checkpoint_write_fails: bool = False
    cdn_503: int = 0
    """503s each new medium answers before it is served."""
    process_crash_at_put: int | None = None
    tweet_writes_fail: int = 0
    duplicate_sends: bool = False


run_plans = st.builds(
    RunPlan,
    posts=st.lists(st.sampled_from(["text", "photo", "video"]), max_size=4).map(tuple),
    x_5xx=st.none() | st.tuples(st.integers(1, 2), st.integers(1, 4)),
    rate_limit=st.none() | st.sampled_from([5.0, 45.0, 900.0]),
    sqs_entry_fails=st.sampled_from([None, "once", "run"]),
    checkpoint_write_fails=st.booleans(),
    cdn_503=st.integers(0, 2),
    process_crash_at_put=st.none() | st.integers(1, 6),
    tweet_writes_fail=st.integers(0, 1),
    duplicate_sends=st.booleans(),
)


@dataclass
class _Window:
    start: float
    end: float
    puts: list[AwsCall] = field(default_factory=list)

    def __contains__(self, now: float) -> bool:
        return self.start <= now < self.end


def _arm(
    sim: Simulation, plan: RunPlan, window: _Window, media: list[FakeMedia]
) -> None:
    def in_run(call: ApiCall | AwsCall) -> bool:
        return call.now in window

    if plan.x_5xx is not None:
        page, attempts = plan.x_5xx
        sim.x.inject(
            lambda call: (
                call.endpoint == "timeline" and call.page == page and in_run(call)
            ),
            lambda _call: httpx.Response(503, json={"title": "Service Unavailable"}),
            times=attempts,
        )
    if plan.rate_limit is not None:
        sim.x.rate_limit_until(window.start + plan.rate_limit, page=1)
    if plan.duplicate_sends:
        sim.aws.inject(
            lambda call: call.operation == "SendMessageBatch" and in_run(call),
            duplicating_send(sim.sqs),
            times=None,
        )
    if plan.sqs_entry_fails is not None:
        sim.aws.inject(
            lambda call: call.operation == "SendMessageBatch" and in_run(call),
            sqs_partial_failure(sim.sqs, lambda entry: entry["Id"] == "0"),
            times=1 if plan.sqs_entry_fails == "once" else None,
        )
    if plan.checkpoint_write_fails:
        sim.aws.fail(
            "PutItem",
            when=lambda call: call.function == sim.fetch.name and in_run(call),
        )
    for medium in media:
        if medium.download_url and plan.cdn_503:
            sim.cdn.fail(medium.download_url, 503, times=plan.cdn_503)
    if plan.process_crash_at_put is not None:
        crash_at = plan.process_crash_at_put

        def nth_put(call: AwsCall) -> bool:
            if call.function == sim.process.name and in_run(call):
                window.puts.append(call)
            return len(window.puts) == crash_at

        sim.aws.crash("PutItem", when=nth_put)
    if plan.tweet_writes_fail:
        sim.aws.fail(
            "PutItem",
            when=lambda call: call.function == sim.process.name and in_run(call),
            times=plan.tweet_writes_fail,
        )


def _post(sim: Simulation, kind: str) -> FakeTweet:
    x = sim.x
    match kind:
        case "photo":
            return x.post(sim.username, media=[x.photo()])
        case "video":
            return x.post(sim.username, media=[x.video()])
        case _:
            return x.post(sim.username)


@settings(
    max_examples=12,
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(plans=st.lists(run_plans, min_size=1, max_size=4))
def test_transient_faults_never_lose_or_duplicate_a_tweet(plans: list[RunPlan]) -> None:
    """Whatever mix of survivable faults hits the runs, after a clean final run every
    Tweet is stored exactly once with its media (each medium downloaded once), the
    queue and DLQ are empty and the checkpoint is the newest Tweet ID."""
    with pytest.MonkeyPatch.context() as mp, mock_aws():
        try:
            sim = Simulation(mp, parameters=PARAMETERS)
            posted: list[FakeTweet] = []
            for plan in plans:
                new = [_post(sim, kind) for kind in plan.posts]
                posted += new
                start = sim.schedule.next_at
                window = _Window(start, start + sim.schedule.spec.every_seconds)
                _arm(sim, plan, window, [m for t in new for m in t.media])
                sim.run(1)
            sim.run(1)
            sim.settle()

            assert_stored_once(sim, posted)
            assert_quiet(sim)
            if posted:
                assert sim.checkpoint(sim.username.lower()) == posted[-1].id
        finally:
            reset_module_state(all_package_modules())
