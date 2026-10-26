"""The backfill cursor: capped, failed and overlapping runs skip no Tweets."""

import logging
import math
from typing import Any

import httpx
import pytest
from conftest import Aws
from fake_timeline import RATE_LIMITED, FakeTimeline, sent_ids
from helpers import USERNAME, FakeSqs, install_x_api, x_client
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from twitter_timeline_data_pipeline.handlers import fetch
from twitter_timeline_data_pipeline.handlers.fetch import (
    EnqueueError,
    FetchResult,
    handler,
    run_fetch,
)
from twitter_timeline_data_pipeline.storage import (
    CheckpointState,
    CheckpointStore,
    Gap,
)
from twitter_timeline_data_pipeline.x_api import XApiError


def run(
    aws: Aws,
    timeline: FakeTimeline,
    sqs: FakeSqs,
    *,
    username: str = USERNAME,
    max_results: int = 100,
    max_pages: int = 5,
    **kwargs: Any,
) -> FetchResult:
    with x_client(timeline) as client:
        return run_fetch(
            username=username,
            x_client=client,
            sqs=sqs.client,
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            max_results=max_results,
            max_pages=max_pages,
            sleep=lambda _s: None,
            **kwargs,
        )


def state(aws: Aws, username: str = USERNAME) -> CheckpointState:
    return CheckpointStore(aws.state_table).load(username)


def seed(aws: Aws, newest_id: int, *gaps: Gap) -> None:
    CheckpointStore(aws.state_table).save(
        USERNAME, CheckpointState(str(newest_id), gaps), expected_version=0
    )


def run_until_done(
    aws: Aws, timeline: FakeTimeline, sqs: FakeSqs, **kwargs: Any
) -> list[FetchResult]:
    results: list[FetchResult] = []
    for _ in range(500):
        results.append(run(aws, timeline, sqs, **kwargs))
        if results[-1].stopped_reason is None:
            return results
    raise AssertionError("never finished")  # pragma: no cover


# ---- no Tweet skipped after a capped run ----------------------------------------


def test_capped_run_with_a_checkpoint_skips_nothing_over_later_runs(aws: Aws) -> None:
    timeline = FakeTimeline(100)
    seed(aws, 100)
    timeline.add(25)
    sqs = FakeSqs()

    first = run(aws, timeline, sqs, max_results=5, max_pages=2)

    assert sorted(sent_ids(sqs)) == list(range(116, 126))
    assert (first.newest_id, first.gaps_open, first.stopped_reason) == (
        "125",
        1,
        "page_cap",
    )
    assert state(aws).gaps == (Gap(until_id="116", since_id="100"),)

    rest = run_until_done(aws, timeline, sqs, max_results=5, max_pages=2)

    assert len(rest) == 2
    assert sorted(sent_ids(sqs)) == list(range(101, 126))  # each exactly once
    assert state(aws) == CheckpointState("125", (), version=state(aws).version)


def test_new_tweets_keep_flowing_while_a_gap_is_open(aws: Aws) -> None:
    timeline = FakeTimeline(100)
    seed(aws, 100)
    timeline.add(25)
    sqs = FakeSqs()
    run(aws, timeline, sqs, max_results=5, max_pages=1)
    timeline.add(3)

    run(aws, timeline, sqs, max_results=5, max_pages=2)

    assert {126, 127, 128} <= set(sent_ids(sqs))
    assert state(aws).newest_id == "128"
    assert len(state(aws).gaps) == 1


def test_gap_phase_orders_by_upper_bound(aws: Aws) -> None:
    timeline = FakeTimeline(70)
    seed(aws, 70, Gap("20", "10"), Gap("60", "50"))
    sqs = FakeSqs()

    run(aws, timeline, sqs, max_results=5, max_pages=1)

    assert sent_ids(sqs) == [59, 58, 57, 56, 55]
    assert state(aws).gaps == (Gap("20", "10"), Gap("55", "50"))


def test_a_gap_beyond_the_apis_window_is_dropped_with_a_warning(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    timeline = FakeTimeline(100, window=50)
    seed(aws, 100, Gap(until_id="30", since_id="5"))

    with caplog.at_level(logging.WARNING, logger=fetch.logger.name):
        result = run(aws, timeline, FakeSqs())

    assert (result.gaps_open, result.stopped_reason) == (0, None)
    assert state(aws).gaps == ()
    assert (
        f"gap for {USERNAME} (since_id=5, until_id=30) is out of the API's reach "
        "(no Tweets returned); dropping it"
    ) in caplog.messages


# ---- rate limits and failures resume instead of restarting ----------------------


def test_rate_limit_mid_run_saves_progress_and_the_next_run_resumes(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    timeline = FakeTimeline(3200, inject=lambda n, _r: RATE_LIMITED if n == 3 else None)
    sqs = FakeSqs()

    with caplog.at_level(logging.WARNING, logger=fetch.logger.name):
        first = run(aws, timeline, sqs, max_pages=32)

    assert (first.stopped_reason, first.enqueued, first.gaps_open) == (
        "rate_limited",
        200,
        1,
    )
    assert any("rate limited, stopping early" in m for m in caplog.messages)
    assert state(aws).gaps == (Gap(until_id="3001"),)
    assert sorted(sent_ids(sqs)) == list(range(3001, 3201))

    second = run(aws, timeline, sqs, max_pages=32)

    assert second.stopped_reason is None
    assert second.enqueued == 3000
    assert timeline.timeline_calls[3 + 1]["until_id"] == "3001"  # resumes at the cursor
    ids = sent_ids(sqs)
    assert len(ids) == len(set(ids)) == 3200  # nothing re-sent


def test_rate_limit_on_the_first_request_changes_nothing(aws: Aws) -> None:
    timeline = FakeTimeline(10, inject=lambda n, _r: RATE_LIMITED if n == 1 else None)

    result = run(aws, timeline, FakeSqs())

    assert (result.stopped_reason, result.pages, result.gaps_open) == (
        "rate_limited",
        0,
        0,
    )
    assert state(aws) == CheckpointState()


def test_failure_on_page_n_resumes_after_the_last_full_page(aws: Aws) -> None:
    timeline = FakeTimeline(
        50,
        inject=lambda n, _r: (
            httpx.Response(400, json={"detail": "boom"}) if n == 4 else None
        ),
    )
    sqs = FakeSqs()

    with pytest.raises(XApiError, match="boom"):
        run(aws, timeline, sqs, max_results=5, max_pages=10)

    assert sorted(sent_ids(sqs)) == list(range(36, 51))
    run_until_done(aws, timeline, sqs, max_results=5, max_pages=10)
    ids = sent_ids(sqs)
    assert len(ids) == len(set(ids)) == 50


def test_enqueue_failure_resends_only_the_page_in_flight(aws: Aws) -> None:
    timeline = FakeTimeline(50)
    sqs = FakeSqs(lambda call, entry_ids: set(entry_ids) if call >= 1 else set())

    with pytest.raises(EnqueueError):
        run(aws, timeline, sqs, max_results=5, max_pages=10)

    assert state(aws) == CheckpointState("50", (Gap("46"),), version=1)
    second = FakeSqs()
    run_until_done(aws, timeline, second, max_results=5, max_pages=10)
    assert sorted(sent_ids(second)) == list(range(1, 46))  # page 46-50 not re-sent


# ---- first-run backfill over many runs -------------------------------------------


@pytest.mark.parametrize("max_pages", [1, 4, 5, 7, 32, 40])
def test_first_run_backfills_the_whole_window_over_ceil_32_over_pages_runs(
    aws: Aws, max_pages: int
) -> None:
    timeline = FakeTimeline(3300)  # the API serves the newest 3,200 only
    sqs = FakeSqs()

    results = run_until_done(aws, timeline, sqs, max_results=100, max_pages=max_pages)

    assert len(results) == math.ceil(32 / max_pages)
    assert sorted(sent_ids(sqs)) == list(range(101, 3301))
    assert results[-1].gaps_open == 0
    assert state(aws).gaps == ()


# ---- overlapping runs ------------------------------------------------------------


@pytest.mark.parametrize("interleave_at", [1, 2])
def test_overlapping_run_cannot_regress_newest_id_or_drop_a_gap(
    aws: Aws, interleave_at: int
) -> None:
    other_result: list[FetchResult] = []
    calls_after_b = 0
    sqs_a, sqs_b = FakeSqs(), FakeSqs()

    def interleave(n: int, _request: httpx.Request) -> httpx.Response | None:
        nonlocal calls_after_b
        if n == interleave_at and not other_result:
            # Run B starts and finishes while run A waits for this response.
            other_result.append(run(aws, timeline, sqs_b, max_results=5, max_pages=1))
            calls_after_b = len(timeline.timeline_calls)
        return None

    timeline = FakeTimeline(100, inject=interleave)
    seed(aws, 100)
    timeline.add(20)

    a = run(aws, timeline, sqs_a, max_results=5, max_pages=3)

    (b,) = other_result
    assert a.stopped_reason == "state_conflict"
    assert len(timeline.timeline_calls) == calls_after_b  # A fetched nothing more
    final = state(aws)
    assert final.newest_id == b.newest_id  # not regressed
    # B's view survives intact: the gap below the pages B (not A) wrote last.
    assert final.gaps == (Gap(until_id=str(121 - 5 * interleave_at), since_id="100"),)
    # Whatever A already enqueued is a duplicate of B's, and the rest still arrives.
    run_until_done(aws, timeline, sqs_b, max_results=5, max_pages=3)
    assert set(sent_ids(sqs_a)) | set(sent_ids(sqs_b)) == set(range(101, 121))


# ---- legacy state ----------------------------------------------------------------


def test_legacy_state_item_is_read_and_upgraded(aws: Aws) -> None:
    aws.state_table.put_item(
        Item={"username": USERNAME, "newest_id": 100, "updated_at": "2026-01-01"}
    )
    timeline = FakeTimeline(110)
    sqs = FakeSqs()

    result = run(aws, timeline, sqs)

    assert sorted(sent_ids(sqs)) == list(range(101, 111))
    assert timeline.timeline_calls[0]["since_id"] == "100"
    assert result.stopped_reason is None
    assert state(aws) == CheckpointState("110", (), version=1)


# ---- manual backfill -------------------------------------------------------------


def test_backfill_gap_is_fetched_before_the_run_ends(aws: Aws) -> None:
    timeline = FakeTimeline(120)
    seed(aws, 120)
    sqs = FakeSqs()

    result = run(aws, timeline, sqs, backfill=Gap(until_id="111", since_id="100"))

    assert sorted(sent_ids(sqs)) == list(range(101, 111))
    assert (result.gaps_open, result.stopped_reason) == (0, None)


def test_backfill_survives_a_cap_and_is_not_added_twice(aws: Aws) -> None:
    timeline = FakeTimeline(120)
    seed(aws, 120)
    gap = Gap(until_id="111", since_id="100")

    run(aws, timeline, FakeSqs(), max_results=5, max_pages=1, backfill=gap)
    assert state(aws).gaps == (Gap("106", "100"),)
    run(
        aws, timeline, FakeSqs(), max_results=5, max_pages=1, backfill=Gap("106", "100")
    )

    assert state(aws).gaps == ()


def test_backfill_without_any_state_starts_from_nothing(aws: Aws) -> None:
    timeline = FakeTimeline(30)
    sqs = FakeSqs()

    result = run(aws, timeline, sqs, max_results=5, max_pages=1, backfill=Gap("12"))

    assert result.newest_id == "30"
    assert result.gaps_open == 2  # the rest of the new Tweets, and the backfill


def test_handler_backfill_event(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    timeline = FakeTimeline(120)
    seed(pipeline_env, 120)
    install_x_api(monkeypatch, fetch, timeline)

    result = handler(
        {"username": USERNAME, "backfill": {"since_id": 100, "until_id": "111"}}, None
    )

    assert result["enqueued"] == 10
    assert result["gaps_open"] == 0
    assert timeline.timeline_calls[-1]["until_id"] == "111"
    assert timeline.timeline_calls[-1]["since_id"] == "100"


def test_handler_backfill_with_one_bound_or_none(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    timeline = FakeTimeline(30)
    install_x_api(monkeypatch, fetch, timeline)
    monkeypatch.setenv("MAX_RESULTS", "5")
    monkeypatch.setenv("MAX_PAGES", "1")

    handler({"username": USERNAME, "backfill": {}}, None)

    assert state(pipeline_env).gaps == (Gap(), Gap(until_id="26"))


@pytest.mark.parametrize(
    "backfill",
    [
        {"since_id": "abc"},
        {"until_id": "12.5"},
        {"until_id": ""},
        {"since_id": "-1"},
        {"since_id": "1", "from": "2"},
        "yes",
        True,
        [1, 2],
    ],
)
def test_handler_rejects_a_malformed_backfill_before_touching_anything(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch, backfill: Any
) -> None:
    timeline = FakeTimeline(30)
    install_x_api(monkeypatch, fetch, timeline)

    with pytest.raises(ValueError):
        handler({"username": USERNAME, "backfill": backfill}, None)

    assert timeline.timeline_calls == []
    assert pipeline_env.state_table.scan()["Count"] == 0


# ---- property: no ID above the initial checkpoint is ever missed -----------------

_failures = st.lists(
    st.tuples(st.integers(1, 6), st.sampled_from(["429", "500"])), max_size=3
)


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    initial=st.integers(0, 60),
    window=st.integers(10, 120),
    rounds=st.lists(
        st.tuples(st.integers(0, 40), st.integers(1, 4), _failures),
        min_size=1,
        max_size=5,
    ),
    max_results=st.sampled_from([5, 7, 20, 100]),
    has_checkpoint=st.booleans(),
)
def test_no_tweet_above_the_initial_checkpoint_is_ever_missed(
    aws: Aws,
    initial: int,
    window: int,
    rounds: list[tuple[int, int, list[tuple[int, str]]]],
    max_results: int,
    has_checkpoint: bool,
) -> None:
    username = USERNAME  # the fake API only knows this account; start from clean state
    aws.state_table.delete_item(Key={"username": username})
    failing: dict[int, str] = {}

    def inject(n: int, _request: httpx.Request) -> httpx.Response | None:
        match failing.get(n):
            case "429":
                return RATE_LIMITED
            case "500":
                return httpx.Response(500)
            case _:
                return None

    timeline = FakeTimeline(initial, window=window, inject=inject)
    checkpoint = initial if has_checkpoint else 0
    if has_checkpoint:
        CheckpointStore(aws.state_table).save(
            username, CheckpointState(str(initial)), expected_version=0
        )
    sqs = FakeSqs()

    def attempt(max_pages: int, plan: list[tuple[int, str]]) -> FetchResult | None:
        """One run with ``plan``'s failures at request numbers counted from here."""
        base = len(timeline.timeline_calls)
        failing.clear()
        for k, kind in plan:
            # 500s are retried by the client, so fail enough requests in a row.
            for n in range(k, k + (4 if kind == "500" else 1)):
                failing[base + n] = kind
        try:
            return run(
                aws,
                timeline,
                sqs,
                username=username,
                max_results=max_results,
                max_pages=max_pages,
            )
        except XApiError:
            return None

    for arrivals, max_pages, plan in rounds:
        timeline.add(arrivals)
        attempt(max_pages, plan)

    for _ in range(300):
        result = attempt(3, [])
        if result is not None and result.stopped_reason is None:
            break
    else:  # pragma: no cover
        raise AssertionError("did not converge")

    expected = {i for i in timeline.served if i > checkpoint}
    assert expected <= set(sent_ids(sqs))
    final = CheckpointStore(aws.state_table).load(username)
    assert final.gaps == ()
    if expected:
        assert final.newest_id == str(timeline.ids[-1])
