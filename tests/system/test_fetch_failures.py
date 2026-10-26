"""The fetch function failing partway (SQS, X API errors, auth, rate limits)."""

from collections.abc import Mapping
from typing import Any

from system.checks import assert_quiet, assert_stored, assert_stored_once
from system.conftest import MakeSim
from system.harness import message_tweet_id
from system.runtime import sqs_partial_failure


def test_sqs_entries_failing_on_every_attempt_fail_the_run_without_loss(
    make_sim: MakeSim,
) -> None:
    """During run 1, SQS rejects two entries of the second ``SendMessageBatch`` on
    every attempt (``Failed``, not ``SenderFault``), so the run raises.

    Expected: run 2 sends what run 1 didn't, and every Tweet is stored once (anything
    sent twice is absorbed as a duplicate).
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    posted = [x.post(user, media=[x.photo()] if i % 3 == 0 else []) for i in range(25)]
    doomed = {posted[-12].id, posted[-15].id}  # both in the second batch of ten
    run2_at = sim.clock.now + 3600

    def rejected(entry: Mapping[str, Any]) -> bool:
        return message_tweet_id(entry["MessageBody"]) in doomed

    sim.aws.inject(
        lambda call: call.operation == "SendMessageBatch" and call.now < run2_at,
        sqs_partial_failure(sim.sqs, rejected),
        times=None,
    )

    (run1,) = sim.run(1)
    assert run1.outcome == "error"
    (run2,) = sim.run(1)
    assert run2.outcome == "ok"

    assert_stored_once(sim, posted)
    assert sim.checkpoint(user.lower()) == posted[-1].id
    assert_quiet(sim)


def test_sqs_entry_failing_once_is_resent_within_the_run(make_sim: MakeSim) -> None:
    """One entry is reported ``Failed`` on its first attempt only.

    Expected: the run retries just that entry and succeeds; each Tweet is delivered
    once.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    posted = [x.post(user) for _ in range(12)]
    target = posted[3].id
    sim.aws.inject(
        lambda call: call.operation == "SendMessageBatch",
        sqs_partial_failure(
            sim.sqs, lambda entry: message_tweet_id(entry["MessageBody"]) == target
        ),
        times=1,
    )

    (run,) = sim.run(1)

    assert run.outcome == "ok"
    assert_stored_once(sim, posted)
    assert all(len(sim.deliveries_of(t.id)) == 1 for t in posted)
    assert_quiet(sim)


def test_x_api_5xx_exhausting_retries_fails_the_run_without_loss(
    make_sim: MakeSim,
) -> None:
    """Page 2 of run 1 answers 503 on all four attempts (the client retries three
    times with backoff).

    Expected: run 1 raises after page 1 was enqueued; run 2 fetches what is missing;
    every Tweet is stored once.
    """
    sim = make_sim(MaxResults=50)
    x, user = sim.x, sim.username
    posted = [x.post(user, media=[x.photo()]) for _ in range(120)]
    sim.x.fail("timeline", 503, page=2, times=4)

    (run1,) = sim.run(1)
    assert run1.outcome == "error"
    (run2,) = sim.run(1)
    assert run2.outcome == "ok"

    assert_stored_once(sim, posted)
    run1_calls = [c for c in sim.x.calls_to("timeline") if c.now < run2.started]
    assert [c.page for c in run1_calls] == [1, 2, 2, 2, 2]  # 3 retries, then give up
    assert_quiet(sim)


def test_token_revoked_mid_run_recovers_after_rotation(make_sim: MakeSim) -> None:
    """The bearer token is revoked between page 1 and page 2 of a run, so the X API
    answers 401. The operator stores a new token with ``put_secret.sh``.

    Expected: the run fails with the auth error (alarm), the cached token is dropped,
    and the next run reads the new token and stores every Tweet with nothing lost.
    """
    sim = make_sim(MaxResults=20)
    x, user = sim.x, sim.username
    posted = [x.post(user) for _ in range(50)]

    def revoke(_call: object) -> None:
        sim.x.tokens = {"rotated-token"}

    sim.x.before(lambda call: call.endpoint == "timeline" and call.page == 2, revoke)

    (run1,) = sim.run(1)
    assert run1.outcome == "error"
    assert type(run1.error).__name__ == "AuthError"
    sim.put_secret("rotated-token")
    posted += [x.post(user) for _ in range(3)]
    (run2,) = sim.run(1)
    assert run2.outcome == "ok"

    assert_stored_once(sim, posted)
    assert_quiet(sim)


def test_token_rotated_within_the_cache_ttl_recovers_on_the_next_invoke(
    make_sim: MakeSim,
) -> None:
    """The token is rotated five minutes after a run, while the old one is still in
    the function's 15-minute token cache, and a manual invoke follows.

    Expected: that invoke fails on the 401 and drops the cache; the following invoke
    reads the new secret and succeeds; nothing is lost.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    posted = [x.post(user)]
    assert sim.invoke_fetch().outcome == "ok"
    sim.advance(300)
    sim.x.tokens = {"rotated-token"}
    sim.put_secret("rotated-token")
    posted.append(x.post(user))

    stale = sim.invoke_fetch()
    fresh = sim.invoke_fetch()
    sim.settle()

    assert (stale.outcome, fresh.outcome) == ("error", "ok")
    assert_stored_once(sim, posted)


def test_rate_limit_with_a_far_reset_loses_nothing_once_later_runs_succeed(
    make_sim: MakeSim,
) -> None:
    """Run 1 hits a 429 on page 2 whose window resets in 15 minutes, beyond what the
    client will wait.

    Expected: run 1 stops (an error today); the next run, after the reset, fetches
    everything; every Tweet is stored once and the checkpoint is the newest ID.
    """
    sim = make_sim(MaxResults=50)
    x, user = sim.x, sim.username
    posted = [x.post(user) for _ in range(140)]
    sim.x.rate_limit_until(sim.clock.now + 15 * 60, page=2)

    sim.run(2)
    posted += [x.post(user) for _ in range(5)]
    sim.run(1)

    assert_stored_once(sim, posted)
    assert sim.checkpoint(user.lower()) == posted[-1].id
    assert_quiet(sim)


def test_rate_limit_with_a_near_reset_is_waited_out_within_the_run(
    make_sim: MakeSim,
) -> None:
    """Page 2 answers 429 with a window that resets in 20 seconds.

    Expected: the run sleeps until the reset (on the simulated clock), continues and
    succeeds; every Tweet is stored once.
    """
    sim = make_sim(MaxResults=50)
    x, user = sim.x, sim.username
    posted = [x.post(user) for _ in range(120)]
    sim.x.rate_limit_until(sim.clock.now + 20, page=2)

    (run,) = sim.run(1)

    assert run.outcome == "ok"
    assert run.ended - run.started >= 20
    assert_stored(sim, posted)
    assert_stored_once(sim, posted)
