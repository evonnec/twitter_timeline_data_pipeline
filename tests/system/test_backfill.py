"""Backlogs bigger than one run's page budget (``MaxPages x MaxResults``)."""

from collections import Counter

from system.checks import assert_quiet, assert_stored_once, enqueued_tweet_ids
from system.conftest import MakeSim


def test_backlog_over_the_page_cap_is_fetched_by_later_runs(make_sim: MakeSim) -> None:
    """With ``MaxPages=2`` and ``MaxResults=5`` (10 Tweets a run), the account posts 25
    Tweets between two runs after the checkpoint was set.

    Expected: later runs fetch the 15 Tweets the capped run couldn't reach; every Tweet
    is stored once and the queue and DLQ end empty.
    """
    sim = make_sim(MaxPages=2, MaxResults=5)
    x, user = sim.x, sim.username
    posted = [x.post(user) for _ in range(3)]
    sim.run(1)
    posted += [x.post(user) for _ in range(25)]

    sim.run(4)

    assert_stored_once(sim, posted)
    assert_quiet(sim)


def test_first_run_backfill_of_a_long_timeline_completes_over_runs(
    make_sim: MakeSim,
) -> None:
    """The account already has 600 posts (within the API's 3,200-post window) when the
    pipeline starts, with ``MaxPages=5`` and ``MaxResults=20`` (100 Tweets a run).

    Expected: successive runs keep backfilling until the timeline is exhausted, so
    after enough runs all 600 posts are stored, once each.
    """
    sim = make_sim(MaxPages=5, MaxResults=20)
    x, user = sim.x, sim.username
    start = sim.clock.now - 600 * 3600
    posted = [x.post(user, at=start + i * 3600) for i in range(600)]

    sim.run(8)

    assert len(sim.tweets()) == 600
    assert_stored_once(sim, posted)
    assert_quiet(sim)


def test_run_failing_on_a_late_page_resumes_without_resending(
    make_sim: MakeSim,
) -> None:
    """Run 1 enqueues pages 1 and 2 of a 150-Tweet backlog, then page 3 answers 429
    with a far reset.

    Expected: run 2 resumes from page 3, so at most one page (the one in flight) is
    sent twice, and every Tweet is stored once.
    """
    sim = make_sim(MaxResults=50)
    x, user = sim.x, sim.username
    posted = [x.post(user) for _ in range(150)]
    sim.x.rate_limit_until(sim.clock.now + 30 * 60, page=3)

    sim.run(2)

    assert_stored_once(sim, posted)
    sends = Counter(enqueued_tweet_ids(sim))
    assert sum(sends.values()) <= len(posted) + 50
    assert_quiet(sim)
