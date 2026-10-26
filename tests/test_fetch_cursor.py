"""The backfill cursor: capped, failed and overlapping runs skip no Tweets."""

import itertools
import logging
import math
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from conftest import Aws
from fake_timeline import (
    EMPTY_OK,
    ERRORS_ONLY,
    RATE_LIMITED,
    FakeTimeline,
    RecordingSqs,
    sent_ids,
)
from helpers import USERNAME, FakeSqs, install_x_api, x_client
from hypothesis import HealthCheck, example, given, settings
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

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
_ticks = itertools.count()


def _daily_clock() -> datetime:
    """Default test clock: every call is a day after the last, so empty gaps can age
    out however few runs a test makes."""
    return EPOCH + timedelta(days=next(_ticks))


def run(
    aws: Aws,
    timeline: FakeTimeline,
    sqs: FakeSqs | RecordingSqs,
    *,
    username: str = USERNAME,
    max_results: int = 100,
    max_pages: int = 5,
    checkpoints: CheckpointStore | None = None,
    **kwargs: Any,
) -> FetchResult:
    with x_client(timeline) as client:
        return run_fetch(
            username=username,
            x_client=client,
            sqs=sqs.client,
            queue_url=aws.queue_url,
            checkpoints=checkpoints or CheckpointStore(aws.state_table),
            max_results=max_results,
            max_pages=max_pages,
            sleep=lambda _s: None,
            **{"now": _daily_clock, **kwargs},
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

    assert len(rest) == 3  # each run spends one request checking for new Tweets
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


def test_gap_phase_serves_the_newest_gap_first_and_always_the_oldest(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(70)
    seed(aws, 70, Gap("20", "10"), Gap("60", "50"))
    sqs = FakeSqs()

    # One request checks for new Tweets, one goes to the oldest gap, one is spare.
    run(aws, timeline, sqs, max_results=5, max_pages=3)

    assert sent_ids(sqs) == [59, 58, 57, 56, 55, 19, 18, 17, 16, 15]
    assert state(aws).gaps == (Gap("55", "50"), Gap("15", "10"))


def test_the_oldest_gap_still_advances_when_the_budget_is_tiny(aws: Aws) -> None:
    timeline = FakeTimeline(70)
    seed(aws, 70, Gap("20", "10"), Gap("60", "50"))
    sqs = FakeSqs()

    run(aws, timeline, sqs, max_results=5, max_pages=1)  # raised to 2 with gaps

    assert sent_ids(sqs) == [19, 18, 17, 16, 15]
    assert len(timeline.timeline_calls) == 2


def test_requests_count_against_the_budget_even_when_they_come_back_empty(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100, window=50)
    seed(aws, 100, *(Gap(str(i * 3 + 3), str(i * 3)) for i in range(1, 14)))

    run(aws, timeline, FakeSqs(), max_pages=2)

    assert len(timeline.timeline_calls) == 2


def at(hours: float) -> Callable[[], datetime]:
    return lambda: EPOCH + timedelta(hours=hours)


def iso(hours: float) -> str:
    return (EPOCH + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_a_gap_beyond_the_apis_window_is_dropped_after_three_runs_over_a_day(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    timeline = FakeTimeline(100, window=50)
    seed(aws, 100, Gap(until_id="30", since_id="5"))

    with caplog.at_level(logging.INFO, logger=fetch.logger.name):
        results = [run(aws, timeline, FakeSqs(), now=at(h)) for h in (0, 1, 2)]
        assert state(aws).gaps == (Gap("30", "5", 2, iso(0)),)  # too young to drop
        results.append(run(aws, timeline, FakeSqs(), now=at(24)))

    assert [r.gaps_open for r in results] == [1, 1, 1, 0]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert [r.getMessage() for r in warnings] == [
        f"dropping gap for {USERNAME} (since_id=5, until_id=30): out of the API's "
        f"reach (empty 3 runs since {iso(0)})"
    ]
    assert state(aws).gaps == ()


def test_many_empty_runs_in_less_than_a_day_never_drop_the_gap(aws: Aws) -> None:
    timeline = FakeTimeline(100, window=50)
    seed(aws, 100, Gap(until_id="30", since_id="5"))

    for i in range(30):
        run(aws, timeline, FakeSqs(), now=at(i * 0.75))  # 0 .. 21.75 hours

    assert state(aws).gaps == (Gap("30", "5", 2, iso(0)),)  # waiting, not re-asked


def test_a_short_api_incident_does_not_drop_a_gap_that_still_has_data(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100)
    seed(aws, 100, Gap(until_id="60", since_id="40"))
    timeline.inject = lambda n, _r: EMPTY_OK if n >= 1 else None
    for h in (0, 1, 2, 3):
        run(aws, timeline, FakeSqs(), now=at(h))
    assert state(aws).gaps == (Gap("60", "40", 2, iso(0)),)  # waiting out the day

    timeline.inject = None
    sqs = FakeSqs()
    run(aws, timeline, sqs, now=at(4))
    assert sent_ids(sqs) == []  # still waiting: delayed, not lost
    run_until_done(aws, timeline, sqs, now=at(25))
    assert sorted(sent_ids(sqs)) == list(range(41, 60))


def test_data_returning_resets_both_empty_fields(aws: Aws) -> None:
    timeline = FakeTimeline(100)
    seed(aws, 100, Gap(until_id="60", since_id="40"))
    timeline.inject = lambda n, _r: EMPTY_OK if n >= 1 else None
    run(aws, timeline, FakeSqs(), now=at(0))
    run(aws, timeline, FakeSqs(), now=at(1))
    assert state(aws).gaps == (Gap("60", "40", 2, iso(0)),)

    timeline.inject = None
    run(aws, timeline, FakeSqs(), max_results=5, max_pages=2, now=at(25))

    assert state(aws).gaps == (Gap("55", "40"),)
    assert state(aws).gaps[0].first_empty_at is None


def test_a_legacy_gap_without_a_timestamp_gets_one_and_is_dropped_a_day_later(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100, window=50)
    aws.state_table.put_item(
        Item={
            "username": USERNAME,
            "newest_id": 100,
            "version": 1,
            "gaps": [{"until_id": 30, "since_id": 5, "empty_runs": 5}],
        }
    )
    assert state(aws).gaps == (Gap("30", "5", 5),)

    run(aws, timeline, FakeSqs(), now=at(0))
    assert state(aws).gaps == (Gap("30", "5", 6, iso(0)),)  # stamped, not dropped
    run(aws, timeline, FakeSqs(), now=at(24))
    assert state(aws).gaps == ()


def test_empty_runs_are_counted_in_the_state_and_reset_when_data_returns(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100, window=50)
    seed(aws, 100, Gap(until_id="80", since_id="5"))
    # Window is 51..100, so (5, 80) holds 51..79 and answers with data.
    run(aws, timeline, FakeSqs(), max_results=5, max_pages=2)

    assert state(aws).gaps == (Gap(until_id="75", since_id="5"),)

    gap = Gap(until_id="30", since_id="5", empty_runs=2)
    store = CheckpointStore(aws.state_table)
    store.save(
        USERNAME, CheckpointState("100", (gap,), 0), expected_version=state(aws).version
    )
    assert state(aws).gaps == (gap,)  # the counter is stored


# ---- a dataless 200 never ends a range --------------------------------------------


def test_errors_only_response_on_a_gap_keeps_the_gap_and_raises(aws: Aws) -> None:
    timeline = FakeTimeline(
        100,
        inject=lambda n, _r: ERRORS_ONLY if n >= 2 else None,  # 1 = new-Tweets check
    )
    seed(aws, 100, Gap(until_id="60", since_id="40"))

    with pytest.raises(XApiError, match="errors and no data"):
        run(aws, timeline, FakeSqs())

    assert state(aws).gaps == (Gap(until_id="60", since_id="40"),)  # not dropped
    timeline.inject = None
    sqs = FakeSqs()
    run_until_done(aws, timeline, sqs)
    assert sorted(sent_ids(sqs)) == list(range(41, 60))


def test_errors_only_on_a_gaps_second_page_keeps_what_remains(aws: Aws) -> None:
    timeline = FakeTimeline(100, inject=lambda n, _r: ERRORS_ONLY if n == 3 else None)
    seed(aws, 100, Gap(until_id="60", since_id="40"))
    sqs = FakeSqs()

    with pytest.raises(XApiError):
        run(aws, timeline, sqs, max_results=5)

    assert sorted(sent_ids(sqs)) == list(range(55, 60))
    assert state(aws).gaps == (Gap(until_id="55", since_id="40"),)
    timeline.inject = None
    run_until_done(aws, timeline, sqs, max_results=5)
    assert sorted(sent_ids(sqs)) == list(range(41, 60))


def test_empty_page_with_a_token_in_the_new_tweets_phase_loses_nothing(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100, empty_token_every=1)  # every answer is empty+token
    seed(aws, 100)
    timeline.add(15)
    sqs = FakeSqs()

    result = run(aws, timeline, sqs, max_pages=3)

    assert (result.enqueued, result.gaps_open) == (0, 1)
    assert state(aws).gaps == (Gap(since_id="100"),)  # still all to fetch
    timeline.empty_token_every = 0
    run_until_done(aws, timeline, sqs)
    assert set(sent_ids(sqs)) == set(range(101, 116))  # a re-sent top page is fine


def test_an_older_gap_is_served_even_when_a_newer_one_fails(aws: Aws) -> None:
    def inject(n: int, request: httpx.Request) -> httpx.Response | None:
        bad = request.url.params.get("until_id") == "60"
        return httpx.Response(400, json={"detail": "boom"}) if bad else None

    timeline = FakeTimeline(100, inject=inject)
    seed(aws, 100, Gap("20", "10"), Gap("60", "50"))
    sqs = FakeSqs()

    with pytest.raises(XApiError, match="boom"):
        run(aws, timeline, sqs, max_results=5, max_pages=5)

    assert {19, 18, 17, 16, 15} <= set(sent_ids(sqs))  # the older gap progressed
    assert Gap("60", "50") in state(aws).gaps


def test_rate_limit_ends_the_run_cleanly_but_not_before_a_deferred_error_raises(
    aws: Aws,
) -> None:
    def inject(n: int, request: httpx.Request) -> httpx.Response | None:
        until = request.url.params.get("until_id")
        if until == "60":
            return httpx.Response(400, json={"detail": "boom"})
        return RATE_LIMITED if until == "20" else None

    timeline = FakeTimeline(100, inject=inject)
    seed(aws, 100, Gap("20", "10"), Gap("60", "50"))

    with pytest.raises(XApiError, match="boom"):
        run(aws, timeline, FakeSqs(), max_pages=5)


def test_a_gap_that_ends_with_a_trailing_token_warns_about_nothing(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    timeline = FakeTimeline(100, trailing_token=True)
    seed(aws, 100, Gap(until_id="60", since_id="40"))
    sqs = FakeSqs()

    with caplog.at_level(logging.INFO, logger=fetch.logger.name):
        run_until_done(aws, timeline, sqs, max_results=5)

    assert sorted(sent_ids(sqs)) == list(range(41, 60))
    assert not [
        r
        for r in caplog.records
        if r.name == fetch.logger.name and r.levelno >= logging.WARNING
    ]


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
def test_first_run_backfills_the_whole_window_over_a_predictable_number_of_runs(
    aws: Aws, max_pages: int
) -> None:
    timeline = FakeTimeline(3300)  # the API serves the newest 3,200 only
    sqs = FakeSqs()

    results = run_until_done(aws, timeline, sqs, max_results=100, max_pages=max_pages)

    # 32 pages in all. Run 1 has no gaps to look after, so it spends every request on
    # pages; later runs spend one request on the new-Tweets check.
    later = max(max_pages - 1, 1)
    assert len(results) == 1 + max(0, math.ceil((32 - max_pages) / later))
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


# ---- a Tweet that arrives between the new-Tweets check and a gap request ----------


def test_gap_page_cannot_move_newest_id_past_a_tweet_that_arrived_mid_run(
    aws: Aws,
) -> None:
    # Property-test find: newest_id is 11 and a backfill gap (12, 14) is open. Tweets
    # 12 and 13 arrive after the new-Tweets check but before the gap request, which
    # returns only 13. Saving newest_id=13 would have hidden 12 forever.
    timeline = FakeTimeline(11)
    seed(aws, 11, Gap(until_id="14", since_id="12"))
    arrived = False

    def inject(n: int, _r: httpx.Request) -> httpx.Response | None:
        nonlocal arrived
        if n == 2 and not arrived:
            arrived = True
            timeline.add(2)
        return None

    timeline.inject = inject
    sqs = FakeSqs()

    run(aws, timeline, sqs, max_results=5, max_pages=3)

    assert state(aws).newest_id == "11"
    assert sent_ids(sqs) == [13]
    run_until_done(aws, timeline, sqs, max_results=5)
    assert sorted(set(sent_ids(sqs))) == [12, 13]


@pytest.mark.parametrize(
    ("backfill", "newest", "expected"),
    [
        (Gap("30", "10"), "15", Gap("16", "10")),  # top above newest: clamped
        (Gap("12", "10"), "15", Gap("12", "10")),  # already below: untouched
        (Gap(None, "10"), "15", Gap("16", "10")),  # open top: anchored
        (Gap("30", "15"), "15", None),  # all above newest_id
        (Gap("30", "14"), "15", Gap("16", "14")),  # only ID 15 remains
        (Gap("16", "15"), "20", None),  # no IDs between the bounds
        (Gap("30", "10"), None, Gap("30", "10")),  # no state yet: as given
        (None, "15", None),
    ],
)
def test_backfill_top_is_clamped_to_newest_id(
    backfill: Gap | None, newest: str | None, expected: Gap | None
) -> None:
    assert fetch._anchored(backfill, newest) == expected


# ---- empty gaps wait instead of piling up ---------------------------------------


def _empty_token_then_empty(_n: int, request: httpx.Request) -> httpx.Response | None:
    """Gap requests: an empty page with a token, then a clean empty one."""
    params = request.url.params
    if "until_id" not in params:
        return None
    if "pagination_token" in params:
        return EMPTY_OK
    return httpx.Response(200, json={"meta": {"result_count": 0, "next_token": "more"}})


def test_empty_pages_with_tokens_then_a_clean_empty_one_still_count_as_empty(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100, inject=_empty_token_then_empty)
    seed(aws, 100, Gap(until_id="60", since_id="40"))

    for h in (0, 1, 2):
        run(aws, timeline, FakeSqs(), now=at(h))
    assert state(aws).gaps == (Gap("60", "40", 2, iso(0)),)

    run(aws, timeline, FakeSqs(), now=at(24))
    assert state(aws).gaps == ()


def test_a_walk_cut_short_inside_empty_pages_does_not_count(aws: Aws) -> None:
    timeline = FakeTimeline(
        100,
        inject=lambda n, r: (
            httpx.Response(200, json={"meta": {"result_count": 0, "next_token": "m"}})
            if "until_id" in r.url.params
            else None
        ),
    )
    seed(aws, 100, Gap(until_id="60", since_id="40"))

    run(aws, timeline, FakeSqs(), max_pages=3)

    assert state(aws).gaps == (Gap("60", "40"),)  # never saw an end: not counted


def test_a_waiting_gap_is_not_requested_and_does_not_count_as_pending(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100)
    seed(
        aws, 100, Gap(until_id="60", since_id="40", empty_runs=2, first_empty_at=iso(0))
    )

    result = run(aws, timeline, FakeSqs(), now=at(5))

    assert len(timeline.timeline_calls) == 1  # only the new-Tweets check
    assert (result.gaps_open, result.stopped_reason) == (1, None)


def test_a_due_gap_is_served_first_with_one_request_so_it_drops_promptly(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(70, window=30)  # serves 41..70
    due = Gap(until_id="20", since_id="10", empty_runs=2, first_empty_at=iso(0))
    seed(aws, 70, Gap(until_id="68", since_id="55"), due)

    run(aws, timeline, FakeSqs(), max_pages=2, now=at(25))

    # One request checks for new Tweets and the other drops the due gap; the working
    # gap, though newer, has to wait for the next run.
    assert state(aws).gaps == (Gap("68", "55"),)


def stride_timeline(count: int) -> FakeTimeline:
    return FakeTimeline(count, trailing_token=True, stride=1000)


def test_hourly_runs_with_trailing_tokens_keep_the_open_gaps_small(aws: Aws) -> None:
    timeline = stride_timeline(200)
    sqs = RecordingSqs()
    sent: list[int] = []
    peak = 0
    requests: list[int] = []
    results: list[FetchResult] = []
    for n in range(72):  # three days, one run an hour
        timeline.add(1 + n % 3)
        sqs = RecordingSqs()
        before = len(timeline.timeline_calls)
        results.append(run(aws, timeline, sqs, max_pages=5, now=at(n)))
        requests.append(len(timeline.timeline_calls) - before)
        sent += sqs.accepted
        peak = max(peak, results[-1].gaps_open)

    assert peak <= 27  # a day's worth of empty gaps waiting, plus a few in progress
    assert all(r.stopped_reason is None for r in results[-12:])  # nothing pending
    assert max(requests[-24:]) <= 4  # steady state: not the whole budget of 5
    assert len(sent) == len(set(sent))  # no Tweet sent twice
    assert set(timeline.served) <= set(sent)


def test_a_burst_completes_in_a_few_runs_despite_standing_empty_gaps(
    aws: Aws,
) -> None:
    timeline = stride_timeline(200)
    sent: set[int] = set()
    burst: list[int] = []
    finished_after = None
    for n in range(72):
        if n == 40:
            burst = timeline.add(1200)  # more than one run's budget
        else:
            timeline.add(1 + n % 3)
        sqs = RecordingSqs()
        run(aws, timeline, sqs, max_pages=5, now=at(n))
        sent |= set(sqs.accepted)
        if burst and finished_after is None and set(burst) <= sent:
            finished_after = n - 40 + 1

    assert finished_after is not None
    assert finished_after <= 8  # 12 pages; before gaps could wait it took 31 runs


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


def test_backfill_without_any_state_merges_into_the_first_run_gap(aws: Aws) -> None:
    timeline = FakeTimeline(30)
    sqs = FakeSqs()

    result = run(aws, timeline, sqs, max_results=5, max_pages=1, backfill=Gap("12"))

    assert result.newest_id == "30"
    assert state(aws).gaps == (Gap(until_id="21"),)  # one gap, not two


def test_open_backfill_on_top_of_a_first_run_state_adds_no_second_gap(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(30)
    run(aws, timeline, FakeSqs(), max_results=5, max_pages=1)
    assert state(aws).gaps == (Gap(until_id="26"),)
    timeline.timeline_calls.clear()

    run(aws, timeline, FakeSqs(), max_results=5, max_pages=2, backfill=Gap())

    # The open backfill is anchored at the newest known Tweet and merged with the gap.
    assert state(aws).gaps == (Gap(until_id="26"),)
    assert [c.get("until_id") for c in timeline.timeline_calls] == [None, "31"]


def test_backfill_entirely_above_newest_is_a_no_op(aws: Aws) -> None:
    timeline = FakeTimeline(30)
    seed(aws, 30)

    run(aws, timeline, FakeSqs(), backfill=Gap(since_id="30"))

    assert state(aws) == CheckpointState("30", (), 1)  # untouched since seed


def test_conflict_summary_reports_the_stored_gaps(aws: Aws) -> None:
    other = Gap(until_id="9", since_id="3")

    def interleave(n: int, _r: httpx.Request) -> httpx.Response | None:
        if n == 1:
            store = CheckpointStore(aws.state_table)
            cur = store.load(USERNAME)
            store.save(
                USERNAME, CheckpointState("200", (other,)), expected_version=cur.version
            )
        return None

    timeline = FakeTimeline(100, inject=interleave)
    seed(aws, 100)
    timeline.add(5)

    result = run(aws, timeline, FakeSqs())

    assert result.stopped_reason == "state_conflict"
    assert (result.gaps_open, result.newest_id) == (1, "200")


def test_overlapping_gaps_in_a_stored_state_are_merged_on_the_first_save(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(70)
    seed(aws, 70, Gap("30", "10"), Gap("25", "20"), Gap("60", "50"))  # unmerged

    run(aws, timeline, FakeSqs(), max_results=5, max_pages=10)

    assert state(aws).gaps == ()  # (25, 20) merged away, so it was not visited twice


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

    assert state(pipeline_env).gaps == (Gap(until_id="26"),)


@pytest.mark.parametrize(
    "backfill",
    [
        {"since_id": "abc"},
        {"until_id": "12.5"},
        {"until_id": ""},
        {"since_id": "-1"},
        {"since_id": "20", "until_id": "10"},
        {"since_id": "5", "until_id": 5},
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


# ---- properties: no ID above the initial checkpoint is ever missed ---------------


EXAMPLES = int(os.environ.get("CURSOR_PROPERTY_EXAMPLES", "70"))


def covered(tweet_id: int, state: CheckpointState) -> bool:
    """Whether the cursor accounts for ``tweet_id``: above newest_id, or in a gap."""
    if state.newest_id is None or tweet_id > int(state.newest_id):
        return True
    return any(g.first <= tweet_id <= g.last for g in state.gaps)


_failures = st.lists(
    st.tuples(st.integers(1, 6), st.sampled_from(["429", "500", "errors"])),
    max_size=3,
)
_round = st.tuples(
    st.integers(0, 40),  # new Tweets arriving before the run
    st.integers(1, 4),  # max_pages
    _failures,  # injected API failures, by request number within the run
    st.one_of(st.none(), st.integers(0, 5)),  # SQS batch that fails for good
)


@settings(
    max_examples=EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    initial=st.integers(0, 60),
    window=st.integers(10, 120),
    rounds=st.lists(_round, min_size=1, max_size=5),
    max_results=st.sampled_from([5, 7, 20, 100]),
    has_checkpoint=st.booleans(),
    short_pages=st.booleans(),
    trailing_token=st.booleans(),
    empty_token_every=st.sampled_from([0, 0, 3, 4, 5]),
    stride=st.sampled_from([1, 1000]),  # dense IDs, and snowflake-like sparse ones
)
def test_no_tweet_above_the_initial_checkpoint_is_ever_missed(
    aws: Aws,
    initial: int,
    window: int,
    rounds: list[tuple[int, int, list[tuple[int, str]], int | None]],
    max_results: int,
    has_checkpoint: bool,
    short_pages: bool,
    trailing_token: bool,
    empty_token_every: int,
    stride: int,
) -> None:
    username = USERNAME  # the fake API only knows this account; start from clean state
    aws.state_table.delete_item(Key={"username": username})
    failing: dict[int, httpx.Response] = {}
    answers = {"429": RATE_LIMITED, "500": httpx.Response(500), "errors": ERRORS_ONLY}

    timeline = FakeTimeline(
        initial,
        window=window,
        inject=lambda n, _r: failing.get(n),
        short_pages=short_pages,
        trailing_token=trailing_token,
        empty_token_every=empty_token_every,
        stride=stride,
    )
    checkpoint = timeline.ids[-1] if timeline.ids and has_checkpoint else 0
    if checkpoint:
        CheckpointStore(aws.state_table).save(
            username, CheckpointState(str(checkpoint)), expected_version=0
        )
    accepted: list[int] = []
    enqueue_failures = 0
    runs_made = 0

    def attempt(
        max_pages: int, plan: list[tuple[int, str]], fail_on_batch: int | None
    ) -> FetchResult | None:
        """One run with ``plan``'s failures at request numbers counted from here."""
        nonlocal enqueue_failures
        base = len(timeline.timeline_calls)
        failing.clear()
        for k, kind in plan:
            # 500s are retried by the client, so fail enough requests in a row.
            for n in range(k, k + (4 if kind == "500" else 1)):
                failing[base + n] = answers[kind]
        nonlocal runs_made
        runs_made += 1
        sqs = RecordingSqs(fail_on_batch)
        try:
            return run(
                aws,
                timeline,
                sqs,
                username=username,
                max_results=max_results,
                max_pages=max_pages,
                now=at(runs_made * 6),  # empty gaps get to wait and to come due
            )
        except EnqueueError:
            enqueue_failures += 1
            return None
        except XApiError:
            return None
        finally:
            accepted.extend(sqs.accepted)

    for arrivals, max_pages, plan, fail_on_batch in rounds:
        timeline.add(arrivals)
        attempt(max_pages, plan, fail_on_batch)

    # The odd answers are transient; convergence is only claimed once they stop.
    timeline.empty_token_every = 0
    for _ in range(300):
        result = attempt(3, [], None)
        if result is not None and result.gaps_open == 0:
            break
    else:  # pragma: no cover
        raise AssertionError("did not converge")

    expected = {i for i in timeline.served if i > checkpoint}
    assert expected <= set(accepted)
    final = CheckpointStore(aws.state_table).load(username)
    assert final.gaps == ()
    if expected:
        assert final.newest_id == str(timeline.ids[-1])
    if not empty_token_every:
        # Only the page in flight when SQS failed is ever sent twice.
        assert len(accepted) - len(set(accepted)) <= max_results * enqueue_failures


@example(
    initial=0,
    has_checkpoint=False,
    window=15,
    arrivals=11,
    stride=1,
    runs=[(1, 5, 0, 0, 0, 0, 0), (3, 5, 2, 0, 2, 0, 0)],
    backfill=(12, 14),
)
@settings(
    max_examples=EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    initial=st.integers(0, 50),
    has_checkpoint=st.booleans(),
    window=st.integers(15, 200),
    arrivals=st.integers(0, 60),
    stride=st.sampled_from([1, 1000]),
    runs=st.lists(
        st.tuples(
            st.integers(1, 4),  # max_pages
            st.sampled_from([5, 7, 100]),  # max_results
            st.integers(0, 4),  # API request to nest another run at (0: none)
            st.integers(0, 4),  # SQS batch to nest another run at (0: none)
            st.integers(0, 15),  # Tweets arriving before the nested run
            st.integers(0, 5),  # request to answer with a 429 (0: none)
            st.integers(0, 6),  # SQS batch that fails for good (0: none)
        ),
        min_size=2,
        max_size=4,
    ),
    backfill=st.one_of(
        st.none(),
        st.tuples(st.integers(0, 40), st.integers(0, 60)).map(
            lambda t: (t[0], t[0] + t[1] + 1)
        ),
    ),
)
def test_nested_overlapping_runs_never_leave_a_hole_at_any_save(
    aws: Aws,
    initial: int,
    has_checkpoint: bool,
    window: int,
    arrivals: int,
    stride: int,
    runs: list[tuple[int, int, int, int, int, int, int]],
    backfill: tuple[int, int] | None,
) -> None:
    """Runs start inside each other (mid-request, mid-enqueue), so their saves
    interleave. After every successful save, each Tweet the API serves is either
    enqueued already, above ``newest_id``, or inside a recorded gap."""
    aws.state_table.delete_item(Key={"username": USERNAME})
    timeline = FakeTimeline(initial, window=window, stride=stride)
    store = CheckedStore(aws.state_table, timeline)
    checkpoint = timeline.ids[-1] if timeline.ids and has_checkpoint else 0
    store.floor = checkpoint
    if checkpoint:
        CheckpointStore(aws.state_table).save(
            USERNAME, CheckpointState(str(checkpoint)), expected_version=0
        )
    timeline.add(arrivals)
    gap = (
        None
        if backfill is None
        else Gap(since_id=str(backfill[0] * stride), until_id=str(backfill[1] * stride))
    )
    pending = list(runs)
    runs_made = 0

    def start_run(extra_backfill: Gap | None = None) -> None:
        nonlocal runs_made
        if not pending:
            return
        max_pages, max_results, nest_http, nest_sqs, arrive, rate_at, fail_at = (
            pending.pop(0)
        )
        nested = False
        base = len(timeline.timeline_calls)
        runs_made += 1
        hours = runs_made * 6

        def nest() -> None:
            nonlocal nested
            if not nested:
                nested = True
                timeline.add(arrive)
                start_run()

        def inject(n: int, _request: httpx.Request) -> httpx.Response | None:
            if n - base == nest_http:
                nest()
            return RATE_LIMITED if n - base == rate_at else None

        def before_batch(call: int) -> None:
            if call + 1 == nest_sqs:
                nest()  # between this run's fetch and its save

        sqs = RecordingSqs(fail_at - 1 if fail_at else None, before_batch)
        store.sqss.append(sqs)
        previous = timeline.inject
        timeline.inject = inject
        try:
            run(
                aws,
                timeline,
                sqs,
                max_results=max_results,
                max_pages=max_pages,
                backfill=extra_backfill,
                checkpoints=store,
                now=at(hours),
            )
        except EnqueueError, XApiError:
            pass
        finally:
            timeline.inject = previous
        nest()

    while pending:
        start_run(gap)
        gap = None

    for i in range(300):
        sqs = RecordingSqs()
        store.sqss.append(sqs)
        result = run(
            aws,
            timeline,
            sqs,
            max_results=5,
            max_pages=3,
            checkpoints=store,
            now=at((runs_made + i + 1) * 6),
        )
        if result.gaps_open == 0:
            break
    else:  # pragma: no cover
        raise AssertionError("did not converge")

    assert store.saves > 0 or not timeline.ids
    assert not store.violations, store.violations[:3]
    assert {i for i in timeline.served if i > checkpoint} <= store.accepted()


class CheckedStore(CheckpointStore):
    """A store that checks the cursor invariant after every successful save: each
    Tweet the API serves is enqueued already (by any run, live), above ``newest_id``
    or inside a gap, and ``newest_id`` never moves backwards."""

    def __init__(self, table: Any, timeline: FakeTimeline) -> None:
        super().__init__(table)
        self.timeline = timeline
        self.sqss: list[RecordingSqs] = []
        self.floor = 0  # IDs at or below this were never ours to fetch
        self.saves = 0
        self.violations: list[str] = []
        self._last: CheckpointState | None = None

    def accepted(self) -> set[int]:
        return {i for sqs in self.sqss for i in sqs.accepted}

    def save(
        self, username: str, state: CheckpointState, *, expected_version: int
    ) -> bool:
        ok = super().save(username, state, expected_version=expected_version)
        if not ok:
            return ok
        self.saves += 1
        last = self._last
        if (
            last
            and last.newest_id
            and (state.newest_id is None or int(state.newest_id) < int(last.newest_id))
        ):
            self.violations.append(f"newest_id regressed {last} -> {state}")
        self._last = state
        sent = self.accepted()
        for i in self.timeline.served:
            if i > self.floor and i not in sent and not covered(i, state):
                self.violations.append(f"hole at {i}: {state}")
                break
        return ok


def test_the_per_save_check_runs_and_can_fail(aws: Aws) -> None:
    timeline = FakeTimeline(30)
    store = CheckedStore(aws.state_table, timeline)
    sqs = RecordingSqs()
    store.sqss.append(sqs)

    run(aws, timeline, sqs, max_results=5, max_pages=2, checkpoints=store)

    assert store.saves == 2
    assert store.violations == []
    # A cursor that hides unfetched Tweets (newest_id past them, no gap) is caught.
    store.save(
        USERNAME,
        CheckpointState(newest_id="30", gaps=(), version=store.load(USERNAME).version),
        expected_version=store.load(USERNAME).version,
    )
    assert store.violations and store.violations[0].startswith("hole at")
