"""The backfill cursor: capped, failed and overlapping runs skip no Tweets."""

import itertools
import logging
import math
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
        assert state(aws).gaps == (Gap("30", "5", 3, iso(0)),)  # too young to drop
        results.append(run(aws, timeline, FakeSqs(), now=at(24)))

    assert [r.gaps_open for r in results] == [1, 1, 1, 0]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert [r.getMessage() for r in warnings] == [
        f"dropping gap for {USERNAME} (since_id=5, until_id=30): out of the API's "
        f"reach (empty 4 runs since {iso(0)})"
    ]
    assert state(aws).gaps == ()


def test_many_empty_runs_in_less_than_a_day_never_drop_the_gap(aws: Aws) -> None:
    timeline = FakeTimeline(100, window=50)
    seed(aws, 100, Gap(until_id="30", since_id="5"))

    for i in range(30):
        run(aws, timeline, FakeSqs(), now=at(i * 0.75))  # 0 .. 21.75 hours

    assert state(aws).gaps == (Gap("30", "5", 30, iso(0)),)


def test_a_short_api_incident_does_not_drop_a_gap_that_still_has_data(
    aws: Aws,
) -> None:
    timeline = FakeTimeline(100)
    seed(aws, 100, Gap(until_id="60", since_id="40"))
    timeline.inject = lambda n, _r: EMPTY_OK if n >= 1 else None
    for h in (0, 1, 2, 3):
        run(aws, timeline, FakeSqs(), now=at(h))
    assert state(aws).gaps == (Gap("60", "40", 4, iso(0)),)

    timeline.inject = None
    sqs = FakeSqs()
    run_until_done(aws, timeline, sqs, now=at(4))
    assert sorted(sent_ids(sqs)) == list(range(41, 60))


def test_data_returning_resets_both_empty_fields(aws: Aws) -> None:
    timeline = FakeTimeline(100)
    seed(aws, 100, Gap(until_id="60", since_id="40"))
    timeline.inject = lambda n, _r: EMPTY_OK if n >= 1 else None
    run(aws, timeline, FakeSqs(), now=at(0))
    run(aws, timeline, FakeSqs(), now=at(1))
    assert state(aws).gaps == (Gap("60", "40", 2, iso(0)),)

    timeline.inject = None
    run(aws, timeline, FakeSqs(), max_results=5, max_pages=2, now=at(2))

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
    max_examples=80,
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
    )
    checkpoint = initial if has_checkpoint else 0
    if has_checkpoint:
        CheckpointStore(aws.state_table).save(
            username, CheckpointState(str(initial)), expected_version=0
        )
    accepted: list[int] = []
    enqueue_failures = 0

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
        sqs = RecordingSqs(fail_on_batch)
        try:
            return run(
                aws,
                timeline,
                sqs,
                username=username,
                max_results=max_results,
                max_pages=max_pages,
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
        if result is not None and result.stopped_reason is None:
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


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    initial=st.integers(0, 50),
    has_checkpoint=st.booleans(),
    window=st.integers(15, 200),
    arrivals=st.integers(0, 60),
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
            lambda t: Gap(since_id=str(t[0]), until_id=str(t[0] + t[1] + 1))
        ),
    ),
)
def test_nested_overlapping_runs_never_leave_a_hole_at_any_save(
    aws: Aws,
    initial: int,
    has_checkpoint: bool,
    window: int,
    arrivals: int,
    runs: list[tuple[int, int, int, int, int, int, int]],
    backfill: Gap | None,
) -> None:
    """Runs start inside each other (mid-request, mid-enqueue), so their saves
    interleave. After every successful save, each Tweet the API serves is either
    enqueued already, above ``newest_id``, or inside a recorded gap."""
    aws.state_table.delete_item(Key={"username": USERNAME})
    store = CheckpointStore(aws.state_table)
    timeline = FakeTimeline(initial, window=window)
    checkpoint = initial if has_checkpoint else 0
    if has_checkpoint:
        store.save(USERNAME, CheckpointState(str(initial)), expected_version=0)
    timeline.add(arrivals)
    accepted: set[int] = set()
    history: list[CheckpointState] = []
    violations: list[str] = []
    original_save = store.save

    def checked_save(
        username: str, new: CheckpointState, *, expected_version: int
    ) -> bool:
        ok = original_save(username, new, expected_version=expected_version)
        if ok:
            if (
                history
                and history[-1].newest_id
                and (
                    new.newest_id is None
                    or int(new.newest_id) < int(history[-1].newest_id)
                )
            ):
                violations.append(f"newest_id regressed {history[-1]} -> {new}")
            history.append(new)
            for i in timeline.served:
                if i > checkpoint and i not in accepted and not covered(i, new):
                    violations.append(f"hole at {i}: {new}")
                    break
        return ok

    store.save = checked_save  # type: ignore[assignment]
    pending = list(runs)

    def start_run(extra_backfill: Gap | None = None) -> None:
        if not pending:
            return
        max_pages, max_results, nest_http, nest_sqs, arrive, rate_at, fail_at = (
            pending.pop(0)
        )
        nested = False
        base = len(timeline.timeline_calls)

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
            )
        except EnqueueError, XApiError:
            pass
        finally:
            timeline.inject = previous
            accepted.update(sqs.accepted)
        nest()

    while pending:
        start_run(backfill)
        backfill = None

    for _ in range(300):
        sqs = RecordingSqs()
        result = run(aws, timeline, sqs, max_results=5, max_pages=3)
        accepted.update(sqs.accepted)
        if result.stopped_reason is None:
            break
    else:  # pragma: no cover
        raise AssertionError("did not converge")

    assert not violations, violations[:3]
    assert {i for i in timeline.served if i > checkpoint} <= accepted
