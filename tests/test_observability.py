"""What each function emits as EMF metrics, and the structured log fields."""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from conftest import Aws, Emf
from fake_timeline import RATE_LIMITED, FakeTimeline
from helpers import (
    PHOTO_URL,
    CdnStub,
    FakeSqs,
    XApiStub,
    install_x_api,
    make_message,
    photo,
    sqs_record,
    synthetic_timeline,
    x_client,
)
from test_deletion_copies import put_quote
from test_fetch_cursor import at
from test_fetch_cursor import run as cursor_run
from test_fetch_cursor import seed as cursor_seed
from test_handlers_compliance import HANDLE, Clock, Lookup, job, put
from test_handlers_compliance import run as compliance_run
from test_handlers_fetch import USERNAME, ids
from test_handlers_fetch import run as fetch_run
from test_handlers_media_retry import (
    BROKEN_URL,
    OTHER_URL,
    TWEET,
    put_item,
    retry_record,
    retrying,
)
from test_handlers_media_retry import run as retry_run
from test_handlers_process import (
    GOOD,
    NO_MEDIA,
    TRANSIENT,
    TRANSIENT_URL,
    UNEXPECTED,
    ExplodingTweetStore,
    RecordingRetryQueue,
    body,
    photo_message,
    receives,
)
from test_quarantine import GOOD1, GOOD2, bad_tweet, body_of, tweet
from test_quarantine import run as quarantine_run
from test_user_id import NEW_ID, OLD_ID, TTL, Api, go
from test_user_id import Clock as UserClock

from twitter_timeline_data_pipeline import metrics as metrics_module
from twitter_timeline_data_pipeline.handlers import fetch, media_retry
from twitter_timeline_data_pipeline.handlers.compliance import ComplianceError
from twitter_timeline_data_pipeline.handlers.fetch import (
    IdentityChangedError,
    handler,
)
from twitter_timeline_data_pipeline.handlers.process import process_records
from twitter_timeline_data_pipeline.storage import (
    CheckpointStore,
    MediaStore,
    PutResult,
    StoredMedia,
    TweetStore,
    WriteContentionError,
)
from twitter_timeline_data_pipeline.x_api import TweetLookup

U: dict[str, str] = {}  # per-account series are opt-in; see the last fetch test


# ---- fetch ---------------------------------------------------------------------


def test_a_fetch_run_emits_its_counts_with_a_username_dimension_and_a_heartbeat(
    aws: Aws, emf: Emf
) -> None:
    fetch_run(aws, synthetic_timeline(ids(25)), FakeSqs())

    assert emf.one("TweetsFetched", **U) == 25
    assert emf.one("TweetsEnqueued", **U) == 25
    assert emf.one("PageCapTruncations", **U) == 0
    assert emf.one("RateLimitedRuns", **U) == 0
    assert emf.one("GapsDropped", **U) == 0
    assert emf.one("PartialErrors", **U) == 0
    assert emf.one("FetchRunSucceeded", **U) == 1
    assert emf.one("FetchRunSucceeded") == 1  # the dimensionless set too
    assert len(emf.documents) == 1  # one line per run


def test_a_run_that_used_its_page_budget_counts_a_truncation(
    aws: Aws, emf: Emf
) -> None:
    result = fetch_run(aws, FakeTimeline(3200), FakeSqs(), max_pages=2)

    assert result.stopped_reason == "page_cap"
    assert emf.one("PageCapTruncations", **U) == 1
    assert emf.one("FetchRunSucceeded", **U) == 1  # it ran fine, there is just more


def test_a_rate_limited_run_is_counted_and_sends_no_heartbeat(
    aws: Aws, emf: Emf
) -> None:
    timeline = FakeTimeline(3200, inject=lambda n, _r: RATE_LIMITED if n == 3 else None)

    result = fetch_run(aws, timeline, FakeSqs(), max_pages=32)

    assert result.stopped_reason == "rate_limited"
    assert emf.one("RateLimitedRuns", **U) == 1
    assert emf.one("FetchRunSucceeded", **U) == 0
    assert emf.one("TweetsEnqueued", **U) == result.enqueued == 200


def test_a_failed_run_still_reports_what_it_did_but_no_heartbeat(
    aws: Aws, emf: Emf
) -> None:
    class DownSqs:
        def send_message_batch(self, **_k: Any) -> Any:
            raise RuntimeError("sqs down")

    with x_client(FakeTimeline(30)) as client, pytest.raises(RuntimeError):
        fetch.run_fetch(
            username=USERNAME,
            x_client=client,
            sqs=DownSqs(),  # type: ignore[arg-type]
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            max_results=100,
            max_pages=2,
        )

    assert emf.one("FetchRunSucceeded", **U) == 0
    assert emf.one("TweetsEnqueued", **U) == 0
    assert emf.one("TweetsFetched", **U) == 30  # fetched, then the enqueue failed


def test_an_identity_change_emits_its_own_metric_before_failing(
    aws: Aws, emf: Emf
) -> None:
    api, clock = Api(), UserClock()
    go(aws, api, clock)
    api.owner = NEW_ID
    clock.advance(TTL)

    with pytest.raises(IdentityChangedError, match=f"{OLD_ID}.*{NEW_ID}"):
        go(aws, api, clock)

    assert emf.one("IdentityChanged", **U) == 1
    assert emf.total("FetchRunSucceeded") == 1  # only the first run's heartbeat


def test_quarantined_items_are_counted_by_kind_and_partial_errors_too(
    aws: Aws, emf: Emf
) -> None:
    page = body_of([tweet(GOOD1), bad_tweet(), tweet(GOOD2)])
    page["errors"] = [
        {
            "title": "Not Found Error",
            "detail": "Could not find tweet with referenced_tweets.id: [1].",
            "type": "https://api.twitter.com/2/problems/resource-not-found",
            "resource_type": "tweet",
            "resource_id": "1",
        }
    ]

    quarantine_run(aws, page, FakeSqs())

    assert emf.one("QuarantinedItems", Kind="tweet") == 1
    assert emf.total("QuarantinedItems") == 1  # the empty set sums over kinds
    assert emf.one("PartialErrors", **U) == 1
    assert emf.one("TweetsEnqueued", **U) == 2


def test_a_message_sqs_rejects_is_quarantined_with_kind_message(
    aws: Aws, emf: Emf, monkeypatch: pytest.MonkeyPatch
) -> None:
    sqs = FakeSqs()

    def reject_all(**kwargs: Any) -> Any:
        return {
            "Successful": [],
            "Failed": [
                {"Id": e["Id"], "SenderFault": True, "Code": "Bad", "Message": "no"}
                for e in kwargs["Entries"]
            ],
        }

    monkeypatch.setattr(sqs.client, "send_message_batch", reject_all, raising=False)

    quarantine_run(aws, body_of([tweet(GOOD1)]), sqs)

    assert emf.one("QuarantinedItems", Kind="message") == 1
    assert emf.one("TweetsEnqueued", **U) == 0


def test_a_dropped_gap_is_counted(aws: Aws, emf: Emf) -> None:
    from twitter_timeline_data_pipeline.storage import Gap

    timeline = FakeTimeline(100, window=50)
    cursor_seed(aws, 100, Gap(until_id="30", since_id="5"))

    for hours in (0, 1, 2, 24):
        cursor_run(aws, timeline, FakeSqs(), now=at(hours))

    assert emf.find("GapsDropped", **U) == [0, 0, 0, 1]


def test_the_handler_adds_the_run_id_to_the_line_and_to_the_summary_log(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    emf: Emf,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_x_api(monkeypatch, fetch, XApiStub())

    with caplog.at_level(logging.INFO, logger=fetch.logger.name):
        handler({"username": USERNAME}, None)

    (doc,) = emf.read()
    (summary,) = [r for r in caplog.records if r.getMessage().startswith("fetched ")]
    assert doc["run_id"] == summary.run_id  # type: ignore[attr-defined]
    assert "Username" not in doc  # per-account series are opt-in


# ---- process ---------------------------------------------------------------------


@pytest.fixture
def cdn() -> CdnStub:
    stub = CdnStub()
    stub.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    stub.fail(TRANSIENT_URL, 503)
    return stub


LATER = datetime(2026, 10, 25, 0, 0, 30, tzinfo=UTC)  # FETCHED_AT + 30 s


def test_a_mixed_batch_reports_stored_failures_retries_media_and_lag(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    records = [
        sqs_record("m-good", body(photo_message(GOOD))),
        sqs_record("m-malformed", "{not json"),
        sqs_record("m-transient", body(photo_message(TRANSIENT, TRANSIENT_URL))),
        sqs_record("m-unexpected", body(make_message(UNEXPECTED))),
        sqs_record("m-plain", body(make_message(NO_MEDIA))),
    ]

    failed = process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=ExplodingTweetStore(
            aws.tweets_table, aws.s3, aws.bucket, UNEXPECTED
        ),
        now=lambda: LATER,
    )

    assert len(failed) == 3
    assert len(emf.read()) == 4  # the invocation's line and one per reason
    assert emf.one("TweetsStored") == 2
    assert emf.one("StaleWritesSkipped") == 0
    assert emf.one("MediaStored") == 1
    assert emf.one("MediaBytes") == len(b"jpeg-good")
    assert emf.one("ProcessingLagSeconds") == [30.0, 30.0]
    assert emf.one("RecordFailures", Reason="malformed") == 1
    assert emf.one("RecordFailures", Reason="error") == 1
    assert emf.total("RecordFailures") == 2  # dimensionless: the sum of the reasons
    assert emf.one("RecordRetries", Reason="media_transient") == 1


def test_units_are_the_declared_ones_on_every_line(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    process_records(
        [sqs_record("m", body(photo_message(GOOD)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        now=lambda: LATER,
    )

    emf.read()
    units = {
        m["Name"]: m["Unit"]
        for d in emf.documents
        for m in d["_aws"]["CloudWatchMetrics"][0]["Metrics"]
    }
    assert units["MediaBytes"] == "Bytes"
    assert units["ProcessingLagSeconds"] == "Seconds"
    assert units["TweetsStored"] == "Count"


def test_a_stale_write_is_counted_and_not_stored(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    media_store = MediaStore(aws.s3, aws.bucket, cdn.client())
    new = make_message(GOOD, fetched_at="2026-10-25T12:00:00Z")
    old = make_message(GOOD, fetched_at="2026-10-20T12:00:00Z")
    process_records(
        [sqs_record("n", body(new))], media_store=media_store, tweet_store=store
    )
    emf.reset()

    process_records(
        [sqs_record("o", body(old))], media_store=media_store, tweet_store=store
    )

    assert emf.one("StaleWritesSkipped") == 1
    assert emf.one("TweetsStored") == 0
    assert emf.find("ProcessingLagSeconds") == []  # nothing was stored, so no lag


def test_media_outcomes_are_counted_by_status(aws: Aws, emf: Emf) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    cdn.fail(TRANSIENT_URL, 404)
    big_url = "https://pbs.twimg.com/media/FakeBig01.jpg"
    cdn.serve(big_url, b"x" * 50, "image/jpeg")
    records = [
        sqs_record("a", body(photo_message(GOOD))),
        sqs_record("b", body(photo_message(TRANSIENT, TRANSIENT_URL))),
        sqs_record("c", body(photo_message(NO_MEDIA, "http://example.com/x.jpg"))),
        sqs_record("d", body(photo_message("1700000000000000050", big_url))),
    ]

    process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=10),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    # the first message's 9 bytes fit under the 10-byte cap
    assert emf.one("MediaStored") == 1
    assert emf.one("MediaUnavailable") == 1
    assert emf.one("MediaSkipped") == 1
    assert emf.one("MediaTooLarge") == 1
    assert emf.one("MediaBytes") == 9


def test_media_missing_from_includes_is_counted(aws: Aws, emf: Emf) -> None:
    from dataclasses import replace

    message = replace(
        make_message(GOOD, tweet={"media_keys": ["3_gone", "3_x"]}),
        missing_media_keys=("3_gone", "3_x"),
    )

    process_records(
        [sqs_record("m", body(message))],
        media_store=MediaStore(aws.s3, aws.bucket, CdnStub().client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert emf.one("MediaMissingFromIncludes") == 2


def test_write_contention_is_counted_and_is_a_retry_not_a_failure(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    class Contended(TweetStore):
        def put_tweet(self, *_a: Any, **_k: Any) -> PutResult:
            raise WriteContentionError("changed under its merge")

    failed = process_records(
        [sqs_record("m1", body(photo_message(GOOD)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=Contended(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == ["m1"]
    assert emf.one("WriteContentionErrors") == 1
    assert emf.one("RecordRetries", Reason="write_contention") == 1
    assert emf.find("RecordFailures") == []


def test_a_record_left_unstarted_for_lack_of_time_is_a_retry(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    failed = process_records(
        [sqs_record("m1", body(make_message(GOOD)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=lambda: 0,
    )

    assert failed == ["m1"]
    assert emf.one("RecordRetries", Reason="out_of_time") == 1


def test_queued_media_retries_are_counted_and_a_failed_send_is_a_failure(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    a, b = photo("3_a", TRANSIENT_URL), photo("3_b", TRANSIENT_URL)
    message = make_message(GOOD, media=(a, b), tweet={"media_keys": ["3_a", "3_b"]})
    queue = RecordingRetryQueue(fail_for=frozenset({"3_a"}))

    failed = process_records(
        [receives(sqs_record("m1", body(message)), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == ["m1"]
    assert emf.one("MediaRetriesQueued") == 1
    assert emf.one("RecordFailures", Reason="retry_queue") == 1
    assert emf.one("TweetsStored") == 1  # the Tweet itself was written


def test_a_broken_metrics_writer_never_fails_a_record(
    aws: Aws, cdn: CdnStub, monkeypatch: pytest.MonkeyPatch, emf: Emf
) -> None:
    def explode(*_a: Any, **_k: Any) -> str:
        raise RuntimeError("no metrics today")

    monkeypatch.setattr("twitter_timeline_data_pipeline.metrics.build", explode)

    failed = process_records(
        [
            sqs_record("ok", body(photo_message(GOOD))),
            sqs_record("bad", "{nope"),
        ],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == ["bad"]  # unchanged by the failing metrics
    stored = aws.tweets_table.get_item(Key={"tweet_id": GOOD}).get("Item")
    assert stored is not None


def test_process_logs_carry_message_and_tweet_ids_as_fields(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO):
        process_records(
            [
                sqs_record("m-good", body(photo_message(GOOD))),
                sqs_record("m-malformed", "{nope"),
            ],
            media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
            tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        )

    (stored,) = [r for r in caplog.records if r.getMessage().startswith("stored tweet")]
    assert (stored.message_id, stored.tweet_id) == ("m-good", GOOD)  # type: ignore[attr-defined]
    (bad,) = [r for r in caplog.records if "is malformed" in r.getMessage()]
    assert bad.message_id == "m-malformed"  # type: ignore[attr-defined]
    assert not hasattr(bad, "tweet_id")  # none was parsed


# ---- media retry ---------------------------------------------------------------


def test_media_retry_emits_outcomes_failures_and_what_it_stored(
    aws: Aws, emf: Emf
) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    cdn.serve(OTHER_URL, b"jpeg-other", "image/jpeg")
    a, b, c = photo("3_a", PHOTO_URL), photo("3_b", BROKEN_URL), photo("3_c", OTHER_URL)
    put_item(
        aws,
        (a, retrying(a)),
        (b, retrying(b)),
        (c, StoredMedia(media_key="3_c", type="photo", status="stored", s3_key="k")),
    )
    cdn.fail(BROKEN_URL, 503)

    failed = retry_run(
        aws,
        cdn,
        [retry_record(a, "r-a"), retry_record(b, "r-b"), retry_record(c, "r-c")],
    )

    assert failed == ["r-b"]
    assert emf.one("MediaRetryOutcomes", Outcome="stored") == 1
    assert emf.one("MediaRetryOutcomes", Outcome="already_settled") == 1
    assert emf.total("MediaRetryOutcomes") == 2  # the failed one has no outcome
    assert emf.one("MediaRetryFailures", Reason="transient") == 1
    assert emf.one("MediaStored") == 1
    assert emf.one("MediaBytes") == len(b"jpeg-good")


def test_media_retry_logs_carry_message_and_tweet_ids(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    a = photo("3_a", PHOTO_URL)
    put_item(aws, (a, retrying(a)))

    with caplog.at_level(logging.INFO):
        retry_run(aws, cdn, [retry_record(a, "r-a")])

    (done,) = [r for r in caplog.records if "is now stored" in r.getMessage()]
    assert (done.message_id, done.tweet_id) == ("r-a", TWEET)  # type: ignore[attr-defined]
    assert media_retry.logger.name == done.name


def test_a_broken_metrics_writer_never_fails_a_media_retry(
    aws: Aws, monkeypatch: pytest.MonkeyPatch, emf: Emf
) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    a = photo("3_a", PHOTO_URL)
    put_item(aws, (a, retrying(a)))
    monkeypatch.setattr(
        "twitter_timeline_data_pipeline.metrics.build", lambda *_a, **_k: 1 / 0
    )

    failed = retry_run(aws, cdn, [retry_record(a, "r-a")])

    assert failed == []


# ---- compliance ----------------------------------------------------------------


def test_a_compliance_run_emits_its_counts_and_the_heartbeat(
    aws: Aws, emf: Emf
) -> None:
    clock = Clock()
    gone = put(aws, 1)
    put(aws, 2)

    compliance_run(aws, Lookup({gone: "deleted"}), clock)

    assert emf.one("ComplianceRunSucceeded") == 1
    assert emf.one("ComplianceFailures") == 0
    assert emf.one("ComplianceChecked") == 2
    assert emf.one("ComplianceTombstoned") == 1
    assert emf.one("ComplianceCleared") == 0
    assert emf.one("CompliancePurged") == 0
    assert emf.one("CompliancePurgeDeferred") == 0
    assert emf.one("ComplianceUnknownAnswers") == 0


def test_compliance_counts_clears_purges_and_unknown_answers(
    aws: Aws, emf: Emf
) -> None:
    clock = Clock()
    cleared, purged, unsure = put(aws, 1), put(aws, 2), put(aws, 3)
    compliance_run(aws, Lookup({cleared: "deleted", purged: "deleted"}), clock)
    emf.reset()
    clock.now += timedelta(hours=25)

    compliance_run(aws, Lookup({purged: "deleted"}, unknown={unsure}), clock)

    assert emf.one("ComplianceCleared") == 1
    assert emf.one("CompliancePurged") == 1
    assert emf.one("ComplianceUnknownAnswers") == 1


def test_scrubbed_copies_are_counted(aws: Aws, emf: Emf) -> None:
    from test_deletion_copies import B

    clock = Clock()
    put_quote(aws, 1, B)

    compliance_run(aws, Lookup(gone={B: "deleted"}), clock, grace=timedelta(0))

    assert emf.one("ComplianceCopiesScrubbed") == 1


def test_a_failed_run_reports_its_failures_and_no_heartbeat_then_raises(
    aws: Aws, emf: Emf
) -> None:
    clock = Clock()
    tweet_id = put(aws, 1)
    lookup = Lookup({tweet_id: "deleted"})
    compliance_run(aws, lookup, clock)
    emf.reset()
    clock.now += timedelta(hours=25)
    failing = job(aws, lookup, clock)
    failing._lookup = lambda chunk: (  # the walk works, the confirmation is unknown
        TweetLookup(unknown=frozenset(chunk))
        if len(chunk) == 1
        else TweetLookup(unavailable=dict.fromkeys(chunk, "deleted"))
    )

    with pytest.raises(ComplianceError, match="purge confirmation unknown"):
        failing.run(HANDLE)

    assert emf.one("ComplianceRunSucceeded") == 0
    assert emf.one("ComplianceFailures") == 1
    assert emf.one("CompliancePurgeDeferred") == 1
    assert emf.one("CompliancePurged") == 0


def test_a_broken_metrics_writer_never_fails_or_changes_a_compliance_run(
    aws: Aws, monkeypatch: pytest.MonkeyPatch, emf: Emf
) -> None:
    monkeypatch.setattr(
        "twitter_timeline_data_pipeline.metrics.build",
        lambda *_a, **_k: 1 / 0,
    )
    gone = put(aws, 1)

    done = compliance_run(aws, Lookup({gone: "deleted"}), Clock())

    assert done.tombstoned == 1  # the tombstone is written; no error was raised


def test_compliance_logs_carry_the_tweet_id(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    gone = put(aws, 1)
    caplog.set_level(logging.INFO)

    compliance_run(aws, Lookup({gone: "deleted"}), Clock(), grace=timedelta(0))

    for prefix in ("tombstoned tweet", "purged tweet"):
        (record,) = [r for r in caplog.records if r.getMessage().startswith(prefix)]
        assert record.tweet_id == gone  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("fetched_at", "expected"),
    [
        ("2026-10-25T00:00:00Z", 30.0),
        ("2026-10-25T00:00:30Z", 0.0),
        ("2026-10-25T00:01:00Z", 0.0),  # a clock a little behind: never negative
        (None, None),
        ("", None),
        ("yesterday", None),
        ("2026-10-25T00:00:00", None),  # no zone: not comparable
    ],
)
def test_processing_lag_needs_a_usable_fetch_time(
    fetched_at: str | None, expected: float | None
) -> None:
    from twitter_timeline_data_pipeline.handlers.process import _lag_seconds

    assert _lag_seconds(fetched_at, LATER) == expected


def test_lag_is_from_the_fetch_not_from_the_tweets_creation(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    ancient = make_message(GOOD, tweet={"created_at": "2019-01-01T00:00:00.000Z"})

    process_records(
        [sqs_record("m", body(ancient))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        now=lambda: LATER,
    )

    assert emf.one("ProcessingLagSeconds") == [30.0]  # a backfill is not "late"


# ---- review round: heartbeat, last receive, log level, opt-in dimensions -----------


def test_per_account_series_are_opt_in(
    aws: Aws, emf: Emf, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetch_run(aws, synthetic_timeline(ids(3)), FakeSqs())
    (default,) = emf.read()
    assert default["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [[]]
    assert "Username" not in default
    emf.reset()

    monkeypatch.setenv("METRICS_PER_ACCOUNT", "true")
    fetch_run(aws, synthetic_timeline(ids(3)), FakeSqs())

    assert emf.one("TweetsFetched", Username=USERNAME) == 3
    assert emf.one("TweetsFetched") == 3  # the total is there as well


def test_per_account_quarantine_lines_carry_both_dimensions(
    aws: Aws, emf: Emf, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("METRICS_PER_ACCOUNT", "1")

    quarantine_run(aws, body_of([tweet(GOOD1), bad_tweet()]), FakeSqs())

    assert emf.one("QuarantinedItems", Username=USERNAME, Kind="tweet") == 1
    assert emf.one("QuarantinedItems") == 1


def test_a_rate_limit_in_the_gap_phase_still_sends_the_heartbeat(
    aws: Aws, emf: Emf
) -> None:
    from twitter_timeline_data_pipeline.storage import Gap

    timeline = FakeTimeline(
        100, window=50, inject=lambda n, _r: RATE_LIMITED if n == 2 else None
    )
    cursor_seed(aws, 100, Gap(until_id="30", since_id="5"))

    result = cursor_run(aws, timeline, FakeSqs(), now=at(0))

    assert result.stopped_reason == "rate_limited"
    assert emf.one("RateLimitedRuns") == 1
    assert emf.one("FetchRunSucceeded") == 1  # the new-Tweets phase had completed


def test_a_rate_limit_in_the_new_tweets_phase_sends_no_heartbeat(
    aws: Aws, emf: Emf
) -> None:
    timeline = FakeTimeline(100, inject=lambda n, _r: RATE_LIMITED if n == 1 else None)

    cursor_run(aws, timeline, FakeSqs(), now=at(0))

    assert emf.one("RateLimitedRuns") == 1
    assert emf.one("FetchRunSucceeded") == 0


def test_an_errors_only_timeline_sends_no_heartbeat(aws: Aws, emf: Emf) -> None:
    from twitter_timeline_data_pipeline.x_api import XApiError

    page = {
        "errors": [
            {
                "title": "Authorization Error",
                "detail": "protected",
                "type": "https://api.twitter.com/2/problems/not-authorized-for-resource",
            }
        ]
    }

    with pytest.raises(XApiError):
        quarantine_run(aws, page, FakeSqs())

    assert emf.one("FetchRunSucceeded") == 0
    assert emf.one("TweetsFetched") == 0


def test_the_last_receive_counts_every_failure_as_a_failure(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    class Contended(TweetStore):
        def put_tweet(self, *_a: Any, **_k: Any) -> PutResult:
            raise WriteContentionError("changed")

    queue = RecordingRetryQueue()
    records = [receives(sqs_record("m1", body(make_message(GOOD))), "5")]
    failed = process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=Contended(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
    )
    failed += process_records(
        [receives(sqs_record("m2", body(make_message(NO_MEDIA))), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
        remaining_ms=lambda: 0,  # no time even to write: straight to the DLQ
    )

    assert failed == ["m1", "m2"]
    assert emf.one("RecordFailures", Reason="write_contention") == 1
    assert emf.one("RecordFailures", Reason="out_of_time") == 1
    assert emf.total("RecordFailures") == 2
    assert emf.find("RecordRetries") == []


def test_before_the_last_receive_the_same_failures_are_only_retries(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    process_records(
        [receives(sqs_record("m", body(make_message(GOOD))), "4")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=RecordingRetryQueue(),
        remaining_ms=lambda: 0,
    )

    assert emf.one("RecordRetries", Reason="out_of_time") == 1
    assert emf.find("RecordFailures") == []


def test_the_tally_is_emitted_even_when_the_loop_raises(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    records = [sqs_record("ok", body(make_message(GOOD))), {"body": "no messageId"}]

    with pytest.raises(KeyError):
        process_records(
            records,
            media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
            tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        )

    assert emf.one("TweetsStored") == 1


def test_media_values_map_every_status() -> None:
    from twitter_timeline_data_pipeline.handlers.process import media_values

    def entry(status: Any, size: int | None = None) -> StoredMedia:
        return StoredMedia(media_key="k", type="photo", status=status, size_bytes=size)

    values = media_values(
        [
            entry("stored", 7),
            entry("exists", 5),
            entry("too_large", 900),
            entry("skipped"),
            entry("skipped_hls"),
            entry("unavailable"),
            entry("retrying", 3),
            entry("missing"),
        ]
    )

    assert values == {
        "MediaStored": 1,
        "MediaExisted": 1,
        "MediaUnavailable": 1,
        "MediaSkipped": 2,  # skipped_hls counts as skipped
        "MediaTooLarge": 1,
        "MediaBytes": 7,  # only what was uploaded now
    }


def test_a_delivery_that_lost_the_race_is_a_settled_elsewhere_outcome(
    aws: Aws, emf: Emf
) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    b = photo("3_b", PHOTO_URL)
    put_item(aws, (b, retrying(b)))
    winner = {"media_key": "3_b", "type": "photo", "status": "stored", "s3_key": "w"}

    class RacingStore(TweetStore):
        def get_media(self, tweet_id: str) -> list[dict[str, Any]] | None:
            entries = super().get_media(tweet_id)
            self._table.update_item(
                Key={"tweet_id": tweet_id},
                UpdateExpression="SET media[0] = :e",
                ExpressionAttributeValues={":e": winner},
            )
            return entries

    media_retry.retry_records(
        [retry_record(b)],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=RacingStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert emf.one("MediaRetryOutcomes", Outcome="settled_elsewhere") == 1
    assert emf.one("MediaStored") == 0  # it did not store anything that counts


def test_media_retry_failures_are_split_by_reason(aws: Aws, emf: Emf) -> None:
    cdn = CdnStub()
    cdn.fail(BROKEN_URL, 503)
    b = photo("3_b", BROKEN_URL)
    put_item(aws, (b, retrying(b)))

    retry_run(
        aws,
        cdn,
        [
            sqs_record("bad", "{nope"),
            retry_record(b, "r-b"),
            retry_record(photo("3_x"), "r-x"),
        ],
    )

    assert emf.one("MediaRetryFailures", Reason="malformed") == 1
    assert emf.one("MediaRetryFailures", Reason="transient") == 1
    assert emf.total("MediaRetryFailures") == 2 + emf.total(
        "MediaRetryFailures", Reason="error"
    )


def test_the_timestamp_is_epoch_milliseconds_and_about_now(emf: Emf) -> None:
    import time

    before = time.time() * 1000
    metrics_doc = metrics_module.build({"TweetsFetched": 1})
    after = time.time() * 1000

    assert int(before) <= metrics_doc["_aws"]["Timestamp"] <= int(after) + 1  # type: ignore[index]


def test_warn_log_level_silences_python_info_but_not_the_metrics(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    from twitter_timeline_data_pipeline import handlers

    package = logging.getLogger("twitter_timeline_data_pipeline")
    before = package.level
    try:
        handlers.apply_log_level({"LOG_LEVEL": "WARN"})
        records: list[logging.LogRecord] = []

        class Collect(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        collector = Collect(level=logging.DEBUG)
        logging.getLogger().addHandler(collector)
        try:
            process_records(
                [sqs_record("m", body(photo_message(GOOD))), sqs_record("bad", "{x")],
                media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
                tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
            )
        finally:
            logging.getLogger().removeHandler(collector)
    finally:
        package.setLevel(before)

    assert {r.levelno for r in records} == {logging.ERROR}  # the malformed one only
    assert not any(r.getMessage().startswith("stored tweet") for r in records)
    assert emf.one("TweetsStored") == 1  # stdout metrics are unaffected
    assert emf.one("RecordFailures", Reason="malformed") == 1


@pytest.mark.parametrize(
    ("value", "level"),
    [("DEBUG", 10), ("info", 20), ("WARN", 30), ("WARNING", 30), ("ERROR", 40)],
)
def test_apply_log_level_maps_the_template_values(value: str, level: int) -> None:
    from twitter_timeline_data_pipeline import handlers

    package = logging.getLogger("twitter_timeline_data_pipeline")
    before = package.level
    try:
        handlers.apply_log_level({"LOG_LEVEL": value})
        assert package.level == level
        handlers.apply_log_level({"LOG_LEVEL": "nonsense"})  # ignored
        handlers.apply_log_level({})
        assert package.level == level
    finally:
        package.setLevel(before)


def test_a_redelivered_message_adds_no_lag_datapoint(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    process_records(
        [
            receives(sqs_record("first", body(make_message(GOOD))), "1"),
            receives(sqs_record("again", body(make_message(NO_MEDIA))), "2"),
        ],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        now=lambda: LATER,
    )

    assert emf.one("TweetsStored") == 2
    assert emf.one("ProcessingLagSeconds") == [30.0]  # only the first receive


def test_a_message_fetched_over_a_day_ago_adds_no_lag_datapoint(
    aws: Aws, cdn: CdnStub, emf: Emf
) -> None:
    process_records(
        [sqs_record("replayed", body(make_message(GOOD)))],  # fetched_at 2026-10-25
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        now=lambda: LATER + timedelta(days=3),
    )

    assert emf.one("TweetsStored") == 1
    assert emf.find("ProcessingLagSeconds") == []


def test_purges_waiting_for_capacity_are_a_backlog_not_a_deferral(
    aws: Aws, emf: Emf, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = Clock()
    gone = put(aws, 1)
    lookup = Lookup({gone: "deleted"})
    compliance_run(aws, lookup, clock)
    emf.reset()
    clock.now += timedelta(hours=25)
    monkeypatch.setattr(
        "twitter_timeline_data_pipeline.handlers.compliance.MAX_CONFIRM_REQUESTS", 0
    )

    compliance_run(aws, lookup, clock)  # the cap leaves it for the next run

    assert emf.one("CompliancePurgeBacklog") == 1
    assert emf.one("CompliancePurgeDeferred") == 0  # nothing the alarm should see
    assert emf.one("ComplianceRunSucceeded") == 1


def test_an_unknown_confirmation_is_a_deferral_and_not_a_backlog(
    aws: Aws, emf: Emf
) -> None:
    clock = Clock()
    tweet_id = put(aws, 1)
    compliance_run(aws, Lookup({tweet_id: "deleted"}), clock)
    emf.reset()
    clock.now += timedelta(hours=25)
    failing = job(aws, Lookup(), clock)
    failing._lookup = lambda chunk: (
        TweetLookup(unknown=frozenset(chunk))
        if len(chunk) == 1
        else TweetLookup(unavailable=dict.fromkeys(chunk, "deleted"))
    )

    with pytest.raises(ComplianceError):
        failing.run(HANDLE)

    assert emf.one("CompliancePurgeDeferred") == 1
    assert emf.one("CompliancePurgeBacklog") == 0


def test_a_scan_that_makes_no_progress_is_a_failed_run_without_a_heartbeat(
    aws: Aws, emf: Emf
) -> None:
    clock = Clock()
    gone = put(aws, 1)
    run_job = job(aws, Lookup({gone: "deleted"}), clock, grace=timedelta(0))

    def stuck(*_a: Any, **_k: Any) -> Any:
        raise ComplianceError(
            "the scan for media held by other Tweets made no progress"
        )

    run_job._store.redact_raw = stuck  # type: ignore[method-assign]

    with pytest.raises(ComplianceError, match="raw pages: ComplianceError"):
        run_job.run(HANDLE)

    assert emf.one("ComplianceRunSucceeded") == 0
    assert emf.one("ComplianceFailures") >= 1


def package_level(code: str, **env: str) -> str:
    import os
    import subprocess
    import sys

    base = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("LOG_LEVEL", "AWS_LAMBDA"))
    }
    out = subprocess.run(
        [sys.executable, "-I", "-c", code],
        env={**base, **env},
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


PRINT_LEVEL = (
    "import logging;import twitter_timeline_data_pipeline.handlers;"
    "print(logging.getLogger('twitter_timeline_data_pipeline').level)"
)


def test_log_level_is_applied_when_the_handlers_are_imported_in_lambda() -> None:
    assert (
        package_level(PRINT_LEVEL, LOG_LEVEL="WARN", AWS_LAMBDA_FUNCTION_NAME="fn")
        == "30"
    )


def test_log_level_is_not_applied_outside_lambda() -> None:
    assert package_level(PRINT_LEVEL, LOG_LEVEL="WARN") == "0"


def test_the_cli_log_level_wins_over_a_shell_log_level() -> None:
    code = (
        "import logging;from twitter_timeline_data_pipeline.cli import main;"
        "main(['fetch','--username','x','--log-level','DEBUG']);"
        "print(logging.getLogger().level, "
        "logging.getLogger('twitter_timeline_data_pipeline').level)"
    )

    assert package_level(code, LOG_LEVEL="WARN") == "10 0"


def test_importing_the_handlers_in_lambda_applies_the_log_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from twitter_timeline_data_pipeline import handlers

    package = logging.getLogger("twitter_timeline_data_pipeline")
    before = package.level
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    try:
        importlib.reload(handlers)
        assert package.level == logging.ERROR
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME")
        package.setLevel(logging.NOTSET)
        importlib.reload(handlers)
        assert package.level == logging.NOTSET  # outside Lambda: untouched
    finally:
        package.setLevel(before)
