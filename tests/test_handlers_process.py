import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from botocore.exceptions import ClientError
from conftest import Aws
from helpers import (
    FETCHED_AT,
    GIF_URL,
    PHOTO_URL,
    USER_ID,
    VIDEO_HI_URL,
    CdnStub,
    ddb_get,
    ddb_items,
    make_message,
    photo,
    sqs_record,
)

from twitter_timeline_data_pipeline import __version__
from twitter_timeline_data_pipeline.config import ConfigError
from twitter_timeline_data_pipeline.handlers import process
from twitter_timeline_data_pipeline.handlers.process import (
    MIN_WRITE_MS,
    SAFETY_MARGIN_MS,
    handler,
    process_records,
)
from twitter_timeline_data_pipeline.models import (
    MAX_STORED_INT,
    Media,
    MediaRetry,
    TweetMessage,
)
from twitter_timeline_data_pipeline.storage import (
    MAX_SIZE_ESTIMATE,
    MEDIA_RETRY_DELAY_SECONDS,
    MediaRetryQueue,
    MediaStore,
    PutResult,
    TweetStore,
    WriteContentionError,
)

SAMPLE_EVENT = Path(__file__).parent.parent / "events" / "sqs.json"
SAMPLE_PHOTO_URL = "https://pbs.twimg.com/media/ExamplePhoto1.jpg"

GOOD = "1700000000000000010"
TRANSIENT = "1700000000000000020"
UNEXPECTED = "1700000000000000030"
NO_MEDIA = "1700000000000000040"
TRANSIENT_URL = "https://pbs.twimg.com/media/FakeBroken01.jpg"


def body(message: TweetMessage) -> str:
    return message.to_json()


def photo_message(tweet_id: str, url: str = PHOTO_URL) -> TweetMessage:
    item = photo(f"3_{tweet_id}", url)
    return make_message(tweet_id, media=(item,), tweet={"media_keys": [item.media_key]})


OLD = "2026-10-20T12:00:00Z"
NEW = "2026-10-25T12:00:00Z"


class ExplodingTweetStore(TweetStore):
    """Fails like a DynamoDB outage for one tweet; behaves normally otherwise."""

    def __init__(self, table: Any, s3_client: Any, bucket: str, bad: str) -> None:
        super().__init__(table, s3_client, bucket)
        self._bad = bad

    def put_tweet(self, message: TweetMessage, media: Any) -> PutResult:
        if message.tweet.id == self._bad:
            raise RuntimeError("simulated DynamoDB outage")
        return super().put_tweet(message, media)


class RecordingRetryQueue(MediaRetryQueue):
    """Records its retries; ``fail_for`` lists the media keys whose send raises."""

    def __init__(self, *, fail_for: frozenset[str] = frozenset()) -> None:
        self.sent: list[MediaRetry] = []
        self._fail_for = fail_for

    def send(self, retry: MediaRetry) -> None:
        if retry.media.media_key in self._fail_for:
            raise RuntimeError("simulated SQS outage")
        self.sent.append(retry)


def stored_tweet_ids(aws: Aws) -> set[str]:
    return {i["tweet_id"] for i in ddb_items(aws.tweets_table)}


def s3_keys(aws: Aws) -> list[str]:
    contents = aws.s3.list_objects_v2(Bucket=aws.bucket).get("Contents", [])
    return sorted(o["Key"] for o in contents if "Key" in o)


@pytest.fixture
def cdn() -> CdnStub:
    stub = CdnStub()
    stub.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    stub.fail(TRANSIENT_URL, 503)
    return stub


def test_reports_exactly_the_failing_records(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    records = [
        sqs_record("m-good", body(photo_message(GOOD))),
        sqs_record("m-malformed", "{not json"),
        sqs_record("m-transient", body(photo_message(TRANSIENT, TRANSIENT_URL))),
        sqs_record("m-unexpected", body(make_message(UNEXPECTED))),
        sqs_record("m-plain", body(make_message(NO_MEDIA))),
        sqs_record(
            "m-schema",
            body(make_message()).replace('"schema_version":1', '"schema_version":9'),
        ),
    ]

    with caplog.at_level(logging.INFO):
        failed = process_records(
            records,
            media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
            tweet_store=ExplodingTweetStore(
                aws.tweets_table, aws.s3, aws.bucket, UNEXPECTED
            ),
        )

    assert failed == ["m-malformed", "m-transient", "m-unexpected", "m-schema"]
    assert stored_tweet_ids(aws) == {GOOD, NO_MEDIA}
    assert s3_keys(aws) == [f"media/{USER_ID}/{GOOD}/3_{GOOD}.jpg"]
    messages = caplog.messages
    assert any("message m-malformed is malformed" in m for m in messages)
    assert any("message m-transient will be retried" in m for m in messages)
    assert any("message m-unexpected failed" in m for m in messages)
    assert any(f"stored tweet {GOOD} media=['3_{GOOD}:stored']" in m for m in messages)


def test_empty_batch_has_no_failures(aws: Aws, cdn: CdnStub) -> None:
    failed = process_records(
        [],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []


def test_stale_record_is_skipped_without_failing_and_logged(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    newer = make_message(
        GOOD, tweet={"public_metrics": {"like_count": 5000}}, fetched_at=NEW
    )
    pic = photo(f"3_{GOOD}")  # the stale message still carries media
    older = make_message(
        GOOD, media=(pic,), tweet={"media_keys": [pic.media_key]}, fetched_at=OLD
    )
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    media_store = MediaStore(aws.s3, aws.bucket, cdn.client())
    process_records(
        [sqs_record("new", body(newer))], media_store=media_store, tweet_store=store
    )

    with caplog.at_level(logging.INFO):
        failed = process_records(
            [sqs_record("old", body(older))],
            media_store=media_store,
            tweet_store=store,
        )

    assert failed == []
    assert any(
        m.startswith(f"stale write skipped for tweet {GOOD}")
        and f"stored fetched_at {NEW}, schema_version 1" in m
        for m in caplog.messages
    )
    assert not any(m.startswith("stored tweet") for m in caplog.messages)
    stored = ddb_get(aws.tweets_table, tweet_id=GOOD)
    assert stored["fetched_at"] == NEW
    assert stored["public_metrics"] == {"like_count": 5000}
    assert (
        len(s3_keys(aws)) == 1
    )  # known limitation: the stale record's media step still ran


def test_all_good_batch_has_no_failures(aws: Aws, cdn: CdnStub) -> None:
    records = [sqs_record(f"m{i}", body(photo_message(GOOD))) for i in range(3)]

    failed = process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []
    assert len(cdn.requests) == 1  # redeliveries skip the download


def test_metadata_is_not_written_when_media_fails_transiently(
    aws: Aws, cdn: CdnStub
) -> None:
    failed = process_records(
        [sqs_record("m1", body(photo_message(TRANSIENT, TRANSIENT_URL)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == ["m1"]
    assert stored_tweet_ids(aws) == set()


def test_second_media_failing_transiently_blocks_metadata_then_redelivery_completes(
    aws: Aws, cdn: CdnStub
) -> None:
    first, second = photo("3_a", PHOTO_URL), photo("3_b", TRANSIENT_URL)
    message = make_message(
        GOOD, media=(first, second), tweet={"media_keys": ["3_a", "3_b"]}
    )
    record = sqs_record("m1", body(message))
    media_store = MediaStore(aws.s3, aws.bucket, cdn.client())
    tweet_store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    assert process_records(
        [record], media_store=media_store, tweet_store=tweet_store
    ) == ["m1"]
    assert stored_tweet_ids(aws) == set()
    assert s3_keys(aws) == [f"media/{USER_ID}/{GOOD}/3_a.jpg"]

    cdn.serve(TRANSIENT_URL, b"jpeg-recovered", "image/jpeg")
    cdn.requests.clear()

    assert (
        process_records([record], media_store=media_store, tweet_store=tweet_store)
        == []
    )
    assert [r.url.host for r in cdn.requests] == ["pbs.twimg.com"]
    assert len(cdn.requests) == 1  # only the missing one is downloaded
    item = ddb_get(aws.tweets_table, tweet_id=GOOD)
    assert [m["status"] for m in item["media"]] == ["exists", "stored"]


def test_permanently_unavailable_media_still_records_the_tweet(
    aws: Aws, cdn: CdnStub
) -> None:
    cdn.fail(TRANSIENT_URL, 404)

    failed = process_records(
        [sqs_record("m1", body(photo_message(TRANSIENT, TRANSIENT_URL)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []
    item = ddb_get(aws.tweets_table, tweet_id=TRANSIENT)
    assert item["media"][0]["status"] == "unavailable"
    assert item["media_count"] == 1


def test_unexpected_exception_in_media_store_is_isolated_to_its_record(
    aws: Aws, cdn: CdnStub
) -> None:
    def broken(request: httpx.Request) -> httpx.Response:
        raise ValueError("not a transport error")

    store = MediaStore(
        aws.s3, aws.bucket, httpx.Client(transport=httpx.MockTransport(broken))
    )
    records = [
        sqs_record("m-media", body(photo_message(GOOD))),
        sqs_record("m-plain", body(make_message(NO_MEDIA))),
    ]

    failed = process_records(
        records,
        media_store=store,
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == ["m-media"]
    assert stored_tweet_ids(aws) == {NO_MEDIA}


@pytest.mark.parametrize(
    "bad_body",
    [
        "",
        "null",
        "[]",
        "42",
        '{"schema_version": 1}',
        '{"schema_version": 1, "username": "u"}',
        '{"schema_version": Infinity}',
        "[" * 200_000,
    ],
)
def test_malformed_bodies_fail_their_record(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture, bad_body: str
) -> None:
    with caplog.at_level(logging.INFO):
        failed = process_records(
            [sqs_record("bad", bad_body)],
            media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
            tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        )

    assert failed == ["bad"]
    assert "message bad is malformed" in caplog.messages


# ---- time budget, size cap and final receive -----------------------------------


def receives(record: dict[str, Any], count: object) -> dict[str, Any]:
    return {**record, "attributes": {"ApproximateReceiveCount": count}}


class Countdown:
    """Stand-in for ``get_remaining_time_in_millis``; set ``ms`` to move time on."""

    def __init__(self, ms: int) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms


class FakeContext:
    def __init__(self, *remaining: int) -> None:
        self._remaining = list(remaining)
        self.calls = 0

    def get_remaining_time_in_millis(self) -> int:
        self.calls += 1
        return (
            self._remaining.pop(0) if len(self._remaining) > 1 else self._remaining[0]
        )


def video_message(tweet_id: str, **media: Any) -> TweetMessage:
    item = Media.from_dict({"media_key": f"7_{tweet_id}", "type": "video", **media})
    return make_message(tweet_id, media=(item,), tweet={"media_keys": [item.media_key]})


def test_unstarted_records_are_reported_and_untouched_when_time_runs_low(
    aws: Aws, cdn: CdnStub
) -> None:
    clock = Countdown(100_000)
    second_url = "https://pbs.twimg.com/media/FakeSecond01.jpg"
    third_url = "https://pbs.twimg.com/media/FakeThird01.jpg"
    for url in (second_url, third_url):
        cdn.serve(url, b"x", "image/jpeg")

    def slow_first() -> httpx.Response:
        clock.ms = SAFETY_MARGIN_MS - 1  # the first download used up the budget
        return httpx.Response(
            200, content=b"jpeg", headers={"Content-Type": "image/jpeg"}
        )

    cdn.responses[PHOTO_URL] = slow_first
    records = [
        sqs_record("m1", body(photo_message(GOOD))),
        sqs_record("m2", body(photo_message(TRANSIENT, second_url))),
        sqs_record("m3", body(make_message(NO_MEDIA))),
    ]
    puts: list[str] = []

    class SpyTweetStore(TweetStore):
        def put_tweet(self, message: TweetMessage, media: Any) -> PutResult:
            puts.append(message.tweet.id)
            return super().put_tweet(message, media)

    failed = process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=SpyTweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=clock,
    )

    assert failed == ["m2", "m3"]
    assert puts == [GOOD]
    assert [r.url.path for r in cdn.requests] == ["/media/FakePhotoId01.jpg"]
    assert stored_tweet_ids(aws) == {GOOD}
    assert len(s3_keys(aws)) == 1


def test_a_batch_that_starts_out_of_time_touches_nothing(
    aws: Aws, cdn: CdnStub
) -> None:
    records = [sqs_record(f"m{i}", body(photo_message(GOOD))) for i in range(3)]

    failed = process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=Countdown(SAFETY_MARGIN_MS - 1),
    )

    assert failed == ["m0", "m1", "m2"]
    assert cdn.requests == []
    assert stored_tweet_ids(aws) == set()
    assert s3_keys(aws) == []


def test_a_record_started_exactly_at_the_margin_is_processed(
    aws: Aws, cdn: CdnStub
) -> None:
    failed = process_records(
        [sqs_record("m1", body(make_message(NO_MEDIA)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=Countdown(SAFETY_MARGIN_MS),
    )

    assert failed == []
    assert stored_tweet_ids(aws) == {NO_MEDIA}


def test_no_time_left_for_a_download_fails_the_record_without_a_request(
    aws: Aws, cdn: CdnStub
) -> None:
    remaining = iter([SAFETY_MARGIN_MS + 5_000, SAFETY_MARGIN_MS - 1])

    failed = process_records(
        [sqs_record("m1", body(photo_message(GOOD)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=lambda: next(remaining),
    )

    assert failed == ["m1"]
    assert cdn.requests == []
    assert stored_tweet_ids(aws) == set()


def slow_cdn(clock: list[float], url: str) -> CdnStub:
    def chunks() -> Any:
        for _ in range(50):
            clock[0] += 10
            yield b"\x00" * 1024

    stub = CdnStub()
    stub.responses[url] = lambda: httpx.Response(
        200, content=chunks(), headers={"Content-Type": "video/mp4"}
    )
    return stub


def test_a_slow_download_stops_at_the_remaining_time_and_the_record_is_retried(
    aws: Aws,
) -> None:
    clock = [0.0]
    cdn = slow_cdn(clock, VIDEO_HI_URL)
    message = video_message(
        GOOD, variants=[{"content_type": "video/mp4", "url": VIDEO_HI_URL}]
    )

    failed = process_records(
        [sqs_record("m1", body(message))],
        media_store=MediaStore(
            aws.s3, aws.bucket, cdn.client(), clock=lambda: clock[0]
        ),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=lambda: SAFETY_MARGIN_MS + 30_000,  # 30 s of download budget
    )

    assert failed == ["m1"]
    assert clock[0] == 30  # stopped at the deadline, not after 50 chunks
    assert stored_tweet_ids(aws) == set()
    assert s3_keys(aws) == []


def test_final_receive_after_a_slow_download_writes_the_tweet_and_queues_a_retry(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    clock = [0.0]
    cdn = slow_cdn(clock, VIDEO_HI_URL)
    message = video_message(
        GOOD, variants=[{"content_type": "video/mp4", "url": VIDEO_HI_URL}]
    )

    queue = RecordingRetryQueue()

    with caplog.at_level(logging.INFO):
        failed = process_records(
            [receives(sqs_record("m1", body(message)), "5")],
            media_store=MediaStore(
                aws.s3, aws.bucket, cdn.client(), clock=lambda: clock[0]
            ),
            tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
            remaining_ms=lambda: SAFETY_MARGIN_MS + 30_000,
            max_receive_count=5,
            retry_queue=queue,
        )

    assert failed == []
    item = ddb_get(aws.tweets_table, tweet_id=GOOD)
    assert item["media"] == [
        {
            "media_key": f"7_{GOOD}",
            "type": "video",
            "status": "retrying",
            "source_url": VIDEO_HI_URL,
            "s3_key": f"media/{USER_ID}/{GOOD}/7_{GOOD}.mp4",
            "variants": [{"content_type": "video/mp4", "url": VIDEO_HI_URL}],
        }
    ]
    (retry,) = queue.sent
    assert (retry.tweet_id, retry.media.media_key) == (GOOD, f"7_{GOOD}")
    assert any("last receive" in m for m in caplog.messages)
    assert s3_keys(aws) == []


def test_final_receive_with_transient_media_failure_still_writes_the_tweet(
    aws: Aws, cdn: CdnStub
) -> None:
    ok, broken = photo("3_a", PHOTO_URL), photo("3_b", TRANSIENT_URL)
    message = make_message(
        GOOD, media=(broken, ok), tweet={"media_keys": ["3_b", "3_a"]}
    )
    queue = RecordingRetryQueue()

    failed = process_records(
        [receives(sqs_record("m1", body(message)), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == []
    item = ddb_get(aws.tweets_table, tweet_id=GOOD)
    assert [(m["media_key"], m["status"]) for m in item["media"]] == [
        ("3_b", "retrying"),
        ("3_a", "stored"),
    ]
    assert item["media"][0]["source_url"] == f"{TRANSIENT_URL}?name=orig"
    assert [r.media.media_key for r in queue.sent] == ["3_b"]
    assert queue.sent[0].fetched_at == FETCHED_AT
    assert queue.sent[0].author_id == USER_ID


def test_final_receive_a_photo_in_s3_is_exists_and_a_503_photo_is_retrying(
    aws: Aws, cdn: CdnStub
) -> None:
    """The evaluator's repro: the photo already stored must keep its key, and the 503
    one must keep its URL and get a retry message, with 19.999 s left."""
    stored, broken = photo("3_a", PHOTO_URL), photo("3_b", TRANSIENT_URL)
    message = make_message(
        GOOD, media=(stored, broken), tweet={"media_keys": ["3_a", "3_b"]}
    )
    key = f"media/{USER_ID}/{GOOD}/3_a.jpg"
    aws.s3.put_object(
        Bucket=aws.bucket, Key=key, Body=b"already", ContentType="image/jpeg"
    )
    queue = RecordingRetryQueue()

    failed = process_records(
        [receives(sqs_record("m1", body(message)), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=Countdown(SAFETY_MARGIN_MS - 1),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == []
    assert cdn.requests == []
    exists, retrying = ddb_get(aws.tweets_table, tweet_id=GOOD)["media"]
    assert (exists["status"], exists["s3_key"]) == ("exists", key)
    assert retrying["status"] == "retrying"
    assert retrying["source_url"] == f"{TRANSIENT_URL}?name=orig"
    assert [r.media.media_key for r in queue.sent] == ["3_b"]


def test_final_receive_cheap_checks_decide_without_a_retry(
    aws: Aws, cdn: CdnStub
) -> None:
    """A media item that can't be downloaded but needs no download to be settled
    (not an allowed host; too big by estimate) is recorded as such, not retried."""
    bad_host = photo("3_h", "https://example.com/x.jpg")
    big = Media.from_dict(
        {
            "media_key": "7_big",
            "type": "video",
            "duration_ms": 10_000,
            "variants": [
                {
                    "content_type": "video/mp4",
                    "bit_rate": 8_000_000,
                    "url": VIDEO_HI_URL,
                }
            ],
        }
    )
    message = make_message(
        GOOD, media=(bad_host, big), tweet={"media_keys": ["3_h", "7_big"]}
    )
    queue = RecordingRetryQueue()

    failed = process_records(
        [receives(sqs_record("m1", body(message)), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=1_000_000),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=Countdown(SAFETY_MARGIN_MS - 1),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == []
    assert queue.sent == []
    statuses = [m["status"] for m in ddb_get(aws.tweets_table, tweet_id=GOOD)["media"]]
    assert statuses == ["skipped", "too_large"]


def test_final_receive_reports_the_record_when_a_retry_cannot_be_sent(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    first, second = photo("3_a", TRANSIENT_URL), photo("3_b", TRANSIENT_URL)
    message = make_message(
        GOOD, media=(first, second), tweet={"media_keys": ["3_a", "3_b"]}
    )
    queue = RecordingRetryQueue(fail_for=frozenset({"3_a"}))

    failed = process_records(
        [receives(sqs_record("m1", body(message)), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == ["m1"]  # reaches the DLQ and its alarm, not dropped silently
    assert [r.media.media_key for r in queue.sent] == ["3_b"]  # others still queued
    assert any("could not queue a retry for media 3_a" in m for m in caplog.messages)


def test_a_stale_write_on_the_last_receive_sends_no_retry(
    aws: Aws, cdn: CdnStub
) -> None:
    newer = make_message(GOOD, fetched_at=NEW)
    older = make_message(
        GOOD,
        media=(photo("3_b", TRANSIENT_URL),),
        tweet={"media_keys": ["3_b"]},
        fetched_at=OLD,
    )
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(newer, [])
    queue = RecordingRetryQueue()

    failed = process_records(
        [receives(sqs_record("m1", body(older)), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=store,
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == []
    assert queue.sent == []


def test_retry_queue_sends_a_delayed_media_retry_message(
    aws: Aws, cdn: CdnStub
) -> None:
    queue = MediaRetryQueue(aws.sqs, aws.retry_queue_url, delay_seconds=300)

    process_records(
        [receives(sqs_record("m1", body(photo_message(GOOD, TRANSIENT_URL))), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
    )

    attributes = aws.sqs.get_queue_attributes(
        QueueUrl=aws.retry_queue_url, AttributeNames=["All"]
    )["Attributes"]
    assert attributes["ApproximateNumberOfMessagesDelayed"] == "1"
    assert attributes["ApproximateNumberOfMessages"] == "0"


def test_retry_queue_message_body_is_the_media_retry_json() -> None:
    calls: list[dict[str, Any]] = []

    class Sqs:
        def send_message(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    retry = MediaRetry(
        tweet_id=GOOD, username="u", media=photo(), fetched_at=FETCHED_AT
    )

    MediaRetryQueue(cast("Any", Sqs()), "https://queue").send(retry)

    (call,) = calls
    assert call["QueueUrl"] == "https://queue"
    assert call["DelaySeconds"] == MEDIA_RETRY_DELAY_SECONDS
    assert MediaRetry.from_json(call["MessageBody"]) == retry


@pytest.mark.parametrize(
    ("count", "max_receive_count"),
    [("4", 5), ("1", 5), (None, 5), ("many", 5), ("5", None)],
)
def test_before_the_final_receive_a_media_failure_still_fails_the_record(
    aws: Aws, cdn: CdnStub, count: object, max_receive_count: int | None
) -> None:
    record = receives(
        sqs_record("m1", body(photo_message(TRANSIENT, TRANSIENT_URL))), count
    )
    queue = RecordingRetryQueue()

    failed = process_records(
        [record],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=max_receive_count,
        retry_queue=queue,
    )

    assert failed == ["m1"]
    assert stored_tweet_ids(aws) == set()
    assert queue.sent == []


def test_a_record_without_attributes_counts_as_not_final(
    aws: Aws, cdn: CdnStub
) -> None:
    failed = process_records(
        [sqs_record("m1", body(photo_message(TRANSIENT, TRANSIENT_URL)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=1,
        retry_queue=RecordingRetryQueue(),
    )

    assert failed == ["m1"]


def test_the_final_receive_does_not_rescue_malformed_or_unexpected_failures(
    aws: Aws, cdn: CdnStub
) -> None:
    records = [
        receives(sqs_record("m-bad", "{nope"), "5"),
        receives(sqs_record("m-boom", body(make_message(UNEXPECTED))), "5"),
    ]

    failed = process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=ExplodingTweetStore(
            aws.tweets_table, aws.s3, aws.bucket, UNEXPECTED
        ),
        max_receive_count=5,
        retry_queue=RecordingRetryQueue(),
    )

    assert failed == ["m-bad", "m-boom"]


def test_media_over_the_cap_is_recorded_and_the_tweet_is_stored(aws: Aws) -> None:
    cdn = CdnStub()
    message = video_message(
        GOOD,
        duration_ms=10_000,
        variants=[
            {"content_type": "video/mp4", "bit_rate": 8_000_000, "url": VIDEO_HI_URL}
        ],
    )

    failed = process_records(
        [sqs_record("m1", body(message))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=1_000_000),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []
    assert cdn.requests == []
    media = ddb_get(aws.tweets_table, tweet_id=GOOD)["media"][0]
    assert (media["status"], media["size_bytes"]) == ("too_large", 10_000_000)
    assert "s3_key" not in media
    assert s3_keys(aws) == []


def test_cheapest_records_run_first_and_unknown_size_videos_last(
    aws: Aws, cdn: CdnStub
) -> None:
    big = video_message(
        "1700000000000000101",
        duration_ms=10_000,
        variants=[
            {"content_type": "video/mp4", "bit_rate": 8_000_000, "url": VIDEO_HI_URL}
        ],
    )
    unknown = video_message(
        "1700000000000000102",
        variants=[{"content_type": "video/mp4", "url": VIDEO_HI_URL}],
    )
    small = video_message(
        "1700000000000000103",
        duration_ms=1_000,
        variants=[
            {"content_type": "video/mp4", "bit_rate": 8_000, "url": VIDEO_HI_URL}
        ],
    )
    gif = make_message(
        "1700000000000000104",
        media=(
            Media.from_dict(
                {
                    "media_key": "16_1",
                    "type": "animated_gif",
                    "variants": [
                        {"content_type": "video/mp4", "bit_rate": 0, "url": GIF_URL}
                    ],
                }
            ),
        ),
    )
    hls = make_message(
        "1700000000000000105",
        media=(
            Media.from_dict(
                {
                    "media_key": "7_hls",
                    "type": "video",
                    "variants": [{"content_type": "x/hls", "url": VIDEO_HI_URL}],
                }
            ),
        ),
    )
    records = [
        sqs_record("unknown", body(unknown)),
        sqs_record("big", body(big)),
        sqs_record("photo", body(photo_message(GOOD))),
        sqs_record("small", body(small)),
        sqs_record("gif", body(gif)),
        sqs_record("hls", body(hls)),
        sqs_record("bad", "{nope"),
        sqs_record("text", body(make_message(NO_MEDIA))),
    ]
    order: list[str] = []

    class Spy(TweetStore):
        def put_tweet(self, message: TweetMessage, media: Any) -> PutResult:
            order.append(message.tweet.id)
            return super().put_tweet(message, media)

    process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=Spy(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert order == [
        GOOD,
        "1700000000000000104",
        "1700000000000000105",
        NO_MEDIA,
        "1700000000000000103",
        "1700000000000000101",
        "1700000000000000102",
    ]


def test_a_huge_bit_rate_is_capped_so_the_tweet_is_stored_as_too_large(
    aws: Aws, cdn: CdnStub
) -> None:
    huge = video_message(
        TRANSIENT,
        duration_ms=10_000,
        variants=[
            {"content_type": "video/mp4", "bit_rate": 10**400, "url": VIDEO_HI_URL}
        ],
    )
    record = sqs_record("huge", body(huge))

    # The estimate is capped, so ordering doesn't overflow and the record sorts last.
    assert process._download_cost(record) == float(MAX_SIZE_ESTIMATE)
    failed = process_records(
        [record, sqs_record("ok", body(photo_message(GOOD)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=1_000_000),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    # The capped size fits a DynamoDB number, so the Tweet is stored with the video
    # recorded as too_large instead of the record failing.
    assert failed == []
    assert stored_tweet_ids(aws) == {GOOD, TRANSIENT}
    (entry,) = ddb_get(aws.tweets_table, tweet_id=TRANSIENT)["media"]
    assert entry["status"] == "too_large"
    assert entry["size_bytes"] == MAX_SIZE_ESTIMATE


def test_a_slow_video_first_does_not_starve_its_siblings_on_the_last_receive(
    aws: Aws,
) -> None:
    clock = [0.0]
    cdn = slow_cdn(clock, VIDEO_HI_URL)
    cdn.serve(PHOTO_URL, b"jpeg", "image/jpeg")
    slow_video = video_message(
        TRANSIENT, variants=[{"content_type": "video/mp4", "url": VIDEO_HI_URL}]
    )
    queue = RecordingRetryQueue()
    records = [
        receives(sqs_record("slow", body(slow_video)), "5"),
        receives(sqs_record("photo", body(photo_message(GOOD))), "5"),
        receives(sqs_record("text", body(make_message(NO_MEDIA))), "5"),
    ]

    failed = process_records(
        records,
        media_store=MediaStore(
            aws.s3, aws.bucket, cdn.client(), clock=lambda: clock[0]
        ),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=lambda: int(SAFETY_MARGIN_MS + 30_000 - clock[0] * 1000),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == []
    assert stored_tweet_ids(aws) == {TRANSIENT, GOOD, NO_MEDIA}
    assert ddb_get(aws.tweets_table, tweet_id=GOOD)["media"][0]["status"] == "stored"
    slow = ddb_get(aws.tweets_table, tweet_id=TRANSIENT)["media"][0]
    assert (slow["status"], slow["source_url"]) == ("retrying", VIDEO_HI_URL)
    assert [r.tweet_id for r in queue.sent] == [TRANSIENT]


def test_last_receive_record_without_time_is_written_with_retrying_media(
    aws: Aws, cdn: CdnStub
) -> None:
    two = make_message(
        NO_MEDIA,
        media=(photo("3_x", PHOTO_URL), photo("3_y", TRANSIENT_URL)),
        tweet={"media_keys": ["3_x", "3_y"]},
    )
    queue = RecordingRetryQueue()
    failed = process_records(
        [
            receives(sqs_record("m1", body(photo_message(GOOD))), "5"),
            receives(sqs_record("m2", body(two)), "5"),
            receives(sqs_record("m3", body(photo_message(TRANSIENT))), "4"),
        ],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=Countdown(SAFETY_MARGIN_MS - 1),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == ["m3"]  # not its last receive
    assert cdn.requests == []
    assert s3_keys(aws) == []
    one = ddb_get(aws.tweets_table, tweet_id=GOOD)["media"][0]
    assert (one["status"], one["source_url"]) == ("retrying", f"{PHOTO_URL}?name=orig")
    assert stored_tweet_ids(aws) == {GOOD, NO_MEDIA}
    assert sorted((r.tweet_id, r.media.media_key) for r in queue.sent) == [
        (GOOD, f"3_{GOOD}"),
        (NO_MEDIA, "3_x"),
        (NO_MEDIA, "3_y"),
    ]


def test_last_receive_record_with_almost_no_time_is_still_reported(
    aws: Aws, cdn: CdnStub
) -> None:
    failed = process_records(
        [receives(sqs_record("m1", body(photo_message(GOOD))), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=Countdown(MIN_WRITE_MS - 1),
        max_receive_count=5,
        retry_queue=RecordingRetryQueue(),
    )

    assert failed == ["m1"]
    assert stored_tweet_ids(aws) == set()


def test_last_receive_records_any_media_exception_as_retrying(
    aws: Aws, cdn: CdnStub
) -> None:
    def deny(**_kw: Any) -> None:
        raise ClientError({"Error": {"Code": "500", "Message": "boom"}}, "HeadObject")

    aws.s3.meta.events.register("before-call.s3.HeadObject", deny)
    records = [receives(sqs_record("m1", body(photo_message(GOOD))), "5")]
    store = MediaStore(aws.s3, aws.bucket, cdn.client())
    queue = RecordingRetryQueue()

    assert (
        process_records(
            records,
            media_store=store,
            tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
            max_receive_count=5,
            retry_queue=queue,
        )
        == []
    )
    # HeadObject fails in the store and again in the cheap checks: still retrying.
    assert ddb_get(aws.tweets_table, tweet_id=GOOD)["media"][0]["status"] == "retrying"
    assert len(queue.sent) == 1

    other = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    earlier = [receives(sqs_record("m2", body(photo_message(NO_MEDIA))), "4")]
    assert process_records(
        earlier,
        media_store=store,
        tweet_store=other,
        max_receive_count=5,
        retry_queue=queue,
    ) == ["m2"]


# ---- handler -------------------------------------------------------------------


def sample_event() -> dict[str, Any]:
    event: dict[str, Any] = json.loads(SAMPLE_EVENT.read_text())
    return event


@pytest.fixture
def process_env(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch, cdn: CdnStub
) -> Aws:
    cdn.serve(SAMPLE_PHOTO_URL, b"sample-photo", "image/jpeg")
    seen: list[float] = []

    def client(timeout: float) -> httpx.Client:
        seen.append(timeout)
        return cdn.client()

    monkeypatch.setattr(process, "_http_client", client)
    return pipeline_env


def test_handler_processes_the_sample_event(process_env: Aws) -> None:
    result = handler(sample_event(), None)

    assert result == {"batchItemFailures": []}
    item = ddb_get(process_env.tweets_table, tweet_id="1000000000000000001")
    assert item["media"][0]["status"] == "stored"
    assert s3_keys(process_env) == [
        "media/1000000000000000000/1000000000000000001/3_1000000000000000002.jpg"
    ]


def test_handler_returns_batch_item_failures_shape(process_env: Aws) -> None:
    event = sample_event()
    event["Records"].append(sqs_record("bad-1", "{broken"))
    event["Records"].append(sqs_record("bad-2", "[]"))

    result = handler(event, None)

    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "bad-1"},
            {"itemIdentifier": "bad-2"},
        ]
    }
    assert "1000000000000000001" in stored_tweet_ids(process_env)


def test_handler_redelivery_is_idempotent(process_env: Aws, cdn: CdnStub) -> None:
    handler(sample_event(), None)
    downloads = len(cdn.requests)

    result = handler(sample_event(), None)

    assert result == {"batchItemFailures": []}
    assert len(cdn.requests) == downloads
    assert process_env.tweets_table.scan()["Count"] == 1


def test_handler_missing_settings_lists_every_missing_variable() -> None:
    with pytest.raises(ConfigError) as info:
        handler(sample_event(), None)

    assert str(info.value) == (
        "missing required environment variables: "
        "TWEETS_TABLE, MEDIA_BUCKET, MEDIA_RETRY_QUEUE_URL"
    )


def test_handler_passes_http_timeout_to_the_client(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch, cdn: CdnStub
) -> None:
    monkeypatch.setenv("HTTP_TIMEOUT_SECONDS", "4.5")
    seen: list[float] = []

    def client(timeout: float) -> httpx.Client:
        seen.append(timeout)
        return cdn.client()

    monkeypatch.setattr(process, "_http_client", client)
    cdn.serve(SAMPLE_PHOTO_URL, b"x", "image/jpeg")

    handler(sample_event(), None)

    assert seen == [4.5]


def test_handler_reports_every_record_when_the_context_has_no_time_left(
    process_env: Aws, cdn: CdnStub
) -> None:
    context = FakeContext(SAFETY_MARGIN_MS - 1)

    result = handler(sample_event(), context)

    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "00000000-0000-0000-0000-000000000001"}
        ]
    }
    assert context.calls == 1
    assert cdn.requests == []
    assert stored_tweet_ids(process_env) == set()


def test_handler_gives_downloads_the_time_left_and_the_final_receive_count(
    process_env: Aws, monkeypatch: pytest.MonkeyPatch, cdn: CdnStub
) -> None:
    monkeypatch.setenv("MAX_RECEIVE_COUNT", "5")
    cdn.fail(SAMPLE_PHOTO_URL, 503)
    event = sample_event()
    event["Records"][0]["attributes"] = {"ApproximateReceiveCount": "5"}

    result = handler(event, FakeContext(SAFETY_MARGIN_MS + 60_000))

    assert result == {"batchItemFailures": []}
    item = ddb_get(process_env.tweets_table, tweet_id="1000000000000000001")
    assert item["media"][0]["status"] == "retrying"
    assert item["media"][0]["source_url"] == f"{SAMPLE_PHOTO_URL}?name=orig"
    attributes = process_env.sqs.get_queue_attributes(
        QueueUrl=process_env.retry_queue_url, AttributeNames=["All"]
    )["Attributes"]
    assert attributes["ApproximateNumberOfMessagesDelayed"] == "1"


def test_handler_applies_max_media_bytes(
    process_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAX_MEDIA_BYTES", "5")

    assert handler(sample_event(), None) == {"batchItemFailures": []}

    item = ddb_get(process_env.tweets_table, tweet_id="1000000000000000001")
    assert item["media"][0]["status"] == "too_large"
    assert s3_keys(process_env) == []


def test_handler_without_records_key_raises_key_error(process_env: Aws) -> None:
    with pytest.raises(KeyError):
        handler({}, None)


def test_http_client_factory_configuration() -> None:
    client = process._http_client(7.0)
    try:
        assert client.timeout.read == 7.0
        assert client.follow_redirects is False
        assert (
            client.headers["User-Agent"]
            == f"twitter-timeline-data-pipeline/{__version__}"
        )
        assert process._http_client(7.0) is client  # cached per timeout
        assert process._http_client(8.0) is not client
    finally:
        client.close()
        process._http_client(8.0).close()


def test_s3_client_has_bounded_timeouts_and_retries(aws: Aws) -> None:
    config: Any = process._s3_client().meta.config

    assert (config.connect_timeout, config.read_timeout) == (5, 10)
    assert config.retries["mode"] == "standard"
    assert config.retries["total_max_attempts"] == 4  # max_attempts=3 retries


def test_sqs_and_dynamodb_clients_have_short_bounded_timeouts(aws: Aws) -> None:
    sqs: Any = process._sqs_client().meta.config
    ddb: Any = process._tweet_store("t", "b")._table.meta.client.meta.config

    for config in (sqs, ddb):
        assert (config.connect_timeout, config.read_timeout) == (3, 5)
        assert config.retries["mode"] == "standard"
        assert config.retries["total_max_attempts"] == 4


def test_enormous_numbers_in_a_message_are_capped_and_the_tweet_is_stored(
    aws: Aws, cdn: CdnStub
) -> None:
    huge = "9" * 45
    data = photo_message(GOOD).to_dict()
    data["tweet"]["public_metrics"] = {"like_count": "__H__"}
    data["media"][0]["width"] = "__H__"
    data["media"][0]["duration_ms"] = "__H__"
    raw = json.dumps(data).replace('"__H__"', huge)

    failed = process_records(
        [sqs_record("m1", raw)],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []
    item = ddb_get(aws.tweets_table, tweet_id=GOOD)
    assert item["public_metrics"]["like_count"] == MAX_STORED_INT
    assert item["media"][0]["status"] == "stored"


def test_aws_client_factories_are_cached(aws: Aws) -> None:
    assert process._s3_client() is process._s3_client()
    assert process._tweet_store("t", "b") is process._tweet_store("t", "b")
    assert process._tweet_store("t", "b") is not process._tweet_store("u", "b")


# ---- poster frames and missing keys (#17) -------------------------------------

PREVIEW_URL = "https://pbs.twimg.com/ext_tw_video_thumb/1/pu/img/p.jpg"


def preview_video_message() -> TweetMessage:
    return video_message(
        GOOD,
        preview_image_url=PREVIEW_URL,
        alt_text="clip",
        variants=[{"content_type": "video/mp4", "url": VIDEO_HI_URL}],
    )


def run_preview_record(
    aws: Aws, cdn: CdnStub, message: TweetMessage, receive: str
) -> tuple[list[str], RecordingRetryQueue]:
    queue = RecordingRetryQueue()
    failed = process_records(
        [receives(sqs_record("m1", body(message)), receive)],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
    )
    return failed, queue


def preview_cdn(preview_status: int | None) -> CdnStub:
    cdn = CdnStub()
    cdn.serve(VIDEO_HI_URL, b"video", "video/mp4")
    if preview_status is None:
        cdn.serve(PREVIEW_URL, b"poster", "image/jpeg")
    else:
        cdn.fail(PREVIEW_URL, preview_status)
    return cdn


def test_a_transient_poster_frame_failure_is_retried_before_the_last_receive(
    aws: Aws,
) -> None:
    failed, queue = run_preview_record(
        aws, preview_cdn(503), preview_video_message(), "2"
    )

    assert failed == ["m1"]
    assert queue.sent == []
    assert stored_tweet_ids(aws) == set()


def test_a_poster_frame_that_fails_on_the_last_receive_is_queued_for_retry(
    aws: Aws,
) -> None:
    failed, queue = run_preview_record(
        aws, preview_cdn(503), preview_video_message(), "5"
    )

    assert failed == []
    (entry,) = ddb_get(aws.tweets_table, tweet_id=GOOD)["media"]
    assert entry["status"] == "retrying"
    assert entry["preview_s3_key"] == f"media/{USER_ID}/{GOOD}/7_{GOOD}_preview.jpg"
    assert entry["preview_image_url"] == PREVIEW_URL
    assert entry["alt_text"] == "clip"
    (retry,) = queue.sent
    assert retry.media.preview_image_url == PREVIEW_URL


def test_a_stored_poster_frame_is_referenced_on_the_item(aws: Aws) -> None:
    failed, queue = run_preview_record(
        aws, preview_cdn(None), preview_video_message(), "1"
    )

    assert failed == [] and queue.sent == []
    (entry,) = ddb_get(aws.tweets_table, tweet_id=GOOD)["media"]
    assert entry["status"] == "stored"
    assert entry["preview_s3_key"] in s3_keys(aws)


def test_missing_media_keys_are_recorded_and_logged(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    pic = photo("3_ok", PHOTO_URL)
    message = make_message(
        GOOD,
        media=(pic,),
        tweet={"media_keys": ["3_gone", "3_ok"]},
    )
    message = replace(message, missing_media_keys=("3_gone",))
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"x", "image/jpeg")

    with caplog.at_level(logging.WARNING):
        failed, _ = run_preview_record(aws, cdn, message, "1")

    assert failed == []
    item = ddb_get(aws.tweets_table, tweet_id=GOOD)
    assert item["media_keys"] == ["3_gone", "3_ok"]
    assert [(e["media_key"], e["status"]) for e in item["media"]] == [
        ("3_gone", "missing"),
        ("3_ok", "stored"),
    ]
    assert item["media_count"] == 2
    assert any("3_gone" in m and "missing" in m for m in caplog.messages)


def photo_cdn() -> CdnStub:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"x", "image/jpeg")
    return cdn


def test_write_contention_fails_the_record_for_redelivery(aws: Aws) -> None:
    class Contended(TweetStore):
        def put_tweet(self, *_a: Any, **_k: Any) -> PutResult:
            raise WriteContentionError("changed under its merge")

    queue = RecordingRetryQueue()
    failed = process_records(
        [receives(sqs_record("m1", body(photo_message(GOOD))), "5")],
        media_store=MediaStore(aws.s3, aws.bucket, photo_cdn().client()),
        tweet_store=Contended(aws.tweets_table, aws.s3, aws.bucket),
        max_receive_count=5,
        retry_queue=queue,
    )

    assert failed == ["m1"]  # not acked as stale: SQS redelivers, then the DLQ alarm
    assert queue.sent == []
