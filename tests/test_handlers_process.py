import json
import logging
from pathlib import Path
from typing import Any

import httpx
import pytest
from conftest import Aws
from helpers import (
    PHOTO_URL,
    USER_ID,
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
from twitter_timeline_data_pipeline.handlers.process import handler, process_records
from twitter_timeline_data_pipeline.models import TweetMessage
from twitter_timeline_data_pipeline.storage import MediaStore, TweetStore

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


class ExplodingTweetStore(TweetStore):
    """Fails like a DynamoDB outage for one tweet; behaves normally otherwise."""

    def __init__(self, table: Any, bad_tweet_id: str) -> None:
        super().__init__(table)
        self._bad = bad_tweet_id

    def put_tweet(self, message: TweetMessage, media: Any) -> None:
        if message.tweet.id == self._bad:
            raise RuntimeError("simulated DynamoDB outage")
        super().put_tweet(message, media)


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
            tweet_store=ExplodingTweetStore(aws.tweets_table, UNEXPECTED),
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
        tweet_store=TweetStore(aws.tweets_table),
    )

    assert failed == []


def test_all_good_batch_has_no_failures(aws: Aws, cdn: CdnStub) -> None:
    records = [sqs_record(f"m{i}", body(photo_message(GOOD))) for i in range(3)]

    failed = process_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table),
    )

    assert failed == []
    assert len(cdn.requests) == 1  # redeliveries skip the download


def test_metadata_is_not_written_when_media_fails_transiently(
    aws: Aws, cdn: CdnStub
) -> None:
    failed = process_records(
        [sqs_record("m1", body(photo_message(TRANSIENT, TRANSIENT_URL)))],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table),
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
    tweet_store = TweetStore(aws.tweets_table)

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
        tweet_store=TweetStore(aws.tweets_table),
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
        records, media_store=store, tweet_store=TweetStore(aws.tweets_table)
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
    ],
)
def test_malformed_bodies_fail_their_record(
    aws: Aws, cdn: CdnStub, bad_body: str
) -> None:
    failed = process_records(
        [sqs_record("bad", bad_body)],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table),
    )

    assert failed == ["bad"]


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
        "missing required environment variables: TWEETS_TABLE, MEDIA_BUCKET"
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


def test_aws_client_factories_are_cached(aws: Aws) -> None:
    assert process._s3_client() is process._s3_client()
    assert process._tweet_store("t") is process._tweet_store("t")
    assert process._tweet_store("t") is not process._tweet_store("u")
