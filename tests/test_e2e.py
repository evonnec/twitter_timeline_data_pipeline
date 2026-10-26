"""Fetch handler -> moto SQS -> process handler -> moto DynamoDB and S3."""

import json
from typing import Any

import httpx
import pytest
from conftest import Aws
from helpers import (
    GIF_URL,
    KISS,
    LONG_POST_ID,
    PHOTO_URL,
    USER_ID,
    VIDEO_HI_URL,
    CdnStub,
    XApiStub,
    as_items,
    ddb_get,
    ddb_items,
    install_x_api,
    long_post_api_object,
    long_post_timeline,
    make_message,
    sqs_record,
    synthetic_timeline,
)

from twitter_timeline_data_pipeline.handlers import fetch, process
from twitter_timeline_data_pipeline.handlers.fetch import handler as fetch_handler
from twitter_timeline_data_pipeline.handlers.process import handler as process_handler

USERNAME = "example_user"
NEWEST = "1700000000000000005"
PHOTO_TWEET, VIDEO_TWEET, GIF_TWEET, PLAIN_TWEET, QUOTE_TWEET = (
    f"170000000000000000{n}" for n in (5, 4, 3, 2, 1)
)
PHOTO_KEY = f"media/{USER_ID}/{PHOTO_TWEET}/3_1700000000000000101.jpg"
VIDEO_KEY = f"media/{USER_ID}/{VIDEO_TWEET}/7_1700000000000000102.mp4"
GIF_KEY = f"media/{USER_ID}/{GIF_TWEET}/16_1700000000000000103.mp4"
VIDEO_PREVIEW_URL = (
    "https://pbs.twimg.com/ext_tw_video_thumb/1700000000000000102/pu/img/fake.jpg"
)
GIF_PREVIEW_URL = "https://pbs.twimg.com/tweet_video_thumb/FakeGif01.jpg"
VIDEO_PREVIEW_KEY = f"media/{USER_ID}/{VIDEO_TWEET}/7_1700000000000000102_preview.jpg"
GIF_PREVIEW_KEY = f"media/{USER_ID}/{GIF_TWEET}/16_1700000000000000103_preview.jpg"
VIDEO_BYTES = b"\x00\x01" * 2_000_000  # 4 MB: crosses the multipart threshold


@pytest.fixture
def cdn() -> CdnStub:
    stub = CdnStub()
    stub.serve(PHOTO_URL, b"jpeg-bytes", "image/jpeg")
    stub.serve(VIDEO_HI_URL, VIDEO_BYTES, "video/mp4")
    stub.serve(GIF_URL, b"gif-as-mp4", "video/mp4")
    stub.serve(VIDEO_PREVIEW_URL, b"video-poster", "image/jpeg")
    stub.serve(GIF_PREVIEW_URL, b"gif-poster", "image/jpeg")
    return stub


@pytest.fixture
def pipeline(pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch, cdn: CdnStub) -> Aws:
    install_x_api(monkeypatch, fetch, XApiStub())
    monkeypatch.setattr(process, "_http_client", lambda _timeout: cdn.client())
    return pipeline_env


def drain(aws: Aws) -> list[dict[str, Any]]:
    """Receive and delete every message, as the Lambda event source mapping would."""
    records: list[dict[str, Any]] = []
    while batch := aws.sqs.receive_message(
        QueueUrl=aws.queue_url, MaxNumberOfMessages=10
    ).get("Messages"):
        for m in batch:
            records.append(sqs_record(m["MessageId"], m["Body"]))
            aws.sqs.delete_message(
                QueueUrl=aws.queue_url, ReceiptHandle=m["ReceiptHandle"]
            )
    return records


def s3_keys(aws: Aws, prefix: str = "") -> list[str]:
    """Object keys outside ``raw/`` (or, with ``prefix``, those under it)."""
    contents = aws.s3.list_objects_v2(Bucket=aws.bucket, Prefix=prefix).get(
        "Contents", []
    )
    return sorted(
        o["Key"]
        for o in contents
        if "Key" in o and (prefix or not o["Key"].startswith("raw/"))
    )


def item(aws: Aws, tweet_id: str) -> dict[str, Any]:
    return ddb_get(aws.tweets_table, tweet_id=tweet_id)


def test_full_pipeline_stores_tweets_and_media(pipeline: Aws, cdn: CdnStub) -> None:
    fetched = fetch_handler({"username": USERNAME}, None)
    records = drain(pipeline)

    processed = process_handler({"Records": records}, None)

    assert fetched["enqueued"] == len(records) == 7
    assert processed == {"batchItemFailures": []}
    assert pipeline.tweets_table.scan()["Count"] == 7
    assert s3_keys(pipeline) == sorted(
        [PHOTO_KEY, VIDEO_KEY, GIF_KEY, VIDEO_PREVIEW_KEY, GIF_PREVIEW_KEY]
    )
    raw_keys = s3_keys(pipeline, "raw/")
    assert len(raw_keys) == fetched["pages"] == 2
    stored = {i["raw_key"] for i in pipeline.tweets_table.scan()["Items"]}
    assert stored == set(raw_keys)

    photo = pipeline.s3.get_object(Bucket=pipeline.bucket, Key=PHOTO_KEY)
    assert photo["Body"].read() == b"jpeg-bytes"
    assert photo["ContentType"] == "image/jpeg"
    assert photo["Metadata"]["tweet-id"] == PHOTO_TWEET
    video = pipeline.s3.head_object(Bucket=pipeline.bucket, Key=VIDEO_KEY)
    assert video["ContentLength"] == len(VIDEO_BYTES)
    assert video["ContentType"] == "video/mp4"

    # Photos are fetched at full size; only the best mp4 of a video is downloaded.
    urls = [str(r.url) for r in cdn.requests]
    assert f"{PHOTO_URL}?name=orig" in urls
    assert VIDEO_HI_URL in urls
    assert not any("lo.mp4" in u or "mid.mp4" in u or ".m3u8" in u for u in urls)
    assert VIDEO_PREVIEW_URL in urls and GIF_PREVIEW_URL in urls
    assert len(urls) == 5

    video_item = item(pipeline, VIDEO_TWEET)
    assert video_item["username"] == USERNAME
    assert video_item["media"][0]["status"] == "stored"
    assert video_item["media"][0]["size_bytes"] == len(VIDEO_BYTES)
    assert video_item["media"][0]["preview_s3_key"] == VIDEO_PREVIEW_KEY
    assert video_item["media"][0]["preview_image_url"] == VIDEO_PREVIEW_URL
    assert video_item["media_keys"] == ["7_1700000000000000102"]
    poster = pipeline.s3.get_object(Bucket=pipeline.bucket, Key=VIDEO_PREVIEW_KEY)
    assert poster["Body"].read() == b"video-poster"
    assert video_item["public_metrics"]["like_count"] == 7
    quote = item(pipeline, QUOTE_TWEET)
    assert quote["referenced_tweets"] == [
        {
            "type": "quoted",
            "id": "1600000000000000009",
            "status": "unavailable",
            "unavailable_since": quote["fetched_at"],
        }
    ]
    # Its media key was missing from includes: recorded, not dropped.
    assert quote["media_count"] == 1
    assert quote["media_keys"] == ["3_1700000000000000199"]
    assert quote["media"] == [
        {"media_key": "3_1700000000000000199", "type": "unknown", "status": "missing"}
    ]
    assert item(pipeline, PLAIN_TWEET)["media"] == []
    assert item(pipeline, "1600000000000000001")["referenced_tweets"][0]["type"] == (
        "replied_to"
    )


def test_stored_tweets_are_queryable_by_user_and_time(pipeline: Aws) -> None:
    fetch_handler({"username": USERNAME}, None)
    process_handler({"Records": drain(pipeline)}, None)

    response = pipeline.tweets_table.query(
        IndexName="username-created_at-index",
        KeyConditionExpression="username = :u",
        ExpressionAttributeValues={":u": USERNAME},
        ScanIndexForward=False,
    )

    items = as_items(response["Items"])
    created = [i["created_at"] for i in items]
    assert len(created) == 7
    assert created == sorted(created, reverse=True)
    assert items[0]["tweet_id"] == PHOTO_TWEET


def test_redelivery_is_idempotent(pipeline: Aws, cdn: CdnStub) -> None:
    fetch_handler({"username": USERNAME}, None)
    records = drain(pipeline)
    process_handler({"Records": records}, None)
    downloads = len(cdn.requests)
    before = {
        k: pipeline.s3.head_object(Bucket=pipeline.bucket, Key=k)["ETag"]
        for k in s3_keys(pipeline)
    }

    again = process_handler({"Records": records}, None)

    assert again == {"batchItemFailures": []}
    assert len(cdn.requests) == downloads  # no second media download
    assert pipeline.tweets_table.scan()["Count"] == 7
    assert {
        k: pipeline.s3.head_object(Bucket=pipeline.bucket, Key=k)["ETag"]
        for k in s3_keys(pipeline)
    } == before


def test_second_fetch_uses_the_checkpoint(
    pipeline: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = XApiStub()
    install_x_api(monkeypatch, fetch, api)
    fetch_handler({"username": USERNAME}, None)
    drain(pipeline)

    second = fetch_handler({"username": USERNAME}, None)

    assert second["since_id"] == NEWEST
    assert second["enqueued"] == 0
    assert api.timeline_requests[-1].url.params["since_id"] == NEWEST
    assert drain(pipeline) == []
    state = ddb_get(pipeline.state_table, username=USERNAME)
    assert int(state["newest_id"]) == int(NEWEST)


def test_new_tweets_after_the_checkpoint_flow_through(
    pipeline: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetch_handler({"username": USERNAME}, None)
    process_handler({"Records": drain(pipeline)}, None)
    newer = ["1700000000000000008", "1700000000000000007"]
    install_x_api(monkeypatch, fetch, synthetic_timeline(newer))

    result = fetch_handler({"username": USERNAME}, None)
    process_handler({"Records": drain(pipeline)}, None)

    assert result["enqueued"] == 2
    assert result["newest_id"] == newer[0]
    assert pipeline.tweets_table.scan()["Count"] == 9


def test_more_tweets_than_one_batch_survive_the_round_trip(
    pipeline: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    tweet_ids = [str(1_700_000_000_000_000_100 + n) for n in range(25, 0, -1)]
    install_x_api(monkeypatch, fetch, synthetic_timeline(tweet_ids))

    fetch_handler({"username": USERNAME}, None)
    records = drain(pipeline)
    failures: list[dict[str, str]] = []
    for start in range(0, len(records), 10):  # Lambda's SQS batch size
        failures += process_handler({"Records": records[start : start + 10]}, None)[
            "batchItemFailures"
        ]

    assert len(records) == 25
    assert failures == []
    assert {i["tweet_id"] for i in ddb_items(pipeline.tweets_table)} == set(tweet_ids)


def test_a_poison_message_fails_alone_and_a_transient_failure_succeeds_on_retry(
    pipeline: Aws, cdn: CdnStub
) -> None:
    cdn.fail(GIF_URL, 503)
    fetch_handler({"username": USERNAME}, None)
    records = drain(pipeline)
    poison = sqs_record("poison", "this is not json")

    first = process_handler({"Records": [poison, *records]}, None)

    gif_record = next(r for r in records if GIF_TWEET in r["body"])
    assert first == {
        "batchItemFailures": [
            {"itemIdentifier": "poison"},
            {"itemIdentifier": gif_record["messageId"]},
        ]
    }
    assert pipeline.tweets_table.scan()["Count"] == 6  # all but the GIF tweet
    assert GIF_KEY not in s3_keys(pipeline)

    cdn.serve(GIF_URL, b"gif-as-mp4", "video/mp4")
    downloads = len(cdn.requests)

    retry = process_handler({"Records": [gif_record]}, None)

    assert retry == {"batchItemFailures": []}
    assert {GIF_KEY, GIF_PREVIEW_KEY} <= set(s3_keys(pipeline))
    assert len(cdn.requests) == downloads + 2  # only the GIF and its poster again
    assert item(pipeline, GIF_TWEET)["media"][0]["status"] == "stored"


def test_media_gone_for_good_is_recorded_not_retried(
    pipeline: Aws, cdn: CdnStub
) -> None:
    cdn.responses[PHOTO_URL] = lambda: httpx.Response(404)
    fetch_handler({"username": USERNAME}, None)

    result = process_handler({"Records": drain(pipeline)}, None)

    assert result == {"batchItemFailures": []}
    assert PHOTO_KEY not in s3_keys(pipeline)
    assert item(pipeline, PHOTO_TWEET)["media"][0]["status"] == "unavailable"


def test_redriving_an_older_message_after_a_newer_one_leaves_the_item_unchanged(
    pipeline: Aws,
) -> None:
    """A DLQ redrive days later must not replace newer metrics (review repro 3)."""

    def message(likes: int, fetched_at: str) -> dict[str, Any]:
        return sqs_record(
            f"m{likes}",
            make_message(
                "7",
                tweet={"public_metrics": {"like_count": likes}},
                fetched_at=fetched_at,
            ).to_json(),
        )

    newer = message(5000, "2026-10-25T12:00:00Z")
    older = message(10, "2026-10-20T12:00:00Z")

    assert process_handler({"Records": [newer]}, None) == {"batchItemFailures": []}
    assert process_handler({"Records": [older]}, None) == {"batchItemFailures": []}

    stored = item(pipeline, "7")
    assert stored["public_metrics"] == {"like_count": 5000}
    assert stored["fetched_at"] == "2026-10-25T12:00:00Z"


def test_long_post_is_stored_in_full(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_x_api(monkeypatch, fetch, long_post_timeline())
    post = long_post_api_object()

    fetched = fetch_handler({"username": USERNAME}, None)
    processed = process_handler({"Records": drain(pipeline_env)}, None)

    assert fetched["enqueued"] == 1
    assert processed == {"batchItemFailures": []}
    stored = item(pipeline_env, LONG_POST_ID)
    assert stored["text"] == post["note_tweet"]["text"]
    assert len(stored["text"]) > 280
    assert stored["display_text"] == post["text"]
    assert stored["is_long_post"] is True


def test_a_worst_case_long_post_is_stored_with_its_full_text_in_s3(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 12,500 x 35-byte emoji sequences: 25,000 weighted characters, a 437,500-byte text.
    text = KISS * 12_500
    stub = long_post_timeline()

    def handler(request: httpx.Request) -> httpx.Response:
        response = stub(request)
        if not request.url.path.endswith("/tweets"):
            return response
        body = response.json()
        body["data"][0]["note_tweet"]["text"] = text
        return httpx.Response(200, json=body)

    install_x_api(monkeypatch, fetch, handler)
    pipeline_env.sqs.set_queue_attributes(
        QueueUrl=pipeline_env.queue_url,
        Attributes={"MaximumMessageSize": "1048576"},
    )

    fetched = fetch_handler({"username": USERNAME}, None)
    processed = process_handler({"Records": drain(pipeline_env)}, None)

    assert fetched["enqueued"] == 1
    assert processed == {"batchItemFailures": []}
    stored = item(pipeline_env, LONG_POST_ID)
    assert stored["text_truncated"] is True
    key = f"text/{USER_ID}/{LONG_POST_ID}.json"
    assert stored["text_s3_key"] == key
    assert len(stored["text"]) < len(text)
    obj = pipeline_env.s3.get_object(Bucket=pipeline_env.bucket, Key=key)
    assert json.loads(obj["Body"].read()) == text
