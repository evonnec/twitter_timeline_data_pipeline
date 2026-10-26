import json
import logging
from typing import Any

import pytest
from botocore.exceptions import ClientError
from conftest import Aws
from helpers import (
    FETCHED_AT,
    PHOTO_URL,
    USER_ID,
    CdnStub,
    ddb_get,
    make_message,
    photo,
    sqs_record,
)

from twitter_timeline_data_pipeline.config import ConfigError
from twitter_timeline_data_pipeline.handlers import media_retry
from twitter_timeline_data_pipeline.handlers.media_retry import (
    handler,
    retry_records,
)
from twitter_timeline_data_pipeline.handlers.process import SAFETY_MARGIN_MS
from twitter_timeline_data_pipeline.models import Media, MediaRetry
from twitter_timeline_data_pipeline.storage import (
    MediaStore,
    StoredMedia,
    TweetStore,
)

TWEET = "1700000000000000010"
BROKEN_URL = "https://pbs.twimg.com/media/FakeBroken01.jpg"
OTHER_URL = "https://pbs.twimg.com/media/FakeOther01.jpg"


@pytest.fixture
def cdn() -> CdnStub:
    stub = CdnStub()
    stub.serve(PHOTO_URL, b"jpeg-good", "image/jpeg")
    stub.serve(OTHER_URL, b"jpeg-other", "image/jpeg")
    return stub


def retrying(media: Media, **extra: Any) -> StoredMedia:
    return StoredMedia(
        media_key=media.media_key,
        type=media.type,
        status="retrying",
        source_url=f"{media.url}?name=orig",
        s3_key=f"media/{USER_ID}/{TWEET}/{media.media_key}.jpg",
        **extra,
    )


def put_item(aws: Aws, *entries: tuple[Media, StoredMedia]) -> None:
    message = make_message(
        TWEET,
        media=tuple(m for m, _ in entries),
        tweet={"media_keys": [m.media_key for m, _ in entries]},
    )
    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(
        message, [s for _, s in entries]
    )


def retry_record(media: Media, message_id: str = "r1") -> dict[str, Any]:
    retry = MediaRetry(
        tweet_id=TWEET,
        username="example_user",
        author_id=USER_ID,
        media=media,
        fetched_at=FETCHED_AT,
    )
    return sqs_record(message_id, retry.to_json())


def run(
    aws: Aws, cdn: CdnStub, records: list[dict[str, Any]], **kwargs: Any
) -> list[str]:
    return retry_records(
        records,
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        **kwargs,
    )


def media_of(aws: Aws) -> list[dict[str, Any]]:
    media: list[dict[str, Any]] = ddb_get(aws.tweets_table, tweet_id=TWEET)["media"]
    return media


def test_success_updates_only_its_entry(aws: Aws, cdn: CdnStub) -> None:
    a, b, c = photo("3_a", OTHER_URL), photo("3_b", PHOTO_URL), photo("3_c", BROKEN_URL)
    put_item(
        aws,
        (a, StoredMedia(media_key="3_a", type="photo", status="stored", s3_key="k-a")),
        (b, retrying(b)),
        (c, retrying(c)),
    )

    failed = run(aws, cdn, [retry_record(b)])

    assert failed == []
    ea, eb, ec = media_of(aws)
    assert (ea["status"], ea["s3_key"]) == ("stored", "k-a")  # untouched
    assert eb["status"] == "stored"
    assert eb["s3_key"] == f"media/{USER_ID}/{TWEET}/3_b.jpg"
    assert eb["source_url"] == f"{PHOTO_URL}?name=orig"
    assert eb["size_bytes"] == len(b"jpeg-good")
    assert ec == {
        "media_key": "3_c",
        "type": "photo",
        "status": "retrying",
        "s3_key": f"media/{USER_ID}/{TWEET}/3_c.jpg",
        "source_url": f"{BROKEN_URL}?name=orig",
    }
    obj = aws.s3.get_object(Bucket=aws.bucket, Key=eb["s3_key"])
    assert obj["Body"].read() == b"jpeg-good"


def test_an_already_stored_file_is_recorded_as_exists(aws: Aws, cdn: CdnStub) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(aws, (b, retrying(b)))
    key = f"media/{USER_ID}/{TWEET}/3_b.jpg"
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b"x", ContentType="image/jpeg")

    assert run(aws, cdn, [retry_record(b)]) == []

    assert media_of(aws)[0]["status"] == "exists"
    assert cdn.requests == []


def test_a_newer_delivery_that_already_stored_it_is_done_without_a_download(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(
        aws,
        (b, StoredMedia(media_key="3_b", type="photo", status="stored", s3_key="k")),
    )

    with caplog.at_level(logging.INFO):
        failed = run(aws, cdn, [retry_record(b)])

    assert failed == []
    assert cdn.requests == []
    assert media_of(aws)[0]["s3_key"] == "k"
    assert any("already stored" in m for m in caplog.messages)


def test_the_condition_fails_when_the_entry_changes_during_the_download(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(aws, (b, retrying(b)))
    winner = {"media_key": "3_b", "type": "photo", "status": "stored", "s3_key": "w"}

    class RacingStore(TweetStore):
        def get_media(self, tweet_id: str) -> list[dict[str, Any]] | None:
            entries = super().get_media(tweet_id)
            # Another delivery stores the file between our read and our update.
            self._table.update_item(
                Key={"tweet_id": tweet_id},
                UpdateExpression="SET media[0] = :e",
                ExpressionAttributeValues={":e": winner},
            )
            return entries

    with caplog.at_level(logging.INFO):
        failed = retry_records(
            [retry_record(b)],
            media_store=MediaStore(aws.s3, aws.bucket, cdn.client()),
            tweet_store=RacingStore(aws.tweets_table, aws.s3, aws.bucket),
        )

    assert failed == []
    assert media_of(aws) == [winner]  # not overwritten
    assert any("settled by another delivery" in m for m in caplog.messages)


def test_a_permanent_404_is_recorded_as_unavailable_with_its_url(
    aws: Aws, cdn: CdnStub
) -> None:
    c = photo("3_c", BROKEN_URL)
    cdn.fail(BROKEN_URL, 404)
    put_item(aws, (c, retrying(c)))

    assert run(aws, cdn, [retry_record(c)]) == []

    entry = media_of(aws)[0]
    assert entry["status"] == "unavailable"
    assert entry["source_url"] == f"{BROKEN_URL}?name=orig"


def test_a_file_over_the_cap_is_recorded_as_too_large(aws: Aws, cdn: CdnStub) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(aws, (b, retrying(b)))

    failed = retry_records(
        [retry_record(b)],
        media_store=MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=3),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []
    assert media_of(aws)[0]["status"] == "too_large"


def test_a_skipped_result_keeps_the_recorded_source_url(aws: Aws, cdn: CdnStub) -> None:
    """The message's media has nothing to download, so the store says ``skipped`` with
    no URL; the entry keeps the one the process function recorded."""
    recorded = photo("3_b", PHOTO_URL)
    put_item(aws, (recorded, retrying(recorded)))
    nothing = Media.from_dict({"media_key": "3_b", "type": "video", "variants": []})

    assert run(aws, cdn, [retry_record(nothing)]) == []

    entry = media_of(aws)[0]
    assert entry["status"] == "skipped"
    assert entry["source_url"] == f"{PHOTO_URL}?name=orig"


def test_a_transient_error_reports_the_record_and_changes_nothing(
    aws: Aws, cdn: CdnStub
) -> None:
    c = photo("3_c", BROKEN_URL)
    cdn.fail(BROKEN_URL, 503)
    put_item(aws, (c, retrying(c)))
    before = media_of(aws)

    assert run(aws, cdn, [retry_record(c)]) == ["r1"]

    assert media_of(aws) == before


def test_a_malformed_message_is_reported(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    tweet_message = make_message(TWEET).to_json()  # the wrong kind of message

    failed = run(
        aws,
        cdn,
        [
            sqs_record("m1", "{nope"),
            sqs_record("m2", tweet_message),
            sqs_record("m3", json.dumps({"kind": "media_retry"})),
        ],
    )

    assert failed == ["m1", "m2", "m3"]
    assert any("retry message m1 is malformed" in m for m in caplog.messages)


def test_a_missing_item_is_reported_to_retry_later(aws: Aws, cdn: CdnStub) -> None:
    assert run(aws, cdn, [retry_record(photo("3_b", PHOTO_URL))]) == ["r1"]


def test_an_entry_that_is_no_longer_listed_is_done(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    other = photo("3_other", OTHER_URL)
    put_item(aws, (other, retrying(other)))

    with caplog.at_level(logging.WARNING):
        failed = run(aws, cdn, [retry_record(photo("3_gone", PHOTO_URL))])

    assert failed == []
    assert cdn.requests == []
    assert media_of(aws)[0]["status"] == "retrying"
    assert any("no longer lists media 3_gone" in m for m in caplog.messages)


def test_an_unexpected_error_is_reported(
    aws: Aws, cdn: CdnStub, caplog: pytest.LogCaptureFixture
) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(aws, (b, retrying(b)))

    def deny(**_kw: Any) -> None:
        raise ClientError({"Error": {"Code": "500", "Message": "boom"}}, "HeadObject")

    aws.s3.meta.events.register("before-call.s3.HeadObject", deny)

    assert run(aws, cdn, [retry_record(b)]) == ["r1"]
    assert any("retry message r1 failed" in m for m in caplog.messages)


def test_no_time_left_reports_the_record_without_touching_anything(
    aws: Aws, cdn: CdnStub
) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(aws, (b, retrying(b)))

    failed = run(aws, cdn, [retry_record(b)], remaining_ms=lambda: SAFETY_MARGIN_MS)

    assert failed == ["r1"]
    assert cdn.requests == []
    assert media_of(aws)[0]["status"] == "retrying"


def test_the_download_gets_the_time_left_minus_the_margin(
    aws: Aws, cdn: CdnStub
) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(aws, (b, retrying(b)))
    budgets: list[float | None] = []

    class Spy(MediaStore):
        def store(
            self, message: Any, media: Any, *, budget_s: float | None = None
        ) -> Any:
            budgets.append(budget_s)
            return super().store(message, media, budget_s=budget_s)

    failed = retry_records(
        [retry_record(b)],
        media_store=Spy(aws.s3, aws.bucket, cdn.client()),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
        remaining_ms=lambda: SAFETY_MARGIN_MS + 60_000,
    )

    assert failed == [] and budgets == [60.0]


# ---- handler ---------------------------------------------------------------------


@pytest.fixture
def retry_env(pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch, cdn: CdnStub) -> Aws:
    monkeypatch.setattr(media_retry, "_http_client", lambda _timeout: cdn.client())
    return pipeline_env


class Context:
    def __init__(self, ms: int) -> None:
        self.ms = ms

    def get_remaining_time_in_millis(self) -> int:
        return self.ms


def test_handler_stores_the_media_and_returns_no_failures(
    retry_env: Aws, cdn: CdnStub
) -> None:
    b = photo("3_b", PHOTO_URL)
    put_item(retry_env, (b, retrying(b)))
    event = {"Records": [retry_record(b)]}

    result = handler(event, Context(SAFETY_MARGIN_MS + 100_000))

    assert result == {"batchItemFailures": []}
    assert media_of(retry_env)[0]["status"] == "stored"


def test_handler_returns_the_failed_message_ids(retry_env: Aws) -> None:
    result = handler({"Records": [sqs_record("bad", "{nope")]}, None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}


def test_handler_applies_max_media_bytes(
    retry_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAX_MEDIA_BYTES", "3")
    b = photo("3_b", PHOTO_URL)
    put_item(retry_env, (b, retrying(b)))

    assert handler({"Records": [retry_record(b)]}, None) == {"batchItemFailures": []}

    assert media_of(retry_env)[0]["status"] == "too_large"


def test_handler_missing_settings_lists_every_missing_variable() -> None:
    with pytest.raises(ConfigError) as info:
        handler({"Records": []}, None)

    assert str(info.value) == (
        "missing required environment variables: TWEETS_TABLE, MEDIA_BUCKET"
    )
