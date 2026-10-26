import io
from collections.abc import Iterator
from typing import Any, cast
from urllib.parse import urlsplit

import boto3
import httpx
import pytest
from botocore.exceptions import ClientError
from conftest import STATE_TABLE, Aws
from helpers import (
    GIF_URL,
    LONG_POST_ID,
    PHOTO_URL,
    USER_ID,
    VIDEO_HI_URL,
    CdnStub,
    as_items,
    ddb_get,
    long_post_api_object,
    make_message,
    photo,
)
from hypothesis import given, settings
from hypothesis import strategies as st
from moto import mock_aws
from mypy_boto3_s3 import S3Client

from twitter_timeline_data_pipeline.models import Media, Tweet, TweetMessage
from twitter_timeline_data_pipeline.storage import (
    ALLOWED_MEDIA_HOSTS,
    CheckpointStore,
    MediaStore,
    PutResult,
    StoredMedia,
    TransientMediaError,
    TweetStore,
    _extension,
    _IterReader,
    _MediaTooLarge,
    _original_size_url,
)

TWEET_ID = "1700000000000000005"
PHOTO = photo()
PHOTO_KEY = f"media/{USER_ID}/{TWEET_ID}/3_1700000000000000101.jpg"
JPEG = b"\xff\xd8\xff\xe0fake-jpeg"


def video(media_key: str = "7_1", url: str = VIDEO_HI_URL) -> Media:
    return Media.from_dict(
        {
            "media_key": media_key,
            "type": "video",
            "variants": [{"content_type": "video/mp4", "bit_rate": 1, "url": url}],
        }
    )


def media_store(aws: Aws, cdn: CdnStub) -> MediaStore:
    return MediaStore(aws.s3, aws.bucket, cdn.client())


# ---- pure helpers -----------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "https://pbs.twimg.com/media/Abc.jpg",
            "https://pbs.twimg.com/media/Abc.jpg?name=orig",
        ),
        (
            "https://pbs.twimg.com/media/Abc?format=jpg&name=small",
            "https://pbs.twimg.com/media/Abc?format=jpg&name=orig",
        ),
        (
            "https://pbs.twimg.com/media/Abc?name=large&format=png&x=",
            "https://pbs.twimg.com/media/Abc?format=png&x=&name=orig",
        ),
        ("https://pbs.twimg.com/ext_tw_video_thumb/1/a.jpg", None),
        ("https://video.twimg.com/media/x.mp4", None),
        ("https://example.com/media/x.jpg", None),
        ("https://pbs.twimg.com.evil.example/media/x.jpg", None),
    ],
)
def test_original_size_only_for_pbs_media_urls(url: str, expected: str | None) -> None:
    assert _original_size_url(url) == (expected or url)


@pytest.mark.parametrize(
    ("content_type", "url", "expected"),
    [
        ("image/jpeg", "https://x/a", ".jpg"),
        ("image/png", "https://x/a.jpg", ".png"),
        ("video/mp4", "https://x/a", ".mp4"),
        (None, "https://x/a.JPG", ".jpg"),
        (None, "https://x/a.webp?name=orig", ".webp"),
        (None, "https://x/a.exe", ""),
        (None, "https://x/a", ""),
        ("application/x-unknown-thing", "https://x/a.png", ".png"),
    ],
)
def test_extension(content_type: str | None, url: str, expected: str) -> None:
    assert _extension(content_type, url) == expected


def test_object_key_uses_author_id() -> None:
    message = make_message(media=(PHOTO,))

    assert MediaStore.object_key(message, PHOTO, ".jpg") == PHOTO_KEY


def test_object_key_falls_back_to_lowercased_username() -> None:
    message = make_message(username="Example_User", tweet={"author_id": None})

    key = MediaStore.object_key(message, PHOTO, ".png")

    assert key == f"media/example_user/{TWEET_ID}/3_1700000000000000101.png"


def test_object_key_allows_empty_extension() -> None:
    assert MediaStore.object_key(make_message(), PHOTO, "").endswith(
        "/3_1700000000000000101"
    )


def test_iter_reader_streams_chunks_and_counts_bytes() -> None:
    reader = _IterReader(iter([b"abc", b"", b"defgh", b"i"]))
    buffered = io.BufferedReader(reader, buffer_size=4)

    assert buffered.read() == b"abcdefghi"
    assert reader.size == 9
    assert reader.readable()


def test_iter_reader_empty_iterator() -> None:
    reader = _IterReader(iter([]))

    assert io.BufferedReader(reader).read() == b""
    assert reader.size == 0


# ---- MediaStore.store -------------------------------------------------------


def test_store_photo_uploads_with_metadata_and_original_size(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, JPEG, "image/jpeg")
    message = make_message(media=(PHOTO,))

    stored = media_store(aws, cdn).store(message, PHOTO)

    assert stored == StoredMedia(
        media_key="3_1700000000000000101",
        type="photo",
        status="stored",
        s3_key=PHOTO_KEY,
        content_type="image/jpeg",
        source_url=f"{PHOTO_URL}?name=orig",
        size_bytes=len(JPEG),
    )
    (request,) = cdn.requests
    assert request.url.params["name"] == "orig"
    obj = aws.s3.get_object(Bucket=aws.bucket, Key=PHOTO_KEY)
    assert obj["Body"].read() == JPEG
    assert obj["ContentType"] == "image/jpeg"
    assert obj["Metadata"] == {
        "tweet-id": TWEET_ID,
        "media-key": "3_1700000000000000101",
        "source-url": f"{PHOTO_URL}?name=orig",
    }


def test_store_content_type_from_response_header_strips_parameters(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, JPEG, "image/jpeg; charset=binary")

    stored = media_store(aws, cdn).store(make_message(media=(PHOTO,)), PHOTO)

    assert stored.content_type == "image/jpeg"
    assert (
        aws.s3.head_object(Bucket=aws.bucket, Key=PHOTO_KEY)["ContentType"]
        == "image/jpeg"
    )


def test_store_without_any_content_type_uses_octet_stream(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.responses[PHOTO_URL] = lambda: httpx.Response(200, content=JPEG)

    stored = media_store(aws, cdn).store(make_message(media=(PHOTO,)), PHOTO)

    assert stored.content_type == "application/octet-stream"


def test_store_does_not_add_name_orig_to_other_hosts(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(VIDEO_HI_URL, b"mp4-bytes")
    item = video()

    stored = media_store(aws, cdn).store(make_message(media=(item,)), item)

    assert stored.source_url == VIDEO_HI_URL
    assert "name" not in cdn.requests[0].url.params


def test_store_video_uses_api_content_type_and_mp4_extension(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(VIDEO_HI_URL, b"mp4-bytes", "application/octet-stream")
    item = video()

    stored = media_store(aws, cdn).store(make_message(media=(item,)), item)

    assert stored.s3_key == f"media/{USER_ID}/{TWEET_ID}/7_1.mp4"
    assert stored.content_type == "video/mp4"
    head = aws.s3.head_object(Bucket=aws.bucket, Key=stored.s3_key or "")
    assert head["ContentType"] == "video/mp4"


def test_store_animated_gif_stores_its_mp4(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(GIF_URL, b"gif-as-mp4")
    gif = Media.from_dict(
        {
            "media_key": "16_1",
            "type": "animated_gif",
            "variants": [{"content_type": "video/mp4", "bit_rate": 0, "url": GIF_URL}],
        }
    )

    stored = media_store(aws, cdn).store(make_message(media=(gif,)), gif)

    assert stored.status == "stored"
    assert stored.s3_key == f"media/{USER_ID}/{TWEET_ID}/16_1.mp4"


def test_store_skips_media_with_nothing_downloadable(aws: Aws) -> None:
    cdn = CdnStub()
    hls_only = Media.from_dict(
        {
            "media_key": "7_2",
            "type": "video",
            "variants": [
                {"content_type": "application/x-mpegURL", "url": "https://v/p.m3u8"}
            ],
        }
    )

    stored = media_store(aws, cdn).store(make_message(media=(hls_only,)), hls_only)

    assert stored == StoredMedia(media_key="7_2", type="video", status="skipped")
    assert cdn.requests == []
    assert "Contents" not in aws.s3.list_objects_v2(Bucket=aws.bucket)


def test_store_existing_object_is_not_downloaded_again(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, JPEG, "image/jpeg")
    store = media_store(aws, cdn)
    message = make_message(media=(PHOTO,))
    store.store(message, PHOTO)
    cdn.requests.clear()

    again = store.store(message, PHOTO)

    assert again.status == "exists"
    assert again.s3_key == PHOTO_KEY
    assert again.content_type == "image/jpeg"
    assert again.size_bytes == len(JPEG)
    assert again.source_url == f"{PHOTO_URL}?name=orig"
    assert cdn.requests == []


@pytest.mark.parametrize("status", [400, 403, 404, 410])
def test_store_permanent_failures_are_unavailable(
    aws: Aws, status: int, caplog: pytest.LogCaptureFixture
) -> None:
    cdn = CdnStub()
    cdn.fail(PHOTO_URL, status)

    with caplog.at_level("WARNING"):
        stored = media_store(aws, cdn).store(make_message(media=(PHOTO,)), PHOTO)

    assert stored.status == "unavailable"
    assert stored.s3_key is None
    assert stored.size_bytes is None
    assert stored.source_url == f"{PHOTO_URL}?name=orig"
    assert f"HTTP {status}" in caplog.text
    assert "Contents" not in aws.s3.list_objects_v2(Bucket=aws.bucket)


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_store_transient_http_failures_raise(aws: Aws, status: int) -> None:
    cdn = CdnStub()
    cdn.fail(PHOTO_URL, status)

    with pytest.raises(TransientMediaError, match=f"HTTP {status}"):
        media_store(aws, cdn).store(make_message(media=(PHOTO,)), PHOTO)

    assert "Contents" not in aws.s3.list_objects_v2(Bucket=aws.bucket)


@pytest.mark.parametrize(
    "error",
    [httpx.ReadTimeout("slow"), httpx.ConnectError("refused")],
    ids=lambda e: type(e).__name__,
)
def test_store_transport_errors_are_transient(aws: Aws, error: Exception) -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise error

    store = MediaStore(
        aws.s3, aws.bucket, httpx.Client(transport=httpx.MockTransport(boom))
    )

    with pytest.raises(TransientMediaError, match=type(error).__name__):
        store.store(make_message(media=(PHOTO,)), PHOTO)


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_store_does_not_follow_redirects(aws: Aws, status: int) -> None:
    cdn = CdnStub()
    cdn.responses[PHOTO_URL] = lambda: httpx.Response(
        status, headers={"Location": "https://cdn.example/final.jpg"}
    )
    cdn.serve("https://cdn.example/final.jpg", JPEG, "image/jpeg")

    stored = media_store(aws, cdn).store(make_message(media=(PHOTO,)), PHOTO)

    assert stored.status == "unavailable"
    assert [r.url.host for r in cdn.requests] == ["pbs.twimg.com"]
    assert "Contents" not in aws.s3.list_objects_v2(Bucket=aws.bucket)


@pytest.mark.parametrize(
    "url",
    [
        "http://pbs.twimg.com/media/Abc.jpg",
        "https://example.com/media/Abc.jpg",
        "https://pbs.twimg.com.evil.example/media/Abc.jpg",
        "https://evil.example/pbs.twimg.com/media/Abc.jpg",
        "https://169.254.169.254/latest/meta-data/",
        "https://localhost/media/Abc.jpg",
        "ftp://pbs.twimg.com/media/Abc.jpg",
    ],
)
def test_store_skips_urls_outside_the_media_hosts(
    aws: Aws, url: str, caplog: pytest.LogCaptureFixture
) -> None:
    cdn = CdnStub()
    item = photo(url=url)

    with caplog.at_level("WARNING"):
        stored = media_store(aws, cdn).store(make_message(media=(item,)), item)

    assert stored == StoredMedia(
        media_key=item.media_key, type="photo", status="skipped"
    )
    assert cdn.requests == []
    assert "not an allowed media host" in caplog.text
    assert repr(urlsplit(url).hostname) in caplog.text
    assert "Contents" not in aws.s3.list_objects_v2(Bucket=aws.bucket)


def test_store_skips_a_malformed_url_instead_of_raising(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    cdn = CdnStub()
    item = photo(url="https://[pbs.twimg.com/media/Abc.jpg")

    with caplog.at_level("WARNING"):
        stored = media_store(aws, cdn).store(make_message(media=(item,)), item)

    assert stored.status == "skipped"
    assert cdn.requests == []
    assert "not an allowed media host" in caplog.text


@pytest.mark.parametrize("host", sorted(ALLOWED_MEDIA_HOSTS))
def test_store_allows_both_x_media_hosts(aws: Aws, host: str) -> None:
    url = f"https://{host}/ext/file.mp4"
    cdn = CdnStub()
    cdn.serve(url, b"data", "video/mp4")
    item = video(url=url)

    stored = media_store(aws, cdn).store(make_message(media=(item,)), item)

    assert stored.status == "stored"


def test_store_streams_a_multi_megabyte_body_as_multipart(aws: Aws) -> None:
    body = bytes(range(256)) * (40 * 1024)  # 10 MiB, above the 8 MiB threshold
    cdn = CdnStub()
    cdn.responses[VIDEO_HI_URL] = lambda: httpx.Response(
        200,
        content=iter([body[i : i + 65536] for i in range(0, len(body), 65536)]),
        headers={"Content-Type": "video/mp4"},
    )
    item = video()

    stored = media_store(aws, cdn).store(make_message(media=(item,)), item)

    assert stored.size_bytes == len(body)
    obj = aws.s3.get_object(Bucket=aws.bucket, Key=stored.s3_key or "")
    assert obj["ContentLength"] == len(body)
    assert obj["Body"].read() == body
    assert obj["ContentType"] == "video/mp4"
    assert obj["Metadata"]["media-key"] == "7_1"


def test_store_empty_body_stores_zero_bytes(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"", "image/jpeg")

    stored = media_store(aws, cdn).store(make_message(media=(PHOTO,)), PHOTO)

    assert stored.size_bytes == 0
    assert stored.status == "stored"


def test_store_reraises_unexpected_s3_head_errors(aws: Aws) -> None:
    class DenyingS3:
        def head_object(self, **_: Any) -> None:
            raise ClientError({"Error": {"Code": "403"}}, "HeadObject")

    store = MediaStore(cast("S3Client", DenyingS3()), aws.bucket, CdnStub().client())

    with pytest.raises(ClientError):
        store.store(make_message(media=(PHOTO,)), PHOTO)


# ---- TweetStore --------------------------------------------------------------


def full_message() -> TweetMessage:
    return make_message(
        media=(PHOTO,),
        tweet={
            "conversation_id": "1700000000000000001",
            "lang": "en",
            "referenced_tweets": [{"type": "quoted", "id": "9"}],
            "public_metrics": {"like_count": 3},
            "edit_history_tweet_ids": [TWEET_ID],
            "media_keys": ["3_1700000000000000101"],
        },
    )


def test_to_item_contains_every_field() -> None:
    stored = StoredMedia(
        media_key="3_1700000000000000101",
        type="photo",
        status="stored",
        s3_key=PHOTO_KEY,
        content_type="image/jpeg",
        source_url=PHOTO_URL,
        size_bytes=7,
    )

    item = TweetStore.to_item(full_message(), [stored])

    assert item.pop("stored_at").endswith("Z")
    assert item == {
        "tweet_id": TWEET_ID,
        "username": "example_user",
        "author_id": USER_ID,
        "text": f"tweet {TWEET_ID}",
        "is_long_post": False,
        "created_at": "2026-10-20T12:00:00.000Z",
        "conversation_id": "1700000000000000001",
        "lang": "en",
        "referenced_tweets": [{"type": "quoted", "id": "9"}],
        "public_metrics": {"like_count": 3},
        "edit_history_tweet_ids": [TWEET_ID],
        "media": [
            {
                "media_key": "3_1700000000000000101",
                "type": "photo",
                "status": "stored",
                "s3_key": PHOTO_KEY,
                "content_type": "image/jpeg",
                "source_url": PHOTO_URL,
                "size_bytes": 7,
            }
        ],
        "media_count": 1,
        "fetched_at": "2026-10-25T00:00:00Z",
        "schema_version": 1,
    }


def test_to_item_stores_the_full_text_of_a_long_post() -> None:
    post = long_post_api_object()
    message = TweetMessage(
        username="example_user", tweet=Tweet.from_api(post), fetched_at="x"
    )

    item = TweetStore.to_item(message, [])

    assert item["tweet_id"] == LONG_POST_ID
    assert item["text"] == post["note_tweet"]["text"]
    assert item["display_text"] == post["text"]
    assert item["is_long_post"] is True


def test_to_item_flags_a_short_post_as_not_long_and_has_no_display_text() -> None:
    item = TweetStore.to_item(make_message(), [])

    assert item["is_long_post"] is False
    assert "display_text" not in item


def test_to_item_drops_none_values_at_every_level() -> None:
    message = make_message(tweet={"author_id": None, "created_at": None, "lang": None})
    skipped = StoredMedia(media_key="7_2", type="video", status="skipped")

    item = TweetStore.to_item(message, [skipped])

    assert None not in item.values()
    for absent in ("author_id", "created_at", "lang", "conversation_id"):
        assert absent not in item
    assert item["media"] == [{"media_key": "7_2", "type": "video", "status": "skipped"}]


def test_put_tweet_round_trips_through_dynamodb(aws: Aws) -> None:
    stored = StoredMedia(
        media_key="3_1700000000000000101",
        type="photo",
        status="stored",
        s3_key=PHOTO_KEY,
        size_bytes=7,
    )

    TweetStore(aws.tweets_table).put_tweet(full_message(), [stored])

    item = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    assert item["username"] == "example_user"
    assert item["public_metrics"] == {"like_count": 3}
    assert item["media"][0]["size_bytes"] == 7
    assert item["media"][0]["s3_key"] == PHOTO_KEY
    assert item["media_count"] == 1
    assert "lang" in item


def test_put_tweet_is_queryable_via_username_index(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table)
    for n, created in enumerate(["2026-10-02", "2026-10-01", "2026-10-03"]):
        store.put_tweet(
            make_message(f"17000000000000000{n:02d}", tweet={"created_at": created}), []
        )

    response = aws.tweets_table.query(
        IndexName="username-created_at-index",
        KeyConditionExpression="username = :u",
        ExpressionAttributeValues={":u": "example_user"},
    )

    assert [i["created_at"] for i in as_items(response["Items"])] == [
        "2026-10-01",
        "2026-10-02",
        "2026-10-03",
    ]


OLDER = "2026-10-20T12:00:00Z"
NEWER = "2026-10-25T12:00:00Z"


def likes(count: int, fetched_at: str, **kwargs: Any) -> TweetMessage:
    return make_message(
        tweet={"public_metrics": {"like_count": count}},
        fetched_at=fetched_at,
        **kwargs,
    )


def stored_item(aws: Aws) -> dict[str, Any]:
    assert aws.tweets_table.scan()["Count"] == 1
    return ddb_get(aws.tweets_table, tweet_id=TWEET_ID)


def test_put_tweet_newer_then_older_skips_the_older(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table)

    assert store.put_tweet(likes(5000, NEWER), []).written is True
    assert store.put_tweet(likes(10, OLDER), []).written is False

    item = stored_item(aws)
    assert item["public_metrics"] == {"like_count": 5000}
    assert item["fetched_at"] == NEWER


def test_put_tweet_older_then_newer_replaces_the_item(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table)

    assert store.put_tweet(likes(10, OLDER), []).written is True
    assert store.put_tweet(likes(5000, NEWER), []).written is True

    item = stored_item(aws)
    assert item["public_metrics"] == {"like_count": 5000}
    assert item["fetched_at"] == NEWER


def test_put_tweet_with_equal_fetched_at_rewrites_idempotently(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table)

    assert store.put_tweet(likes(7, NEWER), []).written is True
    assert store.put_tweet(likes(7, NEWER), []).written is True
    assert (
        store.put_tweet(likes(8, NEWER), []).written is True
    )  # the tie rule: last one wins

    assert stored_item(aws)["public_metrics"] == {"like_count": 8}


def test_put_tweet_older_schema_version_cannot_overwrite_a_newer_one(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table)

    assert store.put_tweet(likes(1, OLDER, schema_version=2), []).written is True
    # fetched later, but in the old format
    assert store.put_tweet(likes(2, NEWER, schema_version=1), []).written is False

    item = stored_item(aws)
    assert item["schema_version"] == 2
    assert item["public_metrics"] == {"like_count": 1}


def test_put_tweet_newer_schema_version_can_overwrite_an_older_one(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table)

    assert store.put_tweet(likes(1, OLDER, schema_version=1), []).written is True
    assert store.put_tweet(likes(2, NEWER, schema_version=2), []).written is True

    assert stored_item(aws)["schema_version"] == 2


def test_put_tweet_overwrites_a_legacy_item_without_schema_version(aws: Aws) -> None:
    aws.tweets_table.put_item(
        Item={"tweet_id": TWEET_ID, "username": "u", "fetched_at": OLDER}
    )

    assert TweetStore(aws.tweets_table).put_tweet(likes(3, NEWER), []).written

    item = stored_item(aws)
    assert item["schema_version"] == 1
    assert item["public_metrics"] == {"like_count": 3}


def test_put_tweet_overwrites_an_item_without_fetched_at(aws: Aws) -> None:
    aws.tweets_table.put_item(Item={"tweet_id": TWEET_ID, "username": "u"})

    assert TweetStore(aws.tweets_table).put_tweet(likes(3, NEWER), []).written

    assert stored_item(aws)["fetched_at"] == NEWER


def test_put_tweet_stale_result_reports_the_stored_values(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table)
    store.put_tweet(likes(1, NEWER, schema_version=2), [])

    result = store.put_tweet(likes(2, OLDER), [])

    assert result == PutResult(
        written=False, stored_fetched_at=NEWER, stored_schema_version=2
    )


def test_put_tweet_reraises_other_dynamodb_errors(aws: Aws) -> None:
    class Failing:
        def put_item(self, **_: Any) -> None:
            raise ClientError(
                {"Error": {"Code": "ProvisionedThroughputExceededException"}},
                "PutItem",
            )

    store = TweetStore(cast(Any, Failing()))

    with pytest.raises(ClientError, match="ProvisionedThroughputExceeded"):
        store.put_tweet(likes(1, NEWER), [])


@settings(max_examples=25, deadline=None)
@given(
    fetched=st.lists(
        st.sampled_from([f"2026-10-{day:02d}T00:00:00Z" for day in range(1, 6)]),
        min_size=1,
        max_size=8,
    )
)
def test_any_delivery_order_ends_with_the_greatest_fetched_at(
    fetched: list[str],
) -> None:
    with mock_aws():
        table = boto3.resource("dynamodb", region_name="us-east-1").create_table(
            TableName="tweets-prop",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "tweet_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tweet_id", "AttributeType": "S"}],
        )
        store = TweetStore(table)
        # like_count is the delivery's own fetched_at day, so the winner is visible
        for stamp in fetched:
            day = int(stamp[8:10])
            store.put_tweet(likes(day, stamp), [])

        item = ddb_get(table, tweet_id=TWEET_ID)
        assert item["fetched_at"] == max(fetched)
        assert item["public_metrics"] == {"like_count": int(max(fetched)[8:10])}


# ---- CheckpointStore ---------------------------------------------------------


def test_checkpoint_get_missing_is_none(aws: Aws) -> None:
    assert CheckpointStore(aws.state_table).get("example_user") is None


def test_checkpoint_first_advance_writes_a_number(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)

    assert store.advance("example_user", TWEET_ID) is True

    item = ddb_get(aws.state_table, username="example_user")
    assert int(item["newest_id"]) == int(TWEET_ID)
    assert str(item["updated_at"]).endswith("Z")
    assert store.get("example_user") == TWEET_ID


@pytest.mark.parametrize(
    ("stored", "new", "expected_changed", "expected_value"),
    [
        ("100", "200", True, "200"),
        ("200", "200", False, "200"),
        ("200", "100", False, "200"),
        ("999", "1000", True, "1000"),  # numeric, not lexicographic
        ("1000", "999", False, "1000"),
        ("1700000000000000001", "1700000000000000002", True, "1700000000000000002"),
    ],
)
def test_checkpoint_advance_only_moves_forward(
    aws: Aws,
    stored: str,
    new: str,
    expected_changed: bool,
    expected_value: str,
) -> None:
    store = CheckpointStore(aws.state_table)
    store.advance("example_user", stored)

    assert store.advance("example_user", new) is expected_changed
    assert store.get("example_user") == expected_value


def test_checkpoint_preserves_snowflake_precision(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    store.advance("example_user", "1999999999999999999")

    assert store.get("example_user") == "1999999999999999999"


def test_checkpoint_username_is_case_insensitive(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)

    store.advance("Example_User", "500")

    assert store.get("EXAMPLE_USER") == "500"
    assert store.get("example_user") == "500"
    assert store.advance("EXAMPLE_user", "400") is False
    assert aws.state_table.scan()["Count"] == 1
    assert aws.state_table.get_item(Key={"username": "example_user"}).get("Item")


def test_checkpoints_are_per_user(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    store.advance("alice", "10")

    assert store.get("bob") is None
    assert store.advance("bob", "5") is True


def test_checkpoint_advance_reraises_other_dynamodb_errors(aws: Aws) -> None:
    store = CheckpointStore(aws.dynamodb.Table(f"{STATE_TABLE}-missing"))

    with pytest.raises(ClientError):
        store.advance("example_user", "1")


# ---- size cap and time budget ------------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def leftover_uploads(aws: Aws) -> list[Any]:
    uploads = aws.s3.list_multipart_uploads(Bucket=aws.bucket).get("Uploads", [])
    return list(uploads)


def keys_in_bucket(aws: Aws) -> list[str]:
    contents = aws.s3.list_objects_v2(Bucket=aws.bucket).get("Contents", [])
    return [o["Key"] for o in contents if "Key" in o]


def streaming_cdn(url: str, chunks: Iterator[bytes], **headers: str) -> CdnStub:
    cdn = CdnStub()
    cdn.responses[url] = lambda: httpx.Response(
        200, content=chunks, headers={"Content-Type": "video/mp4", **headers}
    )
    return cdn


def sized_video(bit_rate: int | None, duration_ms: int | None) -> Media:
    return Media.from_dict(
        {
            "media_key": "7_1",
            "type": "video",
            "duration_ms": duration_ms,
            "variants": [
                {
                    "content_type": "video/mp4",
                    "bit_rate": bit_rate,
                    "url": VIDEO_HI_URL,
                },
                {"content_type": "video/mp4", "bit_rate": 0, "url": GIF_URL},
            ],
        }
    )


def test_iter_reader_raises_once_the_next_chunk_passes_the_cap() -> None:
    reader = _IterReader(iter([b"abcd", b"efgh", b"ijkl"]), max_bytes=10)
    buffered = io.BufferedReader(reader, buffer_size=4)

    with pytest.raises(_MediaTooLarge) as info:
        buffered.read()

    assert info.value.size == 12


def test_iter_reader_allows_exactly_the_cap() -> None:
    reader = _IterReader(iter([b"abcd", b"efgh", b"ij"]), max_bytes=10)

    assert io.BufferedReader(reader, buffer_size=4).read() == b"abcdefghij"


def test_iter_reader_raises_transient_error_at_the_deadline() -> None:
    clock = FakeClock()

    def chunks() -> Iterator[bytes]:
        for _ in range(5):
            clock.now += 3
            yield b"x" * 10

    reader = _IterReader(chunks(), deadline=7, clock=clock)

    with pytest.raises(TransientMediaError, match="time budget"):
        io.BufferedReader(reader, buffer_size=10).read()

    assert reader.size == 30  # the chunk that crossed the deadline was handed out


def test_store_aborts_a_small_stream_past_the_cap(aws: Aws) -> None:
    chunks = iter([b"x" * 512] * 100)
    cdn = streaming_cdn(VIDEO_HI_URL, chunks)
    item = video()
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=1000)

    stored = store.store(make_message(media=(item,)), item)

    assert stored.status == "too_large"
    assert stored.size_bytes == 1024  # the second chunk passed the cap
    assert stored.s3_key is None
    assert keys_in_bucket(aws) == []
    assert leftover_uploads(aws) == []
    assert len(list(chunks)) == 98  # the rest of the body was never read


def test_store_aborts_a_multipart_upload_past_the_cap(aws: Aws) -> None:
    chunk = b"\x00" * 65536
    cdn = streaming_cdn(VIDEO_HI_URL, iter([chunk] * 192))  # 12 MiB, past 9 MiB
    item = video()
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=9 * 1024 * 1024)

    stored = store.store(make_message(media=(item,)), item)

    assert stored.status == "too_large"
    assert stored.size_bytes == 9 * 1024 * 1024 + 65536
    assert keys_in_bucket(aws) == []
    assert leftover_uploads(aws) == []


def test_store_skips_on_the_bit_rate_estimate_without_a_request(aws: Aws) -> None:
    cdn = CdnStub()
    item = sized_video(bit_rate=8_000_000, duration_ms=10_000)  # 10 MB
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=1_000_000)

    stored = store.store(make_message(media=(item,)), item)

    assert stored.status == "too_large"
    assert stored.size_bytes == 10_000_000
    assert stored.source_url == VIDEO_HI_URL
    assert cdn.requests == []
    assert keys_in_bucket(aws) == []


@pytest.mark.parametrize(
    ("bit_rate", "duration_ms"),
    [(None, 10_000), (8_000_000, None), (0, 10_000), (800_000, 10_000)],
)
def test_store_downloads_when_there_is_no_usable_estimate_or_it_fits(
    aws: Aws, bit_rate: int | None, duration_ms: int | None
) -> None:
    cdn = CdnStub()
    cdn.serve(VIDEO_HI_URL, b"mp4", "video/mp4")
    item = sized_video(bit_rate, duration_ms)
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=1_000_000)

    assert store.store(make_message(media=(item,)), item).status == "stored"


def test_store_estimate_ignores_other_variants_and_photos(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"jpg", "image/jpeg")
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=10)
    photo_item = Media.from_dict(
        {"media_key": "3_1", "type": "photo", "url": PHOTO_URL, "duration_ms": 10**9}
    )

    stored = store.store(make_message(media=(photo_item,)), photo_item)

    assert stored.status == "stored"


def test_store_skips_on_content_length_without_reading_the_body(aws: Aws) -> None:
    def never_read() -> Iterator[bytes]:
        raise AssertionError("the body must not be downloaded")
        yield b""

    cdn = streaming_cdn(VIDEO_HI_URL, never_read(), **{"Content-Length": "2000"})
    item = video()
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=1000)

    stored = store.store(make_message(media=(item,)), item)

    assert stored.status == "too_large"
    assert stored.size_bytes == 2000
    assert keys_in_bucket(aws) == []
    assert leftover_uploads(aws) == []


def test_store_accepts_a_body_exactly_at_the_cap(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(PHOTO_URL, b"x" * 100, "image/jpeg")
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=100)

    stored = store.store(make_message(media=(PHOTO,)), PHOTO)

    assert (stored.status, stored.size_bytes) == ("stored", 100)


def test_store_slow_download_hits_its_deadline_and_aborts_the_multipart_upload(
    aws: Aws,
) -> None:
    clock = FakeClock()
    calls: list[str] = []
    for op in ("CreateMultipartUpload", "AbortMultipartUpload"):
        aws.s3.meta.events.register(
            f"before-call.s3.{op}", lambda *, op=op, **_kw: calls.append(op)
        )

    def slow() -> Iterator[bytes]:
        for _ in range(200):
            clock.now += 1
            yield b"\x00" * 1024 * 1024

    cdn = streaming_cdn(VIDEO_HI_URL, slow())
    item = video()
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), clock=clock)

    with pytest.raises(TransientMediaError, match="time budget"):
        store.store(make_message(media=(item,)), item, budget_s=12)

    assert calls.count("CreateMultipartUpload") == 1  # past the 8 MiB threshold
    assert calls.count("AbortMultipartUpload") == 1
    assert keys_in_bucket(aws) == []
    assert leftover_uploads(aws) == []
    assert clock.now == 12  # stopped as soon as the deadline was reached


def test_store_gives_the_http_phases_a_share_of_the_time_left(aws: Aws) -> None:
    clock = FakeClock()
    seen: list[dict[str, Any]] = []

    def stall(request: httpx.Request) -> httpx.Response:
        seen.append(request.extensions["timeout"])
        clock.now += 100  # a header phase that hangs until its timeout
        raise httpx.ReadTimeout("stalled", request=request)

    http = httpx.Client(transport=httpx.MockTransport(stall), timeout=10.0)
    store = MediaStore(aws.s3, aws.bucket, http, clock=clock)

    with pytest.raises(TransientMediaError, match="ReadTimeout"):
        store.store(make_message(media=(PHOTO,)), PHOTO, budget_s=24)

    assert seen == [{"connect": 8.0, "read": 8.0, "write": 8.0, "pool": 8.0}]


def test_store_keeps_the_client_timeout_when_there_is_plenty_of_time(
    aws: Aws,
) -> None:
    seen: list[dict[str, Any]] = []

    def serve(request: httpx.Request) -> httpx.Response:
        seen.append(request.extensions["timeout"])
        return httpx.Response(200, content=b"x")

    http = httpx.Client(transport=httpx.MockTransport(serve), timeout=10.0)
    MediaStore(aws.s3, aws.bucket, http).store(
        make_message(media=(PHOTO,)), PHOTO, budget_s=300
    )
    MediaStore(aws.s3, aws.bucket, http).store(
        make_message("1700000000000000006", media=(PHOTO,)), PHOTO
    )

    assert seen[0]["read"] == 10.0
    assert seen[1]["read"] == 10.0


def test_store_without_a_read_timeout_uses_a_third_of_the_time_left(aws: Aws) -> None:
    seen: list[dict[str, Any]] = []

    def serve(request: httpx.Request) -> httpx.Response:
        seen.append(request.extensions["timeout"])
        return httpx.Response(200, content=b"x")

    http = httpx.Client(transport=httpx.MockTransport(serve), timeout=None)
    MediaStore(aws.s3, aws.bucket, http, clock=FakeClock()).store(
        make_message(media=(PHOTO,)), PHOTO, budget_s=30
    )

    assert seen[0]["read"] == 10.0


def test_store_with_no_time_left_makes_no_request(aws: Aws) -> None:
    cdn = CdnStub()

    with pytest.raises(TransientMediaError, match="no time left"):
        media_store(aws, cdn).store(make_message(media=(PHOTO,)), PHOTO, budget_s=0)

    assert cdn.requests == []
