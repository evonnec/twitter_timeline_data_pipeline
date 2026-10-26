import io
import json
from typing import Any, cast
from urllib.parse import urlsplit

import boto3
import httpx
import pytest
from botocore.exceptions import ClientError
from conftest import STATE_TABLE, Aws
from helpers import (
    GIF_URL,
    KISS,
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
    ITEM_TEXT_LIMIT_BYTES,
    TEXT_PREVIEW_CHARS,
    CheckpointStore,
    MediaStore,
    PutResult,
    StoredMedia,
    TransientMediaError,
    TweetStore,
    _extension,
    _IterReader,
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


# ---- text offload ------------------------------------------------------------

HUGE_TEXT = KISS * 12_500  # 437,500 bytes: over DynamoDB's 400 KB item limit by itself


def huge_message() -> TweetMessage:
    return make_message(
        tweet={"note_text": HUGE_TEXT, "text": "preview"},
        media=tuple(
            Media(
                media_key=f"3_{n}",
                type="photo",
                url=f"https://pbs.twimg.com/media/{n}.jpg",
                alt_text="\u6f22" * 1_000,
            )
            for n in range(4)
        ),
    )


class CountingS3:
    def __init__(self, s3: S3Client) -> None:
        self.s3 = s3
        self.puts = 0

    def put_object(self, **kwargs: Any) -> Any:
        self.puts += 1
        return self.s3.put_object(**kwargs)


def test_a_text_over_the_limit_goes_to_s3_and_the_item_keeps_a_prefix(
    aws: Aws,
) -> None:
    message = huge_message()

    result = TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    assert result.written
    key = f"text/{USER_ID}/{TWEET_ID}.txt"
    obj = aws.s3.get_object(Bucket=aws.bucket, Key=key)
    assert obj["Body"].read().decode("utf-8") == HUGE_TEXT
    assert obj["ContentType"] == "text/plain; charset=utf-8"
    stored = stored_item(aws)
    assert stored["text"] == HUGE_TEXT[:TEXT_PREVIEW_CHARS]
    assert stored["text_truncated"] is True
    assert stored["text_s3_key"] == key
    assert stored["is_long_post"] is True
    assert stored["display_text"] == "preview"
    assert len(json.dumps(stored, default=str).encode()) < 400_000


def test_an_offloaded_item_is_under_the_item_limit_by_the_size_estimate() -> None:
    s3 = CountingS3(cast(Any, None))
    s3.put_object = lambda **_: {}  # type: ignore[method-assign]
    store = TweetStore(cast(Any, None), cast(Any, s3), "b")
    message = huge_message()

    item = store.build_item(message, [])
    item["media"] = [{"alt_text": m.alt_text} for m in message.media]

    assert estimate(item) < 400_000


def estimate(item: Any) -> int:
    match item:
        case dict():
            return 3 + sum(len(k.encode()) + estimate(v) for k, v in item.items())
        case list():
            return 3 + sum(estimate(v) for v in item)
        case str():
            return len(item.encode())
        case _:
            return 8


def test_a_long_post_under_the_limit_makes_no_s3_call(aws: Aws) -> None:
    s3 = CountingS3(aws.s3)
    message = make_message(tweet={"note_text": "x" * ITEM_TEXT_LIMIT_BYTES})

    TweetStore(aws.tweets_table, cast(Any, s3), aws.bucket).put_tweet(message, [])

    assert s3.puts == 0
    stored = stored_item(aws)
    assert stored["text"] == "x" * ITEM_TEXT_LIMIT_BYTES
    assert "text_truncated" not in stored
    assert "text_s3_key" not in stored


def test_the_limit_counts_bytes_not_characters(aws: Aws) -> None:
    s3 = CountingS3(aws.s3)
    # 100,001 CJK characters: under the limit as characters, 300,003 bytes.
    message = make_message(tweet={"note_text": "\u6f22" * 100_001})

    TweetStore(aws.tweets_table, cast(Any, s3), aws.bucket).put_tweet(message, [])

    assert s3.puts == 1


def test_the_preview_is_cut_on_a_code_point_boundary(aws: Aws) -> None:
    # Emoji are 4 bytes and one code point; a cut by bytes would split one.
    message = make_message(tweet={"note_text": "\U0001f600" * 80_000})

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    preview = stored_item(aws)["text"]
    assert preview == "\U0001f600" * TEXT_PREVIEW_CHARS
    assert preview.encode("utf-8").decode("utf-8") == preview


def test_the_text_key_falls_back_to_the_username_without_an_author_id() -> None:
    message = make_message(tweet={"author_id": None})

    assert TweetStore.text_key(message) == f"text/example_user/{TWEET_ID}.txt"


def test_a_failed_text_upload_means_no_item_is_written(aws: Aws) -> None:
    class Failing:
        def put_object(self, **_: Any) -> None:
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")

    store = TweetStore(aws.tweets_table, cast(Any, Failing()), aws.bucket)

    with pytest.raises(ClientError):
        store.put_tweet(huge_message(), [])

    assert aws.tweets_table.scan()["Count"] == 0


def test_put_tweet_round_trips_through_dynamodb(aws: Aws) -> None:
    stored = StoredMedia(
        media_key="3_1700000000000000101",
        type="photo",
        status="stored",
        s3_key=PHOTO_KEY,
        size_bytes=7,
    )

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(full_message(), [stored])

    item = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    assert item["username"] == "example_user"
    assert item["public_metrics"] == {"like_count": 3}
    assert item["media"][0]["size_bytes"] == 7
    assert item["media"][0]["s3_key"] == PHOTO_KEY
    assert item["media_count"] == 1
    assert "lang" in item


def test_put_tweet_is_queryable_via_username_index(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
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
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    assert store.put_tweet(likes(5000, NEWER), []).written is True
    assert store.put_tweet(likes(10, OLDER), []).written is False

    item = stored_item(aws)
    assert item["public_metrics"] == {"like_count": 5000}
    assert item["fetched_at"] == NEWER


def test_put_tweet_older_then_newer_replaces_the_item(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    assert store.put_tweet(likes(10, OLDER), []).written is True
    assert store.put_tweet(likes(5000, NEWER), []).written is True

    item = stored_item(aws)
    assert item["public_metrics"] == {"like_count": 5000}
    assert item["fetched_at"] == NEWER


def test_put_tweet_with_equal_fetched_at_rewrites_idempotently(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    assert store.put_tweet(likes(7, NEWER), []).written is True
    assert store.put_tweet(likes(7, NEWER), []).written is True
    assert (
        store.put_tweet(likes(8, NEWER), []).written is True
    )  # the tie rule: last one wins

    assert stored_item(aws)["public_metrics"] == {"like_count": 8}


def test_put_tweet_older_schema_version_cannot_overwrite_a_newer_one(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    assert store.put_tweet(likes(1, OLDER, schema_version=2), []).written is True
    # fetched later, but in the old format
    assert store.put_tweet(likes(2, NEWER, schema_version=1), []).written is False

    item = stored_item(aws)
    assert item["schema_version"] == 2
    assert item["public_metrics"] == {"like_count": 1}


def test_put_tweet_newer_schema_version_can_overwrite_an_older_one(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    assert store.put_tweet(likes(1, OLDER, schema_version=1), []).written is True
    assert store.put_tweet(likes(2, NEWER, schema_version=2), []).written is True

    assert stored_item(aws)["schema_version"] == 2


def test_put_tweet_overwrites_a_legacy_item_without_schema_version(aws: Aws) -> None:
    aws.tweets_table.put_item(
        Item={"tweet_id": TWEET_ID, "username": "u", "fetched_at": OLDER}
    )

    assert (
        TweetStore(aws.tweets_table, aws.s3, aws.bucket)
        .put_tweet(likes(3, NEWER), [])
        .written
    )

    item = stored_item(aws)
    assert item["schema_version"] == 1
    assert item["public_metrics"] == {"like_count": 3}


def test_put_tweet_overwrites_an_item_without_fetched_at(aws: Aws) -> None:
    aws.tweets_table.put_item(Item={"tweet_id": TWEET_ID, "username": "u"})

    assert (
        TweetStore(aws.tweets_table, aws.s3, aws.bucket)
        .put_tweet(likes(3, NEWER), [])
        .written
    )

    assert stored_item(aws)["fetched_at"] == NEWER


def test_put_tweet_stale_result_reports_the_stored_values(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
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

    store = TweetStore(cast(Any, Failing()), aws.s3, aws.bucket)

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
        store = TweetStore(table, boto3.client("s3"), "unused")
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
