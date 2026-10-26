import io
import json
import logging
from collections.abc import Callable, Iterator
from datetime import datetime
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

from twitter_timeline_data_pipeline.handlers.compliance import ComplianceStore
from twitter_timeline_data_pipeline.models import (
    MAX_STORED_INT,
    Media,
    MediaRetry,
    Tweet,
    TweetMessage,
)
from twitter_timeline_data_pipeline.storage import (
    ALLOWED_MEDIA_HOSTS,
    ITEM_LIMIT_BYTES,
    MAX_GAPS,
    TEXT_PREVIEW_CHARS,
    CheckpointState,
    CheckpointStore,
    Gap,
    MediaRetryQueue,
    MediaStore,
    PutResult,
    StoredMedia,
    TransientMediaError,
    TweetStore,
    WriteContentionError,
    _extension,
    _IterReader,
    _MediaTooLarge,
    _original_size_url,
    estimate_item_size,
    media_entry,
    merge_gaps,
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

    assert stored == StoredMedia(media_key="7_2", type="video", status="skipped_hls")
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
        "media_keys": ["3_1700000000000000101"],
        "media": [
            {
                "media_key": "3_1700000000000000101",
                "type": "photo",
                "status": "stored",
                "s3_key": PHOTO_KEY,
                "content_type": "image/jpeg",
                "source_url": PHOTO_URL,
                "size_bytes": 7,
                "url": PHOTO_URL,
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
OLD_STAMP = "2026-10-20T00:00:00Z"
NEW_STAMP = "2026-10-26T00:00:00Z"


class CountingS3:
    def __init__(self, s3: S3Client) -> None:
        self.s3 = s3
        self.puts = 0

    def put_object(self, **kwargs: Any) -> Any:
        self.puts += 1
        return self.s3.put_object(**kwargs)


def long_urls_media() -> list[StoredMedia]:
    """Four photos with 2,000-character source URLs: a worst realistic media list."""
    return [
        StoredMedia(
            media_key=f"3_{n}",
            type="photo",
            status="stored",
            s3_key=f"media/{USER_ID}/{TWEET_ID}/3_{n}.jpg",
            content_type="image/jpeg",
            source_url="https://pbs.twimg.com/media/" + "a" * 2_000,
            size_bytes=123_456,
        )
        for n in range(4)
    ]


def stored_text(aws: Aws, key: str) -> str:
    body = aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()
    text: str = json.loads(body.decode("utf-8"))  # strict UTF-8
    return text


def test_a_text_over_the_limit_goes_to_s3_and_the_item_keeps_a_prefix(
    aws: Aws,
) -> None:
    message = make_message(tweet={"note_text": HUGE_TEXT, "text": "preview"})

    result = TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(
        message, long_urls_media()
    )

    assert result.written
    key = f"text/{USER_ID}/{TWEET_ID}.json"
    assert aws.s3.head_object(Bucket=aws.bucket, Key=key)["ContentType"] == (
        "application/json"
    )
    assert stored_text(aws, key) == HUGE_TEXT
    stored = stored_item(aws)
    assert stored["text"] == HUGE_TEXT[:TEXT_PREVIEW_CHARS]
    assert stored["text_truncated"] is True
    assert stored["text_s3_key"] == key
    assert stored["is_long_post"] is True
    assert stored["display_text"] == "preview"
    assert "display_text_truncated" not in stored
    assert estimate_item_size(stored) < ITEM_LIMIT_BYTES


def test_a_long_post_under_the_limit_makes_no_s3_call(aws: Aws) -> None:
    s3 = CountingS3(aws.s3)
    message = make_message(tweet={"note_text": "x" * 300_000})

    TweetStore(aws.tweets_table, cast(Any, s3), aws.bucket).put_tweet(message, [])

    assert s3.puts == 0
    stored = stored_item(aws)
    assert stored["text"] == "x" * 300_000
    assert "text_truncated" not in stored
    assert "text_s3_key" not in stored


def test_the_offload_decision_is_made_at_the_whole_item_boundary() -> None:
    """With a full preview and a realistic media list the text allowance is smaller
    than the limit; one byte past the exact boundary is offloaded, the boundary not."""
    s3 = CountingS3(cast(Any, None))
    s3.s3 = cast(Any, type("Ok", (), {"put_object": lambda self, **_: {}})())
    store = TweetStore(cast(Any, None), cast(Any, s3), "b")
    media = long_urls_media()

    def build(n: int) -> dict[str, Any]:
        message = make_message(tweet={"note_text": "x" * n, "text": "p" * 280})
        return store.build_item(message, media)

    base = estimate_item_size(build(1)) - 1
    fits = ITEM_LIMIT_BYTES - base

    assert "text_truncated" not in build(fits)
    assert s3.puts == 0
    assert build(fits + 1)["text_truncated"] is True
    assert s3.puts == 1
    assert fits < ITEM_LIMIT_BYTES - 8_000  # the media and preview really cost


def test_a_huge_preview_is_offloaded_too_and_nothing_is_lost(aws: Aws) -> None:
    """The preview is normally under 300 characters, but nothing guarantees it. A short
    text with a 390 KB preview, and two 348 KB texts, both end under the limit."""
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    cases = {
        "1": ("n" * 3_000, "p" * 390_000),
        "2": ("t" * 348_000, "p" * 348_000),
    }
    for tweet_id, (note, preview) in cases.items():
        message = make_message(
            tweet_id, tweet={"note_text": note, "text": preview}, media=(PHOTO,)
        )
        assert store.put_tweet(message, long_urls_media()).written

        item = ddb_get(aws.tweets_table, tweet_id=tweet_id)
        assert estimate_item_size(item) < 400_000
        assert item["display_text_truncated"] is True
        assert stored_text(aws, item["display_text_s3_key"]) == preview
        assert item["display_text"] == preview[:TEXT_PREVIEW_CHARS]
        full = (
            stored_text(aws, item["text_s3_key"])
            if item.get("text_truncated")
            else item["text"]
        )
        assert full == note
        if tweet_id == "1":
            assert "text_truncated" not in item  # the big field went first
    assert ddb_get(aws.tweets_table, tweet_id="2")["text_truncated"] is True


def test_the_text_object_round_trips_any_characters_in_valid_utf8(aws: Aws) -> None:
    """A lone surrogate, U+FFFE and U+FFFF (which the message format escapes) and
    astral emoji read back exactly, and the stored bytes are strict UTF-8."""
    text = 'a\ud83d b\ufffe c\uffff \U0001f600"\\\n' + KISS * 12_500
    message = make_message(tweet={"note_text": text})

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    key = stored_item(aws)["text_s3_key"]
    raw = aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()
    raw.decode("utf-8")  # strict: no lone surrogates in the bytes
    assert json.loads(raw) == text


def test_a_short_text_with_a_lone_surrogate_is_kept_whole_in_s3(aws: Aws) -> None:
    """DynamoDB can't store a lone surrogate, so the text goes to S3 even when small."""
    message = make_message(tweet={"note_text": "a\ud83d b", "text": "c\udc00"})

    assert (
        TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, []).written
    )

    item = stored_item(aws)
    assert item["text"] == "a\ufffd b"
    assert item["display_text"] == "c\ufffd"
    assert stored_text(aws, item["text_s3_key"]) == "a\ud83d b"
    assert stored_text(aws, item["display_text_s3_key"]) == "c\udc00"


def test_an_offloaded_text_is_written_once_across_redeliveries(aws: Aws) -> None:
    aws.s3.put_bucket_versioning(
        Bucket=aws.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    message = make_message(tweet={"note_text": HUGE_TEXT})

    store.put_tweet(message, [])
    store.put_tweet(message, [])

    versions = aws.s3.list_object_versions(Bucket=aws.bucket)["Versions"]
    assert len(versions) == 1
    assert stored_item(aws)["text_truncated"] is True


def test_the_preview_is_cut_on_a_code_point_boundary(aws: Aws) -> None:
    # Emoji are 4 bytes and one code point; a cut by bytes would split one.
    message = make_message(tweet={"note_text": "\U0001f600" * 90_000})

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    preview = stored_item(aws)["text"]
    assert preview == "\U0001f600" * TEXT_PREVIEW_CHARS
    assert preview.encode("utf-8").decode("utf-8") == preview


def test_text_keys_use_the_author_id_or_the_lowercased_username() -> None:
    message = make_message(tweet={"author_id": None})

    assert TweetStore.text_key(message) == f"text/example_user/{TWEET_ID}.json"
    assert TweetStore.text_key(message, "display_text") == (
        f"text/example_user/{TWEET_ID}.display_text.json"
    )


def test_a_failed_text_upload_means_no_item_is_written(aws: Aws) -> None:
    class Failing:
        def put_object(self, **_: Any) -> None:
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")

    store = TweetStore(aws.tweets_table, cast(Any, Failing()), aws.bucket)

    with pytest.raises(ClientError):
        store.put_tweet(make_message(tweet={"note_text": HUGE_TEXT}), [])

    assert aws.tweets_table.scan()["Count"] == 0


def test_a_newer_preview_does_not_replace_a_stored_long_post(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    full = make_message(
        tweet={"note_text": HUGE_TEXT, "text": "prev"}, fetched_at=OLD_STAMP
    )
    preview = make_message(tweet={"text": "prev"}, fetched_at=NEW_STAMP)
    assert store.put_tweet(full, []).written

    with caplog.at_level("INFO"):
        result = store.put_tweet(preview, [])

    assert not result.written
    assert "preview-only message" in caplog.text
    stored = stored_item(aws)
    assert stored["is_long_post"] is True
    assert stored["text_truncated"] is True
    assert stored_text(aws, stored["text_s3_key"]) == HUGE_TEXT


def test_a_newer_long_post_replaces_a_stored_preview(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    preview = make_message(tweet={"text": "prev"}, fetched_at=OLD_STAMP)
    full = make_message(tweet={"note_text": "the full text"}, fetched_at=NEW_STAMP)
    assert store.put_tweet(preview, []).written

    assert store.put_tweet(full, []).written

    stored = stored_item(aws)
    assert stored["text"] == "the full text"
    assert stored["is_long_post"] is True


def test_a_preview_replaces_a_stored_preview_and_a_legacy_item(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(make_message(tweet={"text": "one"}, fetched_at=OLD_STAMP), [])
    assert store.put_tweet(
        make_message(tweet={"text": "two"}, fetched_at=NEW_STAMP), []
    ).written
    aws.tweets_table.put_item(
        Item={"tweet_id": TWEET_ID, "text": "legacy", "fetched_at": OLD_STAMP}
    )

    assert store.put_tweet(
        make_message(tweet={"text": "three"}, fetched_at=NEW_STAMP), []
    ).written
    assert stored_item(aws)["text"] == "three"


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
        def get_item(self, **_: Any) -> dict[str, Any]:
            return {}

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


def test_load_missing_item_is_the_empty_state(aws: Aws) -> None:
    assert CheckpointStore(aws.state_table).load("example_user") == CheckpointState()


def test_first_save_writes_numbers_and_version_one(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    state = CheckpointState(newest_id=TWEET_ID, gaps=(Gap(until_id="9", since_id="3"),))

    assert store.save("example_user", state, expected_version=0) is True

    item = ddb_get(aws.state_table, username="example_user")
    assert int(item["newest_id"]) == int(TWEET_ID)
    assert item["gaps"] == [{"until_id": 9, "since_id": 3}]
    assert item["version"] == 1
    assert str(item["updated_at"]).endswith("Z")
    assert store.load("example_user") == CheckpointState(
        newest_id=TWEET_ID, gaps=state.gaps, version=1
    )


def test_open_ended_gap_round_trips_without_missing_bounds(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    gaps = (Gap(until_id="50"), Gap(since_id="7"), Gap())

    store.save("example_user", CheckpointState(gaps=gaps), expected_version=0)

    item = ddb_get(aws.state_table, username="example_user")
    assert "newest_id" not in item
    assert item["gaps"] == [{"until_id": 50}, {"since_id": 7}, {}]
    assert store.load("example_user").gaps == gaps


def test_save_requires_the_expected_version(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    store.save("example_user", CheckpointState(newest_id="10"), expected_version=0)

    assert (
        store.save("example_user", CheckpointState("99"), expected_version=0) is False
    )
    assert (
        store.save("example_user", CheckpointState("99"), expected_version=2) is False
    )
    assert store.load("example_user").newest_id == "10"

    assert store.save("example_user", CheckpointState("20"), expected_version=1) is True
    assert store.load("example_user") == CheckpointState("20", (), 2)


def test_legacy_item_without_version_loads_as_version_zero_and_upgrades(
    aws: Aws,
) -> None:
    aws.state_table.put_item(
        Item={"username": "example_user", "newest_id": 500, "updated_at": "x"}
    )
    store = CheckpointStore(aws.state_table)

    state = store.load("example_user")

    assert state == CheckpointState(newest_id="500", gaps=(), version=0)
    assert store.save("example_user", state, expected_version=0) is True
    assert store.load("example_user").version == 1
    assert store.save("example_user", state, expected_version=0) is False


def test_checkpoint_preserves_snowflake_precision(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    state = CheckpointState("1999999999999999999", (Gap("1999999999999999998"),))
    store.save("example_user", state, expected_version=0)

    loaded = store.load("example_user")
    assert loaded.newest_id == "1999999999999999999"
    assert loaded.gaps == (Gap("1999999999999999998"),)


def test_checkpoint_username_is_case_insensitive(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)

    store.save("Example_User", CheckpointState("500"), expected_version=0)

    assert store.load("EXAMPLE_USER").newest_id == "500"
    assert store.load("example_user").newest_id == "500"
    assert store.save("EXAMPLE_user", CheckpointState("1"), expected_version=0) is False
    assert aws.state_table.scan()["Count"] == 1


def test_checkpoints_are_per_user(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    store.save("alice", CheckpointState("10"), expected_version=0)

    assert store.load("bob") == CheckpointState()
    assert store.save("bob", CheckpointState("5"), expected_version=0) is True


def test_save_reraises_other_dynamodb_errors(aws: Aws) -> None:
    store = CheckpointStore(aws.dynamodb.Table(f"{STATE_TABLE}-missing"))

    with pytest.raises(ClientError):
        store.save("example_user", CheckpointState("1"), expected_version=0)


@pytest.mark.parametrize("bad", ["", "abc", "12a", "-5", "1.5", "١٢"])
def test_gap_rejects_non_numeric_ids(bad: str) -> None:
    with pytest.raises(ValueError, match="numeric"):
        Gap(until_id=bad)
    with pytest.raises(ValueError, match="numeric"):
        Gap(since_id=bad)


def test_gaps_sort_by_upper_bound_with_an_open_top_first() -> None:
    gaps = [Gap("9"), Gap(None), Gap("100"), Gap("20")]

    ordered = sorted(gaps, key=lambda g: g.sort_key, reverse=True)

    assert [g.until_id for g in ordered] == [None, "100", "20", "9"]


def test_gap_normalizes_ids_and_rejects_inverted_ranges() -> None:
    assert Gap(until_id="0012", since_id="007") == Gap(until_id="12", since_id="7")
    for since, until in [("20", "10"), ("5", "5"), ("010", "10")]:
        with pytest.raises(ValueError, match="below until_id"):
            Gap(until_id=until, since_id=since)


def test_empty_runs_round_trip_through_the_item(aws: Aws) -> None:
    store = CheckpointStore(aws.state_table)
    gap = Gap(
        until_id="9", since_id="3", empty_runs=2, first_empty_at="2026-01-01T00:00:00Z"
    )

    store.save("example_user", CheckpointState(gaps=(gap,)), expected_version=0)

    assert ddb_get(aws.state_table, username="example_user")["gaps"] == [
        {
            "until_id": 9,
            "since_id": 3,
            "empty_runs": 2,
            "first_empty_at": "2026-01-01T00:00:00Z",
        }
    ]
    assert store.load("example_user").gaps == (gap,)


def test_a_legacy_gap_item_without_first_empty_at_loads_as_none(aws: Aws) -> None:
    aws.state_table.put_item(
        Item={
            "username": "example_user",
            "gaps": [{"until_id": 9, "since_id": 3, "empty_runs": 2}],
        }
    )

    gap = CheckpointStore(aws.state_table).load("example_user").gaps[0]

    assert (gap.empty_runs, gap.first_empty_at) == (2, None)


@pytest.mark.parametrize(
    ("gaps", "expected"),
    [
        ([], ()),
        ([Gap("10", "5"), Gap("20", "15")], (Gap("20", "15"), Gap("10", "5"))),
        ([Gap("10", "5"), Gap("12", "8")], (Gap("12", "5"),)),  # overlap
        (
            [Gap("10", "5"), Gap("15", "9")],
            (Gap("15", "5"),),
        ),  # adjacent (6..9, 10..14)
        ([Gap("10", "5"), Gap("10", "5")], (Gap("10", "5"),)),  # duplicate
        ([Gap("10", "5"), Gap(None, "8")], (Gap(None, "5"),)),
        ([Gap("10", None), Gap("4", "2")], (Gap("10", None),)),
        ([Gap("6", "5"), Gap("3", "2")], ()),  # no IDs inside
        ([Gap("10", "5", 2), Gap("10", "5", 1)], (Gap("10", "5", 1),)),
        ([Gap("10", "5", 2), Gap("12", "8", 2)], (Gap("12", "5", 0),)),
        # Same range: fewer runs and the later first-empty time, so a drop is never
        # sooner than either input's.
        (
            [
                Gap("10", "5", 3, "2026-01-01T00:00:00Z"),
                Gap("10", "5", 2, "2026-01-02T00:00:00Z"),
            ],
            (Gap("10", "5", 2, "2026-01-02T00:00:00Z"),),
        ),
        (
            [
                Gap("10", "5", 3, "2026-01-02T00:00:00Z"),
                Gap("10", "5", 4, "2026-01-01T00:00:00Z"),
            ],
            (Gap("10", "5", 3, "2026-01-02T00:00:00Z"),),
        ),
        (
            [Gap("10", "5", 3, "2026-01-01T00:00:00Z"), Gap("10", "5")],
            (Gap("10", "5"),),
        ),
        (
            [Gap("10", "5", 3), Gap("10", "5", 3, "2026-01-01T00:00:00Z")],
            (Gap("10", "5", 3, "2026-01-01T00:00:00Z"),),
        ),
        (
            [
                Gap("10", "5", 3, "2026-01-01T00:00:00Z"),
                Gap("12", "8", 3, "2026-01-01T00:00:00Z"),
            ],
            (Gap("12", "5"),),
        ),
    ],
)
def test_merge_gaps(gaps: list[Gap], expected: tuple[Gap, ...]) -> None:
    assert merge_gaps(gaps) == expected


def test_merge_gaps_past_the_limit_joins_the_closest_neighbours() -> None:
    gaps = [Gap("15", "10"), Gap("25", "20"), Gap("32", "28"), Gap("55", "50")]

    # Distances between neighbours: 7, 5, 18. The closest pair is joined.
    assert merge_gaps(gaps, limit=3) == (
        Gap("55", "50"),
        Gap("32", "20"),
        Gap("15", "10"),
    )


def test_more_gaps_than_the_cap_are_capped_without_losing_coverage() -> None:
    gaps = [Gap(str(i * 10 + 5), str(i * 10)) for i in range(MAX_GAPS + 50)]

    merged = merge_gaps(gaps)

    assert len(merged) == MAX_GAPS == 1500
    assert all(
        any(m.first <= g.first and g.last <= m.last for m in merged) for g in gaps
    )


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


@given(
    data=st.lists(
        st.tuples(
            st.integers(0, 6),
            st.integers(0, 6),  # runs
            st.integers(0, 5),  # hours since the epoch of the first empty response
        ),
        min_size=2,
        max_size=2,
    )
)
def test_merging_never_makes_a_drop_sooner(data: list[tuple[int, int, int]]) -> None:
    def gap(runs: int, hour: int) -> Gap:
        stamp = f"2026-01-01T{hour:02d}:00:00Z" if runs else None
        return Gap("10", "5", runs, stamp)

    gaps = [gap(r, h) for _, r, h in data]
    (merged,) = merge_gaps(gaps)

    assert merged.empty_runs <= min(g.empty_runs for g in gaps)
    assert (merged.first_empty_at is None) == (merged.empty_runs == 0)
    stamps = [g.first_empty_at for g in gaps if g.first_empty_at]
    if merged.first_empty_at:
        assert merged.first_empty_at == max(stamps)


# ---- precheck, media updates and the retry queue ------------------------------------


def test_precheck_leaves_an_unstorable_media_retrying_with_url_and_planned_key(
    aws: Aws,
) -> None:
    cdn = CdnStub()
    store = MediaStore(aws.s3, aws.bucket, cdn.client())

    result = store.precheck(make_message(media=(PHOTO,)), PHOTO)

    assert result == StoredMedia(
        media_key=PHOTO.media_key,
        type="photo",
        status="retrying",
        s3_key=PHOTO_KEY,
        source_url=f"{PHOTO_URL}?name=orig",
    )
    assert cdn.requests == []


def test_precheck_finds_an_object_already_in_s3(aws: Aws) -> None:
    aws.s3.put_object(
        Bucket=aws.bucket, Key=PHOTO_KEY, Body=JPEG, ContentType="image/jpeg"
    )
    store = MediaStore(aws.s3, aws.bucket, CdnStub().client())

    result = store.precheck(make_message(media=(PHOTO,)), PHOTO)

    assert (result.status, result.s3_key, result.size_bytes) == (
        "exists",
        PHOTO_KEY,
        len(JPEG),
    )


def test_precheck_without_head_makes_no_s3_call(aws: Aws) -> None:
    aws.s3.put_object(Bucket=aws.bucket, Key=PHOTO_KEY, Body=JPEG)
    store = MediaStore(aws.s3, aws.bucket, CdnStub().client())

    result = store.precheck(make_message(media=(PHOTO,)), PHOTO, head=False)

    assert result.status == "retrying"  # it exists, but this check was skipped


def test_precheck_applies_the_size_estimate_and_the_host_allowlist(aws: Aws) -> None:
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
    elsewhere = photo("3_x", "https://example.com/a.jpg")
    store = MediaStore(aws.s3, aws.bucket, CdnStub().client(), max_bytes=1_000_000)
    message = make_message(media=(big, elsewhere))

    assert store.precheck(message, big).status == "too_large"
    assert store.precheck(message, elsewhere).status == "skipped"


def test_precheck_never_raises_when_head_object_fails(aws: Aws) -> None:
    def deny(**_kw: Any) -> None:
        raise ClientError({"Error": {"Code": "500", "Message": "boom"}}, "HeadObject")

    aws.s3.meta.events.register("before-call.s3.HeadObject", deny)
    store = MediaStore(aws.s3, aws.bucket, CdnStub().client())

    result = store.precheck(make_message(media=(PHOTO,)), PHOTO)

    assert (result.status, result.s3_key) == ("retrying", PHOTO_KEY)


def test_get_media_reads_entries_and_reports_a_missing_item(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(
        make_message(media=(PHOTO,)),
        [StoredMedia(media_key=PHOTO.media_key, type="photo", status="skipped")],
    )
    store.put_tweet(make_message("1700000000000000999"), [])

    entries = store.get_media(TWEET_ID)

    assert entries is not None
    assert [e["status"] for e in entries] == ["skipped"]
    assert store.get_media("1700000000000000999") == []
    assert store.get_media("1") is None


def test_resolve_media_replaces_only_a_matching_retrying_entry(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    other = photo("3_other")
    store.put_tweet(
        make_message(media=(PHOTO, other)),
        [
            StoredMedia(media_key=PHOTO.media_key, type="photo", status="retrying"),
            StoredMedia(media_key="3_other", type="photo", status="retrying"),
        ],
    )
    done = {"media_key": PHOTO.media_key, "type": "photo", "status": "stored"}

    assert store.resolve_media(TWEET_ID, 0, PHOTO.media_key, done)
    assert not store.resolve_media(TWEET_ID, 0, PHOTO.media_key, done)  # not retrying
    assert not store.resolve_media(TWEET_ID, 1, PHOTO.media_key, done)  # other key
    assert not store.resolve_media(TWEET_ID, 5, "x", done)  # no such index
    assert not store.resolve_media("1", 0, PHOTO.media_key, done)  # no item
    entries = store.get_media(TWEET_ID)
    assert entries is not None
    assert [e["status"] for e in entries] == ["stored", "retrying"]
    assert aws.tweets_table.scan()["Count"] == 1  # no item was created for "1"


def test_resolve_media_propagates_other_errors(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    def deny(**_kw: Any) -> None:
        raise ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException"}}, "UpdateItem"
        )

    aws.tweets_table.meta.client.meta.events.register(
        "before-call.dynamodb.UpdateItem", deny
    )

    with pytest.raises(ClientError):
        store.resolve_media(TWEET_ID, 0, "k", {})


def test_media_retry_queue_sends_the_delayed_json_message(aws: Aws) -> None:
    queue = MediaRetryQueue(aws.sqs, aws.retry_queue_url, delay_seconds=45)
    retry = MediaRetry(tweet_id=TWEET_ID, username="u", media=PHOTO)

    queue.send(retry)

    attributes = aws.sqs.get_queue_attributes(
        QueueUrl=aws.retry_queue_url, AttributeNames=["All"]
    )["Attributes"]
    assert attributes["ApproximateNumberOfMessagesDelayed"] == "1"
    aws.sqs.purge_queue(QueueUrl=aws.retry_queue_url)


def test_a_declared_content_length_too_big_to_store_is_capped(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.responses[PHOTO_URL] = lambda: httpx.Response(
        200, headers={"content-length": "9" * 45}
    )
    store = MediaStore(aws.s3, aws.bucket, cdn.client())

    result = store.store(make_message(media=(PHOTO,)), PHOTO)

    assert (result.status, result.size_bytes) == ("too_large", MAX_STORED_INT)


def test_the_runbook_scan_finds_items_with_any_retrying_media(aws: Aws) -> None:
    """The filter expression the README gives, against four media per item."""
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    def put(tweet_id: str, statuses: list[str]) -> None:
        items = [photo(f"3_{i}") for i in range(len(statuses))]
        store.put_tweet(
            make_message(tweet_id, media=tuple(items)),
            [
                StoredMedia(media_key=m.media_key, type="photo", status=st)  # type: ignore[arg-type]
                for m, st in zip(items, statuses, strict=True)
            ],
        )

    put("1700000000000000001", ["stored", "stored"])
    put("1700000000000000002", ["stored", "retrying"])
    put("1700000000000000003", ["retrying"])
    put("1700000000000000004", ["stored", "stored", "stored", "retrying"])
    put("1700000000000000005", [])

    found = aws.tweets_table.scan(
        FilterExpression=(
            "media[0].#s = :r OR media[1].#s = :r OR media[2].#s = :r "
            "OR media[3].#s = :r"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":r": "retrying"},
    )

    assert sorted(i["tweet_id"] for i in as_items(found["Items"])) == [
        "1700000000000000002",
        "1700000000000000003",
        "1700000000000000004",
    ]


def test_expires_at_is_written_only_when_retention_is_configured(aws: Aws) -> None:
    message = make_message("1700000000000000001")
    keep = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    expire = TweetStore(aws.tweets_table, aws.s3, aws.bucket, retention_days=30)

    assert "expires_at" not in keep.build_item(message, [])
    item = expire.build_item(message, [])

    stored = datetime.fromisoformat(item["stored_at"]).timestamp()
    assert item["expires_at"] == int(stored) + 30 * 86_400


def test_default_retention_never_writes_expires_at_to_dynamodb(aws: Aws) -> None:
    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(
        make_message("1700000000000000001"), []
    )
    TweetStore(aws.tweets_table, aws.s3, aws.bucket, retention_days=1).put_tweet(
        make_message("1700000000000000002"), []
    )

    items = {i["tweet_id"]: i for i in aws.tweets_table.scan()["Items"]}

    assert "expires_at" not in items["1700000000000000001"]
    assert "expires_at" in items["1700000000000000002"]


# ---- missing keys, media fields, previews (#17, #36) -----------------------------

PREVIEW_URL = "https://pbs.twimg.com/ext_tw_video_thumb/1/pu/img/p.jpg"
HLS = {"content_type": "application/x-mpegURL", "url": "https://video.twimg.com/p.m3u8"}
MP4 = {"content_type": "video/mp4", "bit_rate": 1, "url": VIDEO_HI_URL}
VIDEO_KEY_BASE = f"media/{USER_ID}/{TWEET_ID}/7_1"


def video_with_preview(
    *variants: dict[str, Any], preview: str | None = PREVIEW_URL
) -> Media:
    return Media.from_dict(
        {
            "media_key": "7_1",
            "type": "video",
            "preview_image_url": preview,
            "variants": list(variants or [MP4]),
        }
    )


def preview_cdn() -> CdnStub:
    cdn = CdnStub()
    cdn.serve(VIDEO_HI_URL, b"video", "video/mp4")
    cdn.serve(PREVIEW_URL, b"poster", "image/jpeg")
    return cdn


def test_to_item_records_every_attached_key_with_a_missing_entry() -> None:
    message = make_message(
        media=(PHOTO,),
        tweet={"media_keys": ["3_gone", PHOTO.media_key, "3_gone2"]},
    )
    stored = StoredMedia(media_key=PHOTO.media_key, type="photo", status="stored")

    item = TweetStore.to_item(message, [stored])

    assert item["media_keys"] == ["3_gone", PHOTO.media_key, "3_gone2"]
    assert [(e["media_key"], e["status"]) for e in item["media"]] == [
        ("3_gone", "missing"),
        (PHOTO.media_key, "stored"),
        ("3_gone2", "missing"),
    ]
    assert item["media"][0] == {
        "media_key": "3_gone",
        "type": "unknown",
        "status": "missing",
    }
    assert item["media_count"] == 3


def test_to_item_without_media_has_empty_keys_and_zero_count() -> None:
    item = TweetStore.to_item(make_message(), [])

    assert (item["media_keys"], item["media"], item["media_count"]) == ([], [], 0)


def test_to_item_keeps_stored_media_the_tweet_does_not_list() -> None:
    stored = StoredMedia(media_key=PHOTO.media_key, type="photo", status="stored")

    item = TweetStore.to_item(make_message(media=(PHOTO,)), [stored])

    assert item["media_keys"] == [PHOTO.media_key]
    assert item["media_count"] == 1


def test_to_item_stores_every_media_field() -> None:
    media = Media.from_dict(
        {
            "media_key": "7_1",
            "type": "video",
            "alt_text": "a clip",
            "width": 1280,
            "height": 720,
            "duration_ms": 30_000,
            "preview_image_url": PREVIEW_URL,
            "variants": [HLS, MP4],
        }
    )
    stored = StoredMedia(
        media_key="7_1", type="video", status="stored", preview_s3_key="p"
    )

    (entry,) = TweetStore.to_item(
        make_message(media=(media,), tweet={"media_keys": ["7_1"]}), [stored]
    )["media"]

    assert entry == {
        "media_key": "7_1",
        "type": "video",
        "status": "stored",
        "preview_s3_key": "p",
        "alt_text": "a clip",
        "width": 1280,
        "height": 720,
        "duration_ms": 30_000,
        "preview_image_url": PREVIEW_URL,
        "variants": [
            {"content_type": HLS["content_type"], "url": HLS["url"]},
            {"content_type": "video/mp4", "bit_rate": 1, "url": VIDEO_HI_URL},
        ],
    }


def test_media_fields_are_clamped_and_empty_values_dropped() -> None:
    media = Media.from_dict(
        {
            "media_key": "3_1",
            "type": "photo",
            "width": 10**45,
            "height": -(10**45),
            "alt_text": "",
            "variants": [],
        }
    )

    entry = media_entry(
        StoredMedia(media_key="3_1", type="photo", status="stored"), media
    )

    assert (entry["width"], entry["height"]) == (MAX_STORED_INT, -MAX_STORED_INT)
    assert entry["alt_text"] == ""
    assert "variants" not in entry
    assert media_entry(StoredMedia(media_key="k", type="t", status="missing")) == {
        "media_key": "k",
        "type": "t",
        "status": "missing",
    }


def heavy_media(n: int) -> list[Media]:
    return [
        Media.from_dict(
            {
                "media_key": f"7_{i}",
                "type": "video",
                "alt_text": "é" * 1000,
                "preview_image_url": PREVIEW_URL,
                "variants": [
                    {
                        "content_type": "video/mp4",
                        "bit_rate": 10**18 + v,
                        "url": f"https://video.twimg.com/{'x' * 150}/{v}.mp4",
                    }
                    for v in range(12)
                ],
            }
        )
        for i in range(n)
    ]


def test_item_with_four_heavy_media_fits_and_counts_them(aws: Aws) -> None:
    media = heavy_media(4)
    keys = [m.media_key for m in media]
    message = make_message(media=tuple(media), tweet={"media_keys": keys})
    stored = [StoredMedia(media_key=k, type="video", status="skipped") for k in keys]

    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    item = store.build_item(message, stored)

    media_size = estimate_item_size(item["media"])
    assert 20_000 < media_size < ITEM_LIMIT_BYTES // 4
    assert store.put_tweet(message, stored).written
    saved = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    assert [e["alt_text"] for e in saved["media"]] == ["é" * 1000] * 4
    assert len(saved["media"][3]["variants"]) == 12


def test_text_offload_boundary_accounts_for_the_media_fields(aws: Aws) -> None:
    media = heavy_media(4)
    keys = [m.media_key for m in media]
    stored = [StoredMedia(media_key=k, type="video", status="skipped") for k in keys]
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    def build(text_len: int) -> dict[str, Any]:
        message = make_message(
            media=tuple(media), tweet={"media_keys": keys, "text": "a" * text_len}
        )
        return store.build_item(message, stored)

    base = estimate_item_size(
        TweetStore.to_item(
            make_message(media=tuple(media), tweet={"media_keys": keys, "text": ""}),
            stored,
        )
    )
    fits = ITEM_LIMIT_BYTES - base

    assert base > 20_000  # the media fields are part of what the estimate counts
    assert "text_s3_key" not in build(fits)
    assert build(fits + 1)["text_truncated"] is True


def test_store_stores_the_poster_frame_of_a_video(aws: Aws) -> None:
    cdn = preview_cdn()
    media = video_with_preview()

    stored = media_store(aws, cdn).store(make_message(media=(media,)), media)

    assert stored.status == "stored"
    assert stored.s3_key == f"{VIDEO_KEY_BASE}.mp4"
    assert stored.preview_s3_key == f"{VIDEO_KEY_BASE}_preview.jpg"
    poster = aws.s3.get_object(Bucket=aws.bucket, Key=stored.preview_s3_key)
    assert poster["Body"].read() == b"poster"
    assert poster["ContentType"] == "image/jpeg"
    assert poster["Metadata"]["media-key"] == "7_1"


def test_store_does_not_download_an_existing_poster_frame_again(aws: Aws) -> None:
    cdn = preview_cdn()
    media = video_with_preview()
    message = make_message(media=(media,))
    media_store(aws, cdn).store(message, media)
    downloads = len(cdn.requests)

    again = media_store(aws, cdn).store(message, media)

    assert len(cdn.requests) == downloads
    assert (again.status, again.preview_s3_key) == (
        "exists",
        f"{VIDEO_KEY_BASE}_preview.jpg",
    )


def test_hls_only_video_stores_the_poster_frame_and_is_skipped_hls(aws: Aws) -> None:
    cdn = preview_cdn()
    media = video_with_preview(HLS)

    stored = media_store(aws, cdn).store(make_message(media=(media,)), media)

    assert stored == StoredMedia(
        media_key="7_1",
        type="video",
        status="skipped_hls",
        preview_s3_key=f"{VIDEO_KEY_BASE}_preview.jpg",
        preview_status="stored",
    )
    assert [str(r.url) for r in cdn.requests] == [PREVIEW_URL]


def test_gif_poster_frame_is_stored_but_a_photo_has_none(aws: Aws) -> None:
    cdn = preview_cdn()
    gif = Media.from_dict(
        {
            "media_key": "16_1",
            "type": "animated_gif",
            "preview_image_url": PREVIEW_URL,
            "variants": [MP4],
        }
    )
    pic = Media.from_dict(
        {
            "media_key": "3_1",
            "type": "photo",
            "url": PHOTO_URL,
            "preview_image_url": PREVIEW_URL,
        }
    )
    cdn.serve(PHOTO_URL, b"p", "image/jpeg")
    store = media_store(aws, cdn)
    message = make_message(media=(gif, pic))

    assert store.store(message, gif).preview_s3_key is not None
    assert store.store(message, pic).preview_s3_key is None


@pytest.mark.parametrize("preview", [None, "https://example.com/p.jpg", "http://x"])
def test_no_poster_frame_without_an_allowed_preview_url(
    aws: Aws, preview: str | None
) -> None:
    cdn = preview_cdn()
    media = video_with_preview(preview=preview)

    stored = media_store(aws, cdn).store(make_message(media=(media,)), media)

    assert (stored.status, stored.preview_s3_key) == ("stored", None)
    assert [str(r.url) for r in cdn.requests] == [VIDEO_HI_URL]


@pytest.mark.parametrize("status", [404, 403])
def test_a_permanently_unavailable_poster_frame_is_left_out(
    aws: Aws, status: int
) -> None:
    cdn = preview_cdn()
    cdn.fail(PREVIEW_URL, status)
    media = video_with_preview()

    stored = media_store(aws, cdn).store(make_message(media=(media,)), media)

    assert (stored.status, stored.preview_s3_key) == ("stored", None)
    assert stored.preview_status == "unavailable"


def test_a_poster_frame_over_the_cap_is_left_out(aws: Aws) -> None:
    cdn = preview_cdn()
    cdn.serve(PREVIEW_URL, b"x" * 100, "image/jpeg")
    media = video_with_preview()
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), max_bytes=50)
    cdn.serve(VIDEO_HI_URL, b"v" * 10, "video/mp4")

    stored = store.store(make_message(media=(media,)), media)

    assert (stored.status, stored.preview_s3_key) == ("stored", None)
    assert (stored.preview_status, stored.preview_size_bytes) == ("too_large", 100)
    assert "Contents" in aws.s3.list_objects_v2(Bucket=aws.bucket)
    with pytest.raises(ClientError):
        aws.s3.head_object(Bucket=aws.bucket, Key=f"{VIDEO_KEY_BASE}_preview.jpg")


def test_a_transient_poster_frame_failure_is_raised(aws: Aws) -> None:
    cdn = preview_cdn()
    cdn.fail(PREVIEW_URL, 503)
    media = video_with_preview()

    with pytest.raises(TransientMediaError):
        media_store(aws, cdn).store(make_message(media=(media,)), media)


def test_the_poster_frame_shares_the_download_deadline(aws: Aws) -> None:
    clock = [0.0]
    cdn = preview_cdn()

    def main_eats_the_budget() -> httpx.Response:
        clock[0] += 100
        return httpx.Response(200, content=b"v", headers={"Content-Type": "video/mp4"})

    cdn.responses[VIDEO_HI_URL] = main_eats_the_budget
    media = video_with_preview()
    store = MediaStore(aws.s3, aws.bucket, cdn.client(), clock=lambda: clock[0])

    with pytest.raises(TransientMediaError):
        store.store(make_message(media=(media,)), media, budget_s=50)

    assert [str(r.url) for r in cdn.requests] == [VIDEO_HI_URL]


def test_precheck_plans_the_poster_frame_of_a_retrying_video(aws: Aws) -> None:
    media = video_with_preview()

    result = media_store(aws, CdnStub()).precheck(make_message(media=(media,)), media)

    assert result.status == "retrying"
    assert result.s3_key == f"{VIDEO_KEY_BASE}.mp4"
    assert result.preview_s3_key == f"{VIDEO_KEY_BASE}_preview.jpg"


def test_precheck_keeps_a_settled_file_open_while_its_poster_frame_is_missing(
    aws: Aws,
) -> None:
    media = video_with_preview(HLS)  # skipped_hls: nothing else to store
    store = media_store(aws, CdnStub())
    message = make_message(media=(media,))

    open_ = store.precheck(message, media)
    assert (open_.status, open_.preview_s3_key) == (
        "retrying",
        f"{VIDEO_KEY_BASE}_preview.jpg",
    )

    aws.s3.put_object(Bucket=aws.bucket, Key=f"{VIDEO_KEY_BASE}_preview.jpg", Body=b"p")
    settled = store.precheck(message, media)
    assert (settled.status, settled.preview_s3_key) == (
        "skipped_hls",
        f"{VIDEO_KEY_BASE}_preview.jpg",
    )
    # Without a HeadObject the poster frame can't be confirmed.
    assert store.precheck(message, media, head=False).status == "retrying"


def test_precheck_leaves_a_video_without_a_preview_url_alone(aws: Aws) -> None:
    media = video_with_preview(HLS, preview=None)

    result = media_store(aws, CdnStub()).precheck(make_message(media=(media,)), media)

    assert result == StoredMedia(media_key="7_1", type="video", status="skipped_hls")


def test_precheck_treats_a_failing_poster_head_as_not_stored(aws: Aws) -> None:
    media = video_with_preview(HLS)

    def deny(**_kw: Any) -> None:
        raise ClientError({"Error": {"Code": "500", "Message": "boom"}}, "HeadObject")

    aws.s3.meta.events.register("before-call.s3.HeadObject", deny)

    result = media_store(aws, CdnStub()).precheck(make_message(media=(media,)), media)

    assert result.status == "retrying"


def test_a_disallowed_poster_host_is_logged_as_a_poster_not_a_skipped_media(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    media = video_with_preview(preview="https://example.com/p.jpg")

    with caplog.at_level(logging.WARNING):
        media_store(aws, preview_cdn()).store(make_message(media=(media,)), media)

    assert any(
        "7_1 preview not stored: host 'example.com'" in m for m in caplog.messages
    )
    assert not any("7_1 skipped" in m for m in caplog.messages)


def test_no_time_left_is_raised_before_any_head_object(aws: Aws) -> None:
    calls: list[str] = []
    aws.s3.meta.events.register(
        "before-call.s3.HeadObject", lambda **_: calls.append("head")
    )
    media = video_with_preview(HLS)  # only the poster frame is left to store

    with pytest.raises(TransientMediaError, match="no time left"):
        media_store(aws, preview_cdn()).store(
            make_message(media=(media,)), media, budget_s=0
        )

    assert calls == []


def test_media_entry_keeps_the_media_url() -> None:
    pic = photo()

    entry = media_entry(
        StoredMedia(media_key=pic.media_key, type="photo", status="skipped"), pic
    )

    assert entry["url"] == PHOTO_URL


# ---- merging the stored item (no enrichment is lost) ----------------------------

OLD_FETCH = "2026-10-20T00:00:00Z"
NEW_FETCH = "2026-10-25T00:00:00Z"


def photo_entry_message(
    fetched_at: str, *, present: bool
) -> tuple[TweetMessage, list[StoredMedia]]:
    pic = Media.from_dict(
        {
            "media_key": "3_a",
            "type": "photo",
            "url": PHOTO_URL,
            "alt_text": "a cat",
            "width": 10,
            "height": 20,
        }
    )
    message = make_message(
        media=(pic,) if present else (),
        tweet={"media_keys": ["3_a"]},
        fetched_at=fetched_at,
    )
    stored = (
        [
            StoredMedia(
                media_key="3_a",
                type="photo",
                status="stored",
                s3_key="media/k.jpg",
                source_url=PHOTO_URL,
            )
        ]
        if present
        else []
    )
    return message, stored


def test_a_newer_fetch_with_the_key_missing_keeps_the_stored_entry(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(*photo_entry_message(OLD_FETCH, present=True))

    result = store.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    assert result.written
    item = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    (entry,) = item["media"]
    assert entry["status"] == "stored"
    assert entry["s3_key"] == "media/k.jpg"
    assert (entry["alt_text"], entry["width"], entry["type"]) == ("a cat", 10, "photo")
    assert entry["missing_since"] == NEW_FETCH
    assert entry["last_seen_fetched_at"] == OLD_FETCH
    assert item["fetched_at"] == NEW_FETCH
    assert item["media_keys"] == ["3_a"] and item["media_count"] == 1


def test_missing_since_and_last_seen_are_kept_on_a_later_missing_fetch(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(*photo_entry_message(OLD_FETCH, present=True))
    store.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    store.put_tweet(*photo_entry_message("2026-10-26T00:00:00Z", present=False))

    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert entry["status"] == "stored"
    assert (entry["missing_since"], entry["last_seen_fetched_at"]) == (
        NEW_FETCH,
        OLD_FETCH,
    )


def test_a_missing_entry_is_updated_when_a_newer_fetch_has_the_media(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(*photo_entry_message(OLD_FETCH, present=False))
    (first,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert first["status"] == "missing"

    store.put_tweet(*photo_entry_message(NEW_FETCH, present=True))

    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert entry["status"] == "stored"
    assert "missing_since" not in entry


def test_a_missing_key_over_a_missing_entry_stays_missing(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(*photo_entry_message(OLD_FETCH, present=False))

    store.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert entry == {"media_key": "3_a", "type": "unknown", "status": "missing"}


def test_an_older_message_with_the_key_missing_is_stale_and_changes_nothing(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(*photo_entry_message(NEW_FETCH, present=True))

    result = store.put_tweet(*photo_entry_message(OLD_FETCH, present=False))

    assert not result.written
    assert result.stored_fetched_at == NEW_FETCH
    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert entry["status"] == "stored"
    assert "missing_since" not in entry


def racing_table(table: Any, before_put: Callable[[], object], times: int) -> Any:
    """A table whose put_item first runs ``before_put`` (another writer) ``times``."""

    class Racing:
        remaining = times

        def get_item(self, **kw: Any) -> Any:
            return table.get_item(**kw)

        def put_item(self, **kw: Any) -> Any:
            if self.remaining:
                self.remaining -= 1
                before_put()
            return table.put_item(**kw)

    return Racing()


def test_a_write_between_the_read_and_the_put_is_merged_not_overwritten(
    aws: Aws,
) -> None:
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True))

    def other_writer() -> None:
        # Between the read and the put: a fetch that carries a second media item.
        extra = photo("3_b", PHOTO_URL)
        message = make_message(
            media=(extra,),
            tweet={"media_keys": ["3_a", "3_b"]},
            fetched_at="2026-10-22T00:00:00Z",
        )
        plain.put_tweet(
            message,
            [
                StoredMedia(
                    media_key="3_a", type="photo", status="stored", s3_key="k-a"
                ),
                StoredMedia(
                    media_key="3_b", type="photo", status="stored", s3_key="k-b"
                ),
            ],
        )

    racing = TweetStore(
        racing_table(aws.tweets_table, other_writer, 1), aws.s3, aws.bucket
    )

    result = racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    assert result.written
    item = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    assert item["fetched_at"] == NEW_FETCH
    by_key = {e["media_key"]: e for e in item["media"]}
    # Merged from the re-read item (the other writer's), not the first read.
    assert {k: e["s3_key"] for k, e in by_key.items()} == {"3_a": "k-a", "3_b": "k-b"}
    assert by_key["3_a"]["last_seen_fetched_at"] == "2026-10-22T00:00:00Z"
    assert item["media_keys"] == ["3_a", "3_b"] and item["media_count"] == 2


def test_a_first_write_racing_another_first_write_is_retried(aws: Aws) -> None:
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    racing = TweetStore(
        racing_table(
            aws.tweets_table,
            lambda: plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True)),
            1,
        ),
        aws.s3,
        aws.bucket,
    )

    assert racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False)).written

    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert entry["status"] == "stored"


def test_a_tweet_that_keeps_changing_raises_a_retryable_error(aws: Aws) -> None:
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True))
    counter = [0]

    def always_another_writer() -> None:
        counter[0] += 1
        plain.put_tweet(
            *photo_entry_message(f"2026-10-2{counter[0]}T00:00:00Z", present=True)
        )

    racing = TweetStore(
        racing_table(aws.tweets_table, always_another_writer, 99), aws.s3, aws.bucket
    )

    with pytest.raises(WriteContentionError):
        racing.put_tweet(*photo_entry_message("2026-10-28T00:00:00Z", present=False))

    assert counter[0] == 3  # MERGE_ATTEMPTS
    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert entry["status"] == "stored"


def test_a_media_retry_result_between_the_read_and_the_put_survives(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    retrying = StoredMedia(
        media_key="3_a", type="photo", status="retrying", s3_key="media/k.jpg"
    )
    message, _ = photo_entry_message(OLD_FETCH, present=True)
    store.put_tweet(message, [retrying])

    def retry_lands() -> None:
        assert store.resolve_media(
            TWEET_ID,
            0,
            "3_a",
            {"media_key": "3_a", "type": "photo", "status": "stored", "s3_key": "k2"},
        )

    racing = TweetStore(
        racing_table(aws.tweets_table, retry_lands, 1), aws.s3, aws.bucket
    )

    assert racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False)).written

    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert (entry["status"], entry["s3_key"]) == ("stored", "k2")


def test_a_media_retry_result_on_a_legacy_item_survives(aws: Aws) -> None:
    # Items written before revisions existed take the attribute_not_exists(revision)
    # branch on their first re-fetch; a retry landing mid-merge must still win.
    aws.tweets_table.put_item(
        Item={
            "tweet_id": TWEET_ID,
            "fetched_at": OLD_FETCH,
            "media": [{"media_key": "3_a", "type": "photo", "status": "retrying"}],
        }
    )
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    def retry_lands() -> None:
        assert store.resolve_media(
            TWEET_ID,
            0,
            "3_a",
            {"media_key": "3_a", "type": "photo", "status": "stored", "s3_key": "k2"},
        )

    racing = TweetStore(
        racing_table(aws.tweets_table, retry_lands, 1), aws.s3, aws.bucket
    )

    assert racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False)).written

    item = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    (entry,) = item["media"]
    assert (entry["status"], entry["s3_key"]) == ("stored", "k2")
    assert item["revision"] == 2  # resolve_media's ADD made it 1; the retried put, 2


def test_every_write_bumps_the_revision(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    message, stored = photo_entry_message(OLD_FETCH, present=True)

    store.put_tweet(message, stored)
    assert ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["revision"] == 1
    store.put_tweet(message, stored)
    assert ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["revision"] == 2
    store.put_tweet(
        message, [StoredMedia(media_key="3_a", type="photo", status="retrying")]
    )
    store.resolve_media(
        TWEET_ID, 0, "3_a", {"media_key": "3_a", "type": "photo", "status": "stored"}
    )
    assert ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["revision"] == 4


def test_a_refetch_without_attachments_keeps_the_stored_media(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(*photo_entry_message(OLD_FETCH, present=True))
    bare = make_message(fetched_at=NEW_FETCH)  # no media_keys at all

    assert store.put_tweet(bare, []).written

    item = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    (entry,) = item["media"]
    assert (entry["status"], entry["s3_key"], entry["alt_text"]) == (
        "stored",
        "media/k.jpg",
        "a cat",
    )
    assert entry["missing_since"] == NEW_FETCH
    assert item["media_keys"] == ["3_a"] and item["media_count"] == 1


def test_a_stored_missing_entry_is_carried_as_is_when_the_key_is_unlisted(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(*photo_entry_message(OLD_FETCH, present=False))

    store.put_tweet(make_message(fetched_at=NEW_FETCH), [])

    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert entry == {"media_key": "3_a", "type": "unknown", "status": "missing"}


def test_every_put_reads_the_stored_item_first(aws: Aws) -> None:
    reads: list[str] = []
    aws.dynamodb.meta.client.meta.events.register(
        "before-call.dynamodb.GetItem", lambda **_: reads.append("get")
    )

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(
        *photo_entry_message(OLD_FETCH, present=True)
    )

    assert reads == ["get"]


def test_a_legacy_item_without_stamps_is_merged_too(aws: Aws) -> None:
    aws.tweets_table.put_item(
        Item={
            "tweet_id": TWEET_ID,
            "media": [
                {"media_key": "3_a", "type": "photo", "status": "stored", "s3_key": "k"}
            ],
        }
    )

    result = TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(
        *photo_entry_message(NEW_FETCH, present=False)
    )

    assert result.written
    (entry,) = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["media"]
    assert (entry["status"], entry["s3_key"]) == ("stored", "k")
    assert entry["last_seen_fetched_at"] == NEW_FETCH


# ---- compliance writes join the revision lock (#21) ---------------------------------


def _tombstone(aws: Aws, when: str = "2026-10-26T12:00:00Z") -> Callable[[], object]:
    store = ComplianceStore(aws.tweets_table, aws.state_table, aws.s3, aws.bucket)
    return lambda: store.tombstone(TWEET_ID, "deleted", when)


def test_a_tombstone_between_the_read_and_the_put_makes_the_put_refuse(
    aws: Aws,
) -> None:
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True))
    racing = TweetStore(
        racing_table(aws.tweets_table, _tombstone(aws), 1), aws.s3, aws.bucket
    )

    result = racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    assert result.tombstoned and not result.written
    stored = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    assert stored["deleted_at"] == "2026-10-26T12:00:00Z"
    assert stored["fetched_at"] == OLD_FETCH  # nothing of the new message landed
    assert stored["revision"] == 2


def test_a_tombstone_on_a_legacy_item_between_read_and_put_makes_it_refuse(
    aws: Aws,
) -> None:
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True))
    aws.tweets_table.update_item(
        Key={"tweet_id": TWEET_ID}, UpdateExpression="REMOVE revision"
    )
    racing = TweetStore(
        racing_table(aws.tweets_table, _tombstone(aws), 1), aws.s3, aws.bucket
    )

    result = racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    assert result.tombstoned and not result.written
    assert ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["fetched_at"] == OLD_FETCH


def test_a_deleted_at_that_leaves_the_revision_alone_still_fails_the_put(
    aws: Aws,
) -> None:
    # The base condition carries attribute_not_exists(deleted_at) on its own, not only
    # through the revision guard: a writer that forgets to bump the revision is caught.
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True))

    def silent_tombstone() -> None:
        aws.tweets_table.update_item(
            Key={"tweet_id": TWEET_ID},
            UpdateExpression="SET deleted_at = :d",
            ExpressionAttributeValues={":d": "2026-10-26T12:00:00Z"},
        )

    racing = TweetStore(
        racing_table(aws.tweets_table, silent_tombstone, 1), aws.s3, aws.bucket
    )

    result = racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    assert result.tombstoned and not result.written
    assert ddb_get(aws.tweets_table, tweet_id=TWEET_ID)["fetched_at"] == OLD_FETCH


def test_a_put_that_reads_a_tombstone_refuses_without_writing(aws: Aws) -> None:
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True))
    _tombstone(aws)()
    puts: list[object] = []
    racing = TweetStore(
        racing_table(aws.tweets_table, lambda: puts.append(1), 99), aws.s3, aws.bucket
    )

    result = racing.put_tweet(*photo_entry_message(NEW_FETCH, present=False))

    assert result.tombstoned
    assert puts == []  # refused on the read, before any put


def test_a_put_that_lands_before_the_tombstone_is_kept_under_it(aws: Aws) -> None:
    plain = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain.put_tweet(*photo_entry_message(OLD_FETCH, present=True))
    plain.put_tweet(*photo_entry_message(NEW_FETCH, present=True))

    assert _tombstone(aws)() is True

    stored = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    assert stored["fetched_at"] == NEW_FETCH  # the merge is intact, deleted_at added
    assert stored["deleted_at"] and stored["revision"] == 3
