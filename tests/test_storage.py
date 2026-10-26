import io
from typing import Any, cast
from urllib.parse import urlsplit

import httpx
import pytest
from botocore.exceptions import ClientError
from conftest import STATE_TABLE, Aws
from helpers import (
    GIF_URL,
    PHOTO_URL,
    USER_ID,
    VIDEO_HI_URL,
    CdnStub,
    as_items,
    ddb_get,
    make_message,
    photo,
)
from mypy_boto3_s3 import S3Client

from twitter_timeline_data_pipeline.models import Media, TweetMessage
from twitter_timeline_data_pipeline.storage import (
    ALLOWED_MEDIA_HOSTS,
    MAX_GAPS,
    CheckpointState,
    CheckpointStore,
    Gap,
    MediaStore,
    StoredMedia,
    TransientMediaError,
    TweetStore,
    _extension,
    _IterReader,
    _original_size_url,
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


def test_put_tweet_twice_replaces_the_item(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table)
    store.put_tweet(make_message(tweet={"public_metrics": {"like_count": 1}}), [])
    store.put_tweet(make_message(tweet={"public_metrics": {"like_count": 9}}), [])

    assert aws.tweets_table.scan()["Count"] == 1
    item = ddb_get(aws.tweets_table, tweet_id=TWEET_ID)
    assert item["public_metrics"] == {"like_count": 9}


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
    gap = Gap(until_id="9", since_id="3", empty_runs=2)

    store.save("example_user", CheckpointState(gaps=(gap,)), expected_version=0)

    assert ddb_get(aws.state_table, username="example_user")["gaps"] == [
        {"until_id": 9, "since_id": 3, "empty_runs": 2}
    ]
    assert store.load("example_user").gaps == (gap,)


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


def test_a_hundred_and_fifty_gaps_are_capped_without_losing_coverage() -> None:
    gaps = [Gap(str(i * 10 + 5), str(i * 10)) for i in range(150)]

    merged = merge_gaps(gaps)

    assert len(merged) == MAX_GAPS == 100
    assert all(
        any(m.first <= g.first and g.last <= m.last for m in merged) for g in gaps
    )
