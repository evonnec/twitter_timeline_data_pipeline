import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from helpers import FETCHED_AT, XApiStub, make_message, photo, x_client

from twitter_timeline_data_pipeline.models import (
    SCHEMA_VERSION,
    Media,
    MediaVariant,
    MessageError,
    TimelinePage,
    Tweet,
    TweetMessage,
    is_newer,
    normalize_username,
    utc_now_iso,
)

# ---- helpers ---------------------------------------------------------------


def media_of(type_: str, **fields: Any) -> Media:
    return Media.from_dict({"media_key": "k", "type": type_, **fields})


def mp4(url: str, bit_rate: int | None = None) -> dict[str, Any]:
    variant: dict[str, Any] = {"content_type": "video/mp4", "url": url}
    if bit_rate is not None:
        variant["bit_rate"] = bit_rate
    return variant


# ---- utilities -------------------------------------------------------------


def test_utc_now_iso_is_second_precision_with_z_suffix() -> None:
    stamp = utc_now_iso()

    assert stamp.endswith("Z")
    assert "." not in stamp
    assert "+" not in stamp


@pytest.mark.parametrize(
    ("candidate", "than", "expected"),
    [
        ("5", None, True),
        ("6", "5", True),
        ("5", "5", False),
        ("4", "5", False),
        ("100", "99", True),  # string comparison would say False
        ("99", "100", False),
        ("1700000000000000001", "999999999999999999", True),
    ],
)
def test_is_newer_compares_numerically(
    candidate: str, than: str | None, expected: bool
) -> None:
    assert is_newer(candidate, than) is expected


# ---- MediaVariant / Media ---------------------------------------------------


def test_media_variant_from_dict_reads_every_field() -> None:
    variant = MediaVariant.from_dict(
        {"content_type": "video/mp4", "url": "https://v/x.mp4", "bit_rate": "832000"}
    )

    assert variant == MediaVariant(
        content_type="video/mp4", url="https://v/x.mp4", bit_rate=832000
    )


def test_media_variant_bit_rate_is_optional() -> None:
    variant = MediaVariant.from_dict(
        {"content_type": "application/x-mpegURL", "url": "https://v/x.m3u8"}
    )

    assert variant.bit_rate is None


def test_media_from_dict_reads_every_field() -> None:
    media = Media.from_dict(
        {
            "media_key": "7_1",
            "type": "video",
            "url": "https://u",
            "preview_image_url": "https://p",
            "variants": [mp4("https://v", 5)],
            "width": 1280,
            "height": "720",
            "duration_ms": 15000,
            "alt_text": "alt",
        }
    )

    assert media == Media(
        media_key="7_1",
        type="video",
        url="https://u",
        preview_image_url="https://p",
        variants=(MediaVariant(content_type="video/mp4", url="https://v", bit_rate=5),),
        width=1280,
        height=720,
        duration_ms=15000,
        alt_text="alt",
    )


def test_media_from_dict_defaults_optional_fields() -> None:
    media = Media.from_dict({"media_key": "3_1", "type": "photo"})

    assert media.url is None
    assert media.variants == ()
    assert media.width is None
    assert media.alt_text is None


def test_media_from_dict_treats_null_variants_as_empty() -> None:
    assert media_of("photo", variants=None).variants == ()


@pytest.mark.parametrize("missing", ["media_key", "type"])
def test_media_from_dict_requires_key_and_type(missing: str) -> None:
    data = {"media_key": "k", "type": "photo"}
    del data[missing]

    with pytest.raises(KeyError):
        Media.from_dict(data)


# ---- Media.download_source --------------------------------------------------


def test_download_source_photo_uses_url_without_content_type() -> None:
    assert media_of("photo", url="https://p/a.jpg").download_source() == (
        "https://p/a.jpg",
        None,
    )


def test_download_source_photo_without_url_is_none() -> None:
    assert media_of("photo").download_source() is None


def test_download_source_video_picks_highest_bitrate_mp4_and_ignores_hls() -> None:
    media = media_of(
        "video",
        variants=[
            mp4("https://v/lo.mp4", 256000),
            {
                "content_type": "application/x-mpegURL",
                "url": "https://v/pl.m3u8",
                "bit_rate": 9_999_999,
            },
            mp4("https://v/hi.mp4", 2176000),
            mp4("https://v/mid.mp4", 832000),
        ],
    )

    assert media.download_source() == ("https://v/hi.mp4", "video/mp4")


def test_download_source_animated_gif_uses_its_single_mp4() -> None:
    media = media_of("animated_gif", variants=[mp4("https://v/g.mp4", 0)])

    assert media.download_source() == ("https://v/g.mp4", "video/mp4")


def test_download_source_mp4_without_bit_rate_still_chosen() -> None:
    media = media_of("video", variants=[mp4("https://v/a.mp4")])

    assert media.download_source() == ("https://v/a.mp4", "video/mp4")


def test_download_source_video_without_mp4_is_none() -> None:
    media = media_of(
        "video",
        variants=[{"content_type": "application/x-mpegURL", "url": "https://v/p.m3u8"}],
    )

    assert media.download_source() is None


def test_download_source_video_without_variants_is_none() -> None:
    assert media_of("video").download_source() is None


def test_download_source_unknown_type_falls_back_to_url() -> None:
    assert media_of("hologram", url="https://x/h.bin").download_source() == (
        "https://x/h.bin",
        None,
    )


def test_download_source_unknown_type_without_url_is_none() -> None:
    assert media_of("hologram").download_source() is None


# ---- Tweet ------------------------------------------------------------------


FULL_TWEET: dict[str, Any] = {
    "id": 1700000000000000005,  # ints are coerced to strings
    "text": "hello",
    "created_at": "2026-10-20T12:00:00.000Z",
    "author_id": "42",
    "conversation_id": "1700000000000000001",
    "lang": "en",
    "referenced_tweets": [{"type": "quoted", "id": "9"}],
    "public_metrics": {"like_count": 3, "reply_count": "2"},
    "edit_history_tweet_ids": ["1700000000000000005"],
}


def test_tweet_from_api_reads_media_keys_from_attachments() -> None:
    tweet = Tweet.from_api(
        {**FULL_TWEET, "attachments": {"media_keys": ["3_1", "3_2"]}}
    )

    assert tweet.id == "1700000000000000005"
    assert tweet.text == "hello"
    assert tweet.created_at == "2026-10-20T12:00:00.000Z"
    assert tweet.author_id == "42"
    assert tweet.conversation_id == "1700000000000000001"
    assert tweet.lang == "en"
    assert [(r.type, r.id) for r in tweet.referenced_tweets] == [("quoted", "9")]
    assert dict(tweet.public_metrics) == {"like_count": 3, "reply_count": 2}
    assert tweet.edit_history_tweet_ids == ("1700000000000000005",)
    assert tweet.media_keys == ("3_1", "3_2")


def test_tweet_from_api_minimal_defaults() -> None:
    tweet = Tweet.from_api({"id": "1", "text": "t"})

    assert tweet.created_at is None
    assert tweet.author_id is None
    assert tweet.conversation_id is None
    assert tweet.lang is None
    assert tweet.referenced_tweets == ()
    assert dict(tweet.public_metrics) == {}
    assert tweet.edit_history_tweet_ids == ()
    assert tweet.media_keys == ()


def test_tweet_from_api_ignores_flat_media_keys() -> None:
    assert (
        Tweet.from_api({"id": "1", "text": "t", "media_keys": ["3_1"]}).media_keys == ()
    )


def test_tweet_from_dict_reads_flat_media_keys() -> None:
    tweet = Tweet.from_dict({**FULL_TWEET, "media_keys": ["3_1"]})

    assert tweet.media_keys == ("3_1",)


def test_tweet_from_dict_ignores_attachments() -> None:
    tweet = Tweet.from_dict(
        {"id": "1", "text": "t", "attachments": {"media_keys": ["x"]}}
    )

    assert tweet.media_keys == ()


@pytest.mark.parametrize("missing", ["id", "text"])
def test_tweet_requires_id_and_text(missing: str) -> None:
    data = {"id": "1", "text": "t"}
    del data[missing]

    with pytest.raises(KeyError):
        Tweet.from_api(data)


# ---- TweetMessage -----------------------------------------------------------


def full_message() -> TweetMessage:
    return TweetMessage(
        username="example_user",
        tweet=Tweet.from_dict({**FULL_TWEET, "media_keys": ["7_1"]}),
        media=(
            media_of(
                "video",
                media_key="7_1",
                preview_image_url="https://p",
                variants=[mp4("https://v", 5)],
                width=1,
                height=2,
                duration_ms=3,
                alt_text="a",
            ),
        ),
        fetched_at=FETCHED_AT,
    )


def test_message_json_round_trip_preserves_everything() -> None:
    original = full_message()

    assert TweetMessage.from_json(original.to_json()) == original


def test_message_json_is_compact_and_ascii_escaped() -> None:
    body = make_message(tweet={"text": "caf\u00e9 \U0001f680"}).to_json()

    assert ", " not in body
    assert ": " not in body
    assert body.isascii()
    assert "\\u00e9" in body


@pytest.mark.parametrize(
    "text",
    [
        "caf\u00e9 \U0001f680 \u65e5\u672c\u8a9e",
        "\ud83d",
        "a\ud83db",
        "\uffff",
        "x\ufffe",
    ],
    ids=["non-ascii", "lone-surrogate", "embedded-surrogate", "uffff", "ufffe"],
)
def test_message_json_round_trips_awkward_text(text: str) -> None:
    message = make_message(tweet={"text": text})

    body = message.to_json()

    body.encode("utf-8")  # a lone surrogate would raise here if left unescaped
    assert body.isascii()
    assert TweetMessage.from_json(body).tweet.text == text


def test_message_to_dict_carries_schema_version_and_fetched_at() -> None:
    data = make_message().to_dict()

    assert data["schema_version"] == SCHEMA_VERSION
    assert data["fetched_at"] == FETCHED_AT
    assert data["tweet"]["id"] == "1700000000000000005"


def test_message_fetched_at_defaults_to_now() -> None:
    message = TweetMessage(username="u", tweet=Tweet(id="1", text="t"))

    assert message.fetched_at.endswith("Z")


def test_message_matches_sample_sqs_event() -> None:
    path = Path(__file__).parent.parent / "events" / "sqs.json"
    record = json.loads(path.read_text())["Records"][0]

    message = TweetMessage.from_json(record["body"])

    assert message.tweet.id == "1000000000000000001"
    assert message.media[0].media_key == "3_1000000000000000002"


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ("{not json", "malformed message"),
        ("", "malformed message"),
        ("[1, 2]", "expected a JSON object, got list"),
        ('"text"', "expected a JSON object, got str"),
        ("null", "expected a JSON object, got NoneType"),
        ('{"schema_version": Infinity}', "non-finite JSON constant Infinity"),
        ('{"schema_version": -Infinity}', "non-finite JSON constant -Infinity"),
        ('{"schema_version": NaN}', "non-finite JSON constant NaN"),
        ("[" * 200_000, "RecursionError"),
    ],
)
def test_from_json_rejects_non_objects_and_bad_json(body: str, fragment: str) -> None:
    with pytest.raises(MessageError, match=fragment):
        TweetMessage.from_json(body)


def test_from_json_rejects_overflowing_integers() -> None:
    data = full_message().to_dict()
    data["tweet"]["public_metrics"]["like_count"] = "__HUGE__"
    body = json.dumps(data).replace('"__HUGE__"', "1e999")  # parses as a float inf

    with pytest.raises(MessageError, match="OverflowError"):
        TweetMessage.from_json(body)


@pytest.mark.parametrize("version", [None, 0, 2, "1"])
def test_from_json_rejects_wrong_schema_version(version: object) -> None:
    data = full_message().to_dict()
    data["schema_version"] = version

    with pytest.raises(MessageError, match="unsupported schema_version"):
        TweetMessage.from_json(json.dumps(data))


def test_from_json_rejects_missing_schema_version() -> None:
    data = full_message().to_dict()
    del data["schema_version"]

    with pytest.raises(MessageError, match="unsupported schema_version None"):
        TweetMessage.from_json(json.dumps(data))


@pytest.mark.parametrize("missing", ["username", "tweet", "fetched_at"])
def test_from_json_rejects_missing_top_level_keys(missing: str) -> None:
    data = full_message().to_dict()
    del data[missing]

    with pytest.raises(MessageError, match="malformed message"):
        TweetMessage.from_json(json.dumps(data))


def test_from_json_rejects_tweet_missing_id() -> None:
    data = full_message().to_dict()
    del data["tweet"]["id"]

    with pytest.raises(MessageError, match="malformed message"):
        TweetMessage.from_json(json.dumps(data))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(tweet="not an object"),
        lambda d: d["tweet"].update(public_metrics=[1, 2]),
    ],
    ids=["tweet-is-string", "public_metrics-is-list"],
)
def test_from_json_wraps_wrongly_typed_nested_objects(
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    data = full_message().to_dict()
    mutate(data)

    with pytest.raises(MessageError, match="malformed message"):
        TweetMessage.from_json(json.dumps(data))


def test_message_error_is_a_value_error() -> None:
    assert issubclass(MessageError, ValueError)


# ---- TimelinePage.messages --------------------------------------------------


def page_from_fixture() -> TimelinePage:
    """Parse the page-1 fixture through the real client, as production does."""
    with x_client(XApiStub()) as client:
        pages = list(client.iter_timeline("2244994945", max_pages=1))
    return pages[0]


def test_timeline_page_messages_attach_media_by_key() -> None:
    page = page_from_fixture()

    messages = page.messages("example_user", fetched_at=FETCHED_AT)

    assert [m.tweet.id[-1] for m in messages] == ["5", "4", "3", "2", "1"]
    assert [[m.media_key[:2] for m in msg.media] for msg in messages] == [
        ["3_"],
        ["7_"],
        ["16"],
        [],
        [],
    ]
    assert {m.fetched_at for m in messages} == {FETCHED_AT}
    assert {m.username for m in messages} == {"example_user"}


def test_timeline_page_messages_warn_on_missing_media_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    page = page_from_fixture()

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        messages = page.messages("example_user")

    quoted = messages[-1]
    assert quoted.tweet.media_keys == ("3_1700000000000000199",)
    assert quoted.media == ()
    assert [r.getMessage() for r in caplog.records] == [
        "media key 3_1700000000000000199 of tweet 1700000000000000001 "
        "missing from includes"
    ]


def test_timeline_page_messages_stamp_defaults_to_now() -> None:
    page = TimelinePage(
        tweets=(Tweet(id="1", text="t", media_keys=("m",)),),
        media={"m": photo("m")},
    )

    (message,) = page.messages("u")

    assert message.fetched_at.endswith("Z")
    assert message.media == (photo("m"),)


def test_timeline_page_messages_every_message_round_trips() -> None:
    for message in page_from_fixture().messages("example_user"):
        assert TweetMessage.from_json(message.to_json()) == message


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("NASA", "nasa"),
        ("@NASA", "nasa"),
        ("@@x", "@x"),
        ("already_lower", "already_lower"),
    ],
)
def test_normalize_username_strips_one_at_and_lowercases(
    given: str, expected: str
) -> None:
    assert normalize_username(given) == expected
