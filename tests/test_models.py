import json
import logging
import re
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from helpers import (
    FETCHED_AT,
    FIXTURES,
    XApiStub,
    long_post_api_object,
    make_message,
    photo,
    x_client,
)

from twitter_timeline_data_pipeline.models import (
    MAX_STORED_INT,
    SCHEMA_VERSION,
    Media,
    MediaRetry,
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


def test_message_json_is_compact_utf8() -> None:
    body = make_message(tweet={"text": "caf\u00e9 \U0001f680"}).to_json()

    assert ", " not in body
    assert ": " not in body
    assert "caf\u00e9 \U0001f680" in body  # not ASCII-escaped
    assert "\\u00e9" not in body


def test_message_json_still_escapes_control_characters() -> None:
    body = make_message(tweet={"text": "a\x00b\x1fc\nd"}).to_json()

    assert "\x00" not in body
    assert "\x1f" not in body
    assert TweetMessage.from_json(body).tweet.text == "a\x00b\x1fc\nd"


SQS_ALLOWED = re.compile("[\t\n\r\x20-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]*")


@pytest.mark.parametrize(
    "text",
    [
        "caf\u00e9 \U0001f680 \u65e5\u672c\u8a9e",
        "\ud83d",
        "a\ud83db",
        "\uffff",
        "x\ufffe",
        "\U0001f600",
        "\U0001f468\u200d\U0001f469\u200d\U0001f467",
        "\U0001f3f4\U000e0067\U000e0062\U000e007f",
        "\udfff\ud800\uffff\ufffe\U0010ffff",
    ],
    ids=[
        "non-ascii",
        "lone-surrogate",
        "embedded-surrogate",
        "uffff",
        "ufffe",
        "astral-emoji",
        "zwj",
        "tag-flag",
        "mixed-edges",
    ],
)
def test_message_json_round_trips_awkward_text(text: str) -> None:
    message = make_message(tweet={"text": text, "note_text": text})

    body = message.to_json()

    body.encode("utf-8")  # a lone surrogate would raise here if left unescaped
    assert SQS_ALLOWED.fullmatch(body)
    assert TweetMessage.from_json(body).tweet.text == text
    assert TweetMessage.from_json(body).tweet.note_text == text


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
    assert quoted.missing_media_keys == ("3_1700000000000000199",)
    assert [
        r.getMessage() for r in caplog.records if "media key" in r.getMessage()
    ] == [
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


def message_data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(make_message().to_json())
    data.update(overrides)
    return data


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-10-25T01:00:00+01:00", "2026-10-25T00:00:00Z"),
        ("2026-10-25 23:59:59Z", "2026-10-25T23:59:59Z"),
        ("2026-10-25T12:00:00.500Z", "2026-10-25T12:00:00Z"),
        ("2026-10-25T12:00:00Z", "2026-10-25T12:00:00Z"),
    ],
)
def test_from_dict_normalizes_fetched_at_to_utc_seconds(
    raw: str, expected: str
) -> None:
    assert TweetMessage.from_dict(message_data(fetched_at=raw)).fetched_at == expected


@pytest.mark.parametrize(
    "raw",
    [None, 20261025, ["2026-10-25T00:00:00Z"], "yesterday", "", "2026-10-25T00:00:00"],
)
def test_from_dict_rejects_a_bad_fetched_at(raw: object) -> None:
    with pytest.raises(MessageError, match="fetched_at"):
        TweetMessage.from_dict(message_data(fetched_at=raw))


def test_from_dict_rejects_a_far_future_fetched_at() -> None:
    with pytest.raises(MessageError, match="future"):
        TweetMessage.from_dict(message_data(fetched_at="2999-01-01T00:00:00Z"))


def test_from_dict_accepts_a_slightly_future_fetched_at() -> None:
    soon = (datetime.now(UTC) + timedelta(minutes=1)).isoformat()

    assert TweetMessage.from_dict(message_data(fetched_at=soon))


def test_from_dict_rejects_a_boolean_schema_version() -> None:
    with pytest.raises(MessageError, match="unsupported schema_version"):
        TweetMessage.from_dict(message_data(schema_version=True))


# ---- long posts (note_tweet) and schema compatibility ----------------------


def test_tweet_from_api_reads_note_tweet_text() -> None:
    post = long_post_api_object()

    tweet = Tweet.from_api(post)

    assert tweet.note_text == post["note_tweet"]["text"]
    assert tweet.text == post["text"]
    assert tweet.full_text == tweet.note_text


def test_full_text_is_text_for_a_short_post() -> None:
    tweet = Tweet.from_api({"id": "1", "text": "short"})

    assert tweet.note_text is None
    assert tweet.full_text == "short"


def test_long_post_survives_a_message_round_trip() -> None:
    tweet = Tweet.from_api(long_post_api_object())
    message = TweetMessage(username="u", tweet=tweet, fetched_at=FETCHED_AT)

    data = json.loads(message.to_json())
    parsed = TweetMessage.from_json(message.to_json())

    assert data["tweet"]["note_text"] == tweet.note_text
    assert "note_tweet" not in data["tweet"]  # flat key, entities are out of scope
    assert parsed == message


def test_schema_version_stays_1_for_the_additive_note_text_field() -> None:
    assert SCHEMA_VERSION == 1


def test_golden_v1_message_without_note_text_parses() -> None:
    """A body in the format produced before ``note_text`` existed, kept as a fixture."""
    body = (FIXTURES / "message_v1.json").read_text()
    assert "note_text" not in body

    parsed = TweetMessage.from_json(body)

    assert parsed.schema_version == 1
    assert parsed.tweet.note_text is None
    assert parsed.tweet.full_text == "old format"
    assert parsed.referenced == ()
    assert parsed.tweet.entities is None and parsed.tweet.withheld is None


def test_unknown_keys_are_ignored_at_message_and_tweet_level() -> None:
    """What a previous reader relied on: extra keys never matter, so a newer writer's
    additive fields don't break it."""
    data = json.loads((FIXTURES / "message_v1.json").read_text())
    data["future_field"] = 1
    data["tweet"]["future_field"] = 2

    parsed = TweetMessage.from_dict(data)

    assert parsed == TweetMessage.from_dict(
        json.loads((FIXTURES / "message_v1.json").read_text())
    )


def test_golden_v1_message_without_missing_media_keys_parses() -> None:
    """Messages from before ``missing_media_keys`` existed read as "none reported"."""
    body = (FIXTURES / "message_v1.json").read_text()
    assert "missing_media_keys" not in body

    parsed = TweetMessage.from_json(body)

    assert parsed.missing_media_keys == ()
    assert parsed.schema_version == 1


def test_missing_media_keys_round_trip_and_leave_the_rest_readable() -> None:
    data = json.loads((FIXTURES / "message_v1.json").read_text())
    data["missing_media_keys"] = ["3_9", 4]

    parsed = TweetMessage.from_dict(data)

    assert parsed.missing_media_keys == ("3_9", "4")
    assert TweetMessage.from_json(parsed.to_json()) == parsed
    # What an old reader sees: the same message minus an extra key it ignores.
    del data["missing_media_keys"]
    assert replace(parsed, missing_media_keys=()) == TweetMessage.from_dict(data)


def test_hls_only_means_variants_but_no_mp4() -> None:
    hls = [{"content_type": "application/x-mpegURL", "url": "https://v/p.m3u8"}]

    assert media_of("video", variants=hls).is_hls_only
    assert media_of("animated_gif", variants=hls).is_hls_only
    assert not media_of("video").is_hls_only
    assert not media_of("video", variants=[mp4("https://v", 5)]).is_hls_only
    assert not media_of("photo", variants=hls).is_hls_only


def test_note_post_is_accepted_as_the_renamed_field() -> None:
    tweet = Tweet.from_api({"id": "1", "text": "p", "note_post": {"text": "full"}})

    assert tweet.note_text == "full"
    assert tweet.full_text == "full"


# ---- MediaRetry --------------------------------------------------------------------


def media_retry(**overrides: Any) -> MediaRetry:
    values: dict[str, Any] = {
        "tweet_id": "1700000000000000010",
        "username": "example_user",
        "author_id": "2244994945",
        "fetched_at": "2026-10-25T00:00:00Z",
        "media": Media.from_dict(
            {
                "media_key": "7_1",
                "type": "video",
                "duration_ms": 1000,
                "alt_text": "emoji \U0001f44d \ud800",
                "variants": [
                    {
                        "content_type": "video/mp4",
                        "url": "https://video.twimg.com/a.mp4",
                        "bit_rate": 832000,
                    }
                ],
            }
        ),
    }
    return MediaRetry(**{**values, **overrides})


def test_media_retry_round_trips_through_json() -> None:
    retry = media_retry()

    assert MediaRetry.from_json(retry.to_json()) == retry
    assert json.loads(retry.to_json())["kind"] == "media_retry"


def test_media_retry_without_an_author_round_trips() -> None:
    retry = media_retry(author_id=None)

    assert MediaRetry.from_json(retry.to_json()) == retry


def test_media_retry_escapes_what_sqs_rejects() -> None:
    body = media_retry().to_json()

    assert "\\ud800" in body
    assert "\U0001f44d" in body  # other non-ASCII stays as is


def test_media_retry_and_tweet_messages_are_not_interchangeable() -> None:
    with pytest.raises(MessageError, match="not a media_retry message"):
        MediaRetry.from_json(full_message().to_json())
    with pytest.raises(MessageError):
        TweetMessage.from_json(media_retry().to_json())


def test_media_retry_builds_a_message_naming_the_s3_object() -> None:
    message = media_retry().to_tweet_message()

    assert message.tweet.id == "1700000000000000010"
    assert message.tweet.author_id == "2244994945"
    assert message.media == (media_retry().media,)


@pytest.mark.parametrize(
    "body",
    [
        "{nope",
        "[]",
        '{"kind": "media_retry", "schema_version": Infinity}',
    ],
)
def test_media_retry_rejects_bad_json(body: str) -> None:
    with pytest.raises(MessageError):
        MediaRetry.from_json(body)


@pytest.mark.parametrize("version", [None, 0, 2, "1"])
def test_media_retry_rejects_wrong_schema_version(version: object) -> None:
    data = media_retry().to_dict()
    data["schema_version"] = version

    with pytest.raises(MessageError, match="unsupported schema_version"):
        MediaRetry.from_json(json.dumps(data))


@pytest.mark.parametrize("missing", ["tweet_id", "username", "fetched_at", "media"])
def test_media_retry_rejects_missing_keys(missing: str) -> None:
    data = media_retry().to_dict()
    del data[missing]

    with pytest.raises(MessageError, match="malformed message"):
        MediaRetry.from_json(json.dumps(data))


@pytest.mark.parametrize("tweet_id", ["", None, 12])
def test_media_retry_rejects_a_bad_tweet_id(tweet_id: object) -> None:
    data = media_retry().to_dict()
    data["tweet_id"] = tweet_id

    with pytest.raises(MessageError, match="tweet_id"):
        MediaRetry.from_json(json.dumps(data))


def test_media_retry_rejects_a_media_without_a_key() -> None:
    data = media_retry().to_dict()
    data["media"] = {"type": "photo"}

    with pytest.raises(MessageError, match="malformed message"):
        MediaRetry.from_json(json.dumps(data))


def test_integers_are_clamped_to_what_dynamodb_can_store() -> None:
    huge = 10**45
    media = Media.from_dict(
        {
            "media_key": "m",
            "type": "photo",
            "width": huge,
            "height": -huge,
            "duration_ms": huge,
            "variants": [{"content_type": "video/mp4", "url": "u", "bit_rate": huge}],
        }
    )
    tweet = Tweet.from_dict({"id": "1", "text": "t", "public_metrics": {"likes": huge}})

    assert (media.width, media.height, media.duration_ms) == (
        MAX_STORED_INT,
        -MAX_STORED_INT,
        MAX_STORED_INT,
    )
    assert media.variants[0].bit_rate == MAX_STORED_INT
    assert tweet.public_metrics["likes"] == MAX_STORED_INT


def test_raw_key_is_an_additive_optional_field() -> None:
    """``raw_key`` is omitted when unset (so v1 bodies are byte-identical), round
    trips when set, and a golden v1 body without it still parses."""
    golden = TweetMessage.from_json((FIXTURES / "message_v1.json").read_text())
    assert golden.raw_key is None
    assert "raw_key" not in golden.to_dict()

    keyed = replace(golden, raw_key="raw/1/dt=2026-10-25/r-0001.json.gz")
    again = TweetMessage.from_json(keyed.to_json())

    assert again.raw_key == keyed.raw_key
    assert again.schema_version == SCHEMA_VERSION == 1
    assert json.loads(keyed.to_json())["raw_key"] == keyed.raw_key
