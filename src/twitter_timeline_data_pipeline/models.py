"""Data model shared by the Lambdas.

The fetch Lambda turns X API v2 timeline pages into ``TweetMessage`` objects and sends
each one to SQS as JSON. The process Lambda parses that JSON back. Every message
carries ``schema_version`` so a reader can reject a format it doesn't understand.
"""

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn, Self

logger = logging.getLogger(__name__)

# Stays 1 on purpose. ``note_text`` is an additive, optional field: a reader builds a
# Tweet from explicit keys and ignores unknown ones, so an old processor still parses
# a new message (it just stores the preview), and a new processor reading an old
# message gets ``note_text=None``. Bump only for a change a reader can't ignore.
SCHEMA_VERSION = 1

# SQS accepts only #x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF.
# json.dumps already escapes the control characters below #x20; the rest of what SQS
# rejects (or UTF-8 can't encode) is matched here: surrogate code points (a lone
# surrogate can't be encoded) plus U+FFFE and U+FFFF.
_DISALLOWED_CHARS = re.compile("[\ud800-\udfff\ufffe\uffff]")


def escape_disallowed_chars(json_text: str) -> str:
    """Escape (as ``\\uXXXX``) the characters in JSON text that SQS rejects or UTF-8
    can't encode, so the text encodes as strict UTF-8 and parses back unchanged."""
    return _DISALLOWED_CHARS.sub(lambda m: f"\\u{ord(m[0]):04x}", json_text)


type TweetId = str
type MediaKey = str
type JSONObject = dict[str, Any]


class MessageError(ValueError):
    """An SQS message body that can never be processed (bad JSON or wrong schema)."""


MAX_CLOCK_SKEW = timedelta(minutes=5)


def normalize_fetched_at(value: object) -> str:
    """``value`` as UTC ISO 8601 with ``Z``, to the second; ``MessageError`` if invalid.

    Stored ``fetched_at`` values are compared as strings by the conditional write, so
    every one must have exactly this format, carry a timezone, and not be in the future
    (a far-future value would block every later write for that Tweet).
    """
    if not isinstance(value, str):
        raise MessageError(f"fetched_at must be a string, got {value!r}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise MessageError(f"fetched_at is not an ISO 8601 time: {value!r}") from None
    if parsed.tzinfo is None:
        raise MessageError(f"fetched_at has no timezone: {value!r}")
    parsed = parsed.astimezone(UTC)
    if parsed > datetime.now(UTC) + MAX_CLOCK_SKEW:
        raise MessageError(f"fetched_at is in the future: {value!r}")
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def utc_now_iso() -> str:
    """Current UTC time as ISO 8601 with a ``Z`` suffix, to the second."""
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def is_newer(candidate: TweetId, than: TweetId | None) -> bool:
    """Whether ``candidate`` is a later Tweet than ``than``.

    Tweet IDs are snowflakes: numeric strings that grow over time, so they compare as
    integers, not as strings ("99" < "100").
    """
    return than is None or int(candidate) > int(than)


def normalize_username(username: str) -> str:
    """The form used everywhere: no leading ``@``, lower case (X handles are
    case-insensitive), so one account always maps to one checkpoint and GSI key."""
    return username.removeprefix("@").lower()


def _reject_constant(name: str) -> NoReturn:
    raise ValueError(f"non-finite JSON constant {name}")


# Largest magnitude of any integer that is stored. DynamoDB numbers have 38 digits, so a
# malformed message with a 45-digit count would make every write raise and the record
# would loop to the DLQ forever. Real values are far smaller, so clamp instead.
MAX_STORED_INT = 2**63 - 1


def clamp_int(value: int) -> int:
    """``value`` limited to ``+-MAX_STORED_INT``."""
    return max(-MAX_STORED_INT, min(MAX_STORED_INT, value))


def valid_tweet_id(value: object) -> bool:
    """Whether ``value`` is a Tweet ID: a string of ASCII decimal digits."""
    return isinstance(value, str) and value.isascii() and value.isdecimal()


def _opt_int(value: Any) -> int | None:
    return None if value is None else clamp_int(int(value))


def _opt_str(value: Any) -> str | None:
    return None if value is None else str(value)


@dataclass(frozen=True, slots=True, kw_only=True)
class MediaVariant:
    """One encoding of a video or animated GIF."""

    content_type: str
    url: str
    bit_rate: int | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        return cls(
            content_type=str(data["content_type"]),
            url=str(data["url"]),
            bit_rate=_opt_int(data.get("bit_rate")),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class Media:
    """A media object from the API's ``includes.media``.

    The API and the SQS message use the same field names, so ``from_dict`` reads both.
    """

    media_key: MediaKey
    # A plain str so an unknown future media type is carried through, not rejected.
    type: str
    url: str | None = None
    preview_image_url: str | None = None
    variants: tuple[MediaVariant, ...] = ()
    width: int | None = None
    height: int | None = None
    duration_ms: int | None = None
    alt_text: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        return cls(
            media_key=str(data["media_key"]),
            type=str(data["type"]),
            url=_opt_str(data.get("url")),
            preview_image_url=_opt_str(data.get("preview_image_url")),
            variants=tuple(
                MediaVariant.from_dict(v) for v in data.get("variants") or ()
            ),
            width=_opt_int(data.get("width")),
            height=_opt_int(data.get("height")),
            duration_ms=_opt_int(data.get("duration_ms")),
            alt_text=_opt_str(data.get("alt_text")),
        )

    @property
    def is_hls_only(self) -> bool:
        """A video or animated GIF that lists variants but none is ``video/mp4``
        (the HLS playlist isn't a file), so there is no file to download."""
        return (
            self.type in ("video", "animated_gif")
            and bool(self.variants)
            and self.download_source() is None
        )

    def download_source(self) -> tuple[str, str | None] | None:
        """The ``(url, content_type)`` of the file worth storing, or None.

        Photos have a direct ``url``. Videos and animated GIFs only have variants; the
        highest-bit-rate ``video/mp4`` one is chosen (an animated GIF has a single mp4
        variant with bit rate 0). HLS playlists aren't files, so a video with no mp4
        variant has nothing to download. ``content_type`` is None when the API didn't
        state it; the downloader then uses the response header.
        """
        match self.type:
            case "photo":
                return (self.url, None) if self.url else None
            case "video" | "animated_gif":
                mp4s = [v for v in self.variants if v.content_type == "video/mp4"]
                if not mp4s:
                    return None
                best = max(mp4s, key=lambda v: v.bit_rate or 0)
                return best.url, best.content_type
            case _:
                return (self.url, None) if self.url else None


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferencedTweet:
    type: str  # "retweeted" | "quoted" | "replied_to"
    id: TweetId


@dataclass(frozen=True, slots=True, kw_only=True)
class Tweet:
    id: TweetId
    text: str
    # ISO 8601 exactly as the API returns it, e.g. "2023-05-03T14:52:05.000Z". Kept as a
    # string because it sorts correctly and is the DynamoDB GSI sort key.
    created_at: str | None = None
    author_id: str | None = None
    conversation_id: str | None = None
    lang: str | None = None
    referenced_tweets: tuple[ReferencedTweet, ...] = ()
    public_metrics: Mapping[str, int] = field(default_factory=dict)
    edit_history_tweet_ids: tuple[TweetId, ...] = ()
    media_keys: tuple[MediaKey, ...] = ()
    # Full text of a long post (over 280 characters), from ``note_tweet.text``. For
    # those posts ``text`` is only a truncated preview.
    note_text: str | None = None

    @property
    def full_text(self) -> str:
        """The whole post: ``note_text`` for a long post, else ``text``."""
        return self.note_text or self.text

    @classmethod
    def from_api(cls, data: Mapping[str, Any]) -> Self:
        """Build from an API Tweet object (media keys live under ``attachments``).

        Raises ``ValueError`` unless ``id`` is a decimal string (or a whole number):
        an ID the cursor can't use must not get as far as the queue.
        """
        raw_id = data["id"]
        if type(raw_id) is int and raw_id >= 0:
            raw_id = str(raw_id)  # a JSON number is coerced, a float or null is not
        if not valid_tweet_id(raw_id):
            raise ValueError(f"not a Tweet ID: {raw_id!r}")
        attachments = data.get("attachments") or {}
        # X is renaming the field (the docs and OpenAPI spec show ``note_post``).
        note = data.get("note_tweet") or data.get("note_post") or {}
        return cls._build(
            data,
            media_keys=attachments.get("media_keys") or (),
            note_text=note.get("text"),
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        """Build from the ``tweet`` object of an SQS message (flat ``media_keys``)."""
        return cls._build(
            data,
            media_keys=data.get("media_keys") or (),
            note_text=data.get("note_text"),
        )

    @classmethod
    def _build(
        cls, data: Mapping[str, Any], *, media_keys: Any, note_text: Any
    ) -> Self:
        return cls(
            id=str(data["id"]),
            text=str(data["text"]),
            created_at=_opt_str(data.get("created_at")),
            author_id=_opt_str(data.get("author_id")),
            conversation_id=_opt_str(data.get("conversation_id")),
            lang=_opt_str(data.get("lang")),
            referenced_tweets=tuple(
                ReferencedTweet(type=str(r["type"]), id=str(r["id"]))
                for r in data.get("referenced_tweets") or ()
            ),
            public_metrics={
                str(k): clamp_int(int(v))
                for k, v in (data.get("public_metrics") or {}).items()
            },
            edit_history_tweet_ids=tuple(
                str(i) for i in data.get("edit_history_tweet_ids") or ()
            ),
            media_keys=tuple(str(k) for k in media_keys),
            note_text=_opt_str(note_text),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class TweetMessage:
    """The SQS message body: one Tweet plus the media attached to it."""

    username: str
    tweet: Tweet
    media: tuple[Media, ...] = ()
    # Keys the Tweet attaches that the API's ``includes.media`` did not return
    # (deleted or withheld media). Additive and optional, like ``note_text``: an old
    # reader ignores it, and a message without it means none were reported missing.
    missing_media_keys: tuple[MediaKey, ...] = ()
    fetched_at: str = field(default_factory=utc_now_iso)
    schema_version: int = SCHEMA_VERSION
    raw_key: str | None = None
    """S3 key of the raw API page this Tweet came from (lineage). Additive and
    optional: omitted from the body when None, so an old reader never sees it and a
    message from an older fetcher parses with None."""

    def to_dict(self) -> JSONObject:
        data = asdict(self)
        if data["raw_key"] is None:
            del data["raw_key"]
        return data

    def to_json(self) -> str:
        # UTF-8, escaping only what SQS rejects or UTF-8 can't encode. Escaping every
        # non-ASCII character inflated emoji 3 to 6 times and ate the size limit.
        body = json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)
        return escape_disallowed_chars(body)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        version = data.get("schema_version")
        if type(version) is not int or version != SCHEMA_VERSION:
            raise MessageError(
                f"unsupported schema_version {version!r}; expected {SCHEMA_VERSION}"
            )
        return cls(
            username=str(data["username"]),
            tweet=Tweet.from_dict(data["tweet"]),
            media=tuple(Media.from_dict(m) for m in data.get("media") or ()),
            missing_media_keys=tuple(
                str(k) for k in data.get("missing_media_keys") or ()
            ),
            fetched_at=normalize_fetched_at(data["fetched_at"]),
            schema_version=version,
            raw_key=_opt_str(data.get("raw_key")),
        )

    @classmethod
    def from_json(cls, body: str) -> Self:
        """Parse an SQS body. Raises ``MessageError`` for anything malformed."""
        try:
            data = json.loads(body, parse_constant=_reject_constant)
            if not isinstance(data, dict):
                raise MessageError(f"expected a JSON object, got {type(data).__name__}")
            return cls.from_dict(data)
        except MessageError:
            raise
        except (
            ValueError,
            KeyError,
            TypeError,
            AttributeError,
            OverflowError,
            RecursionError,
        ) as exc:
            raise MessageError(f"malformed message: {exc!r}") from exc


MEDIA_RETRY_KIND = "media_retry"


@dataclass(frozen=True, slots=True, kw_only=True)
class MediaRetry:
    """The media-retry queue message: one media item whose Tweet is already stored.

    ``kind`` separates it from a ``TweetMessage`` (which has no such field), so one
    can't be mistaken for the other if a message lands on the wrong queue.
    """

    tweet_id: TweetId
    username: str
    media: Media
    author_id: str | None = None
    fetched_at: str = field(default_factory=utc_now_iso)
    schema_version: int = SCHEMA_VERSION

    def to_dict(self) -> JSONObject:
        return {
            "kind": MEDIA_RETRY_KIND,
            "schema_version": self.schema_version,
            "tweet_id": self.tweet_id,
            "username": self.username,
            "author_id": self.author_id,
            "fetched_at": self.fetched_at,
            "media": asdict(self.media),
        }

    def to_json(self) -> str:
        body = json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)
        return _DISALLOWED_CHARS.sub(lambda m: f"\\u{ord(m[0]):04x}", body)

    def to_tweet_message(self) -> TweetMessage:
        """A minimal message with what the media store needs to name the S3 object."""
        return TweetMessage(
            username=self.username,
            tweet=Tweet(id=self.tweet_id, text="", author_id=self.author_id),
            media=(self.media,),
            fetched_at=self.fetched_at,
            schema_version=self.schema_version,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        if data.get("kind") != MEDIA_RETRY_KIND:
            raise MessageError(
                f"not a {MEDIA_RETRY_KIND} message: {data.get('kind')!r}"
            )
        version = data.get("schema_version")
        if type(version) is not int or version != SCHEMA_VERSION:
            raise MessageError(
                f"unsupported schema_version {version!r}; expected {SCHEMA_VERSION}"
            )
        tweet_id = data["tweet_id"]
        if not isinstance(tweet_id, str) or not tweet_id:
            raise MessageError(f"tweet_id must be a non-empty string: {tweet_id!r}")
        return cls(
            tweet_id=tweet_id,
            username=str(data["username"]),
            author_id=_opt_str(data.get("author_id")),
            media=Media.from_dict(data["media"]),
            fetched_at=normalize_fetched_at(data["fetched_at"]),
            schema_version=version,
        )

    @classmethod
    def from_json(cls, body: str) -> Self:
        """Parse an SQS body. Raises ``MessageError`` for anything malformed."""
        try:
            data = json.loads(body, parse_constant=_reject_constant)
            if not isinstance(data, dict):
                raise MessageError(f"expected a JSON object, got {type(data).__name__}")
            return cls.from_dict(data)
        except MessageError:
            raise
        except (
            ValueError,
            KeyError,
            TypeError,
            AttributeError,
            OverflowError,
            RecursionError,
        ) as exc:
            raise MessageError(f"malformed message: {exc!r}") from exc


@dataclass(frozen=True, slots=True, kw_only=True)
class RawPage:
    """The request and the unparsed response body of one timeline page, as landed in
    S3. ``params`` never holds credentials (the token is a header, not a parameter)."""

    endpoint: str
    params: Mapping[str, str | int]
    body: JSONObject


@dataclass(frozen=True, slots=True, kw_only=True)
class QuarantinedItem:
    """A Tweet or media object of an API page that could not be parsed. ``item`` is
    the JSON exactly as received; ``tweet_id`` is its ``id`` when it has a usable one,
    so the cursor can still move past it."""

    kind: str  # "tweet" | "media"
    item: Any
    error: str
    tweet_id: TweetId | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TimelinePage:
    """One page of ``GET /2/users/:id/tweets`` with its media expansion resolved."""

    tweets: tuple[Tweet, ...]
    media: Mapping[MediaKey, Media]
    newest_id: TweetId | None = None
    oldest_id: TweetId | None = None
    next_token: str | None = None
    raw: RawPage | None = field(default=None, compare=False, repr=False)
    """The response this page was parsed from; None for a page built by hand."""
    quarantined: tuple[QuarantinedItem, ...] = ()
    """Items of the page that failed to parse. They are not in ``tweets``/``media``."""

    def cursor_ids(self) -> list[int]:
        """The Tweet IDs the cursor may treat as seen: the parsed Tweets, plus the IDs
        of quarantined ones that lie within the page's own ID range (the parsed Tweets
        and ``meta.oldest_id``/``newest_id``). A bad Tweet's ``id`` is untrusted: one
        outside that range could skip real Tweets or push ``newest_id`` past them, so
        it counts as having no ID."""
        ids = [int(t.id) for t in self.tweets]
        bounds = [
            int(i)
            for i in (self.oldest_id, self.newest_id)
            if i is not None and valid_tweet_id(i)  # meta is untrusted too
        ]
        span = [*ids, *bounds]
        if span:
            low, high = min(span), max(span)
            ids += [
                int(q.tweet_id)
                for q in self.quarantined
                if q.tweet_id and low <= int(q.tweet_id) <= high
            ]
        return ids

    def messages(
        self,
        username: str,
        *,
        fetched_at: str | None = None,
        raw_key: str | None = None,
    ) -> list[TweetMessage]:
        """One message per Tweet, each carrying the media its ``media_keys`` name.

        A key missing from ``includes.media`` (deleted or withheld media) is logged and
        carried in ``missing_media_keys`` rather than failing the page or vanishing.
        """
        stamp = fetched_at or utc_now_iso()
        messages = []
        for tweet in self.tweets:
            media = []
            missing = []
            for key in tweet.media_keys:
                if (item := self.media.get(key)) is None:
                    logger.warning(
                        "media key %s of tweet %s missing from includes", key, tweet.id
                    )
                    missing.append(key)
                    continue
                media.append(item)
            messages.append(
                TweetMessage(
                    username=username,
                    tweet=tweet,
                    media=tuple(media),
                    missing_media_keys=tuple(missing),
                    fetched_at=stamp,
                    raw_key=raw_key,
                )
            )
        return messages
