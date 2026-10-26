"""DynamoDB and S3 persistence.

Every write is idempotent, because SQS delivers at least once and the fetch Lambda may
re-send a Tweet after a retry:

- Tweets are keyed by ``tweet_id`` and written conditionally: a delivery replaces the
  stored item only if it is not older (by ``fetched_at`` and ``schema_version``), so a
  stale redelivery can't overwrite newer data. A repeat of the same delivery rewrites
  the same data.
- A post too big for a DynamoDB item has its text uploaded (create-only) to a
  deterministic S3 key first.
- A preview-only message can't replace a stored long post.
- Media objects get a deterministic S3 key, and an object that already exists is not
  downloaded again.
- Media that couldn't be stored on a message's last receive is recorded as
  ``retrying`` and queued for the media-retry function, whose conditional update
  replaces only that entry.
"""

import io
import json
import logging
import mimetypes
import posixpath
import re
import time
from collections.abc import Buffer, Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal, cast, override
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from botocore.exceptions import ClientError

from twitter_timeline_data_pipeline.config import DEFAULT_MAX_MEDIA_BYTES
from twitter_timeline_data_pipeline.models import (
    MAX_STORED_INT,
    Media,
    MediaRetry,
    TweetId,
    TweetMessage,
    clamp_int,
    escape_disallowed_chars,
    utc_now_iso,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sqs import SQSClient

logger = logging.getLogger(__name__)

type MediaStatus = Literal[
    "stored", "exists", "unavailable", "skipped", "too_large", "retrying"
]
"""stored: uploaded now. exists: already in S3. unavailable: the source returned a
4xx other than 429 (or a redirect, which isn't followed). skipped: nothing to download:
a video with only an HLS variant, or a URL that isn't https on an allowed media host.
too_large: over the size cap, by estimate or by counted bytes; ``size_bytes`` holds the
estimate or the count at abort, and nothing is in S3. retrying: transient errors
outlasted the record's last SQS receive, so the Tweet was written without this file and
a message on the media-retry queue will store it later and replace this status;
``source_url`` (and the planned ``s3_key``) are recorded."""

# Media is only downloaded from the X CDN; message bodies are untrusted input.
ALLOWED_MEDIA_HOSTS = frozenset({"pbs.twimg.com", "video.twimg.com"})

_KNOWN_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4"})
_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class TransientMediaError(RuntimeError):
    """A media download failed in a way a retry may fix (timeout, 429, 5xx)."""


class _MediaTooLarge(Exception):
    """The body passed the size cap while streaming; ``size`` is the count so far."""

    def __init__(self, size: int) -> None:
        super().__init__(size)
        self.size = size


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredMedia:
    media_key: str
    type: str
    status: MediaStatus
    s3_key: str | None = None
    content_type: str | None = None
    source_url: str | None = None
    size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class _Target:
    raw_url: str
    url: str  # raw_url at full size
    known_type: str | None
    key: str


class _IterReader(io.RawIOBase):
    """File-like view of a byte iterator, so S3 can stream it without buffering.

    Counts the bytes handed out in ``size``. Raises ``_MediaTooLarge`` when the next
    chunk would pass ``max_bytes`` and ``TransientMediaError`` once ``clock()`` reaches
    ``deadline``. Raising from here makes the S3 transfer abort its multipart upload.
    """

    def __init__(
        self,
        chunks: Iterator[bytes],
        *,
        max_bytes: int | None = None,
        deadline: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._chunks = chunks
        self._max_bytes = max_bytes
        self._deadline = deadline
        self._clock = clock
        self._pending = memoryview(b"")
        self.size = 0

    @override
    def readable(self) -> bool:
        return True

    @override
    def readinto(self, buffer: Buffer) -> int:
        view = memoryview(buffer).cast("B")
        while not self._pending:
            if self._deadline is not None and self._clock() >= self._deadline:
                raise TransientMediaError("download passed its time budget")
            chunk = next(self._chunks, None)
            if chunk is None:
                return 0
            if self._max_bytes is not None and self.size + len(chunk) > self._max_bytes:
                raise _MediaTooLarge(self.size + len(chunk))
            self._pending = memoryview(chunk)
        n = min(len(view), len(self._pending))
        view[:n] = self._pending[:n]
        self._pending = self._pending[n:]
        self.size += n
        return n


def _original_size_url(url: str) -> str:
    """Photos on the media CDN default to a reduced size; ``name=orig`` is full size."""
    parts = urlsplit(url)
    if parts.hostname == "pbs.twimg.com" and parts.path.startswith("/media/"):
        query = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k != "name"
        ]
        query.append(("name", "orig"))
        return urlunsplit(parts._replace(query=urlencode(query)))
    return url


def _extension(content_type: str | None, url: str) -> str:
    """File extension (with dot) knowable before downloading, or ``""``."""
    if content_type and (ext := mimetypes.guess_extension(content_type)):
        return ext
    suffix = posixpath.splitext(urlsplit(url).path)[1].lower()
    return suffix if suffix in _KNOWN_EXTENSIONS else ""


# Cap for a size estimate: any real file is far smaller, and DynamoDB can't store a
# number like the 10**400 a malformed bit_rate would produce (the write would fail).
MAX_SIZE_ESTIMATE = MAX_STORED_INT


def estimated_size(media: Media, url: str) -> int | None:
    """Bytes implied by the chosen variant's ``bit_rate`` and the media's duration.

    None when there is no usable estimate (including a non-positive one); capped at
    ``MAX_SIZE_ESTIMATE`` so it can always be stored.
    """
    if media.type not in ("video", "animated_gif") or not media.duration_ms:
        return None
    for variant in media.variants:
        if variant.url == url and variant.bit_rate:
            estimate = variant.bit_rate * media.duration_ms // 8000
            return min(estimate, MAX_SIZE_ESTIMATE) if estimate > 0 else None
    return None


class MediaStore:
    """Streams media files from the X CDN into S3."""

    def __init__(
        self,
        s3_client: S3Client,
        bucket: str,
        http_client: httpx.Client,
        *,
        max_bytes: int = DEFAULT_MAX_MEDIA_BYTES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._http = http_client
        self._max_bytes = max_bytes
        self._clock = clock

    @staticmethod
    def object_key(message: TweetMessage, media: Media, extension: str) -> str:
        """``media/<author_id>/<tweet_id>/<media_key><extension>``.

        Uses ``author_id`` (stable) and falls back to the lower-cased username when the
        Tweet has none. ``extension`` includes the dot, e.g. ``.jpg``.
        """
        owner = message.tweet.author_id or message.username.lower()
        return f"media/{owner}/{message.tweet.id}/{media.media_key}{extension}"

    def _target(self, message: TweetMessage, media: Media) -> _Target | StoredMedia:
        """Where ``media`` is downloaded from and stored; ``skipped`` if nowhere."""
        skipped = StoredMedia(
            media_key=media.media_key, type=media.type, status="skipped"
        )
        source = media.download_source()
        if source is None:
            return skipped
        raw_url, known_type = source
        try:
            url = _original_size_url(raw_url)
            parts = urlsplit(url)
            host = parts.hostname
        except ValueError:  # malformed URL, e.g. an unclosed IPv6 bracket
            parts, host = None, None
        if parts is None or parts.scheme != "https" or host not in ALLOWED_MEDIA_HOSTS:
            logger.warning(
                "media %s skipped: host %r is not an allowed media host",
                media.media_key,
                host,
            )
            return skipped
        return _Target(
            raw_url=raw_url,
            url=url,
            known_type=known_type,
            key=self.object_key(message, media, _extension(known_type, url)),
        )

    def _cheap_result(
        self, media: Media, target: _Target, *, head: bool = True
    ) -> StoredMedia | None:
        """The outcome knowable without downloading: ``exists`` (HeadObject, skipped
        with ``head=False``) or ``too_large`` (by the size estimate); None when a
        download is needed. Raises for a HeadObject failure other than a missing key."""

        def result(status: MediaStatus, **extra: Any) -> StoredMedia:
            return StoredMedia(
                media_key=media.media_key,
                type=media.type,
                status=status,
                source_url=target.url,
                **extra,
            )

        try:
            if head:
                found = self._s3.head_object(Bucket=self._bucket, Key=target.key)
                return result(
                    "exists",
                    s3_key=target.key,
                    content_type=found.get("ContentType"),
                    size_bytes=found.get("ContentLength"),
                )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in _MISSING_OBJECT_CODES:
                raise
        # Skip before opening a connection when the message already says it's too big.
        estimate = estimated_size(media, target.raw_url)
        if estimate is not None and estimate > self._max_bytes:
            logger.warning(
                "media %s too large: estimated %d bytes", media.media_key, estimate
            )
            return result("too_large", size_bytes=estimate)
        return None

    def precheck(
        self, message: TweetMessage, media: Media, *, head: bool = True
    ) -> StoredMedia:
        """The cheap checks without a download, for a media item that can't be stored.

        Returns ``skipped``, ``too_large`` or ``exists`` when one applies, else
        ``retrying`` with the ``source_url`` and the planned ``s3_key``. ``head=False``
        makes no S3 call. Never raises: a failing HeadObject just leaves the item
        ``retrying``.
        """
        target = self._target(message, media)
        if isinstance(target, StoredMedia):
            return target
        try:
            cheap = self._cheap_result(media, target, head=head)
        except Exception:
            logger.exception("cheap checks failed for media %s", media.media_key)
            cheap = None
        return cheap or StoredMedia(
            media_key=media.media_key,
            type=media.type,
            status="retrying",
            s3_key=target.key,
            source_url=target.url,
        )

    def store(
        self,
        message: TweetMessage,
        media: Media,
        *,
        budget_s: float | None = None,
    ) -> StoredMedia:
        """Upload one media item unless it's already stored.

        Raises ``TransientMediaError`` for failures worth retrying, including a
        download that outlasts ``budget_s`` seconds; returns status ``unavailable``
        for permanent ones and ``too_large`` for files over the size cap, so the Tweet
        is still recorded.
        """
        deadline = None if budget_s is None else self._clock() + budget_s
        target = self._target(message, media)
        if isinstance(target, StoredMedia):
            return target
        url, key = target.url, target.key
        known_type = target.known_type

        def result(status: MediaStatus, **extra: Any) -> StoredMedia:
            return StoredMedia(
                media_key=media.media_key,
                type=media.type,
                status=status,
                source_url=url,
                **extra,
            )

        if (cheap := self._cheap_result(media, target)) is not None:
            return cheap

        timeout: Any = httpx.USE_CLIENT_DEFAULT
        if deadline is not None:
            left = deadline - self._clock()
            if left <= 0:
                raise TransientMediaError("no time left to download")
            # Connect, TLS and headers each get their own timeout, and only body
            # reads are checked against the deadline. A third of the time left per
            # phase keeps the whole exchange inside it, and the body then overruns
            # by at most one read.
            read = self._http.timeout.read
            timeout = httpx.Timeout(left / 3 if read is None else min(read, left / 3))

        try:
            # Redirects stay off so a media host can't bounce the download elsewhere.
            with self._http.stream(
                "GET", url, follow_redirects=False, timeout=timeout
            ) as response:
                status = response.status_code
                if status == 429 or status >= 500:
                    raise TransientMediaError(f"HTTP {status} fetching {url}")
                if status >= 300:
                    logger.warning(
                        "media %s unavailable: HTTP %s", media.media_key, status
                    )
                    return result("unavailable")
                content_type = (
                    known_type
                    or response.headers.get("content-type", "").split(";")[0].strip()
                    or "application/octet-stream"
                )
                declared = response.headers.get("content-length", "")
                if declared.isdigit() and int(declared) > self._max_bytes:
                    logger.warning(
                        "media %s too large: Content-Length %s",
                        media.media_key,
                        declared,
                    )
                    return result("too_large", size_bytes=clamp_int(int(declared)))
                reader = _IterReader(
                    response.iter_bytes(),
                    max_bytes=self._max_bytes,
                    deadline=deadline,
                    clock=self._clock,
                )
                self._s3.upload_fileobj(
                    io.BufferedReader(reader),
                    self._bucket,
                    key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "Metadata": {
                            "tweet-id": message.tweet.id,
                            "media-key": media.media_key,
                            "source-url": url,
                        },
                    },
                )
        except _MediaTooLarge as exc:
            logger.warning(
                "media %s too large: stopped at %d bytes", media.media_key, exc.size
            )
            return result("too_large", size_bytes=exc.size)
        except httpx.TransportError as exc:  # includes timeouts
            raise TransientMediaError(f"{type(exc).__name__} fetching {url}") from exc
        return result(
            "stored", s3_key=key, content_type=content_type, size_bytes=reader.size
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class PutResult:
    """Outcome of ``TweetStore.put_tweet``.

    When ``written`` is False the stored item was newer; ``stored_fetched_at`` and
    ``stored_schema_version`` are its values (None if DynamoDB didn't return them).
    """

    written: bool
    stored_fetched_at: str | None = None
    stored_schema_version: int | None = None


# DynamoDB items are limited to 400 KB (names and values together). A text is moved to
# S3 when the estimated item size is over this, which leaves 50 KB for the estimate's
# error and for the stored prefix and S3 keys. The username-created_at-index projects
# ``text`` too, and an index entry is the key plus the projected attributes, so it
# stays under the limit as well.
ITEM_LIMIT_BYTES = 350_000
# How much of an offloaded text stays in the item as a preview (code points).
TEXT_PREVIEW_CHARS = 2_000


# DynamoDB can't take a lone surrogate (it isn't encodable as UTF-8); the message format
# can carry one, so a text with one is always kept whole in S3 and the item's prefix
# shows U+FFFD in its place.
_LONE_SURROGATE = re.compile("[\ud800-\udfff]")


def estimate_item_size(value: Any) -> int:
    """Approximate DynamoDB size: attribute names and UTF-8 string values, 21 bytes per
    number (the most one takes), 1 per boolean, and 3 per container plus 1 per element
    for the structure. It errs high; DynamoDB's own count is a bit lower."""
    match value:
        case dict():
            return 3 + sum(
                len(k.encode()) + 1 + estimate_item_size(v) for k, v in value.items()
            )
        case list() | tuple():
            return 3 + sum(1 + estimate_item_size(v) for v in value)
        case str():
            return len(value.encode("utf-8", "surrogatepass"))
        case bool():
            return 1
        case _:
            return 21


class TweetStore:
    """Writes Tweet metadata to the tweets table (partition key ``tweet_id``)."""

    def __init__(self, table: Table, s3_client: S3Client, bucket: str) -> None:
        self._table = table
        self._s3 = s3_client
        self._bucket = bucket

    @staticmethod
    def text_key(message: TweetMessage, field: str = "text") -> str:
        """``text/<author_id>/<tweet_id>.json`` (``<tweet_id>.display_text.json`` for
        the preview), with the same owner rule as media."""
        owner = message.tweet.author_id or message.username.lower()
        suffix = "" if field == "text" else f".{field}"
        return f"text/{owner}/{message.tweet.id}{suffix}.json"

    def _offload(self, message: TweetMessage, item: dict[str, Any], field: str) -> None:
        """Upload ``item[field]`` to S3 and leave a prefix and pointer in the item.

        The object is the text as a JSON string (strict UTF-8; characters SQS and UTF-8
        reject are escaped the same way as in the message), so any text, a lone
        surrogate included, reads back exactly with ``json.loads``. The write is
        create-only: a redelivery, or a stale message for the same Tweet (same ID, same
        text), finds it already there and writes nothing.
        """
        key = self.text_key(message, field)
        text: str = item[field]
        body = escape_disallowed_chars(json.dumps(text, ensure_ascii=False))
        try:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
                Metadata={"tweet-id": message.tweet.id},
                IfNoneMatch="*",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "PreconditionFailed":
                raise
        item[field] = _LONE_SURROGATE.sub("\ufffd", text[:TEXT_PREVIEW_CHARS])
        item[f"{field}_truncated"] = True
        item[f"{field}_s3_key"] = key

    def build_item(
        self, message: TweetMessage, media: Sequence[StoredMedia]
    ) -> dict[str, Any]:
        """``to_item``, with texts moved to S3 until the estimated size fits.

        While the item is over ``ITEM_LIMIT_BYTES``, the longer of ``text`` and
        ``display_text`` (when longer than the preview) is offloaded: the item keeps a
        prefix (cut on a code point), ``<field>_truncated: true`` and
        ``<field>_s3_key``. A text with a lone surrogate is offloaded regardless of
        size. Items that fit make no S3 call. This runs before the conditional write.
        """
        item = self.to_item(message, media)
        fields = [f for f in ("text", "display_text") if f in item]
        for field in sorted(fields, key=lambda f: len(item[f]), reverse=True):
            over = (
                estimate_item_size(item) > ITEM_LIMIT_BYTES
                and len(item[field]) > TEXT_PREVIEW_CHARS
            )
            if over or _LONE_SURROGATE.search(item[field]):
                self._offload(message, item, field)
        return item

    @staticmethod
    def to_item(message: TweetMessage, media: Sequence[StoredMedia]) -> dict[str, Any]:
        """The DynamoDB item, in native Python types for the boto3 resource API.

        None values are left out. Includes ``username`` and ``created_at`` for the
        ``username-created_at-index`` GSI.
        """
        tweet = message.tweet
        item: dict[str, Any] = {
            "tweet_id": tweet.id,
            "username": message.username,
            "author_id": tweet.author_id,
            # A long post's full text; the API's truncated preview goes to display_text.
            "text": tweet.full_text,
            "display_text": tweet.text if tweet.note_text else None,
            "is_long_post": bool(tweet.note_text),
            "created_at": tweet.created_at,
            "conversation_id": tweet.conversation_id,
            "lang": tweet.lang,
            "referenced_tweets": [
                {"type": r.type, "id": r.id} for r in tweet.referenced_tweets
            ],
            "public_metrics": dict(tweet.public_metrics),
            "edit_history_tweet_ids": list(tweet.edit_history_tweet_ids),
            "media": [
                {k: v for k, v in asdict(m).items() if v is not None} for m in media
            ],
            "media_count": len(media),
            "fetched_at": message.fetched_at,
            "raw_key": message.raw_key,
            "stored_at": utc_now_iso(),
            "schema_version": message.schema_version,
        }
        return {k: v for k, v in item.items() if v is not None}

    def put_tweet(
        self, message: TweetMessage, media: Sequence[StoredMedia]
    ) -> PutResult:
        """Write the item unless a newer one is stored; say whether it was written.

        The write goes through if there is no item, or if the stored ``fetched_at`` is
        not later than this message's and the stored ``schema_version`` (absent on
        legacy items) is not higher. ``fetched_at`` is an ISO 8601 UTC string with
        second precision, so string comparison is chronological. A tie rewrites the
        item with the same data, which keeps a redelivery idempotent. A message
        without ``note_text`` (a preview, e.g. from a fetcher that predates long posts)
        never replaces a stored long post, whatever its ``fetched_at``: the preview
        would erase the full text. A skipped write returns ``written=False`` with the
        stored values; any other DynamoDB error propagates.
        """
        is_long = bool(message.tweet.note_text)
        keep_long_post = (
            ""
            if is_long
            else " AND (attribute_not_exists(is_long_post) OR is_long_post = :no)"
        )
        values: dict[str, Any] = {
            ":fetched_at": message.fetched_at,
            ":schema_version": message.schema_version,
        }
        if not is_long:
            values[":no"] = False
        try:
            self._table.put_item(
                Item=self.build_item(message, media),
                ConditionExpression=(
                    "attribute_not_exists(tweet_id) OR ("
                    "(attribute_not_exists(fetched_at) "
                    "OR fetched_at <= :fetched_at) AND "
                    "(attribute_not_exists(schema_version) "
                    f"OR schema_version <= :schema_version){keep_long_post})"
                ),
                ExpressionAttributeValues=values,
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == (
                "ConditionalCheckFailedException"
            ):
                old = cast("dict[str, Any]", exc.response.get("Item") or {})
                version = old.get("schema_version", {}).get("N")
                if not is_long and old.get("is_long_post", {}).get("BOOL"):
                    logger.info(
                        "preview-only message for tweet %s not stored: the stored "
                        "item is a long post",
                        message.tweet.id,
                    )
                return PutResult(
                    written=False,
                    stored_fetched_at=old.get("fetched_at", {}).get("S"),
                    stored_schema_version=None if version is None else int(version),
                )
            raise
        return PutResult(written=True)

    def get_media(self, tweet_id: TweetId) -> list[dict[str, Any]] | None:
        """The item's ``media`` entries (strongly consistent), None if no such item."""
        item = self._table.get_item(
            Key={"tweet_id": tweet_id},
            ConsistentRead=True,
            ProjectionExpression="media",
        ).get("Item")
        if item is None:
            return None
        return cast("list[dict[str, Any]]", item.get("media") or [])

    def resolve_media(
        self,
        tweet_id: TweetId,
        index: int,
        media_key: str,
        entry: Mapping[str, Any],
    ) -> bool:
        """Replace ``media[index]`` with ``entry`` if it is still the ``retrying`` entry
        for ``media_key`` of an item that exists; return whether it was replaced.

        False means the item changed since it was read (another delivery stored the
        file, or a newer fetch rewrote the item), so there is nothing left to do. Any
        other DynamoDB error propagates.
        """
        position = f"media[{int(index)}]"
        try:
            self._table.update_item(
                Key={"tweet_id": tweet_id},
                UpdateExpression=f"SET {position} = :entry",
                ConditionExpression=(
                    f"attribute_exists(tweet_id) AND {position}.media_key = :key "
                    f"AND {position}.#status = :retrying"
                ),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":entry": dict(entry),
                    ":key": media_key,
                    ":retrying": "retrying",
                },
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == (
                "ConditionalCheckFailedException"
            ):
                return False
            raise
        return True


# Delay before a media-retry message becomes visible: gives a CDN outage or a brief
# throttle time to pass before the first retry.
MEDIA_RETRY_DELAY_SECONDS = 300


class MediaRetryQueue:
    """Sends media-retry messages to their SQS queue."""

    def __init__(
        self,
        sqs_client: SQSClient,
        queue_url: str,
        *,
        delay_seconds: int = MEDIA_RETRY_DELAY_SECONDS,
    ) -> None:
        self._sqs = sqs_client
        self._queue_url = queue_url
        self._delay = delay_seconds

    def send(self, retry: MediaRetry) -> None:
        """Enqueue ``retry``; any SQS error propagates."""
        self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=retry.to_json(),
            DelaySeconds=self._delay,
        )


def _check_id(value: str | None) -> None:
    if value is not None and not (value.isascii() and value.isdecimal()):
        raise ValueError(f"Tweet ID must be numeric, got {value!r}")


MAX_GAPS = 1500
"""Upper bound on the gap list. A trailing ``next_token`` leaves one empty gap per run,
and each waits 24 h before it can drop, so a 1-minute schedule (the EventBridge
minimum) needs 1,440. At about 110 bytes a gap that is roughly 165 KB, under
DynamoDB's 400 KB item limit. Within the cap nothing is merged away, so waiting
gaps are never joined to (and never delay) gaps that have work to do."""


@dataclass(frozen=True, slots=True)
class Gap:
    """An exclusive range of Tweet IDs, ``since_id < id < until_id``, not fetched yet.

    A missing ``since_id`` means "down to the end of what the API serves"; a missing
    ``until_id`` means "up from the newest Tweet". ``empty_runs`` counts consecutive
    runs in which no page for this gap carried data and the walk ended on a clean
    empty response, and
    ``first_empty_at`` (ISO-8601 UTC, ``...Z``) is when the first of them happened;
    the gap is only dropped after several runs *and* a minimum age (see
    ``handlers.fetch``). Both are unset while ``empty_runs`` is 0. IDs are
    normalized to plain decimal strings.
    """

    until_id: TweetId | None = None
    since_id: TweetId | None = None
    empty_runs: int = 0
    first_empty_at: str | None = None

    def __post_init__(self) -> None:
        for name in ("until_id", "since_id"):
            value = getattr(self, name)
            _check_id(value)
            if value is not None:
                object.__setattr__(self, name, str(int(value)))
        if (
            self.until_id is not None
            and self.since_id is not None
            and int(self.since_id) >= int(self.until_id)
        ):
            raise ValueError(
                f"since_id {self.since_id} must be below until_id {self.until_id}"
            )

    @property
    def sort_key(self) -> float:
        """Upper bound as a number; an open top sorts above every ID."""
        return float("inf") if self.until_id is None else int(self.until_id)

    @property
    def first(self) -> int:
        """Lowest ID in the range."""
        return 1 if self.since_id is None else int(self.since_id) + 1

    @property
    def last(self) -> float:
        """Highest ID in the range (infinite when open at the top)."""
        return float("inf") if self.until_id is None else int(self.until_id) - 1

    def to_item(self) -> dict[str, int | str]:
        # Numbers, not strings: IDs compare as integers.
        item: dict[str, int | str] = {
            name: int(value)
            for name, value in (
                ("until_id", self.until_id),
                ("since_id", self.since_id),
            )
            if value is not None
        }
        if self.empty_runs:
            item["empty_runs"] = self.empty_runs
            if self.first_empty_at:
                item["first_empty_at"] = self.first_empty_at
        return item

    @classmethod
    def from_item(cls, item: Any) -> Gap:
        def tweet_id(name: str) -> TweetId | None:
            value = item.get(name)
            return None if value is None else str(int(value))

        return cls(
            until_id=tweet_id("until_id"),
            since_id=tweet_id("since_id"),
            empty_runs=int(item.get("empty_runs", 0)),
            first_empty_at=item.get("first_empty_at"),  # absent on legacy items
        )


def _union(a: Gap, b: Gap) -> Gap:
    """The smallest gap covering both; only ever adds IDs to re-fetch."""
    same = (a.since_id, a.until_id) == (b.since_id, b.until_id)
    # Merging must never make a drop happen sooner: keep the smaller run count and
    # the later first-empty time (a missing time is unknown, so the known one wins).
    runs = min(a.empty_runs, b.empty_runs) if same else 0
    stamps = [t for t in (a.first_empty_at, b.first_empty_at) if t]
    return Gap(
        until_id=None
        if a.until_id is None or b.until_id is None
        else str(max(int(a.until_id), int(b.until_id))),
        since_id=None
        if a.since_id is None or b.since_id is None
        else str(min(int(a.since_id), int(b.since_id))),
        # Empty responses for part of a range say nothing about the whole.
        empty_runs=runs,
        first_empty_at=max(stamps) if runs and stamps else None,
    )


def merge_gaps(gaps: Iterable[Gap], limit: int = MAX_GAPS) -> tuple[Gap, ...]:
    """Normalize a gap list: drop ranges with no IDs in them, merge overlapping or
    adjacent ones, and, past ``limit``, merge the closest neighbours. A union only
    adds IDs to fetch, so none of this can lose data; at worst it causes duplicates.
    Newest gap first.
    """
    merged: list[Gap] = []
    for gap in sorted((g for g in gaps if g.last >= g.first), key=lambda g: g.first):
        if merged and gap.first <= merged[-1].last + 1:
            merged[-1] = _union(merged[-1], gap)
        else:
            merged.append(gap)
    while len(merged) > limit:
        i = min(
            range(len(merged) - 1), key=lambda i: merged[i + 1].first - merged[i].last
        )
        merged[i : i + 2] = [_union(merged[i], merged[i + 1])]
    return tuple(sorted(merged, key=lambda g: g.sort_key, reverse=True))


@dataclass(frozen=True, slots=True)
class CheckpointState:
    """What one account's state item says.

    ``newest_id`` is the highest Tweet ID enqueued. ``gaps`` are ranges below it (or
    manual backfills) still to fetch. ``version`` is the optimistic-lock counter; items
    written before gaps existed have none and count as version 0.
    """

    newest_id: TweetId | None = None
    gaps: tuple[Gap, ...] = ()
    version: int = 0


class CheckpointStore:
    """Per-username fetch cursor in the state table, written with an optimistic lock."""

    def __init__(self, table: Table) -> None:
        self._table = table

    def load(self, username: str) -> CheckpointState:
        response = self._table.get_item(
            Key={"username": username.lower()}, ConsistentRead=True
        )
        item = cast("dict[str, Any] | None", response.get("Item"))
        if item is None:
            return CheckpointState()
        newest = item.get("newest_id")
        return CheckpointState(
            newest_id=None if newest is None else str(int(newest)),
            gaps=tuple(Gap.from_item(g) for g in item.get("gaps", ())),
            version=int(item.get("version", 0)),
        )

    def save(
        self, username: str, state: CheckpointState, *, expected_version: int
    ) -> bool:
        """Write ``state`` as version ``expected_version + 1``.

        Succeeds only if the stored item is still at ``expected_version`` (or absent,
        for 0), so a slower run can't overwrite a newer cursor. Returns whether it was
        written; ``False`` means another run moved the state.
        """
        item: dict[str, Any] = {
            "username": username.lower(),
            "gaps": [g.to_item() for g in state.gaps],
            "version": expected_version + 1,
            "updated_at": utc_now_iso(),
        }
        if state.newest_id is not None:
            item["newest_id"] = int(state.newest_id)
        # A missing version is 0: a new item or one from before versions existed.
        condition = "version = :expected"
        if expected_version == 0:
            condition = f"attribute_not_exists(version) OR {condition}"
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression=condition,
                ExpressionAttributeValues={":expected": expected_version},
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == (
                "ConditionalCheckFailedException"
            ):
                return False
            raise
        return True
