"""DynamoDB and S3 persistence.

Every write is idempotent, because SQS delivers at least once and the fetch Lambda may
re-send a Tweet after a retry:

- Tweets are keyed by ``tweet_id``; a repeat ``put_item`` replaces the item with the
  same data (plus refreshed metrics).
- Media objects get a deterministic S3 key, and an object that already exists is not
  downloaded again.
"""

import io
import logging
import mimetypes
import posixpath
import time
from collections.abc import Buffer, Callable, Iterator, Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal, cast, override
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from botocore.exceptions import ClientError

from twitter_timeline_data_pipeline.config import DEFAULT_MAX_MEDIA_BYTES
from twitter_timeline_data_pipeline.models import (
    Media,
    TweetId,
    TweetMessage,
    utc_now_iso,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

type MediaStatus = Literal[
    "stored", "exists", "unavailable", "skipped", "too_large", "failed"
]
"""stored: uploaded now. exists: already in S3. unavailable: the source returned a
4xx other than 429 (or a redirect, which isn't followed). skipped: nothing to download:
a video with only an HLS variant, or a URL that isn't https on an allowed media host.
too_large: over the size cap, by estimate or by counted bytes; ``size_bytes`` holds the
estimate or the count at abort, and nothing is in S3. failed: transient errors outlasted
the record's last SQS receive, so the Tweet was written without this media."""

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


def estimated_size(media: Media, url: str) -> int | None:
    """Bytes implied by the chosen variant's ``bit_rate`` and the media's duration."""
    if media.type not in ("video", "animated_gif") or not media.duration_ms:
        return None
    for variant in media.variants:
        if variant.url == url and variant.bit_rate:
            return variant.bit_rate * media.duration_ms // 8000
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
        source = media.download_source()
        if source is None:
            return StoredMedia(
                media_key=media.media_key, type=media.type, status="skipped"
            )
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
            return StoredMedia(
                media_key=media.media_key, type=media.type, status="skipped"
            )
        key = self.object_key(message, media, _extension(known_type, url))

        def result(status: MediaStatus, **extra: Any) -> StoredMedia:
            return StoredMedia(
                media_key=media.media_key,
                type=media.type,
                status=status,
                source_url=url,
                **extra,
            )

        try:
            head = self._s3.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in _MISSING_OBJECT_CODES:
                raise
        else:
            return result(
                "exists",
                s3_key=key,
                content_type=head.get("ContentType"),
                size_bytes=head.get("ContentLength"),
            )

        # Skip before opening a connection when the message already says it's too big.
        estimate = estimated_size(media, raw_url)
        if estimate is not None and estimate > self._max_bytes:
            logger.warning(
                "media %s too large: estimated %d bytes", media.media_key, estimate
            )
            return result("too_large", size_bytes=estimate)

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
                    return result("too_large", size_bytes=int(declared))
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


class TweetStore:
    """Writes Tweet metadata to the tweets table (partition key ``tweet_id``)."""

    def __init__(self, table: Table) -> None:
        self._table = table

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
            "text": tweet.text,
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
            "stored_at": utc_now_iso(),
            "schema_version": message.schema_version,
        }
        return {k: v for k, v in item.items() if v is not None}

    def put_tweet(self, message: TweetMessage, media: Sequence[StoredMedia]) -> None:
        self._table.put_item(Item=self.to_item(message, media))


class CheckpointStore:
    """Per-username high-water mark (newest Tweet ID enqueued) in the state table."""

    def __init__(self, table: Table) -> None:
        self._table = table

    def get(self, username: str) -> TweetId | None:
        response = self._table.get_item(
            Key={"username": username.lower()}, ConsistentRead=True
        )
        item = response.get("Item")
        return None if item is None else str(int(cast(Any, item["newest_id"])))

    def advance(self, username: str, newest_id: TweetId) -> bool:
        """Store ``newest_id`` only if it's newer than what's stored.

        Uses a conditional write so a slower, older run can't move the mark backwards.
        Returns whether the item changed.
        """
        try:
            self._table.put_item(
                # A Number, not a string: IDs compare as integers.
                Item={
                    "username": username.lower(),
                    "newest_id": int(newest_id),
                    "updated_at": utc_now_iso(),
                },
                ConditionExpression=(
                    "attribute_not_exists(username) OR newest_id < :new"
                ),
                ExpressionAttributeValues={":new": int(newest_id)},
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == (
                "ConditionalCheckFailedException"
            ):
                return False
            raise
        return True
