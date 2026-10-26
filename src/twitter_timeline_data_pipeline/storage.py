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
from collections.abc import Buffer, Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal, cast, override
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from botocore.exceptions import ClientError

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

type MediaStatus = Literal["stored", "exists", "unavailable", "skipped"]
"""stored: uploaded now. exists: already in S3. unavailable: the source returned a
4xx other than 429 (or a redirect, which isn't followed). skipped: nothing to download:
a video with only an HLS variant, or a URL that isn't https on an allowed media host."""

# Media is only downloaded from the X CDN; message bodies are untrusted input.
ALLOWED_MEDIA_HOSTS = frozenset({"pbs.twimg.com", "video.twimg.com"})

_KNOWN_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4"})
_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class TransientMediaError(RuntimeError):
    """A media download failed in a way a retry may fix (timeout, 429, 5xx)."""


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

    Counts the bytes handed out in ``size``.
    """

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._pending = memoryview(b"")
        self.size = 0

    @override
    def readable(self) -> bool:
        return True

    @override
    def readinto(self, buffer: Buffer) -> int:
        view = memoryview(buffer).cast("B")
        while not self._pending:
            chunk = next(self._chunks, None)
            if chunk is None:
                return 0
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


class MediaStore:
    """Streams media files from the X CDN into S3."""

    def __init__(
        self, s3_client: S3Client, bucket: str, http_client: httpx.Client
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._http = http_client

    @staticmethod
    def object_key(message: TweetMessage, media: Media, extension: str) -> str:
        """``media/<author_id>/<tweet_id>/<media_key><extension>``.

        Uses ``author_id`` (stable) and falls back to the lower-cased username when the
        Tweet has none. ``extension`` includes the dot, e.g. ``.jpg``.
        """
        owner = message.tweet.author_id or message.username.lower()
        return f"media/{owner}/{message.tweet.id}/{media.media_key}{extension}"

    def store(self, message: TweetMessage, media: Media) -> StoredMedia:
        """Upload one media item unless it's already stored.

        Raises ``TransientMediaError`` for failures worth retrying; returns status
        ``unavailable`` for permanent ones so the Tweet is still recorded.
        """
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

        try:
            # Redirects stay off so a media host can't bounce the download elsewhere.
            with self._http.stream("GET", url, follow_redirects=False) as response:
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
                reader = _IterReader(response.iter_bytes())
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


def _check_id(value: str | None) -> None:
    if value is not None and not (value.isascii() and value.isdecimal()):
        raise ValueError(f"Tweet ID must be numeric, got {value!r}")


MAX_GAPS = 100
"""Upper bound on the gap list, far below DynamoDB's 400 KB item limit."""


@dataclass(frozen=True, slots=True)
class Gap:
    """An exclusive range of Tweet IDs, ``since_id < id < until_id``, not fetched yet.

    A missing ``since_id`` means "down to the end of what the API serves"; a missing
    ``until_id`` means "up from the newest Tweet". ``empty_runs`` counts consecutive
    runs whose first request for this gap came back cleanly empty; the gap is only
    dropped after several (see ``handlers.fetch``). IDs are normalized to plain
    decimal strings.
    """

    until_id: TweetId | None = None
    since_id: TweetId | None = None
    empty_runs: int = 0

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

    def to_item(self) -> dict[str, int]:
        # Numbers, not strings: IDs compare as integers.
        item = {
            name: int(value)
            for name, value in (
                ("until_id", self.until_id),
                ("since_id", self.since_id),
            )
            if value is not None
        }
        if self.empty_runs:
            item["empty_runs"] = self.empty_runs
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
        )


def _union(a: Gap, b: Gap) -> Gap:
    """The smallest gap covering both; only ever adds IDs to re-fetch."""
    same = (a.since_id, a.until_id) == (b.since_id, b.until_id)
    return Gap(
        until_id=None
        if a.until_id is None or b.until_id is None
        else str(max(int(a.until_id), int(b.until_id))),
        since_id=None
        if a.since_id is None or b.since_id is None
        else str(min(int(a.since_id), int(b.since_id))),
        # Empty responses for part of a range say nothing about the whole.
        empty_runs=min(a.empty_runs, b.empty_runs) if same else 0,
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
