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
import os
import posixpath
import re
import time
from collections.abc import Buffer, Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
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
    "stored",
    "exists",
    "unavailable",
    "skipped",
    "skipped_hls",
    "too_large",
    "retrying",
    "missing",
]
"""stored: uploaded now. exists: already in S3. unavailable: the source returned a
4xx other than 429 (or a redirect, which isn't followed). skipped: nothing to download:
no usable URL, or one that isn't https on an allowed media host. skipped_hls: a video
or animated GIF whose only variants are HLS playlists, so there is no file (its
poster frame is still stored, see ``preview_s3_key``). missing: the Tweet attaches the
key but the API's ``includes.media`` did not return it (deleted or withheld media);
nothing else is known about it. too_large: over the size cap, by estimate or by
counted bytes; ``size_bytes`` holds the estimate or the count at abort, and nothing is
in S3. retrying: transient errors
outlasted the record's last SQS receive, so the Tweet was written without this file and
a message on the media-retry queue will store it later and replace this status;
``source_url`` (and the planned ``s3_key`` and ``preview_s3_key``) are recorded. A
poster frame that can't be stored yet also makes the entry ``retrying``."""

# Media is only downloaded from the X CDN; message bodies are untrusted input.
ALLOWED_MEDIA_HOSTS = frozenset({"pbs.twimg.com", "video.twimg.com"})

# TEST ONLY. Comma-separated hostnames that are also allowed, over http or https, so the
# integration tests can serve media from a stub. template.yaml never sets it (a template
# test enforces that), and the handlers log a WARNING when it is active.
MEDIA_HOSTS_OVERRIDE_ENV = "MEDIA_ALLOWED_HOSTS_OVERRIDE"
_HOSTNAME_RE = re.compile(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?")


def media_hosts_override(environ: Mapping[str, str] | None = None) -> frozenset[str]:
    """Extra media hosts from ``MEDIA_ALLOWED_HOSTS_OVERRIDE``; empty when unset.

    Also empty, with an ERROR logged, when ``AWS_ENDPOINT_URL`` is not set: the override
    only makes sense next to an emulated AWS, so a stray copy in a real deployment
    (which uses the default endpoints) is ignored instead of widening the allowlist.

    Raises ``ValueError`` for an entry that isn't a bare hostname (no scheme, port or
    path), so a typo can't silently widen the allowlist.
    """
    env = os.environ if environ is None else environ
    raw = env.get(MEDIA_HOSTS_OVERRIDE_ENV, "")
    hosts = frozenset(h.strip().lower() for h in raw.split(",") if h.strip())
    for host in hosts:
        if not _HOSTNAME_RE.fullmatch(host):
            raise ValueError(
                f"{MEDIA_HOSTS_OVERRIDE_ENV}: {host!r} is not a bare hostname"
            )
    if hosts and not env.get("AWS_ENDPOINT_URL"):
        logger.error(
            "%s is set but AWS_ENDPOINT_URL is not, so this is not an emulated AWS: "
            "ignoring the override. Remove it from this deployment.",
            MEDIA_HOSTS_OVERRIDE_ENV,
        )
        return frozenset()
    return hosts


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
    # The poster frame of a video or animated GIF, when it is stored (or planned).
    preview_s3_key: str | None = None
    # How the poster frame ended: stored, exists, unavailable, too_large or retrying.
    preview_status: MediaStatus | None = None
    preview_size_bytes: int | None = None  # the estimate or count when too_large


def media_entry(stored: StoredMedia, media: Media | None = None) -> dict[str, Any]:
    """The item's ``media`` map for one key: the storage outcome, plus the media's own
    fields from the message (``url``, ``alt_text``, ``width``, ``height``,
    ``duration_ms``, ``preview_image_url``, ``variants``) so nothing the API said is
    lost. None values and an empty variant list are left out."""
    entry: dict[str, Any] = {}
    if media is not None:
        entry = {
            "url": media.url,
            "alt_text": media.alt_text,
            "width": media.width,
            "height": media.height,
            "duration_ms": media.duration_ms,
            "preview_image_url": media.preview_image_url,
            "variants": [
                {k: v for k, v in asdict(variant).items() if v is not None}
                for variant in media.variants
            ]
            or None,
        }
    entry.update(asdict(stored))
    return {k: v for k, v in entry.items() if v is not None}


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
        test_hosts: frozenset[str] = frozenset(),
    ) -> None:
        """``test_hosts`` is for the integration tests only: see
        ``media_hosts_override``."""
        self._test_hosts = test_hosts
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
            media_key=media.media_key,
            type=media.type,
            status="skipped_hls" if media.is_hls_only else "skipped",
        )
        source = media.download_source()
        if source is None:
            return skipped
        raw_url, known_type = source
        target = self._allowed_target(
            media,
            raw_url,
            known_type,
            lambda url: self.object_key(message, media, _extension(known_type, url)),
            "skipped",
        )
        return skipped if target is None else target

    def _preview_target(self, message: TweetMessage, media: Media) -> _Target | None:
        """Where the poster frame of a video or animated GIF is stored, or None when
        there is none to store (other types, no URL, or a host that isn't allowed)."""
        if media.type not in ("video", "animated_gif") or not media.preview_image_url:
            return None
        return self._allowed_target(
            media,
            media.preview_image_url,
            None,
            lambda _: self.object_key(message, media, "_preview.jpg"),
            "preview not stored",
        )

    def _allowed_target(
        self,
        media: Media,
        raw_url: str,
        known_type: str | None,
        key_for: Callable[[str], str],
        outcome: str,
    ) -> _Target | None:
        """A download target for ``raw_url``, or None (logged) when it isn't https on an
        allowed media host."""
        try:
            url = _original_size_url(raw_url)
            parts = urlsplit(url)
            host = parts.hostname
        except ValueError:  # malformed URL, e.g. an unclosed IPv6 bracket
            parts, host = None, None
        allowed = parts is not None and (
            (parts.scheme == "https" and host in ALLOWED_MEDIA_HOSTS)
            or (parts.scheme in ("http", "https") and host in self._test_hosts)
        )
        if parts is None or not allowed:
            logger.warning(
                "media %s %s: host %r is not an allowed media host",
                media.media_key,
                outcome,
                host,
            )
            return None
        return _Target(
            raw_url=raw_url, url=url, known_type=known_type, key=key_for(url)
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
            main = target
        else:
            try:
                cheap = self._cheap_result(media, target, head=head)
            except Exception:
                logger.exception("cheap checks failed for media %s", media.media_key)
                cheap = None
            main = cheap or StoredMedia(
                media_key=media.media_key,
                type=media.type,
                status="retrying",
                s3_key=target.key,
                source_url=target.url,
            )
        preview = self._preview_target(message, media)
        if preview is None:
            return main
        try:
            found = head and self._exists(preview.key)
        except Exception:
            logger.exception("cheap checks failed for preview of %s", media.media_key)
            found = False
        # A poster frame still to be stored keeps the entry open, whatever the file's
        # own outcome: the retry consumer settles both, and the file's outcome is
        # found again there.
        return replace(
            main,
            status=main.status if found else "retrying",
            preview_s3_key=preview.key,
            preview_status="exists" if found else "retrying",
        )

    def _exists(self, key: str) -> bool:
        """Whether ``key`` is in the bucket; raises for an error other than a miss."""
        try:
            self._s3.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in _MISSING_OBJECT_CODES:
                raise
            return False
        return True

    def store(
        self,
        message: TweetMessage,
        media: Media,
        *,
        budget_s: float | None = None,
    ) -> StoredMedia:
        """Upload one media item unless it's already stored.

        A video or animated GIF with a ``preview_image_url`` also gets its poster frame
        stored, the same way (``preview_s3_key``). One that is permanently unavailable
        or too large is just left out of the result.

        Raises ``TransientMediaError`` for failures worth retrying (on either file),
        including a download that outlasts ``budget_s`` seconds; returns status
        ``unavailable`` for permanent ones and ``too_large`` for files over the size
        cap, so the Tweet is still recorded.
        """
        deadline = None if budget_s is None else self._clock() + budget_s
        target = self._target(message, media)
        main = (
            target
            if isinstance(target, StoredMedia)
            else self._download(message, media, target, deadline)
        )
        preview = self._preview_target(message, media)
        if preview is None:
            return main
        stored = self._download(message, media, preview, deadline)
        if stored.s3_key is None:
            logger.warning(
                "media %s preview not stored: %s", media.media_key, stored.status
            )
        return replace(
            main,
            preview_s3_key=stored.s3_key,
            preview_status=stored.status,
            preview_size_bytes=stored.size_bytes
            if stored.status == "too_large"
            else None,
        )

    def _download(
        self,
        message: TweetMessage,
        media: Media,
        target: _Target,
        deadline: float | None,
    ) -> StoredMedia:
        """Store the file at ``target`` unless it is already in S3."""
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

        # Before the HeadObject, so a file (or poster frame) with no time left makes
        # no S3 call either.
        if deadline is not None and deadline - self._clock() <= 0:
            raise TransientMediaError("no time left to download")
        if (cheap := self._cheap_result(media, target)) is not None:
            return cheap

        timeout: Any = httpx.USE_CLIENT_DEFAULT
        if deadline is not None:
            left = max(deadline - self._clock(), 0.001)
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


# How often ``put_tweet`` re-reads and retries when another write lands between its read
# of the stored item and its put; after that it raises ``WriteContentionError``.
MERGE_ATTEMPTS = 3
# What is read from the stored item to merge from, and to guard the put with.
_MERGE_PROJECTION = (
    "media, referenced_tweets, entities, display_entities, in_reply_to_user_id, "
    "possibly_sensitive, edit_controls, withheld, fetched_at, revision"
)
# Fields of a stored referenced-post entry that are carried forward as a whole.
_REFERENCE_KEYS = (
    "text",
    "text_truncated",
    "text_s3_key",
    "display_text",
    "display_text_truncated",
    "display_text_s3_key",
    "is_long_post",
    "author_id",
    "author_username",
    "created_at",
    "entities",
)
# Fields a message only carries when the API was asked for entities: when it wasn't,
# a stored value is kept (when it was, the API's answer wins, absent included).
_ENTITY_REQUEST_FIELDS = (
    "entities",
    "display_entities",
    "possibly_sensitive",
    "edit_controls",
    "withheld",
)


class WriteContentionError(RuntimeError):
    """The Tweet item kept changing between ``put_tweet``'s read and its put. The
    message must be retried (nothing is stored), not treated as stale."""


def _decimals(value: Any) -> Any:
    """``value`` with floats as ``Decimal(repr(f))`` (DynamoDB's resource API rejects
    floats), so an annotation's probability is stored exactly as the API sent it."""
    match value:
        case float():
            return Decimal(repr(value))
        case dict():
            return {k: _decimals(v) for k, v in value.items()}
        case list():
            return [_decimals(v) for v in value]
        case _:
            return value


def reference_s3_keys(item: Mapping[str, Any]) -> set[str]:
    """The S3 keys of the referenced posts' offloaded texts in a stored item."""
    return {
        ref[name]
        for ref in item.get("referenced_tweets") or ()
        for name in ("text_s3_key", "display_text_s3_key")
        if name in ref
    }


def _carry_references(
    item: dict[str, Any], previous: Mapping[str, Any], fetched_at: str
) -> None:
    stored = {
        (r.get("type"), r.get("id")): r for r in previous.get("referenced_tweets") or ()
    }
    for i, ref in enumerate(item.get("referenced_tweets", ())):
        kept = stored.get((ref.get("type"), ref.get("id")))
        if kept is None:
            continue
        if ref.get("status") == "unavailable":
            # Deletion wins: the original is gone (deleted, protected or withheld),
            # so its copy is dropped, not carried (``put_tweet`` deletes its S3
            # objects). Only the fact is kept, and when it was first noted.
            ref["unavailable_since"] = kept.get("unavailable_since", fetched_at)
        elif "text" not in ref and "status" not in ref:
            # The API said nothing about this original (the expansion is off): keep
            # what is stored, marked as no longer refreshed.
            merged = {k: kept[k] for k in (*_REFERENCE_KEYS, "status") if k in kept}
            merged |= {"type": ref["type"], "id": ref["id"]}
            if "unavailable_since" in kept:
                merged["unavailable_since"] = kept["unavailable_since"]
            if "text" in kept:
                merged["stale_since"] = kept.get("stale_since", fetched_at)
                merged["last_seen_fetched_at"] = kept.get(
                    "last_seen_fetched_at", previous.get("fetched_at", fetched_at)
                )
            item["referenced_tweets"][i] = merged


def carry_forward(
    item: dict[str, Any],
    previous: Mapping[str, Any],
    fetched_at: str,
    requested: Mapping[str, bool] | None = None,
) -> None:
    """Merge what the stored item ``previous`` knows into the new ``item``, in place.

    This is the one place where enrichment is merged, not replaced: a newer fetch that
    no longer sees a thing must not erase it (and orphan its S3 objects). Add a rule
    here for each field a later message can lose.

    Media: a stored entry whose key the new item has as ``missing``, or does not list
    at all (a re-fetch without ``attachments``), is kept whole (type, fields, S3
    keys), unless it is itself ``missing``. A kept entry gets ``missing_since`` (the
    first fetch that lost it) and ``last_seen_fetched_at`` (the fetch that last saw
    it). A key that is present again simply replaces its entry.

    ``in_reply_to_user_id`` (it never changes) is always kept when the new item lacks
    it. ``entities``, ``display_entities``, ``possibly_sensitive``, ``edit_controls``
    and ``withheld`` are kept only when the message did not ask the API for them
    (``requested["entities"]`` false or unknown); when it did, the new answer wins.

    Referenced posts: a reference the API said nothing about (the expansion is off)
    keeps the stored copy (text, author, date, entities and a stored ``unavailable``
    status), with ``stale_since`` (the first fetch that lacked it) and
    ``last_seen_fetched_at``. A reference reported ``unavailable`` does NOT keep its
    copy: deletion wins, only ``{id, type, status, unavailable_since}`` remain. One
    with fresh text replaces the stored copy.
    """
    if "in_reply_to_user_id" not in item and "in_reply_to_user_id" in previous:
        item["in_reply_to_user_id"] = previous["in_reply_to_user_id"]
    if not (requested or {}).get("entities"):
        for name in _ENTITY_REQUEST_FIELDS:
            if name not in item and name in previous:
                item[name] = previous[name]
    _carry_references(item, previous, fetched_at)
    media: list[dict[str, Any]] = item.setdefault("media", [])
    index = {e["media_key"]: i for i, e in enumerate(media)}
    for kept in previous.get("media") or ():
        key = kept.get("media_key")
        i = index.get(key)
        if i is not None and media[i].get("status") != "missing":
            continue
        if kept.get("status") != "missing":
            kept = {
                **kept,
                "missing_since": kept.get("missing_since", fetched_at),
                "last_seen_fetched_at": kept.get(
                    "last_seen_fetched_at", previous.get("fetched_at", fetched_at)
                ),
            }
        if i is None:
            index[key] = len(media)
            media.append(kept)
            item.setdefault("media_keys", []).append(key)
        else:
            media[i] = kept
    item["media_count"] = len(media)


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

    def _offload(
        self,
        message: TweetMessage,
        item: dict[str, Any],
        field: str,
        key: str | None = None,
    ) -> None:
        """Upload ``item[field]`` to S3 and leave a prefix and pointer in the item.
        ``item`` is the dict holding the field (the item, or a referenced post's
        entry) and ``key`` the S3 key (default: the Tweet's text key for ``field``).

        The object is the text as a JSON string (strict UTF-8; characters SQS and UTF-8
        reject are escaped the same way as in the message), so any text, a lone
        surrogate included, reads back exactly with ``json.loads``. The write is
        create-only: a redelivery, or a stale message for the same Tweet (same ID, same
        text), finds it already there and writes nothing.
        """
        key = key or self.text_key(message, field)
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
        self,
        message: TweetMessage,
        media: Sequence[StoredMedia],
        previous: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """``to_item``, with texts moved to S3 until the estimated size fits.

        While the item is over ``ITEM_LIMIT_BYTES``, the longer of ``text`` and
        ``display_text`` (when longer than the preview) is offloaded: the item keeps a
        prefix (cut on a code point), ``<field>_truncated: true`` and
        ``<field>_s3_key``. A text with a lone surrogate is offloaded regardless of
        size. Items that fit make no S3 call. This runs before the conditional write.
        ``previous`` (the stored item) is merged in first, see ``carry_forward``, so the
        size estimate counts what will be written.
        """
        item = self.to_item(message, media)
        if previous is not None:
            carry_forward(item, previous, message.fetched_at, message.requested)
        fields = [f for f in ("text", "display_text") if f in item]
        for field in sorted(fields, key=lambda f: len(item[f]), reverse=True):
            over = (
                estimate_item_size(item) > ITEM_LIMIT_BYTES
                and len(item[field]) > TEXT_PREVIEW_CHARS
            )
            if over or _LONE_SURROGATE.search(item[field]):
                self._offload(message, item, field)
        # Referenced posts' texts: kept inline unless the item is still too big (a
        # long original quoted by a long post) or the text can't be stored inline.
        owner = message.tweet.author_id or message.username.lower()
        refs = [
            (ref, f)
            for ref in item.get("referenced_tweets", ())
            for f in ("text", "display_text")
            if f in ref
        ]
        for ref, name in sorted(refs, key=lambda r: len(r[0][r[1]]), reverse=True):
            over = (
                estimate_item_size(item) > ITEM_LIMIT_BYTES
                and len(ref[name]) > TEXT_PREVIEW_CHARS
            )
            if over or _LONE_SURROGATE.search(ref[name]):
                suffix = "" if name == "text" else f".{name}"
                key = f"text/{owner}/{message.tweet.id}.ref.{ref['id']}{suffix}.json"
                self._offload(message, ref, name, key)
        return item

    @staticmethod
    def referenced_entries(message: TweetMessage) -> list[dict[str, Any]]:
        """``referenced_tweets`` for the item: the Tweet's ``{type, id}`` pairs with the
        copies the message carries merged in (original text and author). A copy the
        Tweet doesn't list is appended, so nothing in the message is dropped."""
        copies = {(r.type, r.id): r for r in message.referenced}
        pairs = [(r.type, r.id) for r in message.tweet.referenced_tweets]
        pairs += [k for k in copies if k not in pairs]
        entries = []
        for type_, id_ in pairs:
            entry: dict[str, Any] = {"type": type_, "id": id_}
            if (copy := copies.get((type_, id_))) is not None:
                entry |= {
                    "text": copy.full_text,
                    "display_text": copy.text if copy.note_text else None,
                    "is_long_post": True if copy.note_text else None,
                    "author_id": copy.author_id,
                    "author_username": copy.author_username,
                    "created_at": copy.created_at,
                    "entities": dict(copy.entities) if copy.entities else None,
                    "status": copy.status,
                    "unavailable_since": (
                        message.fetched_at if copy.status == "unavailable" else None
                    ),
                }
            entries.append({k: v for k, v in entry.items() if v is not None})
        return entries

    @staticmethod
    def to_item(message: TweetMessage, media: Sequence[StoredMedia]) -> dict[str, Any]:
        """The DynamoDB item, in native Python types for the boto3 resource API.

        None values are left out. Includes ``username`` and ``created_at`` for the
        ``username-created_at-index`` GSI.
        """
        tweet = message.tweet
        # One entry per attached key, in the Tweet's order. A key with no stored
        # outcome is one the API's ``includes.media`` didn't return: kept as
        # ``missing`` so it can't pass for a text-only Tweet. Media outside the key
        # list (a message that lists none) follows, so nothing stored is dropped.
        stored = {m.media_key: m for m in media}
        described = {m.media_key: m for m in message.media}
        keys = list(dict.fromkeys([*tweet.media_keys, *stored]))
        entries = [
            media_entry(
                stored.get(key)
                or StoredMedia(media_key=key, type="unknown", status="missing"),
                described.get(key),
            )
            for key in keys
        ]
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
            "entities": dict(tweet.entities) if tweet.entities else None,
            "display_entities": (
                dict(tweet.display_entities) if tweet.display_entities else None
            ),
            "in_reply_to_user_id": tweet.in_reply_to_user_id,
            "possibly_sensitive": tweet.possibly_sensitive,
            "edit_controls": dict(tweet.edit_controls) if tweet.edit_controls else None,
            "withheld": dict(tweet.withheld) if tweet.withheld else None,
            "referenced_tweets": TweetStore.referenced_entries(message),
            "public_metrics": dict(tweet.public_metrics),
            "edit_history_tweet_ids": list(tweet.edit_history_tweet_ids),
            "media_keys": keys,
            "media": entries,
            "media_count": len(entries),
            "fetched_at": message.fetched_at,
            "raw_key": message.raw_key,
            "stored_at": utc_now_iso(),
            "schema_version": message.schema_version,
        }
        return {k: _decimals(v) for k, v in item.items() if v is not None}

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
        stored values.

        The stored item is read first (consistent) and merged in by ``carry_forward``;
        the put is conditional on its ``revision``, which every write increments (any
        other in-place updater of the item must ``ADD revision 1`` too, as
        ``resolve_media`` does). ``WriteContentionError`` if it keeps changing; any
        other DynamoDB error propagates.
        """
        is_long = bool(message.tweet.note_text)
        keep_long_post = (
            ""
            if is_long
            else " AND (attribute_not_exists(is_long_post) OR is_long_post = :no)"
        )
        base = (
            "attribute_not_exists(tweet_id) OR ("
            "(attribute_not_exists(fetched_at) "
            "OR fetched_at <= :fetched_at) AND "
            "(attribute_not_exists(schema_version) "
            f"OR schema_version <= :schema_version){keep_long_post})"
        )
        for _ in range(MERGE_ATTEMPTS):
            values: dict[str, Any] = {
                ":fetched_at": message.fetched_at,
                ":schema_version": message.schema_version,
            }
            if not is_long:
                values[":no"] = False
            # Read what is stored, merge it in, and guard the put with its revision,
            # so any write that lands in between (another put, or a media-retry
            # update, which bumps the revision too) is merged on the next attempt,
            # not overwritten.
            previous = cast(
                "dict[str, Any] | None",
                self._table.get_item(
                    Key={"tweet_id": message.tweet.id},
                    ConsistentRead=True,
                    ProjectionExpression=_MERGE_PROJECTION,
                ).get("Item"),
            )
            read_revision = None if previous is None else previous.get("revision")
            if previous is None:
                guard = "attribute_not_exists(tweet_id)"
            elif read_revision is None:
                guard = "attribute_not_exists(revision)"
            else:
                values[":read_revision"] = read_revision
                guard = "revision = :read_revision"
            item = self.build_item(message, media, previous)
            item["revision"] = int(read_revision or 0) + 1
            try:
                self._table.put_item(
                    Item=item,
                    ConditionExpression=f"({base}) AND {guard}",
                    ExpressionAttributeValues=values,
                    ReturnValuesOnConditionCheckFailure="ALL_OLD",
                )
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") != (
                    "ConditionalCheckFailedException"
                ):
                    raise
                old = cast("dict[str, Any]", exc.response.get("Item") or {})
                now = old.get("revision", {}).get("N")
                # A failure is either a write since the read (the revision moved, or
                # the item appeared) or the base condition on the state that was
                # read, which is a stale message.
                if previous is None or (None if now is None else int(now)) != (
                    None if read_revision is None else int(read_revision)
                ):
                    logger.info(
                        "tweet %s changed while it was merged; reading it again",
                        message.tweet.id,
                    )
                    continue
                return self._stale(message, old, is_long=is_long)
            if previous is not None:
                self._delete_orphans(
                    reference_s3_keys(previous) - reference_s3_keys(item), message
                )
            return PutResult(written=True)
        raise WriteContentionError(
            f"tweet {message.tweet.id} changed under {MERGE_ATTEMPTS} merge attempts"
        )

    def _delete_orphans(self, keys: set[str], message: TweetMessage) -> None:
        """Delete the S3 texts of referenced posts the written item no longer holds (an
        original that is now ``unavailable``: deletion wins). After the put, so a lost
        race or a stale message never leaves an item pointing at a deleted object; a
        failure is logged, not raised, because a retry would no longer see the keys."""
        for key in sorted(keys):
            try:
                self._s3.delete_object(Bucket=self._bucket, Key=key)
            except ClientError:
                logger.exception(
                    "could not delete %s of an unavailable original (tweet %s)",
                    key,
                    message.tweet.id,
                )

    @staticmethod
    def _stale(
        message: TweetMessage, old: Mapping[str, Any], *, is_long: bool
    ) -> PutResult:
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
                # ADD revision: this write must fail a concurrent put_tweet's guard.
                UpdateExpression=f"SET {position} = :entry ADD revision :one",
                ConditionExpression=(
                    f"attribute_exists(tweet_id) AND {position}.media_key = :key "
                    f"AND {position}.#status = :retrying"
                ),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":entry": dict(entry),
                    ":key": media_key,
                    ":retrying": "retrying",
                    ":one": 1,
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
    written before gaps existed have none and count as version 0. ``user_id`` is the
    numeric ID the handle resolved to, ``user_id_resolved_at`` (ISO-8601 UTC, ``Z``)
    when it was last confirmed; items written before they existed have neither.
    """

    newest_id: TweetId | None = None
    gaps: tuple[Gap, ...] = ()
    version: int = 0
    user_id: str | None = None
    user_id_resolved_at: str | None = None


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
        user_id = item.get("user_id")
        return CheckpointState(
            newest_id=None if newest is None else str(int(newest)),
            gaps=tuple(Gap.from_item(g) for g in item.get("gaps", ())),
            version=int(item.get("version", 0)),
            user_id=None if user_id is None else str(user_id),
            user_id_resolved_at=item.get("user_id_resolved_at"),
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
        if state.user_id is not None:
            item["user_id"] = state.user_id
        if state.user_id_resolved_at is not None:
            item["user_id_resolved_at"] = state.user_id_resolved_at
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
