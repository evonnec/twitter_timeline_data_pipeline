"""Raw API pages in S3: landing them on fetch and reading them back for replay.

Every timeline page the fetch run receives is written, gzipped, to the media bucket
under ``raw/`` *before* its messages are enqueued. Key layout::

    raw/<user_id>/dt=YYYY-MM-DD/<run_id>-<page_no>.json.gz

``dt`` is the UTC date of the fetch, ``run_id`` is unique per run and ``page_no``
counts the pages of that run from 1. Each object is an *envelope*::

    {"schema_version": 1, "username", "user_id", "fetched_at",
     "request": {"endpoint", "params"}, "response": <the body as received>}

so a page can be re-parsed later (``replay``) without calling the API again. The
object keys are unique per page and never overwritten.
"""

import functools
import gzip
import json
import logging
import uuid
import zlib
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from twitter_timeline_data_pipeline.config import ConfigError
from twitter_timeline_data_pipeline.models import (
    JSONObject,
    RawPage,
    ReferencedPost,
    TweetMessage,
    split_entities,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

RAW_PREFIX = "raw/"
ENVELOPE_VERSION = 1


class RawPageError(ValueError):
    """A raw object that can't be read back (not gzip, not JSON, wrong envelope)."""


@functools.cache
def s3_client() -> S3Client:
    return boto3.client("s3")


def raw_key(user_id: str, fetched_at: str, run_id: str, page_no: int) -> str:
    """``raw/<user_id>/dt=<date of fetched_at>/<run_id>-<page_no>.json.gz``."""
    return f"{RAW_PREFIX}{user_id}/dt={fetched_at[:10]}/{run_id}-{page_no:04d}.json.gz"


def new_run_id() -> str:
    return uuid.uuid4().hex


@dataclass(frozen=True, slots=True, kw_only=True)
class Envelope:
    """One landed page: who and when, the request, and the response body."""

    username: str
    user_id: str
    fetched_at: str
    request: Mapping[str, Any]
    response: JSONObject
    redactions: tuple[str, ...] = ()
    """IDs of Tweets removed from ``response`` or to be ignored (a deletion request);
    replay skips them. Written only by the compliance job; omitted when empty."""

    def to_dict(self) -> JSONObject:
        data: JSONObject = {
            "schema_version": ENVELOPE_VERSION,
            "username": self.username,
            "user_id": self.user_id,
            "fetched_at": self.fetched_at,
            "request": dict(self.request),
            "response": self.response,
        }
        if self.redactions:
            data["redactions"] = list(self.redactions)
        return data

    def to_bytes(self) -> bytes:
        # ASCII-only JSON: a lone surrogate in the response (valid JSON escape) can't
        # be written as UTF-8, and this keeps every byte of the body.
        text = json.dumps(self.to_dict(), separators=(",", ":"))
        return gzip.compress(text.encode(), mtime=0)

    @classmethod
    def from_bytes(cls, data: bytes) -> Envelope:
        """Parse a landed object. Raises ``RawPageError`` for anything malformed."""
        try:
            parsed = json.loads(gzip.decompress(data))
        except (OSError, EOFError, zlib.error, ValueError) as exc:
            raise RawPageError(f"not a gzipped JSON object: {exc!r}") from exc
        if not isinstance(parsed, dict):
            raise RawPageError(f"expected a JSON object, got {type(parsed).__name__}")
        version = parsed.get("schema_version")
        if type(version) is not int or version != ENVELOPE_VERSION:
            raise RawPageError(
                f"unsupported envelope schema_version {version!r}; "
                f"expected {ENVELOPE_VERSION}"
            )
        try:
            response = parsed["response"]
            request = parsed["request"]
            if not isinstance(response, dict) or not isinstance(request, dict):
                raise TypeError("request and response must be objects")
            redactions = parsed.get("redactions", [])
            if not isinstance(redactions, list) or not all(
                isinstance(r, str) for r in redactions
            ):
                raise TypeError("redactions must be a list of Tweet IDs")
            return cls(
                username=str(parsed["username"]),
                user_id=str(parsed["user_id"]),
                fetched_at=str(parsed["fetched_at"]),
                request=request,
                response=response,
                redactions=tuple(redactions),
            )
        except (KeyError, TypeError) as exc:
            raise RawPageError(f"malformed envelope: {exc!r}") from exc


def redact_envelope(body: bytes, tweet_ids: Collection[str]) -> tuple[bytes, set[str]]:
    """``body`` (a landed object) without the Tweets in ``tweet_ids``.

    Removes them from ``response.data`` and from ``response.includes.tweets`` (the
    posts another Tweet quotes, replies to or retweets), drops the ``includes.media``
    only they used, and adds their IDs to ``redactions`` (which replay honors).
    Returns the new object and the IDs it removed; with none removed the new object
    is ``body`` unchanged. Raises ``RawPageError`` if ``body`` can't be read.
    """
    envelope = Envelope.from_bytes(body)
    response = envelope.response
    includes = response.get("includes")
    included = includes.get("tweets") if isinstance(includes, dict) else None
    data = response.get("data")
    tweets = [
        t
        for source in (data, included)
        if isinstance(source, list)
        for t in source
        if isinstance(t, dict) and "id" in t
    ]
    gone = {str(t["id"]) for t in tweets} & {str(i) for i in tweet_ids}
    if not gone:
        return body, set()

    def media_keys(tweets: list[Any]) -> set[str]:
        return {
            str(k)
            for t in tweets
            if isinstance(t, dict)
            for k in (t.get("attachments") or {}).get("media_keys", ())
        }

    def keep(source: list[Any]) -> list[Any]:
        return [
            t for t in source if not (isinstance(t, dict) and str(t.get("id")) in gone)
        ]

    dropped = media_keys([t for t in tweets if str(t["id"]) in gone])
    still_used = media_keys([t for t in tweets if str(t["id"]) not in gone])
    new_response: JSONObject = dict(response)
    if isinstance(data, list):
        new_response["data"] = keep(data)
    if isinstance(includes, dict):
        new_includes = dict(includes)
        if isinstance(included, list):
            new_includes["tweets"] = keep(included)
        if isinstance(includes.get("media"), list):
            new_includes["media"] = [
                m
                for m in includes["media"]
                if not (
                    isinstance(m, dict)
                    and str(m.get("media_key")) in dropped - still_used
                )
            ]
        new_response["includes"] = new_includes
    redacted = replace(
        envelope,
        response=new_response,
        redactions=tuple(sorted({*envelope.redactions, *gone})),
    )
    return redacted.to_bytes(), gone


class RawPageStore:
    """Writes the raw pages of one fetch run."""

    def __init__(self, s3: S3Client, bucket: str, *, run_id: str | None = None) -> None:
        self.s3 = s3
        self.bucket = bucket
        self.run_id = run_id or new_run_id()

    def put(
        self,
        *,
        username: str,
        user_id: str,
        fetched_at: str,
        page_no: int,
        page: RawPage,
    ) -> str:
        """Upload one page and return its key. Raises if the upload fails: the caller
        must not enqueue the page's messages then."""
        # Written once: the key holds the run's ID, so an existing object is this same
        # page from an earlier attempt, and is left as it is.
        key = raw_key(user_id, fetched_at, self.run_id, page_no)
        envelope = Envelope(
            username=username,
            user_id=user_id,
            fetched_at=fetched_at,
            request={"endpoint": page.endpoint, "params": dict(page.params)},
            response=page.body,
        )
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=envelope.to_bytes(),
                ContentType="application/json",
                ContentEncoding="gzip",
                IfNoneMatch="*",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "PreconditionFailed":
                raise
            logger.info("raw page %s already landed", key)
        return key


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """``s3://bucket/raw/prefix`` -> ``(bucket, prefix)``. ``ConfigError`` unless the
    prefix is under ``raw/``. The prefix always ends in ``/``, so it matches whole path
    segments (``raw/1`` does not select ``raw/10/...``)."""
    parsed = urlparse(uri)
    prefix = parsed.path.removeprefix("/")
    if parsed.scheme != "s3" or not parsed.netloc or not prefix.startswith(RAW_PREFIX):
        raise ConfigError(f"expected s3://<bucket>/{RAW_PREFIX}<prefix>, got {uri!r}")
    return parsed.netloc, prefix if prefix.endswith("/") else prefix + "/"


def iter_raw_objects(
    s3: S3Client,
    bucket: str,
    prefix: str,
    *,
    start_after: str | None = None,
    skipped: list[str] | None = None,
    page_size: int = 1000,
) -> Iterator[tuple[str, bytes]]:
    """``(key, body)`` of every object under ``prefix``, in key order (paginated),
    after ``start_after`` if given. Zero-byte keys ending in ``/`` (console "folders")
    are not pages: they are skipped and appended to ``skipped``."""
    paginator = s3.get_paginator("list_objects_v2")
    options: dict[str, Any] = {"StartAfter": start_after} if start_after else {}
    for listing in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        PaginationConfig={"PageSize": page_size},
        **options,
    ):
        for entry in listing.get("Contents", ()):
            key = entry["Key"]
            if key.endswith("/") and entry.get("Size", 0) == 0:
                if skipped is not None:
                    skipped.append(key)
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            yield key, body


def restore_omitted(
    s3: S3Client, bucket: str, message: TweetMessage, *, best_effort: bool = False
) -> TweetMessage:
    """``message`` with what the fetch Lambda left out to keep the body under SQS's
    limit read back from the raw page it names: the referenced posts' ``note_text``
    and ``entities``, and the Tweet's own ``entities`` and ``display_entities``. A
    message that omitted nothing is returned as it is.

    The envelope's ``redactions`` are honoured: a referenced post that is redacted
    comes back as a bare reference (nothing of it is restored). Entries of
    ``includes`` that are not posts are skipped. Raises ``RawPageError`` (and
    ``ClientError`` for S3) when the page or a value can't be found. With
    ``best_effort`` (the last receive) each value is restored on its own: one that
    can't be found keeps its omitted marker and the rest is still restored, and an
    unreadable page restores nothing instead of raising."""
    if not message.has_omitted:
        return message
    if message.raw_key is None:
        if best_effort:
            return message
        raise RawPageError("content was omitted but there is no raw_key")
    try:
        body = s3.get_object(Bucket=bucket, Key=message.raw_key)["Body"].read()
        envelope = Envelope.from_bytes(body)
    except (ClientError, RawPageError) as exc:
        if not best_effort:
            raise
        logger.warning("raw page %s unreadable: %r", message.raw_key, exc)
        return message
    redacted = set(envelope.redactions)
    includes = envelope.response.get("includes")
    posts: dict[str, ReferencedPost] = {}
    for name in ("tweets", "posts"):  # X is renaming tweets to posts
        entries = includes.get(name) if isinstance(includes, Mapping) else None
        for entry in entries if isinstance(entries, list) else ():
            try:
                parsed = ReferencedPost.from_api(entry)
            except Exception:  # quarantined by the fetch; nothing to restore from
                continue
            posts[parsed.id] = parsed

    def missing(what: str) -> None:
        if not best_effort:
            raise RawPageError(f"raw page {message.raw_key} has no {what}")

    restored = []
    for ref in message.referenced:
        if ref.id in redacted:
            ref = ReferencedPost(id=ref.id, type=ref.type)
        else:
            post = posts.get(ref.id)
            if ref.note_text_omitted:
                if post is None or post.note_text is None:
                    missing(f"note for {ref.id}")
                else:
                    ref = replace(
                        ref, note_text=post.note_text, note_text_omitted=False
                    )
            if ref.entities_omitted:
                if post is None or post.entities is None:
                    missing(f"entities for {ref.id}")
                else:
                    ref = replace(ref, entities=post.entities, entities_omitted=False)
        restored.append(ref)
    tweet = message.tweet
    if tweet.entities_omitted:
        data = envelope.response.get("data")
        for entry in data if isinstance(data, list) else ():
            if isinstance(entry, Mapping) and str(entry.get("id")) == tweet.id:
                entities, display = split_entities(entry)
                tweet = replace(
                    tweet,
                    entities=entities,
                    display_entities=display,
                    entities_omitted=False,
                )
                break
        else:
            missing(f"tweet {tweet.id}")
    return replace(message, tweet=tweet, referenced=tuple(restored))
