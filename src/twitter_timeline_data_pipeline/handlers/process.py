"""Process Lambda: SQS -> Tweet metadata in DynamoDB, media in S3.

Uses the SQS partial batch response: only records that failed are retried, and after
``maxReceiveCount`` receives they land in the DLQ. A timed-out invocation returns no
partial response and would retry the whole batch, so the work is bounded by the time
the invocation has left.
"""

import functools
import logging
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any

import boto3
import httpx
from botocore.config import Config

from twitter_timeline_data_pipeline import USER_AGENT
from twitter_timeline_data_pipeline.config import Settings
from twitter_timeline_data_pipeline.models import (
    Media,
    MediaRetry,
    MessageError,
    TweetMessage,
)
from twitter_timeline_data_pipeline.storage import (
    MediaRetryQueue,
    MediaStore,
    StoredMedia,
    TransientMediaError,
    TweetStore,
    estimated_size,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sqs import SQSClient

logger = logging.getLogger(__name__)

# Time kept back from the invocation. Each download gets the time left minus this, and
# the HTTP phases and body reads are bounded by it, but a stalled body read can still
# overrun by one httpx read timeout (10 s by default). Finishing or aborting the
# multipart upload and writing the DynamoDB item need a few more seconds. 20 s covers
# both, and leaves 100 s of the 120 s timeout for downloads.
SAFETY_MARGIN_MS = 20_000
# Enough to write one DynamoDB item when a last-receive record can't download anything.
# With less, the S3 existence checks are skipped but the Tweet is still written and
# its media queued for retry (a few quick calls).
MIN_WRITE_MS = 3_000


def _download_cost(record: Mapping[str, Any]) -> float:
    """Estimated media bytes of a record, for ordering; unknown-size videos are last."""
    try:
        message = TweetMessage.from_json(record["body"])
        cost = 0.0
        for media in message.media:
            source = media.download_source()
            if source is None or media.type not in ("video", "animated_gif"):
                continue
            estimate = estimated_size(media, source[0])
            if estimate is not None:
                cost += estimate
            elif media.type == "video":
                return float("inf")
        return cost
    except Exception:
        # Ordering is only a heuristic. A malformed record fails fast, and a value that
        # breaks the arithmetic (an enormous bit_rate overflows the float sum) must
        # not fail the batch: the record is processed, or fails, on its own.
        return 0


def _receive_count(record: Mapping[str, Any]) -> int:
    try:
        return int(record["attributes"]["ApproximateReceiveCount"])
    except KeyError, TypeError, ValueError:
        return 0


def process_records(
    records: Iterable[Mapping[str, Any]],
    *,
    media_store: MediaStore,
    tweet_store: TweetStore,
    remaining_ms: Callable[[], int] | None = None,
    max_receive_count: int | None = None,
    retry_queue: MediaRetryQueue | None = None,
) -> list[str]:
    """Process SQS records; return the ``messageId`` of each one that failed.

    Records run cheapest first (by estimated media bytes), so one slow video can't
    starve the rest of its batch. With ``remaining_ms`` (Lambda's
    ``get_remaining_time_in_millis``), a record isn't started once less than
    ``SAFETY_MARGIN_MS`` is left: it is returned as failed untouched, so the invocation
    ends before its timeout and SQS retries only those.

    With ``max_receive_count`` and ``retry_queue``, a record on its last receive does
    not fail on media it can't store (the DLQ would get it without the metadata and
    the file would be lost): each such media item first gets the cheap checks (skipped,
    too large, already in S3), and one that is still unstored is recorded as
    ``retrying`` with its ``source_url``. The Tweet is written, then one media-retry
    message per ``retrying`` item is sent. If any of those sends fails the record is
    reported failed, so it reaches the DLQ and its alarm rather than being dropped
    silently. That also covers a record with no time left to download, as long as
    ``MIN_WRITE_MS`` remain to write it.
    """
    pending = sorted(records, key=_download_cost)
    failed: list[str] = []
    for record in pending:
        message_id = record["messageId"]
        last_receive = (
            max_receive_count is not None
            and retry_queue is not None
            and _receive_count(record) >= max_receive_count
        )
        if (
            remaining_ms is not None
            and remaining_ms() < SAFETY_MARGIN_MS
            and not (last_receive and remaining_ms() >= MIN_WRITE_MS)
        ):
            logger.warning("message %s not started: out of time", message_id)
            failed.append(message_id)
            continue
        try:
            message = TweetMessage.from_json(record["body"])
            # Media first: a transient failure must leave no metadata that points at
            # media that isn't there.
            stored: list[StoredMedia] = []
            retrying: list[Media] = []
            for m in message.media:
                try:
                    budget_s = (
                        None
                        if remaining_ms is None
                        else (remaining_ms() - SAFETY_MARGIN_MS) / 1000
                    )
                    if budget_s is not None and budget_s <= 0:
                        raise TransientMediaError("no time left to download")
                    stored.append(media_store.store(message, m, budget_s=budget_s))
                except Exception as exc:
                    if not last_receive:
                        raise
                    logger.error(
                        "message %s media %s failed on its last receive: %r",
                        message_id,
                        m.media_key,
                        exc,
                    )
                    entry = media_store.precheck(
                        message,
                        m,
                        head=remaining_ms is None or remaining_ms() >= MIN_WRITE_MS,
                    )
                    stored.append(entry)
                    if entry.status == "retrying":
                        retrying.append(m)
            result = tweet_store.put_tweet(message, stored)
        except MessageError:
            logger.exception("message %s is malformed", message_id)
            failed.append(message_id)
        except TransientMediaError as exc:
            logger.warning("message %s will be retried: %s", message_id, exc)
            failed.append(message_id)
        except Exception:
            logger.exception("message %s failed", message_id)
            failed.append(message_id)
        else:
            if not result.written:
                # Not a failure: a newer version is already stored, so a retry or the
                # DLQ would only repeat this.
                logger.info(
                    "stale write skipped for tweet %s (message fetched_at %s, "
                    "schema_version %s; stored fetched_at %s, schema_version %s)",
                    message.tweet.id,
                    message.fetched_at,
                    message.schema_version,
                    result.stored_fetched_at,
                    result.stored_schema_version,
                )
                continue
            if retrying:
                assert retry_queue is not None  # only the last receive collects any
                if not _enqueue_retries(retry_queue, message, retrying, message_id):
                    failed.append(message_id)
                    continue
            logger.info(
                "stored tweet %s media=%s",
                message.tweet.id,
                [f"{s.media_key}:{s.status}" for s in stored],
            )
    return failed


def _enqueue_retries(
    retry_queue: MediaRetryQueue,
    message: TweetMessage,
    media: Iterable[Media],
    message_id: str,
) -> bool:
    """Send a media-retry message per item (all of them, even if one send fails);
    False if any send failed."""
    ok = True
    for m in media:
        try:
            retry_queue.send(
                MediaRetry(
                    tweet_id=message.tweet.id,
                    username=message.username,
                    author_id=message.tweet.author_id,
                    fetched_at=message.fetched_at,
                    media=m,
                )
            )
        except Exception:
            logger.exception(
                "message %s: could not queue a retry for media %s",
                message_id,
                m.media_key,
            )
            ok = False
    return ok


@functools.cache
def _s3_client() -> S3Client:
    # Bounded so a hung HeadObject or UploadPart can't eat the invocation.
    return boto3.client(
        "s3",
        config=Config(
            connect_timeout=5,
            read_timeout=10,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


@functools.cache
def _sqs_client() -> SQSClient:
    return boto3.client("sqs")


@functools.cache
def _http_client(timeout: float) -> httpx.Client:
    return httpx.Client(
        timeout=timeout,
        headers={"User-Agent": USER_AGENT},
    )


@functools.cache
def _tweet_store(table_name: str, bucket: str) -> TweetStore:
    return TweetStore(
        boto3.resource("dynamodb").Table(table_name), _s3_client(), bucket
    )


def handler(event: Any, context: Any) -> dict[str, list[dict[str, str]]]:
    settings = Settings.from_env()
    settings.require("tweets_table", "media_bucket", "media_retry_queue_url")
    # narrowed by require()
    assert settings.tweets_table and settings.media_bucket
    assert settings.media_retry_queue_url

    failed = process_records(
        event["Records"],
        media_store=MediaStore(
            _s3_client(),
            settings.media_bucket,
            _http_client(settings.http_timeout),
            max_bytes=settings.max_media_bytes,
        ),
        tweet_store=_tweet_store(settings.tweets_table, settings.media_bucket),
        remaining_ms=getattr(context, "get_remaining_time_in_millis", None),
        max_receive_count=settings.max_receive_count,
        retry_queue=MediaRetryQueue(_sqs_client(), settings.media_retry_queue_url),
    )
    return {"batchItemFailures": [{"itemIdentifier": i} for i in failed]}
