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
from twitter_timeline_data_pipeline.models import MessageError, TweetMessage
from twitter_timeline_data_pipeline.storage import (
    MediaStore,
    StoredMedia,
    TransientMediaError,
    TweetStore,
    estimated_size,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

# Time kept back from the invocation. Each download gets the time left minus this, and
# the HTTP phases and body reads are bounded by it, but a stalled body read can still
# overrun by one httpx read timeout (10 s by default). Finishing or aborting the
# multipart upload and writing the DynamoDB item need a few more seconds. 20 s covers
# both, and leaves 100 s of the 120 s timeout for downloads.
SAFETY_MARGIN_MS = 20_000
# Enough to write one DynamoDB item when a last-receive record can't download anything.
MIN_WRITE_MS = 3_000


def _download_cost(record: Mapping[str, Any]) -> float:
    """Estimated media bytes of a record, for ordering; unknown-size videos are last."""
    try:
        message = TweetMessage.from_json(record["body"])
    except MessageError, KeyError, TypeError:
        return 0  # fails fast
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
) -> list[str]:
    """Process SQS records; return the ``messageId`` of each one that failed.

    Records run cheapest first (by estimated media bytes), so one slow video can't
    starve the rest of its batch. With ``remaining_ms`` (Lambda's
    ``get_remaining_time_in_millis``), a record isn't started once less than
    ``SAFETY_MARGIN_MS`` is left: it is returned as failed untouched, so the invocation
    ends before its timeout and SQS retries only those. With ``max_receive_count``, a
    record on its last receive has its failing media recorded as ``failed`` and its
    Tweet written anyway, since a failed record would go to the DLQ without its
    metadata. That includes a record with no time left to download, as long as
    ``MIN_WRITE_MS`` remain to write it.
    """
    pending = sorted(records, key=_download_cost)
    failed: list[str] = []
    for record in pending:
        message_id = record["messageId"]
        last_receive = (
            max_receive_count is not None
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
                    stored.append(
                        StoredMedia(media_key=m.media_key, type=m.type, status="failed")
                    )
            tweet_store.put_tweet(message, stored)
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
            logger.info(
                "stored tweet %s media=%s",
                message.tweet.id,
                [f"{s.media_key}:{s.status}" for s in stored],
            )
    return failed


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
def _http_client(timeout: float) -> httpx.Client:
    return httpx.Client(
        timeout=timeout,
        headers={"User-Agent": USER_AGENT},
    )


@functools.cache
def _tweet_store(table_name: str) -> TweetStore:
    return TweetStore(boto3.resource("dynamodb").Table(table_name))


def handler(event: Any, context: Any) -> dict[str, list[dict[str, str]]]:
    settings = Settings.from_env()
    settings.require("tweets_table", "media_bucket")
    assert settings.tweets_table and settings.media_bucket  # narrowed by require()

    failed = process_records(
        event["Records"],
        media_store=MediaStore(
            _s3_client(),
            settings.media_bucket,
            _http_client(settings.http_timeout),
            max_bytes=settings.max_media_bytes,
        ),
        tweet_store=_tweet_store(settings.tweets_table),
        remaining_ms=getattr(context, "get_remaining_time_in_millis", None),
        max_receive_count=settings.max_receive_count,
    )
    return {"batchItemFailures": [{"itemIdentifier": i} for i in failed]}
