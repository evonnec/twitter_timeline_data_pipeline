"""Process Lambda: SQS -> Tweet metadata in DynamoDB, media in S3.

Uses the SQS partial batch response: only records that failed are retried, and after
``maxReceiveCount`` receives they land in the DLQ.
"""

import functools
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

import boto3
import httpx

from twitter_timeline_data_pipeline import __version__
from twitter_timeline_data_pipeline.config import Settings
from twitter_timeline_data_pipeline.models import MessageError, TweetMessage
from twitter_timeline_data_pipeline.storage import (
    MediaStore,
    StoredMedia,
    TransientMediaError,
    TweetStore,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

USER_AGENT = f"twitter-timeline-data-pipeline/{__version__}"


def process_records(
    records: Iterable[Mapping[str, Any]],
    *,
    media_store: MediaStore,
    tweet_store: TweetStore,
) -> list[str]:
    """Process SQS records; return the ``messageId`` of each one that failed."""
    failed: list[str] = []
    for record in records:
        message_id = record["messageId"]
        try:
            message = TweetMessage.from_json(record["body"])
            # Media first: a transient failure must leave no metadata that points at
            # media that isn't there.
            stored: list[StoredMedia] = [
                media_store.store(message, m) for m in message.media
            ]
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
    return boto3.client("s3")


@functools.cache
def _http_client(timeout: float) -> httpx.Client:
    return httpx.Client(
        timeout=timeout,
        follow_redirects=True,
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
            _s3_client(), settings.media_bucket, _http_client(settings.http_timeout)
        ),
        tweet_store=_tweet_store(settings.tweets_table),
    )
    return {"batchItemFailures": [{"itemIdentifier": i} for i in failed]}
