"""Media-retry Lambda: SQS -> a media file stored late, and its Tweet item updated.

The process function queues one message per media item it had to give up on during its
message's last receive (the Tweet is already stored, with that media ``retrying``).
This function downloads the file the way the process function does and then replaces
that one entry of the Tweet item, only if it is still the ``retrying`` entry for the
same ``media_key``.

A message is never deleted unless the media was dealt with: stored, permanently
unavailable (404, too large, skipped), or already settled by another delivery. Every
other outcome (a transient download error, an item that isn't written yet, a malformed
message, a timeout) reports the record as failed. SQS then redelivers it after the
visibility timeout and, after ``MediaRetryMaxReceiveCount`` receives, moves it to the
media-retry dead-letter queue, which has an alarm. Nothing is dropped silently.
"""

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict
from typing import Any

from twitter_timeline_data_pipeline.config import Settings
from twitter_timeline_data_pipeline.handlers.process import (
    SAFETY_MARGIN_MS,
    _http_client,
    _s3_client,
    _tweet_store,
)
from twitter_timeline_data_pipeline.models import MediaRetry, MessageError
from twitter_timeline_data_pipeline.storage import (
    MediaStore,
    TransientMediaError,
    TweetStore,
)

logger = logging.getLogger(__name__)


def _retry_one(
    record: Mapping[str, Any],
    *,
    media_store: MediaStore,
    tweet_store: TweetStore,
    remaining_ms: Callable[[], int] | None,
) -> None:
    """Handle one record; raises for anything that must be retried."""
    retry = MediaRetry.from_json(record["body"])
    key = retry.media.media_key
    entries = tweet_store.get_media(retry.tweet_id)
    if entries is None:
        # The Tweet write may not have landed (or was lost): try again later.
        raise TransientMediaError(f"tweet {retry.tweet_id} is not stored yet")
    index = next((i for i, e in enumerate(entries) if e.get("media_key") == key), None)
    if index is None:
        logger.warning(
            "tweet %s no longer lists media %s: nothing to update", retry.tweet_id, key
        )
        return
    current = entries[index]
    if current.get("status") != "retrying":
        logger.info(
            "media %s of tweet %s is already %s",
            key,
            retry.tweet_id,
            current.get("status"),
        )
        return
    # After the reads above, so their time isn't counted as available for the download.
    budget_s = (
        None if remaining_ms is None else (remaining_ms() - SAFETY_MARGIN_MS) / 1000
    )
    if budget_s is not None and budget_s <= 0:
        raise TransientMediaError("not enough time left to start")
    stored = media_store.store(retry.to_tweet_message(), retry.media, budget_s=budget_s)
    entry = {k: v for k, v in asdict(stored).items() if v is not None}
    if "source_url" not in entry and current.get("source_url"):
        entry["source_url"] = current["source_url"]  # keep it for a skipped result
    if tweet_store.resolve_media(retry.tweet_id, index, key, entry):
        logger.info(
            "media %s of tweet %s is now %s", key, retry.tweet_id, stored.status
        )
    else:
        logger.info(
            "media %s of tweet %s was settled by another delivery", key, retry.tweet_id
        )


def retry_records(
    records: Iterable[Mapping[str, Any]],
    *,
    media_store: MediaStore,
    tweet_store: TweetStore,
    remaining_ms: Callable[[], int] | None = None,
) -> list[str]:
    """Process media-retry records; return the ``messageId`` of each one that failed."""
    failed: list[str] = []
    for record in records:
        message_id = record["messageId"]
        try:
            _retry_one(
                record,
                media_store=media_store,
                tweet_store=tweet_store,
                remaining_ms=remaining_ms,
            )
        except MessageError:
            logger.exception("retry message %s is malformed", message_id)
            failed.append(message_id)
        except TransientMediaError as exc:
            logger.warning("retry message %s will be retried: %s", message_id, exc)
            failed.append(message_id)
        except Exception:
            logger.exception("retry message %s failed", message_id)
            failed.append(message_id)
    return failed


def handler(event: Any, context: Any) -> dict[str, list[dict[str, str]]]:
    settings = Settings.from_env()
    settings.require("tweets_table", "media_bucket")
    assert settings.tweets_table and settings.media_bucket  # narrowed by require()

    failed = retry_records(
        event["Records"],
        media_store=MediaStore(
            _s3_client(),
            settings.media_bucket,
            _http_client(settings.http_timeout),
            max_bytes=settings.max_media_bytes,
        ),
        tweet_store=_tweet_store(settings.tweets_table, settings.media_bucket),
        remaining_ms=getattr(context, "get_remaining_time_in_millis", None),
    )
    return {"batchItemFailures": [{"itemIdentifier": i} for i in failed]}
