"""Fetch Lambda: X API timeline -> SQS.

``run_fetch`` is the testable core; ``handler`` wires it to AWS. The checkpoint moves
only after every message of the run is enqueued, so a failure can cause duplicates on
the next run (processing is idempotent) but never lost Tweets.
"""

import dataclasses
import functools
import itertools
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3

from twitter_timeline_data_pipeline.config import ConfigError, Settings
from twitter_timeline_data_pipeline.credentials import clear_cache, get_bearer_token
from twitter_timeline_data_pipeline.models import TweetId, is_newer
from twitter_timeline_data_pipeline.storage import CheckpointStore
from twitter_timeline_data_pipeline.x_api import AuthError, XClient

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient
    from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef

logger = logging.getLogger(__name__)

SQS_BATCH_SIZE = 10  # SendMessageBatch limit
SEND_RETRIES = 2  # extra attempts for entries SQS reports as Failed
RETRY_BACKOFF_SECONDS = 0.5


class EnqueueError(RuntimeError):
    """Some messages could not be sent to SQS even after retries."""


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchResult:
    username: str
    user_id: str
    since_id: TweetId | None
    newest_id: TweetId | None
    pages: int
    tweets: int
    enqueued: int
    truncated: bool
    """The last page still had a ``next_token``: the page cap cut the run short."""


def _send_batch(
    sqs: SQSClient,
    queue_url: str,
    bodies: Sequence[str],
    sleep: Callable[[float], None],
) -> None:
    """Send up to 10 bodies, retrying only the entries SQS reports as Failed."""
    pending: list[SendMessageBatchRequestEntryTypeDef] = [
        {"Id": str(i), "MessageBody": body} for i, body in enumerate(bodies)
    ]
    for attempt in range(SEND_RETRIES + 1):
        response = sqs.send_message_batch(QueueUrl=queue_url, Entries=pending)
        failed = {f["Id"]: f for f in response.get("Failed", [])}
        if not failed:
            return
        pending = [e for e in pending if e["Id"] in failed]
        if attempt < SEND_RETRIES:
            logger.warning(
                "%d of %d messages failed to enqueue (attempt %d): %s",
                len(pending),
                len(bodies),
                attempt + 1,
                next(iter(failed.values())).get("Message", "no detail"),
            )
            sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise EnqueueError(f"{len(pending)} messages still failing after retries")


def run_fetch(
    *,
    username: str,
    x_client: XClient,
    sqs: SQSClient,
    queue_url: str,
    checkpoints: CheckpointStore,
    max_results: int,
    max_pages: int,
    since_id: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchResult:
    """Enqueue every new Tweet of ``username``, then advance the checkpoint.

    ``since_id`` overrides the stored checkpoint (the checkpoint is still advanced).
    """
    user_id = x_client.get_user_id(username)
    since_id = since_id or checkpoints.get(username)

    pages = tweets = enqueued = 0
    newest_id: TweetId | None = None
    oldest_id: TweetId | None = None
    truncated = False
    for page in x_client.iter_timeline(
        user_id, since_id=since_id, max_results=max_results, max_pages=max_pages
    ):
        pages += 1
        messages = page.messages(username)
        tweets += len(messages)
        for chunk in itertools.batched(messages, SQS_BATCH_SIZE, strict=False):
            _send_batch(sqs, queue_url, [m.to_json() for m in chunk], sleep)
            enqueued += len(chunk)
        for tweet in page.tweets:
            if is_newer(tweet.id, newest_id):
                newest_id = tweet.id
            if oldest_id is None or not is_newer(tweet.id, oldest_id):
                oldest_id = tweet.id
        truncated = page.next_token is not None

    if newest_id is not None:
        checkpoints.advance(username, newest_id)
    if truncated and since_id is not None:
        logger.warning(
            "page cap reached for %s: Tweets between since_id %s and the oldest "
            "fetched (%s) were skipped",
            username,
            since_id,
            oldest_id,
        )
    return FetchResult(
        username=username,
        user_id=user_id,
        since_id=since_id,
        newest_id=newest_id,
        pages=pages,
        tweets=tweets,
        enqueued=enqueued,
        truncated=truncated,
    )


@functools.cache
def sqs_client() -> SQSClient:
    return boto3.client("sqs")


@functools.cache
def checkpoint_store(table_name: str) -> CheckpointStore:
    return CheckpointStore(boto3.resource("dynamodb").Table(table_name))


def handler(event: Any, context: Any) -> dict[str, Any]:
    settings = Settings.from_env()
    settings.require("queue_url", "state_table")
    assert settings.queue_url and settings.state_table  # narrowed by require()

    username = event.get("username") if isinstance(event, dict) else None
    username = username or settings.username
    if not username:
        raise ConfigError("no username in the event and TWITTER_USERNAME is unset")

    token = get_bearer_token(settings)
    max_rate_limit_wait = 60.0
    if remaining_ms := getattr(context, "get_remaining_time_in_millis", None):
        max_rate_limit_wait = max(0.0, min(60.0, remaining_ms() / 1000 - 15))

    try:
        with XClient(
            token,
            base_url=settings.x_api_base_url,
            timeout=settings.http_timeout,
            max_rate_limit_wait=max_rate_limit_wait,
        ) as x_client:
            result = run_fetch(
                username=username,
                x_client=x_client,
                sqs=sqs_client(),
                queue_url=settings.queue_url,
                checkpoints=checkpoint_store(settings.state_table),
                max_results=settings.max_results,
                max_pages=settings.max_pages,
            )
    except AuthError:
        clear_cache()  # the token may have been rotated; re-read it next time
        raise
    logger.info(
        "fetched %s: %d pages, %d tweets enqueued, newest_id=%s, truncated=%s",
        result.username,
        result.pages,
        result.enqueued,
        result.newest_id,
        result.truncated,
    )
    return dataclasses.asdict(result)
