"""Fetch Lambda: X API timeline -> SQS.

``run_fetch`` is the testable core; ``handler`` wires it to AWS. The state item holds a
high-water mark (``newest_id``) plus a list of *gaps*: ID ranges known to be unfetched.
The state moves only after a page is fully enqueued, and every truncated or failed run
leaves a gap behind rather than a hole, so a later run picks up where it stopped. The
price is duplicates (at most the page in flight), which processing absorbs.
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
from twitter_timeline_data_pipeline.models import (
    TimelinePage,
    TweetId,
    normalize_username,
)
from twitter_timeline_data_pipeline.storage import CheckpointState, CheckpointStore, Gap
from twitter_timeline_data_pipeline.x_api import AuthError, RateLimitError, XClient

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
    gaps_open: int
    """Ranges still to fetch when the run ended."""
    stopped_reason: str | None
    """None when nothing is left to fetch; otherwise ``page_cap`` (budget used up),
    ``rate_limited`` or ``state_conflict`` (another run moved the state)."""


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


class _StateConflict(Exception):
    """Another run changed the state item since this run read it."""


@dataclass(slots=True)
class _Run:
    """Mutable bookkeeping of one run: counters, the page budget and the cursor."""

    username: str
    user_id: str
    x_client: XClient
    sqs: SQSClient
    queue_url: str
    checkpoints: CheckpointStore
    state: CheckpointState
    max_results: int
    max_pages: int
    sleep: Callable[[float], None]
    pages: int = 0
    tweets: int = 0
    enqueued: int = 0

    @property
    def budget(self) -> int:
        return self.max_pages - self.pages

    def enqueue(self, page: TimelinePage) -> list[int]:
        """Send the page's messages; returns its Tweet IDs."""
        self.pages += 1
        messages = page.messages(self.username)
        self.tweets += len(messages)
        for chunk in itertools.batched(messages, SQS_BATCH_SIZE, strict=False):
            _send_batch(
                self.sqs, self.queue_url, [m.to_json() for m in chunk], self.sleep
            )
            self.enqueued += len(chunk)
        return [int(t.id) for t in page.tweets]

    def commit(self, seen: Sequence[int], gaps: Sequence[Gap]) -> None:
        """Persist the cursor: ``newest_id`` never drops, ``gaps`` replace the list."""
        newest = max([int(self.state.newest_id or 0), *seen])
        new = CheckpointState(
            newest_id=str(newest) if newest else None,
            gaps=tuple(gaps),
            version=self.state.version + 1,
        )
        if not self.checkpoints.save(
            self.username, new, expected_version=self.state.version
        ):
            raise _StateConflict
        self.state = new

    def fetch_new(self, since_id: TweetId | None) -> None:
        """Phase 1: Tweets newer than ``since_id``, newest first.

        While a page still has a ``next_token``, the unfetched rest of the phase is
        recorded as a gap just below the lowest ID seen, so a cap or failure here never
        leaves a hole.
        """
        others = list(self.state.gaps)
        lowest: int | None = None
        open_gap: Gap | None = None
        for page in self.x_client.iter_timeline(
            self.user_id,
            since_id=since_id,
            max_results=self.max_results,
            max_pages=self.budget,
        ):
            ids = self.enqueue(page)
            lowest = min(ids) if lowest is None else min(lowest, *ids)
            open_gap = (
                Gap(until_id=str(lowest), since_id=since_id)
                if page.next_token
                else None
            )
            self.commit(ids, [*others, *([open_gap] if open_gap else [])])
        if open_gap and self.pages < self.max_pages:  # an empty page ended the walk
            self.commit([], others)

    def fill_gaps(self) -> None:
        """Phase 2: spend what is left of the budget on the recorded gaps.

        Newest gap first: its Tweets are the ones adjacent to the live stream, and the
        API serves only the most recent ~3,200 posts, so the oldest gaps are the ones
        that age out of reach anyway. Each page lowers the gap's ``until_id``.
        """
        for gap in sorted(self.state.gaps, key=lambda g: g.sort_key, reverse=True):
            if self.budget <= 0:
                return
            budget = self.budget
            others = list(self.state.gaps)
            others.remove(gap)
            current: Gap | None = gap
            used = 0
            for page in self.x_client.iter_timeline(
                self.user_id,
                since_id=gap.since_id,
                until_id=gap.until_id,
                max_results=self.max_results,
                max_pages=budget,
            ):
                used += 1
                ids = self.enqueue(page)
                current = (
                    Gap(until_id=str(min(ids)), since_id=gap.since_id)
                    if page.next_token
                    else None
                )
                self.commit(ids, [*others, *([current] if current else [])])
            if current is not None and used < budget:
                logger.warning(
                    "gap for %s (since_id=%s, until_id=%s) is out of the API's reach "
                    "(no Tweets returned); dropping it",
                    self.username,
                    gap.since_id,
                    current.until_id,
                )
                self.commit([], others)


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
    backfill: Gap | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchResult:
    """Enqueue the new Tweets of ``username``, then work through any recorded gaps.

    ``username`` is normalized (no ``@``, lower case; X handles are case-insensitive)
    and used for the lookup, the messages and the state item. ``since_id`` overrides
    the stored ``newest_id`` as the lower bound of the new-Tweets phase; ``backfill``
    adds a gap to fetch. ``max_pages`` is shared by both phases. A 429 ends the run
    cleanly (``stopped_reason="rate_limited"``); other errors propagate, with progress
    up to the last full page already saved.
    """
    username = normalize_username(username)
    user_id = x_client.get_user_id(username)
    run = _Run(
        username=username,
        user_id=user_id,
        x_client=x_client,
        sqs=sqs,
        queue_url=queue_url,
        checkpoints=checkpoints,
        state=checkpoints.load(username),
        max_results=max_results,
        max_pages=max_pages,
        sleep=sleep,
    )
    since_id = since_id or run.state.newest_id
    stopped_reason: str | None = None
    try:
        if backfill and backfill not in run.state.gaps:
            run.commit([], [*run.state.gaps, backfill])
        run.fetch_new(since_id)
        run.fill_gaps()
    except RateLimitError as exc:
        logger.warning("rate limited, stopping early; progress is saved: %s", exc)
        stopped_reason = "rate_limited"
    except _StateConflict:
        logger.warning(
            "state for %s changed during the run; stopping. Messages already "
            "enqueued are harmless duplicates",
            username,
        )
        stopped_reason = "state_conflict"
    gaps_open = len(run.state.gaps)
    if stopped_reason is None and gaps_open:
        stopped_reason = "page_cap"
    if gaps_open:
        logger.info("open gaps for %s: %s", username, run.state.gaps)
    return FetchResult(
        username=username,
        user_id=user_id,
        since_id=since_id,
        newest_id=run.state.newest_id,
        pages=run.pages,
        tweets=run.tweets,
        enqueued=run.enqueued,
        gaps_open=gaps_open,
        stopped_reason=stopped_reason,
    )


@functools.cache
def sqs_client() -> SQSClient:
    return boto3.client("sqs")


@functools.cache
def checkpoint_store(table_name: str) -> CheckpointStore:
    return CheckpointStore(boto3.resource("dynamodb").Table(table_name))


def _parse_backfill(raw: Any) -> Gap | None:
    """The event's ``backfill`` object (``since_id``/``until_id``, both optional)."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError('"backfill" must be an object with since_id and/or until_id')
    unknown = raw.keys() - {"since_id", "until_id"}
    if unknown:
        raise ValueError(f"unknown backfill keys: {sorted(unknown)}")
    return Gap(
        since_id=None if raw.get("since_id") is None else str(raw["since_id"]),
        until_id=None if raw.get("until_id") is None else str(raw["until_id"]),
    )


def handler(event: Any, context: Any) -> dict[str, Any]:
    settings = Settings.from_env()
    settings.require("queue_url", "state_table")
    assert settings.queue_url and settings.state_table  # narrowed by require()

    username = event.get("username") if isinstance(event, dict) else None
    username = username or settings.username
    if not username:
        raise ConfigError("no username in the event and TWITTER_USERNAME is unset")
    backfill = _parse_backfill(
        event.get("backfill") if isinstance(event, dict) else None
    )

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
                backfill=backfill,
            )
    except AuthError:
        clear_cache()  # the token may have been rotated; re-read it next time
        raise
    logger.info(
        "fetched %s: %d pages, %d tweets enqueued, newest_id=%s, gaps_open=%d, "
        "stopped_reason=%s",
        result.username,
        result.pages,
        result.enqueued,
        result.newest_id,
        result.gaps_open,
        result.stopped_reason,
    )
    return dataclasses.asdict(result)
