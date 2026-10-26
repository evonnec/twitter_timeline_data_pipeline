"""Fetch Lambda: X API timeline -> SQS.

``run_fetch`` is the testable core; ``handler`` wires it to AWS. The state item holds a
high-water mark (``newest_id``) plus a list of *gaps*: ID ranges known to be unfetched.
The state moves only after a page is fully enqueued, and every truncated or failed run
leaves a gap behind rather than a hole, so a later run picks up where it stopped. The
price is duplicates (at most the page in flight), which processing absorbs.
"""

import dataclasses
import functools
import logging
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

import boto3

from twitter_timeline_data_pipeline.config import ConfigError, Settings
from twitter_timeline_data_pipeline.credentials import clear_cache, get_bearer_token
from twitter_timeline_data_pipeline.models import (
    TimelinePage,
    TweetId,
    TweetMessage,
    normalize_username,
)
from twitter_timeline_data_pipeline.storage import (
    CheckpointState,
    CheckpointStore,
    Gap,
    merge_gaps,
)
from twitter_timeline_data_pipeline.x_api import AuthError, RateLimitError, XClient

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient
    from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef

logger = logging.getLogger(__name__)

SQS_BATCH_SIZE = 10  # SendMessageBatch limit
SEND_RETRIES = 2  # extra attempts for entries SQS reports as Failed
RETRY_BACKOFF_SECONDS = 0.5
# SQS's maximum for one message and for the total of one SendMessageBatch call (1 MiB).
# The queues' MaximumMessageSize in template.yaml must equal it. The DynamoDB item
# holds the same text as UTF-8, so a body under this limit stays under its 400 KB item
# limit (checked in the tests); re-check that if the limit or the item layout changes.
SQS_MAX_MESSAGE_BYTES = 1_048_576


class EnqueueError(RuntimeError):
    """Some messages could not be sent to SQS even after retries."""


class MessageTooLargeError(EnqueueError):
    """A Tweet's message body is over the SQS limit. Nothing is dropped: the page is
    left unfetched in the cursor and the run fails (alarm fires), so the data can be
    handled deliberately."""


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
    """None when nothing is left to fetch; otherwise ``page_cap`` (request budget used
    up), ``gaps_pending`` (gaps remain, e.g. waiting out empty responses),
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


def _encode(messages: Sequence[TweetMessage]) -> list[str]:
    """The JSON bodies, or ``MessageTooLargeError`` if any exceeds the SQS limit."""
    bodies = []
    for message in messages:
        body = message.to_json()
        size = len(body.encode())
        if size > SQS_MAX_MESSAGE_BYTES:
            raise MessageTooLargeError(
                f"tweet {message.tweet.id}: message body is {size} bytes, over the "
                f"{SQS_MAX_MESSAGE_BYTES} byte SQS limit"
            )
        bodies.append(body)
    return bodies


def _chunk_bodies(bodies: Sequence[str]) -> Iterator[list[str]]:
    """Group bodies into SendMessageBatch calls: at most ``SQS_BATCH_SIZE`` entries and
    ``SQS_MAX_MESSAGE_BYTES`` in total (SQS limits the whole call, not just each
    message). A body under the per-message limit always fits alone."""
    chunk: list[str] = []
    total = 0
    for body in bodies:
        size = len(body.encode())
        if chunk and (
            len(chunk) == SQS_BATCH_SIZE or total + size > SQS_MAX_MESSAGE_BYTES
        ):
            yield chunk
            chunk, total = [], 0
        chunk.append(body)
        total += size
    if chunk:
        yield chunk


class _StateConflict(Exception):
    """Another run changed the state item since this run read it."""


EMPTY_RUNS_TO_DROP = 3
"""Consecutive runs whose first request for a gap came back cleanly empty before the
gap is dropped. One empty response can be a hiccup; three in a row mean the range has
aged out of the API's window."""


@dataclass(slots=True)
class _Run:
    """Mutable bookkeeping of one run: counters, the request budget and the cursor."""

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
    requests: int = 0
    tweets: int = 0
    enqueued: int = 0
    deferred_error: Exception | None = None

    @property
    def budget(self) -> int:
        """Timeline requests left. Every request counts, empty ones included."""
        return self.max_pages - self.requests

    def walk(
        self, *, since_id: TweetId | None, until_id: TweetId | None, budget: int
    ) -> Iterator[TimelinePage]:
        """``iter_timeline`` that charges every request it makes to the run's budget.

        That includes the final request that came back empty (which yields nothing)
        and a request that failed.
        """
        walked = 0
        last: TimelinePage | None = None
        try:
            for page in self.x_client.iter_timeline(
                self.user_id,
                since_id=since_id,
                until_id=until_id,
                max_results=self.max_results,
                max_pages=budget,
            ):
                walked += 1
                self.requests += 1
                self.pages += 1
                last = page
                yield page
        except Exception:
            self.requests += 1
            raise
        if walked < budget and (last is None or last.next_token):
            self.requests += 1  # ended on a clean empty response

    def enqueue(self, page: TimelinePage) -> list[int]:
        """Send the page's messages; returns its Tweet IDs."""
        messages = page.messages(self.username)
        self.tweets += len(messages)
        # Every body is encoded (and size-checked) before anything is sent, so an
        # over-limit Tweet fails the page with nothing enqueued and the cursor unmoved.
        for chunk in _chunk_bodies(_encode(messages)):
            _send_batch(self.sqs, self.queue_url, chunk, self.sleep)
            self.enqueued += len(chunk)
        return [int(t.id) for t in page.tweets]

    def commit(self, seen: Sequence[int], gaps: Iterable[Gap]) -> None:
        """Persist the cursor: ``newest_id`` never drops, ``gaps`` replace the list
        (merged, so overlapping ranges collapse)."""
        newest = max([int(self.state.newest_id or 0), *seen])
        new = CheckpointState(
            newest_id=str(newest) if newest else None,
            gaps=merge_gaps(gaps),
            version=self.state.version + 1,
        )
        if not self.checkpoints.save(
            self.username, new, expected_version=self.state.version
        ):
            raise _StateConflict
        self.state = new

    def fetch_new(self, since_id: TweetId | None, budget: int) -> None:
        """Phase 1: Tweets newer than ``since_id``, newest first.

        While a page still has a ``next_token``, the unfetched rest of the phase is
        recorded as a gap just below the lowest ID seen, so a cap or failure here never
        leaves a hole. A walk that ends on an empty response keeps that gap; the gap
        phase decides whether it is really exhausted.
        """
        others = list(self.state.gaps)
        lowest: int | None = None
        for page in self.walk(since_id=since_id, until_id=None, budget=budget):
            ids = self.enqueue(page)
            if ids:
                lowest = min(ids) if lowest is None else min(lowest, *ids)
            open_gap = (
                Gap(
                    until_id=None if lowest is None else str(lowest),
                    since_id=since_id,
                )
                if page.next_token
                else None
            )
            self.commit(ids, [*others, *([open_gap] if open_gap else [])])

    def fill_gaps(self) -> None:
        """Phase 2: spend what is left of the budget on the recorded gaps.

        Newest gap first: its Tweets are the ones adjacent to the live stream, and the
        API serves only the most recent ~3,200 posts, so the oldest gaps are the ones
        that age out of reach anyway. But the oldest gap is always left one request, so
        it keeps being advanced or pruned however busy the newer ones are. A failure in
        one gap is remembered and re-raised at the end of the run, so the older gaps
        still get their turn.
        """
        ordered = sorted(self.state.gaps, key=lambda g: g.sort_key, reverse=True)
        for gap in ordered:
            if self.budget <= 0:
                return
            if gap not in self.state.gaps:  # merged into another by an earlier save
                continue
            oldest = all(g.sort_key >= gap.sort_key for g in self.state.gaps)
            budget = self.budget - (0 if oldest else 1)
            if budget <= 0:
                continue
            try:
                self.fill_gap(gap, budget)
            except RateLimitError, AuthError, _StateConflict:
                raise  # every other gap would fail the same way
            except Exception as exc:
                logger.warning(
                    "gap for %s (since_id=%s, until_id=%s) failed: %s",
                    self.username,
                    gap.since_id,
                    gap.until_id,
                    exc,
                )
                self.deferred_error = self.deferred_error or exc

    def fill_gap(self, gap: Gap, budget: int) -> None:
        others = list(self.state.gaps)
        others.remove(gap)
        pages = 0
        for page in self.walk(
            since_id=gap.since_id, until_id=gap.until_id, budget=budget
        ):
            pages += 1
            ids = self.enqueue(page)
            if not ids:  # an empty page pointing onward: nothing learned
                continue
            # Data came back, so the range is alive: reset the empty-response count.
            remaining = (
                [Gap(until_id=str(min(ids)), since_id=gap.since_id)]
                if page.next_token
                else []
            )
            self.commit(ids, [*others, *remaining])
        if pages:
            return
        # The first request came back cleanly empty (no data, no token, no errors).
        runs = gap.empty_runs + 1
        if runs < EMPTY_RUNS_TO_DROP:
            logger.info(
                "gap for %s (since_id=%s, until_id=%s) came back empty (%d of %d "
                "runs before it is dropped)",
                self.username,
                gap.since_id,
                gap.until_id,
                runs,
                EMPTY_RUNS_TO_DROP,
            )
            self.commit([], [*others, replace(gap, empty_runs=runs)])
            return
        logger.warning(
            "gap for %s (since_id=%s, until_id=%s) came back empty %d runs in a row; "
            "the range is out of the API's reach, dropping it",
            self.username,
            gap.since_id,
            gap.until_id,
            runs,
        )
        self.commit([], others)


def _anchored(backfill: Gap | None, newest_id: TweetId | None) -> Gap | None:
    """Give an open-topped backfill the newest known Tweet as its top.

    Everything above ``newest_id`` is the new-Tweets phase's job, so the backfill
    only needs to cover what is at or below it; that also keeps it from overlapping
    the open-ended gap a first run leaves.
    """
    if backfill is None or backfill.until_id is not None or newest_id is None:
        return backfill
    if backfill.since_id is not None and int(backfill.since_id) >= int(newest_id):
        return None  # entirely above newest_id
    return replace(backfill, until_id=str(int(newest_id) + 1))


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
    adds a gap to fetch. ``max_pages`` is a budget of timeline requests shared by both
    phases (when there are gaps it is at least 2, and one request is held back for the
    oldest gap). A 429 ends the run cleanly (``stopped_reason="rate_limited"``) after
    saving; other errors propagate, with progress up to the last full page saved. An
    error in one gap is raised only after the remaining gaps have had their turn.
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
        backfill = _anchored(backfill, run.state.newest_id)
        if backfill and backfill not in run.state.gaps:
            run.commit([], [*run.state.gaps, backfill])
        if run.state.gaps:
            # One request checks for new Tweets and one goes to the oldest gap, so
            # neither can starve the other even with MAX_PAGES=1.
            run.max_pages = max(max_pages, 2)
        run.fetch_new(since_id, run.max_pages - (1 if run.state.gaps else 0))
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
        run.state = checkpoints.load(username)  # report what is stored now
    if run.deferred_error:
        raise run.deferred_error
    gaps_open = len(run.state.gaps)
    if stopped_reason is None and gaps_open:
        stopped_reason = "page_cap" if run.budget <= 0 else "gaps_pending"
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
