"""Replay landed raw pages: re-parse them and enqueue the messages. No API calls.

``replay`` reads the envelopes under an ``s3://<bucket>/raw/<prefix>``, parses each
response with the same ``parse_timeline_page`` the fetch uses (a Tweet that still
fails to parse is quarantined, as in the fetch; a fixed parser recovers it) and
builds the same messages the fetch built, with the envelope's ``username``, its
original ``fetched_at`` and the object's key as ``raw_key``. Keeping the original
``fetched_at`` makes a replay safe to run at any time: the processor's conditional
write never lets it overwrite an item fetched later, and replaying twice rewrites the
same data. It never touches the fetch cursor. Tweets listed in an envelope's
``redactions`` (deletion requests) are never replayed.
"""

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from twitter_timeline_data_pipeline.models import (
    RawPage,
    TimelinePage,
    TweetMessage,
    normalize_fetched_at,
)
from twitter_timeline_data_pipeline.quarantine import QuarantineStore
from twitter_timeline_data_pipeline.raw import Envelope, RawPageError, iter_raw_objects
from twitter_timeline_data_pipeline.x_api import parse_timeline_page

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class ReplayResult:
    objects: int
    tweets: int
    enqueued: int
    """Messages sent; 0 for a dry run."""
    skipped: tuple[str, ...] = ()
    """Keys that are not pages (zero-byte ``/`` markers)."""
    last_key: str | None = None
    """The last object fully handled: pass it as ``--start-after`` to resume."""


def without_redactions(page: TimelinePage, redactions: tuple[str, ...]) -> TimelinePage:
    """``page`` minus the redacted Tweets and the media only they used."""
    if not redactions:
        return page
    gone = set(redactions)
    kept = tuple(t for t in page.tweets if t.id not in gone)
    used = {key for t in kept for key in t.media_keys}
    dropped = {key for t in page.tweets if t.id in gone for key in t.media_keys}
    media = {k: m for k, m in page.media.items() if k in used or k not in dropped}
    quarantined = tuple(q for q in page.quarantined if q.tweet_id not in gone)
    # A redacted Tweet quoted by another is no longer an included copy either: the
    # reference then reads as unavailable, never as the deleted content.
    included = {i: p for i, p in page.included.items() if i not in gone}
    return replace(
        page, tweets=kept, media=media, quarantined=quarantined, included=included
    )


def messages_from_raw(
    s3: S3Client,
    bucket: str,
    prefix: str,
    *,
    quarantine: QuarantineStore | None = None,
    start_after: str | None = None,
    skipped: list[str] | None = None,
    page_size: int = 1000,
) -> Iterator[tuple[str, list[TweetMessage]]]:
    """``(key, messages)`` for each raw object under ``prefix``, in key order.

    Items that fail to parse are written to ``quarantine`` (none: only logged, as in a
    dry run) and the rest of the page is replayed. Raises ``RawPageError`` (naming the
    key) for an object that can't be read back, including one whose ``fetched_at`` is
    not a valid time.
    """
    for page_no, (key, body) in enumerate(
        iter_raw_objects(
            s3,
            bucket,
            prefix,
            start_after=start_after,
            skipped=skipped,
            page_size=page_size,
        ),
        start=1,
    ):
        try:
            envelope = Envelope.from_bytes(body)
            fetched_at = normalize_fetched_at(envelope.fetched_at)
            # The request's params say what was asked for (entities, expansions),
            # which the messages record, so a replay builds the fetch's own messages.
            raw = RawPage(
                endpoint=str(envelope.request.get("endpoint", "")),
                params=dict(envelope.request.get("params") or {}),
                body=envelope.response,
            )
            page = without_redactions(
                parse_timeline_page(envelope.response, raw=raw), envelope.redactions
            )
            messages = page.messages(
                envelope.username, fetched_at=fetched_at, raw_key=key
            )
        except (RawPageError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise RawPageError(f"{key}: {exc}") from exc
        if quarantine is not None:
            for q in page.quarantined:
                quarantine.put(
                    kind=q.kind,
                    item=q.item,
                    error=q.error,
                    raw_key=key,
                    user_id=envelope.user_id,
                    fetched_at=envelope.fetched_at,
                    page_no=page_no,
                )
        yield key, messages


def run_replay(
    *,
    s3: S3Client,
    bucket: str,
    prefix: str,
    send: Callable[[list[TweetMessage]], int],
    quarantine: QuarantineStore | None,
    start_after: str | None = None,
) -> ReplayResult:
    """Hand the messages of every raw page under ``prefix`` to ``send`` (which returns
    how many it sent; a dry run prints them and returns 0). ``quarantine`` has no
    default: pass a store for a real replay, None only for a dry run (items are then
    just logged)."""
    objects = tweets = enqueued = 0
    last_key = None
    skipped: list[str] = []
    for key, messages in messages_from_raw(
        s3,
        bucket,
        prefix,
        quarantine=quarantine,
        start_after=start_after,
        skipped=skipped,
    ):
        objects += 1
        tweets += len(messages)
        enqueued += send(messages)
        last_key = key
        logger.info("replayed %s: %d tweets", key, len(messages))
    return ReplayResult(
        objects=objects,
        tweets=tweets,
        enqueued=enqueued,
        skipped=tuple(skipped),
        last_key=last_key,
    )
