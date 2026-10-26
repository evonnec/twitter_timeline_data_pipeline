"""Replay landed raw pages: re-parse them and enqueue the messages. No API calls.

``replay`` reads the envelopes under an ``s3://<bucket>/raw/<prefix>``, parses each
response with the same ``parse_timeline_page`` the fetch uses (a Tweet that still
fails to parse is quarantined, as in the fetch; a fixed parser recovers it) and
builds the same messages the fetch built, with the envelope's ``username``, its
original ``fetched_at`` and the object's key as ``raw_key``. Keeping the original
``fetched_at`` makes a replay safe to run at any time: the processor's conditional
write never lets it overwrite an item fetched later, and replaying twice rewrites the
same data. It never touches the fetch cursor.
"""

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from twitter_timeline_data_pipeline.models import TweetMessage
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


def messages_from_raw(
    s3: S3Client,
    bucket: str,
    prefix: str,
    *,
    quarantine: QuarantineStore | None = None,
    page_size: int = 1000,
) -> Iterator[tuple[str, list[TweetMessage]]]:
    """``(key, messages)`` for each raw object under ``prefix``, in key order.

    Items that fail to parse are written to ``quarantine`` (none: only logged, as in a
    dry run) and the rest of the page is replayed. Raises ``RawPageError`` (naming the
    key) for an object that can't be read back.
    """
    for page_no, (key, body) in enumerate(
        iter_raw_objects(s3, bucket, prefix, page_size=page_size), start=1
    ):
        try:
            envelope = Envelope.from_bytes(body)
            page = parse_timeline_page(envelope.response)
            messages = page.messages(
                envelope.username, fetched_at=envelope.fetched_at, raw_key=key
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
    quarantine: QuarantineStore | None = None,
) -> ReplayResult:
    """Hand the messages of every raw page under ``prefix`` to ``send`` (which returns
    how many it sent; a dry run prints them and returns 0)."""
    objects = tweets = enqueued = 0
    for key, messages in messages_from_raw(s3, bucket, prefix, quarantine=quarantine):
        objects += 1
        tweets += len(messages)
        enqueued += send(messages)
        logger.info("replayed %s: %d tweets", key, len(messages))
    return ReplayResult(objects=objects, tweets=tweets, enqueued=enqueued)
