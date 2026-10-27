"""Quarantine: items that could not be processed, kept in S3 instead of dropped.

An item lands here when one Tweet or media object of an API page fails to parse, or
when SQS rejects a message as unsendable (``SenderFault``). The rest of the page goes
on, and the item stays recoverable twice: in the raw page (``raw_key``) and here.
Key layout::

    quarantine/<user_id>/dt=YYYY-MM-DD/<run_id>-<page_no>-<index>.json

Each object is plain JSON::

    {"kind", "raw_key", "error", "item": <the JSON>, "tweet_id"?}

``tweet_id`` (additive) is the ID of the Tweet the object holds, when the kind has one:
``item.id`` of a ``tweet`` or ``included_tweet``, ``item.tweet.id`` of a ``message``.
Objects written before it existed are matched by those same paths.

``kind`` is ``tweet``, ``media``, ``included_tweet`` or ``included_user`` (an entry of
the page's ``includes``) or ``message``.

Fix the parser, then replay the raw prefix (``twitter-pipeline replay``): replay
re-parses the raw pages, so the fixed parser picks the item up again.
"""

import json
import logging
from typing import TYPE_CHECKING, Any

from twitter_timeline_data_pipeline.raw import new_run_id

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


class QuarantineUnavailableError(RuntimeError):
    """An item needs quarantining but there is nowhere to put it (no media bucket).
    The run fails with the cursor unmoved rather than drop the item."""


QUARANTINE_PREFIX = "quarantine/"
QUARANTINED_MESSAGE = "quarantined %s: key=%s raw_key=%s error=%s"
"""Stable, greppable text of the WARNING logged per quarantined object."""


def quarantine_key(
    user_id: str, fetched_at: str, run_id: str, page_no: int, index: int
) -> str:
    """``quarantine/<user_id>/dt=<date>/<run_id>-<page_no>-<index>.json``."""
    return (
        f"{QUARANTINE_PREFIX}{user_id}/dt={fetched_at[:10]}/"
        f"{run_id}-{page_no:04d}-{index:03d}.json"
    )


def item_tweet_id(kind: str, item: Any) -> str | None:
    """The ID of the Tweet a quarantined ``item`` holds (None for the other kinds, or
    when it has no usable ID)."""
    holder = item
    if kind == "message" and isinstance(item, dict):
        holder = item.get("tweet")
    elif kind not in ("tweet", "included_tweet"):
        return None
    tweet_id = holder.get("id") if isinstance(holder, dict) else None
    return tweet_id if isinstance(tweet_id, str) and tweet_id.isdecimal() else None


class QuarantineStore:
    """Writes the quarantine objects of one run (or replay)."""

    def __init__(self, s3: S3Client, bucket: str, *, run_id: str | None = None) -> None:
        self.s3 = s3
        self.bucket = bucket
        self.run_id = run_id or new_run_id()
        self._counts: dict[int, int] = {}

    def put(
        self,
        *,
        kind: str,
        item: Any,
        error: str,
        raw_key: str | None,
        user_id: str,
        fetched_at: str,
        page_no: int,
    ) -> str:
        """Upload one object and return its key. Raises if the upload fails: the
        caller must then fail the page rather than drop the item."""
        index = self._counts.get(page_no, 0)
        key = quarantine_key(user_id, fetched_at, self.run_id, page_no, index)
        document: dict[str, Any] = {
            "kind": kind,
            "raw_key": raw_key,
            "error": error,
            "item": item,
        }
        if (tweet_id := item_tweet_id(kind, item)) is not None:
            document["tweet_id"] = tweet_id
        body = json.dumps(document, separators=(",", ":"))
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode(),
            ContentType="application/json",
        )
        self._counts[page_no] = index + 1
        logger.warning(QUARANTINED_MESSAGE, kind, key, raw_key, error)
        return key
