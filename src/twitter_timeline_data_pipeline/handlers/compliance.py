"""Compliance Lambda: find Tweets deleted or made unavailable at the source, tombstone
them, then remove their content and every S3 object after a grace period.

This is the one deliberate exception to the pipeline's keep-everything rule. Each
scheduled run:

1. Walks the whole table in one consistent-key-order ``Scan`` (a projection, eventually
   consistent reads, a cursor in the state table under ``compliance##scan``, wrapping
   at the end), at most ``COMPLIANCE_MAX_LOOKUPS`` IDs and twice that many items per
   run, and not past the invocation's time. For each item it collects the Tweet's own
   ID (until it is purged) and the IDs of the posts its ``referenced_tweets`` hold a
   copy of, deduplicated, one budget for both. It does not matter under which handle
   or account an item was stored.
2. Looks each batch of 100 IDs up on X (``XClient.lookup_tweets``):

   * an unavailable Tweet gets ``deleted_at``, ``deletion_reason``, the sparse-index
     marker ``tombstone`` and loses ``expires_at`` (TTL must not delete a tombstone
     before its purge);
   * a Tweet reported present has its tombstone cleared, whatever its age, until it
     is purged;
   * an ID the API said nothing certain about is left alone: nothing is ever
     tombstoned, cleared or scrubbed on ambiguity, a failed lookup or a parser fault;
   * a copy of an unavailable original is marked the way a fetch marks it
     (``status`` ``unavailable``, ``unavailable_reason``, ``unavailable_since``, the
     copy kept whole); a copy of an original that is present again loses the mark; a
     copy marked for more than ``DELETION_GRACE_HOURS`` is scrubbed: the entry becomes
     ``{id, type, status, unavailable_reason, unavailable_since, scrubbed_at}``, the
     objects under ``text/<owner>/<id>.ref.<ref_id>.`` are deleted in every version,
     and the post is removed from the ``includes`` of the holder's raw pages and
     quarantine (never from a page's ``data``: the post's own pages are its own purge
     after its own grace).
3. Purges every tombstone older than ``DELETION_GRACE_HOURS``, after a fresh lookup of
   the due IDs that confirms each is still unavailable (present clears it, unknown or
   a failed lookup defers the purge and fails the run). A purge sets a ``purging``
   marker before it deletes anything, removes the Tweet from every raw page that
   holds it (resumable across runs, see ``ComplianceStore.redact_raw``) and from
   ``quarantine/``, deletes every version of everything under ``text/<owner>/<id>.``
   and ``media/<owner>/<id>/`` and the keys the item names, and replaces the item with
   a content-free tombstone (``tweet_id``, ``deleted_at``, ``deletion_reason``,
   ``purged_at``, ``revision``; ``tombstone`` = ``2`` and ``sweep_owners`` until the
   next run sweeps the prefixes once more, for uploads that were in flight). The
   tombstone stays for good: ``TweetStore.put_tweet`` refuses a Tweet that has one.

Every write to a Tweet item is conditional and does ``ADD revision 1``, so it joins
``put_tweet``'s optimistic lock: a ``put_tweet`` that read the item before cannot land
afterwards, and one that reads it after sees ``deleted_at`` and refuses. The function
runs with reserved concurrency 1.

Tombstones are found through the sparse ``tombstone-deleted_at-index`` GSI. Failures
are logged and counted, the run does as much of the rest as it can, and then raises so
the error alarm fires.
"""

import contextlib
import functools
import gzip
import json
import logging
import zlib
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from twitter_timeline_data_pipeline.config import Settings
from twitter_timeline_data_pipeline.credentials import clear_cache, get_bearer_token
from twitter_timeline_data_pipeline.models import utc_now_iso
from twitter_timeline_data_pipeline.quarantine import item_tweet_id
from twitter_timeline_data_pipeline.raw import RawPageError, redact_envelope
from twitter_timeline_data_pipeline.x_api import (
    MAX_LOOKUP_IDS,
    AuthError,
    TweetLookup,
    XClient,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

TOMBSTONE_INDEX = "tombstone-deleted_at-index"
TOMBSTONE_MARKER = "1"
"""``tombstone`` of an unavailable Tweet that is waiting for its purge."""
SWEEP_MARKER = "2"
"""``tombstone`` of a purged Tweet whose prefixes get one more sweep."""
CURSOR_PREFIX = "compliance#"
# State-table keys: the walk's cursor, and the progress of a raw-page redaction.
SCAN_CURSOR = "#scan"
REDACT_PREFIX = "compliance#redact#"
ACTIVE_UNITS = REDACT_PREFIX + "active"
# Most IDs (and holder items) one redaction batch takes: its state stays far below
# DynamoDB's 400 KB item limit; the rest wait for the next batch.
BATCH_CAP = 1000
# The function's IAM policy only allows these prefixes.
ALLOWED_KEY_PREFIXES = ("media/", "text/")
# Items read per scan request.
SCAN_PAGE = 100
# Requests of purge confirmation (100 IDs each) one run makes; the rest waits.
MAX_CONFIRM_REQUESTS = 5
# A Tweet lists at most one referenced post per type, plus its own ID.
MAX_IDS_PER_ITEM = 4
# Pages between two saves of a redaction's progress.
PROGRESS_EVERY = 25
# Lost races (the item changed between a read and a conditional write) before giving up
# until the next run.
WRITE_ATTEMPTS = 3
# What a stored referenced-post entry holds of the original; an entry with none of these
# is already scrubbed (or never had a copy) and needs no check.
COPY_FIELDS = (
    "text",
    "display_text",
    "text_s3_key",
    "display_text_s3_key",
    "author_id",
    "author_username",
    "created_at",
    "entities",
    "display_entities",
    "entities_s3_key",
    "display_entities_s3_key",
)
# What survives the purge on the tombstone.
TOMBSTONE_KEEPS = frozenset(
    {
        "tweet_id",
        "deleted_at",
        "deletion_reason",
        "revision",
        "purged_at",
        "tombstone",
        "sweep_owners",
    }
)
# Time kept back from the invocation before the job stops taking new work.
SAFETY_MARGIN_MS = 30_000


class ComplianceError(RuntimeError):
    """Part of a run failed; the rest was still done, and the next run retries."""


def _is_conditional_failure(exc: ClientError) -> bool:
    return (
        exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"
    )


def _entry_reason(reason: str | None) -> str:
    """The ``unavailable_reason`` a fetch writes for a lookup's reason: ``not_found``
    for a deleted post, ``not_authorized`` for protected, suspended and withheld."""
    return "not_found" if reason == "deleted" else "not_authorized"


def _iso(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")


def _day(item: Mapping[str, Any]) -> str:
    """``YYYY-MM-DD`` of the item's creation ("" when unknown: from the start)."""
    return str(item.get("created_at") or "")[:10]


def _mentions(body: bytes, ids: Collection[str]) -> bool:
    """Whether the object (gzipped or not) contains any of the IDs as text."""
    candidates = [body]
    with contextlib.suppress(OSError, EOFError, zlib.error):
        candidates.append(gzip.decompress(body))
    return any(i.encode() in c for c in candidates for i in ids)


class _OutOfTime(Exception):
    """A long read stopped for lack of time: the batch goes on in the next run."""


def _strip_message_copies(document: dict[str, Any], ids: Collection[str]) -> Any:
    """The quarantined ``message`` without the copies of the posts ``ids`` in its
    ``referenced`` (only the ID, type and unavailable status stay); None if it holds
    none, or is not a message."""
    item = document.get("item")
    if document.get("kind") != "message" or not isinstance(item, dict):
        return None
    refs = item.get("referenced")
    if not isinstance(refs, list):
        return None
    changed = False
    stripped: list[Any] = []
    content = ("text", "note_text", "author_id", "author_username", "created_at")
    for ref in refs:
        if (
            isinstance(ref, dict)
            and str(ref.get("id")) in ids
            and any(ref.get(k) for k in (*content, "entities"))
        ):
            keep = ("id", "type", "unavailable_reason", "unavailable_since")
            ref = {k: ref[k] for k in keep if ref.get(k)} | {"status": "unavailable"}
            changed = True
        stripped.append(ref)
    if not changed:
        return None
    return {**document, "item": {**item, "referenced": stripped}}


@dataclass(frozen=True, slots=True)
class RedactResult:
    """Where a raw-page redaction stands: ``done`` when every page of ``ids`` from
    ``first_day`` on, and the quarantine, has been handled."""

    done: bool
    ids: frozenset[str]
    first_day: str
    pages: int = 0
    quarantined: int = 0
    holders: tuple[str, ...] = ()

    def covers(self, ids: Collection[str], first_day: str) -> bool:
        return self.done and set(ids) <= self.ids and first_day >= self.first_day


class ComplianceStore:
    """The DynamoDB and S3 operations of the compliance job."""

    def __init__(
        self,
        tweets: Table,
        state: Table,
        s3_client: S3Client,
        bucket: str,
        retention_days: int = 0,
    ) -> None:
        self._retention_days = retention_days
        self._tweets = tweets
        self._state = state
        self._s3 = s3_client
        self._bucket = bucket

    # --- state: the walk's cursor and the redaction progress ---------------------

    def _load_state(self, name: str) -> dict[str, Any] | None:
        item = self._state.get_item(Key={"username": name}, ConsistentRead=True).get(
            "Item"
        )
        return cast("dict[str, Any] | None", item)

    def load_cursor(self, name: str) -> dict[str, Any] | None:
        """The ``ExclusiveStartKey`` to resume from (``name`` is ``SCAN_CURSOR``);
        None starts a new pass."""
        item = self._load_state(CURSOR_PREFIX + name)
        raw = None if item is None else item.get("cursor")
        if not raw:
            return None
        try:
            cursor = json.loads(str(raw))
        except ValueError:
            logger.warning("compliance cursor %s is unreadable; restarting", name)
            return None
        return cursor if isinstance(cursor, dict) else None

    def save_cursor(self, name: str, cursor: Mapping[str, Any] | None) -> None:
        item: dict[str, Any] = {
            "username": CURSOR_PREFIX + name,
            "updated_at": utc_now_iso(),
        }
        if cursor is not None:
            item["cursor"] = json.dumps(cursor, sort_keys=True)
        self._state.put_item(Item=item)

    # --- reads -----------------------------------------------------------------

    def page_items(
        self, start: Mapping[str, Any] | None, limit: int
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """Up to ``limit`` items of the table (a ``Scan`` in key order, eventually
        consistent: every decision is made again under the revision lock) with what the
        walk needs, and the key to continue from (None at the end)."""
        kwargs: dict[str, Any] = {
            "Limit": limit,
            "ProjectionExpression": (
                "tweet_id, referenced_tweets, revision, deleted_at, purged_at, "
                "author_id, username, created_at, raw_key"
            ),
        }
        if start is not None:
            kwargs["ExclusiveStartKey"] = dict(start)
        page = self._tweets.scan(**kwargs)
        return (
            cast("list[dict[str, Any]]", page["Items"]),
            cast("dict[str, Any] | None", page.get("LastEvaluatedKey")),
        )

    def _query_marker(self, marker: str) -> dict[str, str]:
        found: dict[str, str] = {}
        kwargs: dict[str, Any] = {
            "IndexName": TOMBSTONE_INDEX,
            "KeyConditionExpression": "tombstone = :t",
            "ExpressionAttributeValues": {":t": marker},
        }
        while True:
            page = self._tweets.query(**kwargs)
            for item in page["Items"]:
                found[str(item["tweet_id"])] = str(item["deleted_at"])
            if "LastEvaluatedKey" not in page:
                return found
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def tombstones(self) -> dict[str, str]:
        """Every tombstoned Tweet ID with its ``deleted_at`` (from the sparse GSI)."""
        return self._query_marker(TOMBSTONE_MARKER)

    def swept_pending(self) -> dict[str, str]:
        """Purged Tweets whose prefixes still get one more sweep."""
        return self._query_marker(SWEEP_MARKER)

    @staticmethod
    def lookup_ids(item: Mapping[str, Any]) -> list[str]:
        """The IDs to look up for a scanned item: its own (until it is purged) and the
        posts it holds a copy of."""
        own = [] if "purged_at" in item else [str(item["tweet_id"])]
        return list(dict.fromkeys([*own, *ComplianceStore.copy_ids(item)]))

    # --- tombstones -------------------------------------------------------------

    def tombstone(self, tweet_id: str, reason: str, now: str) -> bool:
        """Mark the Tweet unavailable unless it already is (or is gone); True if set."""
        try:
            self._tweets.update_item(
                Key={"tweet_id": tweet_id},
                # ADD revision: a put_tweet that read the item before must now fail its
                # revision condition, re-read, see deleted_at and refuse. REMOVE
                # expires_at: TTL must not delete the tombstone before its purge.
                UpdateExpression=(
                    "SET deleted_at = :now, deletion_reason = :reason, tombstone = :t "
                    "REMOVE expires_at ADD revision :one"
                ),
                ConditionExpression=(
                    "attribute_exists(tweet_id) AND attribute_not_exists(deleted_at)"
                ),
                ExpressionAttributeValues={
                    ":now": now,
                    ":reason": reason,
                    ":t": TOMBSTONE_MARKER,
                    ":one": 1,
                },
            )
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise
        return True

    def clear_tombstone(self, tweet_id: str) -> bool:
        """Remove the tombstone of a Tweet that is not yet purging or purged, whatever
        its age; True if one was removed. Once a purge has begun it is not undone (the
        post was confirmed gone right before it began). With retention on, the item
        gets its ``expires_at`` back, from ``first_stored_at``."""
        update = "REMOVE deleted_at, deletion_reason, tombstone ADD revision :one"
        condition = (
            "attribute_exists(deleted_at) AND attribute_not_exists(purged_at) "
            "AND attribute_not_exists(purging)"
        )
        values: dict[str, Any] = {":one": 1}
        if self._retention_days > 0:
            item = self.load_item(tweet_id)
            if item is None:
                return False
            lock, lock_values = self._lock(item)
            condition += f" AND {lock}"
            values |= lock_values
            first = item.get("first_stored_at") or item.get("stored_at")
            if first:
                stored = datetime.fromisoformat(str(first)).timestamp()
                values[":expires"] = int(stored) + self._retention_days * 86_400
                update = "SET expires_at = :expires " + update
        try:
            self._tweets.update_item(
                Key={"tweet_id": tweet_id},
                UpdateExpression=update,
                ConditionExpression=condition,
                ExpressionAttributeValues=values,
            )
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise
        return True

    # --- S3 -----------------------------------------------------------------------

    @staticmethod
    def s3_keys(item: Mapping[str, Any]) -> list[str]:
        """Every S3 key the item points to: ``*s3_key`` attributes of the item and of
        each ``media`` entry (``s3_key``, ``preview_s3_key``, ``text_s3_key`` ...)."""
        holders = [item, *(m for m in item.get("media", ()) if isinstance(m, Mapping))]
        keys = {
            v
            for holder in holders
            for k, v in holder.items()
            if k.endswith("s3_key") and isinstance(v, str) and v
        }
        return sorted(keys)

    def delete_object_versions(self, key: str) -> int:
        """Delete every version and delete marker of an item's object (``media/`` or
        ``text/`` only); return how many."""
        if not key.startswith(ALLOWED_KEY_PREFIXES):
            raise ComplianceError(
                f"refusing to delete key outside media/ and text/: {key}"
            )
        versions = self._list_versions(key)
        for version_id in (v for k, v in versions if k == key):
            self._s3.delete_object(Bucket=self._bucket, Key=key, VersionId=version_id)
        return sum(1 for k, _ in versions if k == key)

    def _list_versions(self, prefix: str) -> list[tuple[str, str]]:
        """``(key, version_id)`` of every version and delete marker under ``prefix``
        (all listed before any is deleted: deleting while paging would move the
        markers)."""
        found: list[tuple[str, str]] = []
        kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": prefix}
        while True:
            page = self._s3.list_object_versions(**kwargs)
            found += [
                (entry["Key"], entry["VersionId"])
                for entry in (*page.get("Versions", ()), *page.get("DeleteMarkers", ()))
                if entry["Key"].startswith(prefix)
            ]
            if not page.get("IsTruncated"):
                return found
            kwargs["KeyMarker"] = page["NextKeyMarker"]
            kwargs["VersionIdMarker"] = page["NextVersionIdMarker"]

    def delete_prefix_versions(self, prefix: str) -> tuple[int, int]:
        """Delete every version and delete marker of every object whose key starts with
        ``prefix`` (under ``text/`` or ``media/``); return ``(objects, versions)``.
        End the prefix with ``.`` (text) or ``/`` (media) so that Tweet 123 never
        matches 1234."""
        if not prefix.startswith(ALLOWED_KEY_PREFIXES):
            raise ComplianceError(
                f"refusing to delete prefix outside media/ and text/: {prefix}"
            )
        found = self._list_versions(prefix)
        for key, version_id in found:
            self._s3.delete_object(Bucket=self._bucket, Key=key, VersionId=version_id)
        return len({k for k, _ in found}), len(found)

    @staticmethod
    def owners(item: Mapping[str, Any]) -> list[str]:
        """The folders the item's objects could be under: author ID, lower-cased
        username, the folder of one of its own text keys, and ``sweep_owners``."""
        owners = {
            str(v).lower() if k == "username" else str(v)
            for k in ("author_id", "username")
            if (v := item.get(k))
        }
        owners.update(str(o) for o in item.get("sweep_owners") or ())
        for entry in (item, *(item.get("referenced_tweets") or ())):
            if not isinstance(entry, Mapping):
                continue
            for name in ("text_s3_key", "display_text_s3_key"):
                key = entry.get(name)
                if isinstance(key, str) and key.startswith("text/"):
                    owners.add(key.split("/")[1])
        return sorted(owners)

    @classmethod
    def text_prefixes(
        cls, item: Mapping[str, Any], tweet_id: str, middle: str = ""
    ) -> set[str]:
        """``text/<owner>/<tweet_id>.<middle>`` for each owner the item could have been
        written under, so objects whose key the item lost are found too."""
        return {f"text/{o}/{tweet_id}.{middle}" for o in cls.owners(item)}

    @classmethod
    def media_prefixes(cls, item: Mapping[str, Any], tweet_id: str) -> set[str]:
        """``media/<owner>/<tweet_id>/`` for each owner the item could be under."""
        return {f"media/{o}/{tweet_id}/" for o in cls.owners(item)}

    def load_item(self, tweet_id: str) -> dict[str, Any] | None:
        item = self._tweets.get_item(
            Key={"tweet_id": tweet_id}, ConsistentRead=True
        ).get("Item")
        return cast("dict[str, Any] | None", item)

    # --- raw pages and quarantine -----------------------------------------------

    def _list(self, prefix: str, start_after: str | None) -> Iterator[str]:
        """Keys under ``prefix`` after ``start_after`` (to the end of the prefix)."""
        options: dict[str, Any] = {"StartAfter": start_after} if start_after else {}
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix, **options):
            for entry in page.get("Contents", ()):
                yield entry["Key"]

    def _read(self, key: str) -> tuple[bytes, str] | None:
        """``(body, etag)`` of the object, None if it is gone."""
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read(), response["ETag"]
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return None
            raise

    # --- redaction batches ---------------------------------------------------------
    #
    # A batch is the unit of a resumable redaction: the IDs to remove, where in the raw
    # and quarantine listings it stands, the holder items to scrub once it is done (a
    # copy batch) and the page keys whose older versions are still to be deleted. It
    # lives in the state table under ``compliance#redact#<unit>`` (at most BATCH_CAP IDs
    # and holders, so it stays far below DynamoDB's 400 KB); the units of copy batches
    # are listed in ``compliance#redact#active`` so every run goes on with them.

    def _put_state(self, state: Mapping[str, Any], **changes: Any) -> dict[str, Any]:
        saved = {**state, **changes, "updated_at": utc_now_iso()}
        self._state.put_item(Item=saved)
        return saved

    def active_units(self) -> list[str]:
        record = self._load_state(ACTIVE_UNITS)
        return [] if record is None else [str(u) for u in record.get("units", ())]

    def _set_active(self, unit: str, active: bool) -> None:
        units = [u for u in self.active_units() if u != unit]
        self._put_state(
            {"username": ACTIVE_UNITS}, units=[*units, unit] if active else units
        )

    def load_batch(self, unit: str) -> dict[str, Any] | None:
        return self._load_state(REDACT_PREFIX + unit)

    def start_batch(
        self,
        unit: str,
        *,
        mode: str,
        author: str | None,
        ids: Collection[str],
        first_day: str,
        holders: Collection[str] = (),
        explicit: Collection[str] = (),
        media_keys: Collection[str] = (),
    ) -> dict[str, Any]:
        """Create the batch of ``unit`` (``mode`` ``own``: a purge, ``copy``: a scrub of
        copies; at most BATCH_CAP IDs and holders, the rest wait for the next batch)."""
        state = {
            "username": REDACT_PREFIX + unit,
            "mode": mode,
            "author": author,
            "ids": sorted({str(i) for i in ids})[:BATCH_CAP],
            "holders": sorted({str(h) for h in holders})[:BATCH_CAP],
            "explicit": sorted({str(k) for k in explicit})[:BATCH_CAP],
            "media_keys": sorted({str(k) for k in media_keys}),
            "first_day": first_day,
            "pending_versions": [],
            "complete": False,
        }
        saved = self._put_state(state)
        if mode == "copy":
            self._set_active(unit, True)
        return saved

    def finish_batch(self, unit: str) -> None:
        """A copy batch is done and its holders are scrubbed: it is no longer active."""
        self._set_active(unit, False)

    def redact_raw(
        self,
        unit: str,
        items: Sequence[Mapping[str, Any]],
        ids: Collection[str],
        *,
        first_day: str,
        author: str | None,
        stop: Callable[[], bool] = lambda: False,
    ) -> RedactResult:
        """The batch of a purge (``own`` mode): remove the Tweets ``ids`` from the raw
        pages and quarantine that hold them, going on with the batch already in
        progress for ``unit`` first (whatever ``ids`` the caller now asks for: see
        ``RedactResult.covers``), reusing a finished one that covers the request, and
        starting a new one otherwise. See ``advance`` for what a batch does."""
        wanted = {str(i) for i in ids}
        state = self.load_batch(unit)
        if state is not None and not state.get("complete"):
            pass  # finish the batch in progress
        elif (
            state is not None
            and state.get("mode") == "own"
            and wanted <= {str(i) for i in state["ids"]}
            and first_day >= str(state["first_day"])
        ):
            return self._result(state, done=True)
        else:
            explicit = {
                k
                for i in items
                if isinstance(k := i.get("raw_key"), str) and k.startswith("raw/")
            }
            taken = set(sorted(wanted)[:BATCH_CAP])
            self.start_batch(
                unit,
                mode="own",
                author=author,
                ids=taken,
                first_day=first_day,
                explicit=explicit,
                # the keys of the Tweets in the batch, so none is lost to the cap
                media_keys={
                    str(k)
                    for i in items
                    if str(i["tweet_id"]) in taken
                    for k in i.get("media_keys") or ()
                },
            )
        result = self.advance(unit, stop=stop)
        assert result is not None  # the batch was just saved
        return result

    @staticmethod
    def _result(
        state: Mapping[str, Any], *, done: bool, pages: int = 0, quarantined: int = 0
    ) -> RedactResult:
        return RedactResult(
            done,
            frozenset(str(i) for i in state["ids"]),
            str(state["first_day"]),
            pages,
            quarantined,
            tuple(str(h) for h in state.get("holders", ())),
        )

    def advance(
        self, unit: str, *, stop: Callable[[], bool] = lambda: False
    ) -> RedactResult | None:
        """Go on with the batch of ``unit`` (None if there is none).

        First the older versions of the pages it rewrote before are deleted
        (``pending_versions``: a failure after a redacted put must not keep the
        unredacted version). Then the pages named by the holders' ``raw_key``; then
        the listing of ``raw/<author>/`` from ``first_day`` (a re-fetch lands the same
        Tweet in more than one page, and a Tweet that is quoted by a later one is held
        in ``includes`` of that one's pages: there is no upper day), and the same
        under ``quarantine/``. A page is rewritten without the Tweets (an ``own``
        batch also from ``data``, with the IDs added to ``redactions`` so a replay
        skips them), put back with ``If-Match`` on the ETag that was read, and the
        versions older than the new one are deleted. Quarantine objects are matched by
        their real shape (``item.id`` of a ``tweet`` or ``included_tweet``,
        ``item.tweet.id`` of a ``message``, the top-level ``tweet_id`` newer writers
        add, and ``media`` by the doomed Tweets' ``media_keys`` unless another stored
        Tweet lists the key) and deleted; a ``message`` that merely holds a doomed
        post's copy in ``referenced`` is rewritten without that copy. A ``copy`` batch
        deletes only ``included_tweet`` objects.

        Resumable: the position is saved every ``PROGRESS_EVERY`` pages and when
        ``stop()`` says time is up (then the result is not ``done``).

        A page that can't be parsed but contains one of the IDs raises
        ``ComplianceError``: the Tweet is never marked purged with its content left.
        """
        loaded = self.load_batch(unit)
        if loaded is None:
            return None
        state: dict[str, Any] = loaded
        if state.get("complete"):
            return self._result(state, done=True)
        work = {str(i) for i in state["ids"]}
        own = state.get("mode") == "own"
        author = state.get("author")
        first = str(state["first_day"])
        media_keys = {str(k) for k in state.get("media_keys", ())}
        pages = quarantined = 0
        held: set[str] | None = None

        def save(**changes: Any) -> None:
            nonlocal state
            state = self._put_state(state, **changes)

        def track(key: str, pending: bool) -> None:
            keys = [k for k in state.get("pending_versions", ()) if k != key]
            save(pending_versions=[*keys, key] if pending else keys)

        def flush() -> None:
            for key in list(state.get("pending_versions", ())):
                self._delete_noncurrent_versions(key)
                track(key, False)

        def held_elsewhere() -> set[str]:
            """The media keys another item still lists. The scan keeps its place in
            the batch state, so a very large table is covered over several runs; a
            run that cannot read even one page is a failure (the alarm), not a wait."""
            nonlocal held
            if held is None:
                scan = state.get("held_scan") or {}
                cursor = json.loads(scan["cursor"]) if scan.get("cursor") else None
                found, cursor, finished, read = self.scan_held(
                    media_keys, work, cursor, scan.get("held", ()), stop
                )
                if not finished:
                    save(
                        held_scan={
                            "held": sorted(found),
                            "cursor": json.dumps(cursor) if cursor else "",
                        }
                    )
                    if read == 0:
                        raise ComplianceError(
                            "the scan for media held by other Tweets made no progress"
                        )
                    raise _OutOfTime
                held = found
            return held

        def unfinished() -> RedactResult:
            return self._result(state, done=False, pages=pages, quarantined=quarantined)

        flush()
        explicit = list(state.get("explicit", ()))
        for number in range(int(state.get("explicit_done", 0)), len(explicit)):
            if stop():
                save(explicit_done=number)
                return unfinished()
            pages += self._redact_page(explicit[number], work, own, track)
            if (number + 1) % PROGRESS_EVERY == 0:
                save(explicit_done=number + 1)
        if explicit:
            save(explicit_done=len(explicit))
        if author is not None and not state.get("raw_done"):
            start = state.get("raw_after") or (
                f"raw/{author}/dt={first}/" if first else None
            )
            last = None
            for handled, key in enumerate(self._list(f"raw/{author}/", start), 1):
                if stop():
                    save(raw_after=last or start or "")
                    return unfinished()
                pages += self._redact_page(key, work, own, track)
                last = key
                if handled % PROGRESS_EVERY == 0:
                    save(raw_after=key)
            save(raw_after=last or start or "", raw_done=True)
        if author is not None:
            start = state.get("quar_after") or (
                f"quarantine/{author}/dt={first}/" if first else None
            )
            last = None
            for handled, key in enumerate(
                self._list(f"quarantine/{author}/", start), 1
            ):
                if stop():
                    save(quar_after=last or start or "")
                    return unfinished()
                try:
                    quarantined += self._quarantined(
                        key, work, own, media_keys, held_elsewhere, track
                    )
                except _OutOfTime:
                    save(quar_after=last or start or "")
                    return unfinished()
                last = key
                if handled % PROGRESS_EVERY == 0:
                    save(quar_after=key)
        flush()
        save(complete=True)
        return self._result(state, done=True, pages=pages, quarantined=quarantined)

    def scan_held(
        self,
        keys: Collection[str],
        exclude: Collection[str],
        cursor: Mapping[str, Any] | None,
        held: Collection[str],
        stop: Callable[[], bool],
    ) -> tuple[set[str], dict[str, Any] | None, bool, int]:
        """Go on with the projected scan for the ``media_keys`` among ``keys`` that
        another stored item (not in ``exclude``, not purged) still lists, from
        ``cursor`` with ``held`` found so far. Returns ``(held, cursor, finished,
        pages)``; it stops between two pages when ``stop()`` says time is up."""
        wanted = set(keys)
        found = set(held)
        kwargs: dict[str, Any] = {
            "ProjectionExpression": "tweet_id, media_keys, purged_at"
        }
        if cursor is not None:
            kwargs["ExclusiveStartKey"] = dict(cursor)
        pages = 0
        position = dict(cursor) if cursor is not None else None
        while wanted:
            if stop():
                return found, position, False, pages
            page = self._tweets.scan(**kwargs)
            pages += 1
            for item in cast("list[dict[str, Any]]", page["Items"]):
                if str(item["tweet_id"]) in exclude or "purged_at" in item:
                    continue
                found |= wanted & {str(k) for k in item.get("media_keys") or ()}
            position = cast("dict[str, Any] | None", page.get("LastEvaluatedKey"))
            if position is None:
                break
            kwargs["ExclusiveStartKey"] = position
        return found, None, True, pages

    def media_keys_held_by_others(
        self,
        keys: Collection[str],
        exclude: Collection[str],
        stop: Callable[[], bool] = lambda: False,
    ) -> set[str]:
        """The whole scan of ``scan_held`` in one go; raises ``_OutOfTime`` if
        ``stop()`` says time is up before it is finished."""
        found, _, finished, _ = self.scan_held(keys, exclude, None, (), stop)
        if not finished:
            raise _OutOfTime
        return found

    def _put_rewritten(
        self,
        key: str,
        body: bytes,
        etag: str,
        content_type: str,
        track: Callable[[str, bool], None],
        gzipped: bool = False,
    ) -> bool:
        """Put the rewritten object back with ``If-Match`` and delete the versions older
        than the new one. The key is recorded as pending before the put and removed
        after the delete, so a failure in between is finished by the next run. False if
        the object changed since it was read (read it again)."""
        track(key, True)
        try:
            put = (
                self._s3.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=body,
                    ContentType=content_type,
                    ContentEncoding="gzip",
                    IfMatch=etag,
                )
                if gzipped
                else self._s3.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=body,
                    ContentType=content_type,
                    IfMatch=etag,
                )
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in (
                "PreconditionFailed",
                "412",
            ):
                track(key, False)
                return False
            raise
        if version := put.get("VersionId"):
            self._delete_older_versions(key, version)
        track(key, False)
        return True

    def _redact_page(
        self,
        key: str,
        ids: set[str],
        from_data: bool,
        track: Callable[[str, bool], None] = lambda key, pending: None,
    ) -> int:
        for _ in range(WRITE_ATTEMPTS):
            read = self._read(key)
            if read is None:
                return 0
            body, etag = read
            try:
                redacted, gone = redact_envelope(body, ids, from_data=from_data)
            except RawPageError as exc:
                if _mentions(body, ids):
                    raise ComplianceError(
                        f"raw page {key} can't be parsed but names a deleted post: "
                        f"{exc}"
                    ) from exc
                logger.warning("raw page %s is unreadable; left as it is", key)
                return 0
            if not gone:
                return 0
            if self._put_rewritten(
                key, redacted, etag, "application/json", track, gzipped=True
            ):
                logger.warning(
                    "redacted raw page %s: removed tweets %s", key, sorted(gone)
                )
                return 1
        raise ComplianceError(f"raw page {key} changed under {WRITE_ATTEMPTS} attempts")

    def _delete_older_versions(self, key: str, version: str) -> None:
        """Delete the versions of ``key`` older than ``version`` (newest first in the
        listing), keeping it and anything newer."""
        listed = [
            v for k, v in self._list_versions(key) if k == key
        ]  # newest first per S3
        if version in listed:
            for older in listed[listed.index(version) + 1 :]:
                self._s3.delete_object(Bucket=self._bucket, Key=key, VersionId=older)

    def _delete_noncurrent_versions(self, key: str) -> None:
        """Delete every version of ``key`` but the current one. Raw pages and
        quarantine objects are only rewritten by this job, so a version older than the
        current one is a copy from before a redaction. If the current entry is a delete
        marker nothing is deleted (someone removed the object: leave what is left)."""
        entries: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": key}
        while True:
            page = self._s3.list_object_versions(**kwargs)
            entries += [
                {**e, "marker": marker}
                for marker, group in (
                    (False, page.get("Versions", ())),
                    (True, page.get("DeleteMarkers", ())),
                )
                for e in group
                if e["Key"] == key
            ]
            if not page.get("IsTruncated"):
                break
            kwargs["KeyMarker"] = page["NextKeyMarker"]
            kwargs["VersionIdMarker"] = page["NextVersionIdMarker"]
        current = next((e for e in entries if e["IsLatest"]), None)
        if current is None or current["marker"]:
            return
        for entry in entries:
            if not entry["IsLatest"]:
                self._s3.delete_object(
                    Bucket=self._bucket, Key=key, VersionId=entry["VersionId"]
                )

    def _quarantined(
        self,
        key: str,
        ids: Collection[str],
        own: bool,
        media_keys: Collection[str],
        held: Callable[[], set[str]],
        track: Callable[[str, bool], None],
    ) -> int:
        """Delete (or, for a message that only holds a copy, rewrite) one quarantine
        object; 1 if it was changed."""
        for _ in range(WRITE_ATTEMPTS):
            read = self._read(key)
            if read is None:
                return 0
            body, etag = read
            try:
                raw = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
                document = json.loads(raw)
            except OSError, EOFError, zlib.error, ValueError:
                if _mentions(body, ids):
                    raise ComplianceError(
                        f"quarantine object {key} can't be parsed but names a "
                        "deleted post"
                    ) from None
                return 0
            if not isinstance(document, dict):
                return 0
            kind = document.get("kind")
            item = document.get("item")
            tweet_id = document.get("tweet_id") or item_tweet_id(str(kind), item)
            doomed = tweet_id is not None and str(tweet_id) in ids
            if not own and kind != "included_tweet":
                doomed = False  # a copy scrub never deletes a post's own objects
            if (
                not doomed
                and own
                and kind == "media"
                and isinstance(item, dict)
                and str(item.get("media_key")) in media_keys
            ):
                doomed = str(item["media_key"]) not in held()
            if doomed:
                for _, version_id in self._list_versions(key):
                    self._s3.delete_object(
                        Bucket=self._bucket, Key=key, VersionId=version_id
                    )
                logger.warning("deleted quarantined object %s", key)
                return 1
            stripped = _strip_message_copies(document, ids)
            if stripped is None:
                return 0
            if self._put_rewritten(
                key,
                json.dumps(stripped, separators=(",", ":")).encode(),
                etag,
                "application/json",
                track,
            ):
                logger.warning("stripped copies of deleted posts from %s", key)
                return 1
        raise ComplianceError(
            f"quarantine object {key} changed under {WRITE_ATTEMPTS} attempts"
        )

    # --- purge ------------------------------------------------------------------

    @staticmethod
    def _lock(item: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        """The condition (and its values) that the item still has the revision read."""
        revision = item.get("revision")
        if revision is None:
            return "attribute_not_exists(revision)", {}
        return "revision = :read", {":read": revision}

    def begin_purge(self, item: Mapping[str, Any]) -> dict[str, Any] | None:
        """Set the ``purging`` marker on a tombstoned, unpurged item (before anything
        is deleted) and return the item as updated; None if it can't be purged now
        (gone, cleared, already purged or no longer due). A marker already set (a
        purge that went on over several runs) is kept."""
        tweet_id = str(item["tweet_id"])
        current: Mapping[str, Any] | None = item
        for _ in range(WRITE_ATTEMPTS):
            if (
                current is None
                or current.get("tombstone") != TOMBSTONE_MARKER
                or "deleted_at" not in current
                or "purged_at" in current
            ):
                return None
            if "purging" in current:
                return dict(current)
            lock, values = self._lock(current)
            try:
                response = self._tweets.update_item(
                    Key={"tweet_id": tweet_id},
                    UpdateExpression="SET purging = :now ADD revision :one",
                    ConditionExpression=(
                        "attribute_exists(tweet_id) AND deleted_at = :deleted AND "
                        f"attribute_not_exists(purged_at) AND {lock}"
                    ),
                    ExpressionAttributeValues={
                        ":now": utc_now_iso(),
                        ":deleted": current["deleted_at"],
                        ":one": 1,
                        **values,
                    },
                    ReturnValues="ALL_NEW",
                )
            except ClientError as exc:
                if not _is_conditional_failure(exc):
                    raise
                current = self.load_item(tweet_id)
                continue
            return cast("dict[str, Any]", response["Attributes"])
        raise ComplianceError(
            f"tweet {tweet_id} changed under {WRITE_ATTEMPTS} purge starts"
        )

    def finish_purge(self, tweet_id: str) -> tuple[int, int] | None:
        """Delete the item's S3 objects (all versions), then replace the item with a
        content-free tombstone. Call it after ``begin_purge`` and the raw pages.

        Returns ``(objects, versions)`` removed, or None if the item is no longer a
        purge in progress (an operator removed its tombstone: it is not counted as
        purged). The objects are the keys the item names and everything under
        ``text/<owner>/<tweet_id>.`` (the text, the ``.ref.`` texts, the entities
        objects, also those whose key the item lost: the trailing dot keeps Tweet 123
        from matching 1234) and ``media/<owner>/<tweet_id>/``. S3 goes first, so a
        failure leaves the item naming the keys for the retry.

        The tombstone keeps ``tweet_id``, ``deleted_at``, ``deletion_reason`` and
        ``revision``, gains ``purged_at``, ``tombstone`` = ``2`` and ``sweep_owners``
        (until ``sweep``), and loses every other attribute. The write is conditional on
        the revision read and the marker, and does ``ADD revision 1``: a write that
        lands in between fails it, and the item is read again and the objects deleted
        again (``ComplianceError`` after ``WRITE_ATTEMPTS`` lost races, the next run
        retries).
        """
        objects = versions = 0
        for _ in range(WRITE_ATTEMPTS):
            current = self.load_item(tweet_id)
            if (
                current is None
                or "purging" not in current
                or "deleted_at" not in current
                or "purged_at" in current
            ):
                return None
            keys = self.s3_keys(current)
            versions += sum(self.delete_object_versions(key) for key in keys)
            objects += len(keys)
            prefixes = sorted(
                self.text_prefixes(current, tweet_id)
                | self.media_prefixes(current, tweet_id)
            )
            for prefix in prefixes:
                n_objects, n_versions = self.delete_prefix_versions(prefix)
                objects += n_objects
                versions += n_versions
            lock, values = self._lock(current)
            drop = sorted(set(current) - TOMBSTONE_KEEPS)
            names = {f"#a{n}": name for n, name in enumerate(drop)}
            request: dict[str, Any] = {
                "Key": {"tweet_id": tweet_id},
                "UpdateExpression": (
                    "SET purged_at = :purged, tombstone = :swept, "
                    "sweep_owners = :owners"
                    + (f" REMOVE {', '.join(names)}" if names else "")
                    + " ADD revision :one"
                ),
                "ConditionExpression": (
                    "attribute_exists(purging) AND deleted_at = :deleted "
                    f"AND attribute_not_exists(purged_at) AND {lock}"
                ),
                "ExpressionAttributeValues": {
                    ":purged": utc_now_iso(),
                    ":swept": SWEEP_MARKER,
                    ":owners": self.owners(current),
                    ":deleted": current["deleted_at"],
                    ":one": 1,
                    **values,
                },
            }
            if names:
                request["ExpressionAttributeNames"] = names
            try:
                self._tweets.update_item(**request)
            except ClientError as exc:
                if not _is_conditional_failure(exc):
                    raise
                continue
            return objects, versions
        raise ComplianceError(
            f"tweet {tweet_id} changed under {WRITE_ATTEMPTS} purge attempts"
        )

    def sweep(self, tweet_id: str) -> int:
        """Sweep the media and text prefixes of a purged Tweet once more (an upload that
        was in flight during the purge lands after it), then drop its sweep marker.
        Returns the number of versions deleted."""
        item = self.load_item(tweet_id)
        if item is None or item.get("tombstone") != SWEEP_MARKER:
            return 0
        deleted = 0
        for prefix in sorted(
            self.text_prefixes(item, tweet_id) | self.media_prefixes(item, tweet_id)
        ):
            deleted += self.delete_prefix_versions(prefix)[1]
        try:
            self._tweets.update_item(
                Key={"tweet_id": tweet_id},
                UpdateExpression="REMOVE tombstone, sweep_owners ADD revision :one",
                ConditionExpression="attribute_exists(purged_at)",
                ExpressionAttributeValues={":one": 1},
            )
        except ClientError as exc:
            if not _is_conditional_failure(exc):
                raise
        return deleted

    # --- copies of referenced posts -------------------------------------------------

    @staticmethod
    def holds_copy(entry: Mapping[str, Any]) -> bool:
        """Whether a ``referenced_tweets`` entry still holds content of the original."""
        return any(name in entry for name in COPY_FIELDS)

    @staticmethod
    def gone_since(entry: Mapping[str, Any]) -> str | None:
        """When the copy's original was first seen unavailable (``unavailable_since``
        of an entry with ``status`` ``unavailable``, set by a fetch or by this job).
        It only starts the grace clock; a scrub needs this job's own lookup to confirm
        that the original is still gone."""
        if entry.get("status") == "unavailable" and "unavailable_since" in entry:
            return str(entry["unavailable_since"])
        return None

    @classmethod
    def copy_ids(cls, item: Mapping[str, Any]) -> list[str]:
        """The IDs of the referenced posts the item holds a copy of (in order)."""
        if "deleted_at" in item:
            return []
        entries = item.get("referenced_tweets") or ()
        return list(
            dict.fromkeys(
                str(e["id"])
                for e in entries
                if isinstance(e, Mapping) and "id" in e and cls.holds_copy(e)
            )
        )

    @classmethod
    def scrub_due(
        cls, item: Mapping[str, Any], gone: Collection[str], now: str, cutoff: str
    ) -> list[str]:
        """The IDs among ``gone`` whose copy in ``item`` is past the grace period
        (``unavailable_since``, ``now`` for a first sighting, is at or before
        ``cutoff``)."""
        if "deleted_at" in item:
            return []
        return [
            str(e["id"])
            for e in item.get("referenced_tweets") or ()
            if isinstance(e, Mapping)
            and str(e.get("id")) in gone
            and cls.holds_copy(e)
            and str(cls.gone_since(e) or now) <= cutoff
        ]

    def update_copies(
        self,
        item: Mapping[str, Any],
        outcome: Mapping[str, str | None],
        *,
        now: str,
        cutoff: str,
        mark_only: bool = False,
    ) -> tuple[int, int, int]:
        """Apply what the X API said about the posts ``item`` holds copies of.

        ``outcome`` maps an ID to None when it is still available at the source, or to
        the lookup's reason when it is gone. For a copy of a gone post: the first time,
        the entry gets ``status: "unavailable"``, ``unavailable_reason`` (``not_found``
        for a deleted post, ``not_authorized`` for a protected, suspended or withheld
        one) and ``unavailable_since`` = ``now``, as a fetch marks it; once
        ``unavailable_since`` is at or before ``cutoff`` (the grace period has passed)
        the copy is scrubbed: the entry becomes ``{id, type, status, unavailable_reason,
        unavailable_since, scrubbed_at}``, so its text, author and entities are gone
        (``scrubbed_at`` keeps a redelivered message from putting them back, see
        ``storage._merge_reference``), and its S3 objects (everything under
        ``text/<owner>/<tweet_id>.ref.<ref_id>.``, with the entities objects, every
        version, and any key the entry names) are deleted before the write. For a copy
        of a post that is available again, the mark (the three fields) is removed. A
        copy whose post is not in ``outcome`` is left alone.

        One conditional ``UpdateItem`` that does ``ADD revision 1``; if the item
        changed since it was read it is read again and the decision is made again
        (``ComplianceError`` after ``WRITE_ATTEMPTS`` lost races). Returns
        ``(marked, unmarked, scrubbed)``.
        """
        tweet_id = str(item["tweet_id"])
        current: Mapping[str, Any] | None = item
        for _ in range(WRITE_ATTEMPTS):
            if current is None or "deleted_at" in current:
                return 0, 0, 0  # gone, or tombstoned: the purge takes the whole item
            sets: list[str] = []
            removes: list[str] = []
            values: dict[str, Any] = {}
            names: dict[str, str] = {}
            marked = unmarked = scrubbed = 0
            for n, entry in enumerate(current.get("referenced_tweets") or ()):
                if not isinstance(entry, Mapping) or not self.holds_copy(entry):
                    continue
                ref_id = str(entry.get("id"))
                if ref_id not in outcome:
                    continue
                position = f"referenced_tweets[{n}]"
                since = self.gone_since(entry)
                if outcome[ref_id] is None:
                    # Available again: clear the mark (the fetch's and this job's).
                    cleared = [
                        name
                        for name in (
                            "status",
                            "unavailable_reason",
                            "unavailable_since",
                        )
                        if name in entry and since is not None
                    ]
                    for name in cleared:
                        names[f"#{name}"] = name
                        removes.append(f"{position}.#{name}")
                    unmarked += bool(cleared)
                elif not mark_only and str(since or now) <= cutoff:
                    for prefix in self.text_prefixes(
                        current, tweet_id, f"ref.{ref_id}."
                    ):
                        self.delete_prefix_versions(prefix)
                    for name in (
                        "text_s3_key",
                        "display_text_s3_key",
                        "entities_s3_key",
                        "display_entities_s3_key",
                    ):
                        if isinstance(key := entry.get(name), str) and key:
                            self.delete_object_versions(key)
                    sets.append(f"{position} = :scrub{n}")
                    values[f":scrub{n}"] = {
                        "id": ref_id,
                        "type": entry.get("type"),
                        "status": "unavailable",
                        "unavailable_reason": entry.get("unavailable_reason")
                        or _entry_reason(outcome[ref_id]),
                        "unavailable_since": str(since or now),
                        "scrubbed_at": now,
                    }
                    scrubbed += 1
                elif since is None:  # the first sighting starts the grace clock
                    names["#status"] = "status"
                    sets.append(
                        f"{position}.#status = :unavailable, "
                        f"{position}.unavailable_reason = :reason{n}, "
                        f"{position}.unavailable_since = :since"
                    )
                    values[":unavailable"] = "unavailable"
                    values[f":reason{n}"] = _entry_reason(outcome[ref_id])
                    values[":since"] = now
                    marked += 1
            if not (sets or removes):
                return 0, 0, 0
            lock, lock_values = self._lock(current)
            request: dict[str, Any] = {
                "Key": {"tweet_id": tweet_id},
                "UpdateExpression": (
                    (f"SET {', '.join(sets)} " if sets else "")
                    + (f"REMOVE {', '.join(removes)} " if removes else "")
                    + "ADD revision :one"
                ),
                "ConditionExpression": (
                    "attribute_exists(tweet_id) AND "
                    f"attribute_not_exists(deleted_at) AND {lock}"
                ),
                "ExpressionAttributeValues": {":one": 1, **values, **lock_values},
            }
            if names:
                request["ExpressionAttributeNames"] = names
            try:
                self._tweets.update_item(**request)
            except ClientError as exc:
                if not _is_conditional_failure(exc):
                    raise
                current = self.load_item(tweet_id)
                continue
            if scrubbed:
                logger.warning(
                    "scrubbed %d copy(ies) of deleted posts in tweet %s",
                    scrubbed,
                    tweet_id,
                )
            return marked, unmarked, scrubbed
        raise ComplianceError(
            f"tweet {tweet_id} changed under {WRITE_ATTEMPTS} copy updates"
        )


class Compliance:
    """One run of the job over the whole table."""

    def __init__(
        self,
        store: ComplianceStore,
        lookup: Callable[[list[str]], TweetLookup],
        *,
        max_lookups: int,
        grace: timedelta,
        remaining_ms: Callable[[], int] | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._store = store
        self._lookup = lookup
        self._max_lookups = max_lookups
        self._grace = grace
        self._remaining_ms = remaining_ms
        self._now = now
        self._reserve_ms = SAFETY_MARGIN_MS  # time kept back from the phase running
        self.checked = 0
        self.ref_checked = 0
        self.purge_checked = 0
        self.scrub_checked = 0
        self.tombstoned = 0
        self.cleared = 0
        self.purged = 0
        self.deferred = 0
        self.swept = 0
        self.raw_pages_redacted = 0
        self.ref_raw_pages = 0
        self.unknown = 0
        self.copies_marked = 0
        self.copies_unmarked = 0
        self.copies_scrubbed = 0
        self.failures: list[str] = []
        self.wrapped = False
        self.auth_failed = False

    def run(self, username: str | None = None) -> None:
        """Walk, tombstone, clear, scrub copies, purge and sweep; raises
        ``ComplianceError`` if any step failed (after doing everything it could).
        ``username`` is only logged: the walk covers every item of the table."""
        tombstones = self._store.tombstones()
        sweeping = self._store.swept_pending()
        try:
            self._walk(tombstones)
        except Exception as exc:
            self.auth_failed = isinstance(exc, AuthError)
            logger.exception("compliance walk failed")
            self.failures.append(f"walk: {type(exc).__name__}")
        # The purges come first (a confirmed purge must not wait behind a long copy
        # batch); while copy batches are waiting they keep half of the time that is
        # left, so a purge's listing cannot starve them either.
        if self._remaining_ms is not None and self._store.active_units():
            left = self._remaining_ms() - SAFETY_MARGIN_MS
            self._reserve_ms = SAFETY_MARGIN_MS + max(0, left) // 2
        self._purge_due(tombstones)
        self._reserve_ms = SAFETY_MARGIN_MS
        self._advance_copy_batches()
        self._sweep(sweeping)
        logger.info(
            "compliance run for %s: checked=%d tombstoned=%d cleared=%d purged=%d "
            "unknown=%d failures=%d wrapped=%s",
            username or "all accounts",
            self.checked,
            self.tombstoned,
            self.cleared,
            self.purged,
            self.unknown,
            len(self.failures),
            self.wrapped,
        )
        logger.info(
            "compliance references: checked=%d marked=%d unmarked=%d scrubbed=%d "
            "raw_pages=%d purge_checked=%d deferred=%d swept=%d",
            self.ref_checked,
            self.copies_marked,
            self.copies_unmarked,
            self.copies_scrubbed,
            self.ref_raw_pages,
            self.purge_checked,
            self.deferred,
            self.swept,
        )
        if self.failures:
            raise ComplianceError("; ".join(self.failures))

    def _time_low(self) -> bool:
        return (
            self._remaining_ms is not None and self._remaining_ms() < self._reserve_ms
        )

    def _out_of_time(self) -> bool:
        if self._time_low():
            logger.warning("out of time: the rest waits for the next run")
            return True
        return False

    def _walk(self, tombstones: dict[str, str]) -> None:
        """One pass step over the table: look up the own and referenced IDs of as many
        items as the budget allows (item by item, so the cursor can stop between two),
        apply the answers, and scrub the copies that are due."""
        cursor = self._store.load_cursor(SCAN_CURSOR)
        budget = self._max_lookups
        scan_cap = 2 * self._max_lookups  # items per run, whatever they hold
        scanned = 0
        outcome: dict[str, str | None] = {}  # ID -> None if available, else its reason
        asked: set[str] = set()
        pending: list[dict[str, Any]] = []  # items with a copy to scrub
        now = _iso(self._now())
        cutoff = _iso(self._now() - self._grace)
        try:
            while budget > 0 and scanned < scan_cap:
                if self._out_of_time():
                    return
                items, next_cursor = self._store.page_items(
                    cursor, min(SCAN_PAGE, scan_cap - scanned)
                )
                scanned += len(items)
                taken: list[dict[str, Any]] = []
                batch: dict[str, None] = {}
                fits = True
                for item in items:
                    fresh = [
                        i
                        for i in self._store.lookup_ids(item)
                        if i not in asked and i not in batch
                    ]
                    if len(batch) + len(fresh) > budget:
                        fits = False
                        break
                    batch.update(dict.fromkeys(fresh))
                    taken.append(item)
                if not fits and not taken:
                    logger.warning(
                        "compliance walk: one item needs more lookups than the budget "
                        "has left (COMPLIANCE_MAX_LOOKUPS=%d, at least %d is advised)",
                        self._max_lookups,
                        MAX_IDS_PER_ITEM,
                    )
                    return
                own = {str(i["tweet_id"]) for i in taken}
                ids = list(batch)
                for start in range(0, len(ids), MAX_LOOKUP_IDS):
                    chunk = ids[start : start + MAX_LOOKUP_IDS]
                    result = self._lookup(chunk)
                    asked.update(chunk)
                    self._apply(result, tombstones)
                    outcome.update(dict.fromkeys(result.present))
                    outcome.update(result.unavailable)
                    self.checked += len(chunk)
                    self.ref_checked += sum(1 for i in chunk if i not in own)
                budget -= len(ids)
                for item in taken:
                    self._handle_copies(item, outcome, now, cutoff, pending)
                if not fits:
                    cursor = {"tweet_id": taken[-1]["tweet_id"]}
                    self._store.save_cursor(SCAN_CURSOR, cursor)
                    return
                cursor = next_cursor
                self._store.save_cursor(SCAN_CURSOR, cursor)
                if cursor is None:
                    self.wrapped = True
                    return
        finally:
            self._scrub_copies(pending, outcome, now, cutoff)

    def _handle_copies(
        self,
        item: dict[str, Any],
        outcome: Mapping[str, str | None],
        now: str,
        cutoff: str,
        pending: list[dict[str, Any]],
    ) -> None:
        gone = {i for i, reason in outcome.items() if reason is not None}
        if self._store.scrub_due(item, gone, now, cutoff):
            pending.append(item)  # after the raw pages are redacted
            return
        try:
            marked, unmarked, _ = self._store.update_copies(
                item, outcome, now=now, cutoff=cutoff
            )
        except Exception as exc:
            logger.exception("updating copies in tweet %s failed", item["tweet_id"])
            self.failures.append(f"copies {item['tweet_id']}: {type(exc).__name__}")
            return
        self.copies_marked += marked
        self.copies_unmarked += unmarked

    def _scrub_copies(
        self,
        pending: list[dict[str, Any]],
        outcome: Mapping[str, str | None],
        now: str,
        cutoff: str,
    ) -> None:
        """Register the copies that are past the grace period as a redaction batch per
        holder author: the raw pages' includes and the quarantine are cleaned first,
        and the holders are scrubbed when the batch is done, in whatever run that is
        (``_advance_copy_batches``). A batch already in progress is finished first; the
        walk registers the rest when it next reaches them."""
        gone = {i for i, reason in outcome.items() if reason is not None}
        by_author: dict[str | None, list[dict[str, Any]]] = {}
        for item in pending:
            by_author.setdefault(item.get("author_id"), []).append(item)
        if not by_author:
            return
        active = set(self._store.active_units())
        for author, group in by_author.items():
            unit = f"copy#{author or '-'}"
            if unit in active:
                # One batch per author at a time. The waiting items still get their
                # first sighting recorded (the grace clock must start), and the walk
                # registers them when it next reaches them.
                logger.info(
                    "a redaction of deleted copies is in progress for %s: %d tweet(s) "
                    "wait for the walk to reach them again",
                    author,
                    len(group),
                )
                for item in group:
                    try:
                        marked, _, _ = self._store.update_copies(
                            item, outcome, now=now, cutoff=cutoff, mark_only=True
                        )
                    except Exception as exc:
                        logger.exception(
                            "marking copies in %s failed", item["tweet_id"]
                        )
                        self.failures.append(
                            f"copies {item['tweet_id']}: {type(exc).__name__}"
                        )
                        continue
                    self.copies_marked += marked
                continue
            due = {
                i
                for item in group
                for i in self._store.scrub_due(item, gone, now, cutoff)
            }
            try:
                self._store.start_batch(
                    unit,
                    mode="copy",
                    author=author,
                    ids=due,
                    first_day=min(_day(item) for item in group),
                    holders=[str(item["tweet_id"]) for item in group],
                    explicit=[
                        k
                        for item in group
                        if isinstance(k := item.get("raw_key"), str)
                        and k.startswith("raw/")
                    ],
                )
            except Exception as exc:
                logger.exception("registering a redaction of deleted copies failed")
                self.failures.append(f"raw pages of copies: {type(exc).__name__}")

    def _advance_copy_batches(self) -> None:
        """Go on with every copy batch in progress (whichever run registered it): clean
        its pages while time allows, and when one is done scrub its holders."""
        for unit in self._store.active_units():
            if self._out_of_time():
                return
            try:
                result = self._store.advance(unit, stop=self._time_low)
            except Exception as exc:
                logger.exception("redacting raw pages of deleted posts failed")
                self.failures.append(f"raw pages of copies: {type(exc).__name__}")
                continue
            if result is None:
                self._store.finish_batch(unit)  # the batch is gone
                continue
            self.ref_raw_pages += result.pages
            self.raw_pages_redacted += result.pages
            if result.done:
                self._finish_copy_batch(unit, result)

    def _finish_copy_batch(self, unit: str, result: RedactResult) -> None:
        """The pages of a copy batch are clean: confirm with a fresh lookup that its
        posts are still unavailable (one that is present or unknown is not scrubbed),
        then scrub the copies in the holders, read again. Idempotent: a failure leaves
        the batch active and the next run does it again."""
        outcome: dict[str, str | None] = {}
        ids = sorted(result.ids)
        for start in range(0, len(ids), MAX_LOOKUP_IDS):
            chunk = ids[start : start + MAX_LOOKUP_IDS]
            try:
                answer = self._lookup(chunk)
            except Exception as exc:
                self.auth_failed = self.auth_failed or isinstance(exc, AuthError)
                logger.exception("confirming deleted copies failed")
                self.failures.append(f"copy confirmation: {type(exc).__name__}")
                return
            self.scrub_checked += len(chunk)
            outcome.update(dict.fromkeys(answer.present))
            outcome.update(answer.unavailable)
        now = _iso(self._now())
        cutoff = _iso(self._now() - self._grace)
        complete = True
        for holder in result.holders:
            try:
                item = self._store.load_item(holder)
                if item is None:
                    continue
                marked, unmarked, scrubbed = self._store.update_copies(
                    item, outcome, now=now, cutoff=cutoff
                )
            except Exception as exc:
                logger.exception("scrubbing copies in tweet %s failed", holder)
                self.failures.append(f"copies {holder}: {type(exc).__name__}")
                complete = False
                continue
            self.copies_marked += marked
            self.copies_unmarked += unmarked
            self.copies_scrubbed += scrubbed
        if complete:
            self._store.finish_batch(unit)

    def _apply(self, result: TweetLookup, tombstones: dict[str, str]) -> None:
        now = _iso(self._now())
        for tweet_id, reason in result.unavailable.items():
            if self._store.tombstone(tweet_id, reason, now):
                tombstones[tweet_id] = now
                self.tombstoned += 1
                logger.warning(
                    "tombstoned tweet %s: reason=%s deleted_at=%s",
                    tweet_id,
                    reason,
                    now,
                )
        for tweet_id in result.present:
            if tweet_id in tombstones and self._store.clear_tombstone(tweet_id):
                del tombstones[tweet_id]
                self.cleared += 1
                logger.info("tweet %s is available again: tombstone cleared", tweet_id)
        if result.unknown:
            self.unknown += len(result.unknown)
            logger.info(
                "%d tweet(s) could not be classified; left as they are",
                len(result.unknown),
            )

    def _confirm_due(self, due: list[str], tombstones: dict[str, str]) -> list[str]:
        """The IDs of ``due`` that a lookup right now still reports unavailable. One
        reported present is cleared; an unknown answer or a failed lookup defers the
        purge and fails the run (the alarm), it never purges."""
        confirmed: list[str] = []
        for number, start in enumerate(range(0, len(due), MAX_LOOKUP_IDS)):
            if number >= MAX_CONFIRM_REQUESTS or self._time_low():
                # Not a failure: the rest is confirmed (and purged) in the next run.
                self.deferred += len(due) - start
                logger.info(
                    "%d due tombstone(s) wait for the next run", len(due) - start
                )
                break
            chunk = due[start : start + MAX_LOOKUP_IDS]
            try:
                result = self._lookup(chunk)
            except Exception as exc:
                self.auth_failed = self.auth_failed or isinstance(exc, AuthError)
                logger.exception("confirming %d due tombstone(s) failed", len(chunk))
                self.failures.append(f"purge confirmation: {type(exc).__name__}")
                self.deferred += len(chunk)
                continue
            self.purge_checked += len(chunk)
            for tweet_id in result.present:
                if self._store.clear_tombstone(tweet_id):
                    tombstones.pop(tweet_id, None)
                    self.cleared += 1
                    logger.info(
                        "tweet %s is available again: tombstone cleared before its "
                        "purge",
                        tweet_id,
                    )
            confirmed += [i for i in chunk if i in result.unavailable]
            unsure = [i for i in chunk if i in result.unknown]
            if unsure:
                self.deferred += len(unsure)
                self.failures.append(f"purge confirmation unknown: {len(unsure)}")
                logger.error("purge deferred, the API gave no answer for %s", unsure)
        return confirmed

    def _purge_due(self, tombstones: Mapping[str, str]) -> None:
        cutoff = _iso(self._now() - self._grace)
        due = [
            t for t, deleted_at in sorted(tombstones.items()) if deleted_at <= cutoff
        ]
        if not due or self._out_of_time():
            return
        # Once a purge has begun (``purging``) it always completes: it was confirmed
        # unavailable after the grace period right before it began, and it is not
        # asked about again. The others are confirmed now.
        loaded: dict[str, dict[str, Any]] = {}
        fresh: list[str] = []
        going: list[str] = []
        for tweet_id in due:
            if self._out_of_time():
                break
            try:
                item = self._store.load_item(tweet_id)
            except Exception as exc:
                logger.exception("reading tweet %s failed", tweet_id)
                self.failures.append(f"purge {tweet_id}: {type(exc).__name__}")
                continue
            if item is None or str(item.get("deleted_at", "9")) > cutoff:
                logger.info("tweet %s is no longer due: skipped", tweet_id)
                continue
            loaded[tweet_id] = item
            (going if "purging" in item else fresh).append(tweet_id)
        confirmed = self._confirm_due(fresh, cast("dict[str, str]", tombstones))
        begun: list[dict[str, Any]] = []
        for tweet_id in [*going, *confirmed]:
            if self._out_of_time():
                break
            try:
                item = self._store.begin_purge(loaded[tweet_id])
            except Exception as exc:
                logger.exception("starting the purge of tweet %s failed", tweet_id)
                self.failures.append(f"purge {tweet_id}: {type(exc).__name__}")
                continue
            if item is None:
                logger.info("tweet %s was already removed or cleared", tweet_id)
                continue
            begun.append(item)
        by_author: dict[str | None, list[dict[str, Any]]] = {}
        for item in begun:
            by_author.setdefault(item.get("author_id"), []).append(item)
        for author, group in by_author.items():
            ids = {str(i["tweet_id"]) for i in group}
            try:
                result = self._store.redact_raw(
                    f"own#{author or '-'}",
                    group,
                    ids,
                    first_day=min(_day(i) for i in group),
                    author=author,
                    stop=self._time_low,
                )
            except Exception as exc:
                # Nothing is purged: the items keep naming what is still to remove.
                logger.exception("redacting raw pages failed")
                self.failures.append(f"raw pages: {type(exc).__name__}")
                continue
            self.raw_pages_redacted += result.pages
            for item in group:
                tweet_id = str(item["tweet_id"])
                if not result.covers({tweet_id}, _day(item)):
                    self.deferred += 1
                    logger.info(
                        "purge of tweet %s waits: raw pages are not yet redacted",
                        tweet_id,
                    )
                    continue
                if self._out_of_time():
                    return
                self._finish(tweet_id, item, result)

    def _finish(
        self, tweet_id: str, item: Mapping[str, Any], result: RedactResult
    ) -> None:
        try:
            removed = self._store.finish_purge(tweet_id)
        except Exception as exc:
            logger.exception("purging tweet %s failed", tweet_id)
            self.failures.append(f"purge {tweet_id}: {type(exc).__name__}")
            return
        if removed is None:
            logger.warning(
                "tweet %s is no longer being purged (its tombstone was removed): "
                "not counted as purged",
                tweet_id,
            )
            return
        self.purged += 1
        logger.warning(
            "purged tweet %s: s3_objects=%d s3_versions=%d raw_pages=%d "
            "quarantined=%d deleted_at=%s",
            tweet_id,
            removed[0],
            removed[1],
            result.pages,
            result.quarantined,
            item["deleted_at"],
        )

    def _sweep(self, sweeping: Mapping[str, str]) -> None:
        """The purged Tweets of an earlier run: sweep their prefixes once more."""
        for tweet_id in sorted(sweeping):
            if self._out_of_time():
                return
            try:
                self.swept += self._store.sweep(tweet_id)
            except Exception as exc:
                logger.exception("sweeping tweet %s failed", tweet_id)
                self.failures.append(f"sweep {tweet_id}: {type(exc).__name__}")


def _short_timeouts() -> Config:
    return Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 3})


@functools.cache
def _store(
    tweets_table: str, state_table: str, bucket: str, retention_days: int = 0
) -> ComplianceStore:
    dynamodb = boto3.resource("dynamodb", config=_short_timeouts())
    return ComplianceStore(
        dynamodb.Table(tweets_table),
        dynamodb.Table(state_table),
        boto3.client("s3", config=_short_timeouts()),
        bucket,
        retention_days,
    )


def handler(event: Any, context: Any) -> dict[str, Any]:
    settings = Settings.from_env()
    settings.require("tweets_table", "state_table", "media_bucket")
    # narrowed by require()
    assert settings.tweets_table and settings.state_table and settings.media_bucket
    username = event.get("username") if isinstance(event, dict) else None

    remaining_ms = getattr(context, "get_remaining_time_in_millis", None)
    max_rate_limit_wait = 60.0
    if remaining_ms:
        max_rate_limit_wait = max(0.0, min(60.0, remaining_ms() / 1000 - 15))
    token = get_bearer_token(settings)
    with XClient(
        token,
        base_url=settings.x_api_base_url,
        timeout=settings.http_timeout,
        max_rate_limit_wait=max_rate_limit_wait,
    ) as x_client:
        job = Compliance(
            _store(
                settings.tweets_table,
                settings.state_table,
                settings.media_bucket,
                settings.retention_days,
            ),
            x_client.lookup_tweets,
            max_lookups=settings.compliance_max_lookups,
            grace=timedelta(hours=settings.deletion_grace_hours),
            remaining_ms=remaining_ms,
        )
        try:
            job.run(username)
        finally:
            if job.auth_failed:
                clear_cache()  # the token may have been rotated; re-read it next time
    return {
        "checked": job.checked,
        "tombstoned": job.tombstoned,
        "cleared": job.cleared,
        "purged": job.purged,
        "unknown": job.unknown,
        "wrapped": job.wrapped,
        "raw_pages_redacted": job.raw_pages_redacted,
        "references_checked": job.ref_checked,
        "copies_scrubbed": job.copies_scrubbed,
        "purge_deferred": job.deferred,
    }
