"""Compliance Lambda: find Tweets deleted or made unavailable at the source, tombstone
them, then remove their content and every S3 object after a grace period.

This is the one deliberate exception to the pipeline's keep-everything rule. Each
scheduled run:

1. Walks the stored Tweets of the configured account through the
   ``username-created_at-index`` GSI, at most ``COMPLIANCE_MAX_LOOKUPS`` IDs per run,
   resuming from a cursor kept in the state table under ``compliance#<handle>`` (X
   handles can't contain ``#``, so it never collides with a fetch checkpoint) and
   wrapping around when the walk reaches the end.
2. Looks each batch of 100 IDs up on X (``XClient.lookup_tweets``). An unavailable
   Tweet gets ``deleted_at``, ``deletion_reason`` and the sparse-index marker
   ``tombstone``. A present Tweet whose tombstone is still inside the grace period has
   it cleared (a protected account may come back). An ID the API said nothing certain
   about is left alone: nothing is ever tombstoned on ambiguity.
3. Purges every tombstone older than ``DELETION_GRACE_HOURS``: the Tweet is removed
   from every raw API page that holds it (the page is rewritten, the ID goes into its
   ``redactions``, older versions are deleted) and from ``quarantine/``; all versions
   of every S3 object the item points to are deleted; and the item is replaced by a
   content-free tombstone (``tweet_id``, ``deleted_at``, ``deletion_reason``,
   ``purged_at``). S3 goes first, so a retry after a failure still finds the keys. The
   tombstone stays for good: ``TweetStore.put_tweet`` refuses a Tweet that has one, so
   a replay or redrive can't bring the Tweet back.

4. Walks the stored items for the posts they *quote, reply to or retweet* (the
   ``referenced_tweets`` entries hold a copy of the original: text, author, entities,
   and for a long original an S3 object ``text/<owner>/<id>.ref.<ref_id>[...].json``).
   The IDs are looked up like the account's own (deduplicated, in the same
   ``COMPLIANCE_MAX_LOOKUPS`` budget: at most half of it, resuming from its own
   cursor ``compliance#`` + ``#references`` over a bounded, resumable table scan). The
   first run that finds an original unavailable marks the copy the way a fetch does
   (``status: "unavailable"``, ``unavailable_reason``, ``unavailable_since``, the copy
   kept whole; a fetch that saw the error first has already started the clock); one
   that finds it available again removes the mark; a copy marked for more
   than ``DELETION_GRACE_HOURS`` is scrubbed (the entry becomes ``{id, type, status:
   "unavailable", unavailable_since}`` (plus ``unavailable_reason`` if set), its S3
   objects under ``text/<owner>/<id>.ref.<ref_id>.`` are deleted in every version,
   and the post is removed from the raw pages' ``includes.tweets``).

Every write to a Tweet item (tombstone, clearing it, the purge, a copy mark or scrub)
is conditional and does ``ADD revision 1``, so it joins ``put_tweet``'s optimistic
lock: a ``put_tweet`` that read the item before cannot land afterwards, and one that
reads it after sees ``deleted_at`` and refuses.

Tombstones are found through the sparse ``tombstone-deleted_at-index`` GSI, which holds
only tombstoned items, so no run scans the table. Failures are logged and counted, the
run does as much of the rest as it can, and then raises so the error alarm fires.
"""

import functools
import gzip
import json
import logging
import zlib
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from twitter_timeline_data_pipeline.config import Settings
from twitter_timeline_data_pipeline.credentials import clear_cache, get_bearer_token
from twitter_timeline_data_pipeline.models import utc_now_iso
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

USERNAME_INDEX = "username-created_at-index"
TOMBSTONE_INDEX = "tombstone-deleted_at-index"
TOMBSTONE_MARKER = "1"
CURSOR_PREFIX = "compliance#"
# The function's IAM policy only allows these prefixes.
ALLOWED_KEY_PREFIXES = ("media/", "text/")
# State-table key (after ``compliance#``) of the references walk. Handles can't contain
# ``#``, so it never collides with an account's cursor.
REFERENCES_CURSOR = "#references"
# Items read per scan request of the references walk.
SCAN_PAGE = 100
# A Tweet lists at most one referenced post per type (retweeted, quoted, replied_to).
MAX_REFS_PER_ITEM = 3
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
TOMBSTONE_KEEPS = frozenset({"tweet_id", "deleted_at", "deletion_reason", "revision"})
# Time kept back from the invocation before the purge stops taking new items.
SAFETY_MARGIN_MS = 30_000


class ComplianceError(RuntimeError):
    """Part of a run failed; the rest was still done, and the next run retries."""


def _handle(username: str) -> str:
    return username.removeprefix("@").lower()


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


class ComplianceStore:
    """The DynamoDB and S3 operations of the compliance job."""

    def __init__(
        self, tweets: Table, state: Table, s3_client: S3Client, bucket: str
    ) -> None:
        self._tweets = tweets
        self._state = state
        self._s3 = s3_client
        self._bucket = bucket

    # --- cursor ----------------------------------------------------------------

    def load_cursor(self, handle: str) -> dict[str, Any] | None:
        """The ``ExclusiveStartKey`` to resume from (``handle`` is an account's handle
        or ``REFERENCES_CURSOR``); None starts a new pass."""
        item = self._state.get_item(
            Key={"username": CURSOR_PREFIX + handle}, ConsistentRead=True
        ).get("Item")
        raw = None if item is None else item.get("cursor")
        if not raw:
            return None
        try:
            cursor = json.loads(str(raw))
        except ValueError:
            logger.warning("compliance cursor for %s is unreadable; restarting", handle)
            return None
        return cursor if isinstance(cursor, dict) else None

    def save_cursor(self, handle: str, cursor: Mapping[str, Any] | None) -> None:
        item: dict[str, Any] = {
            "username": CURSOR_PREFIX + handle,
            "updated_at": utc_now_iso(),
        }
        if cursor is not None:
            item["cursor"] = json.dumps(cursor, sort_keys=True)
        self._state.put_item(Item=item)

    # --- reads -----------------------------------------------------------------

    def page_ids(
        self, handle: str, start: Mapping[str, Any] | None, limit: int
    ) -> tuple[list[str], dict[str, Any] | None]:
        """Up to ``limit`` stored Tweet IDs of ``handle`` and the key to continue
        from (None when the index is exhausted)."""
        kwargs: dict[str, Any] = {
            "IndexName": USERNAME_INDEX,
            "KeyConditionExpression": "username = :u",
            "ExpressionAttributeValues": {":u": handle},
            "ProjectionExpression": "tweet_id",
            "Limit": limit,
        }
        if start is not None:
            kwargs["ExclusiveStartKey"] = dict(start)
        page = self._tweets.query(**kwargs)
        ids = [str(i["tweet_id"]) for i in page["Items"]]
        return ids, cast("dict[str, Any] | None", page.get("LastEvaluatedKey"))

    def page_references(
        self, start: Mapping[str, Any] | None, limit: int
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """Up to ``limit`` items of the table (a consistent scan, in key order) with
        what the references walk needs, and the key to continue from (None at the
        end). Tombstoned items are included: a purge has no ``referenced_tweets``."""
        kwargs: dict[str, Any] = {
            "Limit": limit,
            "ConsistentRead": True,
            "ProjectionExpression": (
                "tweet_id, referenced_tweets, revision, deleted_at, author_id, "
                "username, created_at, raw_key"
            ),
        }
        if start is not None:
            kwargs["ExclusiveStartKey"] = dict(start)
        page = self._tweets.scan(**kwargs)
        return (
            cast("list[dict[str, Any]]", page["Items"]),
            cast("dict[str, Any] | None", page.get("LastEvaluatedKey")),
        )

    def tombstones(self) -> dict[str, str]:
        """Every tombstoned Tweet ID with its ``deleted_at`` (from the sparse GSI)."""
        found: dict[str, str] = {}
        kwargs: dict[str, Any] = {
            "IndexName": TOMBSTONE_INDEX,
            "KeyConditionExpression": "tombstone = :t",
            "ExpressionAttributeValues": {":t": TOMBSTONE_MARKER},
        }
        while True:
            page = self._tweets.query(**kwargs)
            for item in page["Items"]:
                found[str(item["tweet_id"])] = str(item["deleted_at"])
            if "LastEvaluatedKey" not in page:
                return found
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    # --- tombstones -------------------------------------------------------------

    def tombstone(self, tweet_id: str, reason: str, now: str) -> bool:
        """Mark the Tweet unavailable unless it already is (or is gone); True if set."""
        try:
            self._tweets.update_item(
                Key={"tweet_id": tweet_id},
                # ADD revision: a put_tweet that read the item before must now fail its
                # revision condition, re-read, see deleted_at and refuse.
                UpdateExpression=(
                    "SET deleted_at = :now, deletion_reason = :reason, tombstone = :t "
                    "ADD revision :one"
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

    def clear_tombstone(self, tweet_id: str, cutoff: str) -> bool:
        """Remove a tombstone that is newer than ``cutoff``; True if one was removed."""
        try:
            self._tweets.update_item(
                Key={"tweet_id": tweet_id},
                UpdateExpression=(
                    "REMOVE deleted_at, deletion_reason, tombstone ADD revision :one"
                ),
                ConditionExpression=(
                    "deleted_at > :cutoff AND attribute_not_exists(purged_at)"
                ),
                ExpressionAttributeValues={":cutoff": cutoff, ":one": 1},
            )
        except ClientError as exc:
            if _is_conditional_failure(exc):
                return False
            raise
        return True

    # --- purge ------------------------------------------------------------------

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
        return self._delete_versions(key)

    def _delete_versions(self, key: str, *, keep: str | None = None) -> int:
        """Delete the versions and delete markers of ``key`` except ``keep``."""
        # List everything first, then delete: removing versions while paging would
        # move the page markers under the listing.
        version_ids: list[str] = []
        kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": key}
        while True:
            page = self._s3.list_object_versions(**kwargs)
            version_ids += [
                entry["VersionId"]
                for entry in (*page.get("Versions", ()), *page.get("DeleteMarkers", ()))
                if entry["Key"] == key and entry["VersionId"] != keep
            ]  # the prefix also matches longer keys, hence the key check
            if not page.get("IsTruncated"):
                break
            kwargs["KeyMarker"] = page["NextKeyMarker"]
            kwargs["VersionIdMarker"] = page["NextVersionIdMarker"]
        for version_id in version_ids:
            self._s3.delete_object(Bucket=self._bucket, Key=key, VersionId=version_id)
        return len(version_ids)

    def delete_prefix_versions(self, prefix: str) -> tuple[int, int]:
        """Delete every version and delete marker of every object whose key starts with
        ``prefix`` (under ``text/``); return ``(objects, versions)``. End the prefix
        with ``.`` so that Tweet 123 never matches 1234."""
        if not prefix.startswith("text/"):
            raise ComplianceError(f"refusing to delete prefix outside text/: {prefix}")
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
                break
            kwargs["KeyMarker"] = page["NextKeyMarker"]
            kwargs["VersionIdMarker"] = page["NextVersionIdMarker"]
        for key, version_id in found:
            self._s3.delete_object(Bucket=self._bucket, Key=key, VersionId=version_id)
        return len({k for k, _ in found}), len(found)

    @staticmethod
    def text_prefixes(
        item: Mapping[str, Any], tweet_id: str, middle: str = ""
    ) -> set[str]:
        """``text/<owner>/<tweet_id>.<middle>`` for each owner the item could have been
        written under (author ID, lower-cased username, or the folder of one of its own
        text keys), so objects whose key the item lost are found too."""
        owners = {
            str(v).lower() if k == "username" else str(v)
            for k in ("author_id", "username")
            if (v := item.get(k))
        }
        for entry in (item, *(item.get("referenced_tweets") or ())):
            if not isinstance(entry, Mapping):
                continue
            for name in ("text_s3_key", "display_text_s3_key"):
                key = entry.get(name)
                if isinstance(key, str) and key.startswith("text/"):
                    owners.add(key.split("/")[1])
        return {f"text/{owner}/{tweet_id}.{middle}" for owner in sorted(owners)}

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

    def _read(self, key: str) -> bytes | None:
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read()
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return None
            raise

    def redact_raw(
        self, items: Sequence[Mapping[str, Any]], ids: Collection[str] | None = None
    ) -> tuple[int, int]:
        """Remove Tweets from every raw page that holds them.

        The Tweets are ``ids``, by default the items' own. A raw page holds a Tweet in
        ``data`` and, when another Tweet quotes, replies to or retweets it, in
        ``includes.tweets`` (a later page than its own, so there is no upper day
        limit). Pages are found from each item's ``raw_key`` and by listing
        ``raw/<author_id>/`` from the item's creation day (a re-fetch lands the same
        Tweet in more than one page) onward. A page is rewritten without the Tweets
        and with their IDs in ``redactions`` (so a replay skips them), put back at the
        same key, and its older versions are deleted. ``quarantine/`` objects naming
        one of the Tweets (``tweet_id``) are deleted with all their versions. Returns
        the number of raw pages rewritten and quarantine objects deleted. The cost is
        one listing per account and one read per page since the oldest item.
        """
        ids = {
            str(i) for i in (ids if ids is not None else (x["tweet_id"] for x in items))
        }
        raw_keys = {
            k
            for i in items
            if isinstance(k := i.get("raw_key"), str) and k.startswith("raw/")
        }
        quarantined: set[str] = set()
        first_day: dict[str, str] = {}  # per author: the earliest creation day
        for item in items:
            if user := item.get("author_id"):
                created = str(item.get("created_at") or "")[:10]
                first_day[str(user)] = min(first_day.get(str(user), created), created)
        for user, first in first_day.items():
            after = f"/dt={first}/" if first else None
            raw_keys.update(self._list(f"raw/{user}/", after and f"raw/{user}{after}"))
            quarantined.update(
                self._list(f"quarantine/{user}/", after and f"quarantine/{user}{after}")
            )
        pages = sum(self._redact_page(key, ids) for key in sorted(raw_keys))
        removed = sum(self._drop_quarantined(key, ids) for key in sorted(quarantined))
        return pages, removed

    def _redact_page(self, key: str, ids: set[str]) -> int:
        body = self._read(key)
        if body is None:
            return 0
        try:
            redacted, gone = redact_envelope(body, ids)
        except RawPageError:
            logger.warning("raw page %s is unreadable; left as it is", key)
            return 0
        if not gone:
            return 0
        put = self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=redacted,
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        if version := put.get("VersionId"):
            self._delete_versions(key, keep=version)
        logger.warning("redacted raw page %s: removed tweets %s", key, sorted(gone))
        return 1

    def _drop_quarantined(self, key: str, ids: set[str]) -> int:
        body = self._read(key)
        if body is None:
            return 0
        try:
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            document = json.loads(body)
        except OSError, EOFError, zlib.error, ValueError:
            return 0
        if not isinstance(document, dict) or str(document.get("tweet_id")) not in ids:
            return 0
        self._delete_versions(key)
        logger.warning("deleted quarantined object %s", key)
        return 1

    @staticmethod
    def _lock(item: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        """The condition (and its values) that the item still has the revision read."""
        revision = item.get("revision")
        if revision is None:
            return "attribute_not_exists(revision)", {}
        return "revision = :read", {":read": revision}

    def purge(self, item: Mapping[str, Any]) -> tuple[int, int]:
        """Delete the item's S3 objects (all versions), then replace the item with a
        content-free tombstone.

        Returns ``(objects, versions)`` removed. The objects are the ones the item
        points to and everything under ``text/<owner>/<tweet_id>.`` (the Tweet's text
        and its referenced posts' ``.ref.`` texts, also those whose key the item lost;
        the trailing dot keeps Tweet 123 from matching 1234). S3 goes first: if a
        delete fails, the item still names the keys for the retry.

        The tombstone keeps ``tweet_id``, ``deleted_at``, ``deletion_reason`` and
        ``revision``, gains ``purged_at`` and loses every other attribute (the sparse
        index marker too, so it is not purged again). The write is conditional on the
        revision read and does ``ADD revision 1``, so a media retry or ``put_tweet``
        that lands in between fails it; the item is then read again and the objects
        deleted again (``ComplianceError`` after ``WRITE_ATTEMPTS`` lost races, and
        the next run retries). ``put_tweet`` refuses any Tweet that has a
        ``deleted_at``, so a replay or redrive can't bring the Tweet back.
        """
        tweet_id = str(item["tweet_id"])
        current: Mapping[str, Any] = item
        objects = versions = 0
        for _ in range(WRITE_ATTEMPTS):
            keys = self.s3_keys(current)
            versions += sum(self.delete_object_versions(key) for key in keys)
            objects += len(keys)
            for prefix in sorted(self.text_prefixes(current, tweet_id)):
                n_objects, n_versions = self.delete_prefix_versions(prefix)
                objects += n_objects
                versions += n_versions
            lock, values = self._lock(current)
            drop = sorted(set(current) - TOMBSTONE_KEEPS)
            names = {f"#a{n}": name for n, name in enumerate(drop)}
            request: dict[str, Any] = {
                "Key": {"tweet_id": tweet_id},
                "UpdateExpression": (
                    "SET purged_at = :purged"
                    + (f" REMOVE {', '.join(names)}" if names else "")
                    + " ADD revision :one"
                ),
                "ConditionExpression": (
                    "attribute_exists(tweet_id) AND deleted_at = :deleted "
                    f"AND attribute_not_exists(purged_at) AND {lock}"
                ),
                "ExpressionAttributeValues": {
                    ":purged": utc_now_iso(),
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
                fresh = self.load_item(tweet_id)
                if fresh is None or "purged_at" in fresh or "deleted_at" not in fresh:
                    # Gone, already purged, or the tombstone was cleared meanwhile.
                    return objects, versions
                current = fresh
                continue
            return objects, versions
        raise ComplianceError(
            f"tweet {tweet_id} changed under {WRITE_ATTEMPTS} purge attempts"
        )

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
    ) -> tuple[int, int, int]:
        """Apply what the X API said about the posts ``item`` holds copies of.

        ``outcome`` maps an ID to None when it is still available at the source, or to
        the lookup's reason when it is gone. For a copy of a gone post: the first time,
        the entry gets ``status: "unavailable"``, ``unavailable_reason`` (``not_found``
        for a deleted post, ``not_authorized`` for a protected, suspended or withheld
        one) and ``unavailable_since`` = ``now``, as a fetch marks it; once
        ``unavailable_since`` is at or before ``cutoff`` (the grace period has passed)
        the copy is scrubbed: the entry becomes ``{id, type, status, unavailable_reason,
        unavailable_since}``, so its text, author and entities are gone, and its S3
        objects (everything under ``text/<owner>/<tweet_id>.ref.<ref_id>.``, with the
        entities objects, every version, and any key the entry names) are deleted
        before the write. For a copy of a post that is available again, the mark (the
        three fields) is removed. A copy whose post is not in ``outcome`` is left alone.

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
                elif str(since or now) <= cutoff:
                    for prefix in self._ref_prefixes(current, tweet_id, ref_id):
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
                        "unavailable_since": str(since or now),
                    }
                    reason = entry.get("unavailable_reason") or _entry_reason(
                        outcome[ref_id]
                    )
                    values[f":scrub{n}"]["unavailable_reason"] = reason
                    scrubbed += 1
                elif since is None:
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

    def _ref_prefixes(
        self, item: Mapping[str, Any], tweet_id: str, ref_id: str
    ) -> set[str]:
        return self.text_prefixes(item, tweet_id, f"ref.{ref_id}.")


class Compliance:
    """One run of the job over one account."""

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
        self.checked = 0
        self.tombstoned = 0
        self.cleared = 0
        self.purged = 0
        self.raw_pages_redacted = 0
        self.unknown = 0
        self.ref_checked = 0
        self.ref_raw_pages = 0
        self.copies_marked = 0
        self.copies_unmarked = 0
        self.copies_scrubbed = 0
        self.failures: list[str] = []
        self.wrapped = False
        self.auth_failed = False

    def run(self, username: str) -> None:
        """Walk, tombstone, clear and purge; raises ``ComplianceError`` if any step
        failed (after doing everything it could)."""
        handle = _handle(username)
        tombstones = self._store.tombstones()
        try:
            self._walk_references(tombstones)
        except Exception as exc:
            self.auth_failed = isinstance(exc, AuthError)
            logger.exception("compliance references walk failed")
            self.failures.append(f"references walk: {type(exc).__name__}")
        try:
            self._walk(handle, tombstones)
        except Exception as exc:
            self.auth_failed = isinstance(exc, AuthError)
            logger.exception("compliance lookup walk for %s failed", handle)
            self.failures.append(f"lookup walk: {type(exc).__name__}")
        self._purge_due(tombstones)
        logger.info(
            "compliance run for %s: checked=%d tombstoned=%d cleared=%d purged=%d "
            "unknown=%d failures=%d wrapped=%s",
            handle,
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
            "raw_pages=%d",
            self.ref_checked,
            self.copies_marked,
            self.copies_unmarked,
            self.copies_scrubbed,
            self.ref_raw_pages,
        )
        if self.failures:
            raise ComplianceError("; ".join(self.failures))

    def _walk(self, handle: str, tombstones: dict[str, str]) -> None:
        cursor = self._store.load_cursor(handle)
        budget = self._max_lookups - self.ref_checked
        cutoff = _iso(self._now() - self._grace)
        while budget > 0:
            ids, next_cursor = self._store.page_ids(
                handle, cursor, min(MAX_LOOKUP_IDS, budget)
            )
            if ids:
                self._apply(self._lookup(ids), tombstones, cutoff)
                self.checked += len(ids)
                budget -= len(ids)
            cursor = next_cursor
            # After each batch, so a later failure still keeps the progress made.
            self._store.save_cursor(handle, cursor)
            if cursor is None:
                self.wrapped = True
                return

    def _walk_references(self, tombstones: dict[str, str]) -> None:
        """Look up the posts the stored items hold copies of and mark, unmark or scrub
        the copies (see ``ComplianceStore.update_copies``). Uses at most half of the
        lookup budget, and each ID once per run; resumes from its own cursor over a
        scan of the table, stopping after the last item whose IDs fit the budget."""
        budget = self._max_lookups // 2
        if budget < MAX_REFS_PER_ITEM:
            logger.warning(
                "COMPLIANCE_MAX_LOOKUPS=%d leaves no room for the references walk "
                "(needs at least %d); copies of deleted posts are not checked",
                self._max_lookups,
                2 * MAX_REFS_PER_ITEM,
            )
            return
        cursor = self._store.load_cursor(REFERENCES_CURSOR)
        outcome: dict[str, str | None] = {}  # ID -> None if available, else its reason
        asked: set[str] = set()
        pending: list[dict[str, Any]] = []  # items with a copy to scrub, once raw is
        now = _iso(self._now())
        cutoff = _iso(self._now() - self._grace)
        try:
            while budget > 0:
                items, next_cursor = self._store.page_references(cursor, SCAN_PAGE)
                taken: list[dict[str, Any]] = []
                batch: dict[str, None] = {}
                fits = True
                for item in items:
                    fresh = [
                        i
                        for i in self._store.copy_ids(item)
                        if i not in asked and i not in batch
                    ]
                    if len(batch) + len(fresh) > budget:
                        fits = False
                        break
                    batch.update(dict.fromkeys(fresh))
                    taken.append(item)
                if not fits and not taken:
                    logger.warning(
                        "references walk: one item needs more lookups than left"
                    )
                    return
                ids = list(batch)
                for start in range(0, len(ids), MAX_LOOKUP_IDS):
                    chunk = ids[start : start + MAX_LOOKUP_IDS]
                    result = self._lookup(chunk)
                    asked.update(chunk)
                    self._apply(result, tombstones, cutoff)
                    outcome.update(dict.fromkeys(result.present))
                    outcome.update(result.unavailable)
                    self.ref_checked += len(chunk)
                budget -= len(ids)
                for item in taken:
                    self._handle_copies(item, outcome, now, cutoff, pending)
                if not fits:
                    cursor = {"tweet_id": taken[-1]["tweet_id"]}
                    self._store.save_cursor(REFERENCES_CURSOR, cursor)
                    return
                cursor = next_cursor
                self._store.save_cursor(REFERENCES_CURSOR, cursor)
                if cursor is None:
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
        """Scrub the copies past the grace period: the raw pages first (nothing is
        scrubbed from an item if that fails, so the item still names what is left)."""
        if not pending:
            return
        gone = {i for i, reason in outcome.items() if reason is not None}
        due = {
            i
            for item in pending
            for i in self._store.scrub_due(item, gone, now, cutoff)
        }
        try:
            pages, _ = self._store.redact_raw(pending, due)
        except Exception as exc:
            logger.exception("redacting raw pages of deleted posts failed")
            self.failures.append(f"raw pages of copies: {type(exc).__name__}")
            return
        self.ref_raw_pages += pages
        self.raw_pages_redacted += pages
        for item in pending:
            try:
                marked, unmarked, scrubbed = self._store.update_copies(
                    item, outcome, now=now, cutoff=cutoff
                )
            except Exception as exc:
                logger.exception(
                    "scrubbing copies in tweet %s failed", item["tweet_id"]
                )
                self.failures.append(f"copies {item['tweet_id']}: {type(exc).__name__}")
                continue
            self.copies_marked += marked
            self.copies_unmarked += unmarked
            self.copies_scrubbed += scrubbed

    def _apply(
        self, result: TweetLookup, tombstones: dict[str, str], cutoff: str
    ) -> None:
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
            if tombstones.get(tweet_id, "") > cutoff and self._store.clear_tombstone(
                tweet_id, cutoff
            ):
                del tombstones[tweet_id]
                self.cleared += 1
                logger.info("tweet %s is available again: tombstone cleared", tweet_id)
        if result.unknown:
            self.unknown += len(result.unknown)
            logger.info(
                "%d tweet(s) could not be classified; left as they are",
                len(result.unknown),
            )

    def _purge_due(self, tombstones: Mapping[str, str]) -> None:
        cutoff = _iso(self._now() - self._grace)
        due = [
            t for t, deleted_at in sorted(tombstones.items()) if deleted_at <= cutoff
        ]
        items: list[dict[str, Any]] = []
        for tweet_id in due:
            if self._out_of_time():
                return
            try:
                item = self._store.load_item(tweet_id)
            except Exception as exc:
                logger.exception("reading tweet %s failed", tweet_id)
                self.failures.append(f"purge {tweet_id}: {type(exc).__name__}")
                continue
            if item is None or "tombstone" not in item:
                logger.info("tweet %s was already removed", tweet_id)
                continue
            items.append(item)
        if not items:
            return
        try:
            pages, quarantined = self._store.redact_raw(items)
        except Exception as exc:
            # Nothing is purged: the item must keep naming what is still to remove.
            logger.exception("redacting raw pages failed")
            self.failures.append(f"raw pages: {type(exc).__name__}")
            return
        self.raw_pages_redacted += pages
        for item in items:
            tweet_id = str(item["tweet_id"])
            if self._out_of_time():
                return
            try:
                objects, versions = self._store.purge(item)
            except Exception as exc:
                logger.exception("purging tweet %s failed", tweet_id)
                self.failures.append(f"purge {tweet_id}: {type(exc).__name__}")
                continue
            self.purged += 1
            logger.warning(
                "purged tweet %s: s3_objects=%d s3_versions=%d raw_pages=%d "
                "quarantined=%d deleted_at=%s",
                tweet_id,
                objects,
                versions,
                pages,
                quarantined,
                item["deleted_at"],
            )

    def _out_of_time(self) -> bool:
        if self._remaining_ms is not None and self._remaining_ms() < SAFETY_MARGIN_MS:
            logger.warning("out of time: remaining purges wait for the next run")
            return True
        return False


def _short_timeouts() -> Config:
    return Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 3})


@functools.cache
def _store(tweets_table: str, state_table: str, bucket: str) -> ComplianceStore:
    dynamodb = boto3.resource("dynamodb", config=_short_timeouts())
    return ComplianceStore(
        dynamodb.Table(tweets_table),
        dynamodb.Table(state_table),
        boto3.client("s3", config=_short_timeouts()),
        bucket,
    )


def handler(event: Any, context: Any) -> dict[str, Any]:
    settings = Settings.from_env()
    settings.require("tweets_table", "state_table", "media_bucket")
    # narrowed by require()
    assert settings.tweets_table and settings.state_table and settings.media_bucket
    username = event.get("username") if isinstance(event, dict) else None
    username = username or settings.username
    if not username:
        raise RuntimeError("no username in the event and TWITTER_USERNAME is unset")

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
            _store(settings.tweets_table, settings.state_table, settings.media_bucket),
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
    }
