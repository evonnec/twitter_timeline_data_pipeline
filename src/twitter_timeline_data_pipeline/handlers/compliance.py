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
3. Purges every tombstone older than ``DELETION_GRACE_HOURS``: all versions of every S3
   object the item points to, then the item itself (S3 first, so a retry after a
   failure still finds the keys).

Tombstones are found through the sparse ``tombstone-deleted_at-index`` GSI, which holds
only tombstoned items, so no run scans the table. Failures are logged and counted, the
run does as much of the rest as it can, and then raises so the error alarm fires.
"""

import functools
import json
import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from twitter_timeline_data_pipeline.config import Settings
from twitter_timeline_data_pipeline.credentials import clear_cache, get_bearer_token
from twitter_timeline_data_pipeline.models import utc_now_iso
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
        """The GSI ``ExclusiveStartKey`` to resume from; None starts a new pass."""
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
                UpdateExpression=(
                    "SET deleted_at = :now, deletion_reason = :reason, tombstone = :t"
                ),
                ConditionExpression=(
                    "attribute_exists(tweet_id) AND attribute_not_exists(deleted_at)"
                ),
                ExpressionAttributeValues={
                    ":now": now,
                    ":reason": reason,
                    ":t": TOMBSTONE_MARKER,
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
                UpdateExpression="REMOVE deleted_at, deletion_reason, tombstone",
                ConditionExpression="deleted_at > :cutoff",
                ExpressionAttributeValues={":cutoff": cutoff},
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
        """Delete every version and delete marker of ``key``; return how many."""
        if not key.startswith(ALLOWED_KEY_PREFIXES):
            raise ComplianceError(
                f"refusing to delete key outside media/ and text/: {key}"
            )
        # List everything first, then delete: removing versions while paging would
        # move the page markers under the listing.
        version_ids: list[str] = []
        kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": key}
        while True:
            page = self._s3.list_object_versions(**kwargs)
            version_ids += [
                entry["VersionId"]
                for entry in (*page.get("Versions", ()), *page.get("DeleteMarkers", ()))
                if entry["Key"] == key  # the prefix also matches longer keys
            ]
            if not page.get("IsTruncated"):
                break
            kwargs["KeyMarker"] = page["NextKeyMarker"]
            kwargs["VersionIdMarker"] = page["NextVersionIdMarker"]
        for version_id in version_ids:
            self._s3.delete_object(Bucket=self._bucket, Key=key, VersionId=version_id)
        return len(version_ids)

    def purge(self, tweet_id: str) -> tuple[int, int] | None:
        """Delete the Tweet's S3 objects (all versions), then its item.

        Returns ``(objects, versions)`` removed, or None if the item was already gone.
        S3 goes first: if a delete fails, the item still names the keys for the retry.
        """
        item = self._tweets.get_item(
            Key={"tweet_id": tweet_id}, ConsistentRead=True
        ).get("Item")
        if item is None:
            return None
        keys = self.s3_keys(item)
        versions = sum(self.delete_object_versions(key) for key in keys)
        self._tweets.delete_item(Key={"tweet_id": tweet_id})
        return len(keys), versions


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
        self.unknown = 0
        self.failures: list[str] = []
        self.wrapped = False
        self.auth_failed = False

    def run(self, username: str) -> None:
        """Walk, tombstone, clear and purge; raises ``ComplianceError`` if any step
        failed (after doing everything it could)."""
        handle = _handle(username)
        tombstones = self._store.tombstones()
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
        if self.failures:
            raise ComplianceError("; ".join(self.failures))

    def _walk(self, handle: str, tombstones: dict[str, str]) -> None:
        cursor = self._store.load_cursor(handle)
        budget = self._max_lookups
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
        for tweet_id, deleted_at in sorted(tombstones.items()):
            if deleted_at > cutoff:
                continue
            if (
                self._remaining_ms is not None
                and self._remaining_ms() < SAFETY_MARGIN_MS
            ):
                logger.warning("out of time: remaining purges wait for the next run")
                return
            try:
                result = self._store.purge(tweet_id)
            except Exception as exc:
                logger.exception("purging tweet %s failed", tweet_id)
                self.failures.append(f"purge {tweet_id}: {type(exc).__name__}")
                continue
            if result is None:
                logger.info("tweet %s was already removed", tweet_id)
                continue
            self.purged += 1
            logger.warning(
                "purged tweet %s: s3_objects=%d s3_versions=%d deleted_at=%s",
                tweet_id,
                result[0],
                result[1],
                deleted_at,
            )


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
    }
