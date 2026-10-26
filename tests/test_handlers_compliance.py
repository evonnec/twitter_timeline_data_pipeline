"""The compliance job: tombstone, clear, purge (moto), cursor and budget."""

import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import boto3
import pytest
from botocore.exceptions import ClientError
from conftest import STATE_TABLE, TWEETS_TABLE, Aws
from helpers import (
    TOKEN,
    install_x_api,
    make_message,
    photo,
)

from twitter_timeline_data_pipeline import credentials
from twitter_timeline_data_pipeline.config import ConfigError
from twitter_timeline_data_pipeline.handlers import compliance
from twitter_timeline_data_pipeline.handlers.compliance import (
    Compliance,
    ComplianceError,
    ComplianceStore,
)
from twitter_timeline_data_pipeline.models import Media
from twitter_timeline_data_pipeline.storage import StoredMedia, TweetStore
from twitter_timeline_data_pipeline.x_api import AuthError, TweetLookup, XApiError

HANDLE = "example_user"
NOW = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
GRACE = timedelta(hours=24)


def tid(n: int) -> str:
    return str(1_700_000_000_000_000_000 + n)


def store_for(aws: Aws) -> ComplianceStore:
    return ComplianceStore(aws.tweets_table, aws.state_table, aws.s3, aws.bucket)


def put(aws: Aws, n: int, *, with_media: bool = False, **tweet: Any) -> str:
    """Store Tweet ``n`` (with a photo and an offloaded text when asked)."""
    tweet_id = tid(n)
    entries: list[StoredMedia] = []
    media: tuple[Media, ...] = ()
    if with_media:
        media = (photo(f"3_{n}"),)
        key = f"media/1/{tweet_id}-0.jpg"
        aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b"img")
        entries = [
            StoredMedia(media_key=f"3_{n}", type="photo", status="stored", s3_key=key)
        ]
    message = make_message(
        tweet_id,
        media=media,
        tweet={"created_at": f"2026-10-{10 + n:02d}T12:00:00.000Z", **tweet},
    )
    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, entries)
    return tweet_id


def item(aws: Aws, tweet_id: str) -> dict[str, Any] | None:
    return cast(
        "dict[str, Any] | None",
        aws.tweets_table.get_item(Key={"tweet_id": tweet_id}).get("Item"),
    )


class Lookup:
    """A scripted ``XClient.lookup_tweets``: ``gone`` IDs are unavailable."""

    def __init__(
        self, gone: dict[str, str] | None = None, unknown: set[str] | None = None
    ) -> None:
        self.gone = gone or {}
        self.unknown = unknown or set()
        self.calls: list[list[str]] = []

    def __call__(self, ids: list[str]) -> TweetLookup:
        self.calls.append(list(ids))
        return TweetLookup(
            present=frozenset(
                i for i in ids if i not in self.gone and i not in self.unknown
            ),
            unavailable={i: self.gone[i] for i in ids if i in self.gone},
            unknown=frozenset(i for i in ids if i in self.unknown),
        )


class Clock:
    step = timedelta(seconds=1)

    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now


def job(
    aws: Aws,
    lookup: Callable[[list[str]], TweetLookup],
    clock: Clock,
    *,
    max_lookups: int = 1000,
    grace: timedelta = GRACE,
    **kwargs: Any,
) -> Compliance:
    return Compliance(
        store_for(aws),
        lookup,
        max_lookups=max_lookups,
        grace=grace,
        now=clock,
        **kwargs,
    )


def run(aws: Aws, lookup: Lookup, clock: Clock, **kwargs: Any) -> Compliance:
    compliance_job = job(aws, lookup, clock, **kwargs)
    compliance_job.run(HANDLE)
    return compliance_job


def assert_purged(aws: Aws, tweet_id: str) -> None:
    """Only the content-free tombstone is left."""
    stored = item(aws, tweet_id)
    assert stored is not None
    assert set(stored) == {"tweet_id", "deleted_at", "deletion_reason", "purged_at"}


def versions(aws: Aws, key: str) -> list[str]:
    listed = aws.s3.list_object_versions(Bucket=aws.bucket, Prefix=key)
    return [
        v["VersionId"]
        for v in (*listed.get("Versions", []), *listed.get("DeleteMarkers", []))
        if v["Key"] == key
    ]


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def versioned(aws: Aws) -> Aws:
    aws.s3.put_bucket_versioning(
        Bucket=aws.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    return aws


@pytest.mark.parametrize("reason", ["deleted", "protected", "suspended"])
def test_unavailable_tweet_is_tombstoned_immediately(
    aws: Aws, clock: Clock, reason: str, caplog: pytest.LogCaptureFixture
) -> None:
    gone, kept = put(aws, 1), put(aws, 2)
    caplog.set_level(logging.INFO)

    result = run(aws, Lookup({gone: reason}), clock)

    assert item(aws, gone) == {
        **(item(aws, gone) or {}),
        "deleted_at": "2026-10-26T12:00:00Z",
        "deletion_reason": reason,
        "tombstone": "1",
    }
    assert "deleted_at" not in (item(aws, kept) or {})
    assert (result.checked, result.tombstoned, result.purged) == (2, 1, 0)
    assert f"tombstoned tweet {gone}: reason={reason}" in caplog.text


def test_tombstone_keeps_content_until_the_grace_period_passes(
    aws: Aws, clock: Clock
) -> None:
    gone = put(aws, 1, with_media=True)
    run(aws, Lookup({gone: "deleted"}), clock)

    clock.now += GRACE - timedelta(seconds=1)
    run(aws, Lookup({gone: "deleted"}), clock)

    stored = item(aws, gone)
    assert stored is not None
    assert aws.s3.head_object(Bucket=aws.bucket, Key=stored["media"][0]["s3_key"])


def test_purge_after_grace_removes_every_s3_version_and_the_item(
    versioned: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    aws = versioned
    gone, kept = put(aws, 1, with_media=True), put(aws, 2, with_media=True)
    media_key = f"media/1/{gone}-0.jpg"
    aws.s3.put_object(Bucket=aws.bucket, Key=media_key, Body=b"v2")  # a 2nd version
    aws.s3.put_object(Bucket=aws.bucket, Key=f"media/1/{gone}-0.jpg.other", Body=b"x")
    text_keys = [f"text/1/{gone}.json", f"text/1/{gone}.display_text.json"]
    for key in text_keys:
        aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b"t")
    aws.tweets_table.update_item(
        Key={"tweet_id": gone},
        UpdateExpression="SET text_s3_key = :a, display_text_s3_key = :b, "
        "media[0].preview_s3_key = :p",
        ExpressionAttributeValues={
            ":a": text_keys[0],
            ":b": text_keys[1],
            ":p": f"media/1/{gone}-preview.jpg",
        },
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=f"media/1/{gone}-preview.jpg", Body=b"p")
    aws.s3.delete_object(Bucket=aws.bucket, Key=text_keys[1])  # leaves a delete marker
    assert len(versions(aws, media_key)) == 2
    lookup = Lookup({gone: "deleted"})
    run(aws, lookup, clock)
    caplog.set_level(logging.INFO)

    clock.now += GRACE + timedelta(seconds=1)
    result = run(aws, lookup, clock)

    assert_purged(aws, gone)
    for key in [media_key, *text_keys, f"media/1/{gone}-preview.jpg"]:
        assert versions(aws, key) == []
    assert versions(aws, f"media/1/{gone}-0.jpg.other")  # a different key is untouched
    assert item(aws, kept) is not None
    assert versions(aws, f"media/1/{kept}-0.jpg")
    assert result.purged == 1
    assert f"purged tweet {gone}: s3_objects=4" in caplog.text


def test_zero_grace_purges_in_the_same_run(aws: Aws, clock: Clock) -> None:
    gone = put(aws, 1, with_media=True)

    result = run(aws, Lookup({gone: "deleted"}), clock, grace=timedelta(0))

    assert (result.tombstoned, result.purged) == (1, 1)
    assert_purged(aws, gone)


def test_reappearing_tweet_clears_its_tombstone(aws: Aws, clock: Clock) -> None:
    tweet = put(aws, 1)
    run(aws, Lookup({tweet: "protected"}), clock)
    clock.now += timedelta(hours=1)

    result = run(aws, Lookup(), clock)

    stored = item(aws, tweet) or {}
    assert not {"deleted_at", "deletion_reason", "tombstone"} & set(stored)
    assert result.cleared == 1
    assert store_for(aws).tombstones() == {}


def test_tombstone_is_not_overwritten_by_a_later_unavailable_result(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    run(aws, Lookup({tweet: "protected"}), clock)
    clock.now += timedelta(hours=1)

    result = run(aws, Lookup({tweet: "deleted"}), clock)

    stored = item(aws, tweet) or {}
    assert stored["deleted_at"] == "2026-10-26T12:00:00Z"
    assert stored["deletion_reason"] == "protected"
    assert result.tombstoned == 0


def test_present_tweet_past_its_grace_period_is_purged_not_cleared(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    run(aws, Lookup({tweet: "deleted"}), clock)
    clock.now += GRACE + timedelta(hours=1)

    result = run(aws, Lookup(), clock)

    assert (result.cleared, result.purged) == (0, 1)
    assert_purged(aws, tweet)


def test_ambiguous_lookup_never_tombstones(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    tweets = [put(aws, 1), put(aws, 2)]
    caplog.set_level(logging.INFO)

    result = run(aws, Lookup(unknown=set(tweets)), clock)

    assert result.unknown == 2
    assert all("deleted_at" not in (item(aws, t) or {}) for t in tweets)
    assert "could not be classified" in caplog.text


def test_unknown_result_does_not_clear_or_extend_a_tombstone(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    run(aws, Lookup({tweet: "deleted"}), clock)

    run(aws, Lookup(unknown={tweet}), clock)

    assert (item(aws, tweet) or {})["deleted_at"] == "2026-10-26T12:00:00Z"


def test_tombstone_of_a_vanished_item_is_a_no_op(aws: Aws, clock: Clock) -> None:
    assert (
        store_for(aws).tombstone(tid(404), "deleted", "2026-10-26T12:00:00Z") is False
    )
    assert item(aws, tid(404)) is None


def test_cursor_resumes_across_runs_and_wraps(aws: Aws, clock: Clock) -> None:
    ids = [put(aws, n) for n in range(1, 6)]  # oldest first in the GSI
    lookup = Lookup()

    first = run(aws, lookup, clock, max_lookups=2)
    second = run(aws, lookup, clock, max_lookups=2)
    third = run(aws, lookup, clock, max_lookups=2)
    fourth = run(aws, lookup, clock, max_lookups=2)

    assert lookup.calls[0] == ids[:2]
    assert lookup.calls[1] == ids[2:4]
    assert lookup.calls[2] == ids[4:]
    assert lookup.calls[3] == ids[:2]  # wrapped
    assert (first.wrapped, second.wrapped, third.wrapped, fourth.wrapped) == (
        False,
        False,
        True,
        False,
    )
    state = aws.state_table.get_item(Key={"username": f"compliance#{HANDLE}"})["Item"]
    assert json.loads(str(state["cursor"]))["tweet_id"] == ids[1]
    assert aws.state_table.get_item(Key={"username": HANDLE}).get("Item") is None


def test_lookup_budget_is_honored_in_batches_of_100(aws: Aws, clock: Clock) -> None:
    store_many(aws, 230)
    lookup = Lookup()

    result = run(aws, lookup, clock, max_lookups=150)

    assert [len(c) for c in lookup.calls] == [100, 50]
    assert result.checked == 150


def test_empty_account_wraps_without_lookups(aws: Aws, clock: Clock) -> None:
    lookup = Lookup()

    result = run(aws, lookup, clock)

    assert lookup.calls == []
    assert result.wrapped


def test_unreadable_cursor_restarts_the_pass(aws: Aws, clock: Clock) -> None:
    ids = [put(aws, 1), put(aws, 2)]
    for bad in ("not json", "[1]"):
        aws.state_table.put_item(
            Item={"username": f"compliance#{HANDLE}", "cursor": bad}
        )
        lookup = Lookup()

        run(aws, lookup, clock, max_lookups=2)

        assert lookup.calls[0] == ids


def test_handle_is_normalized(aws: Aws, clock: Clock) -> None:
    tweet = put(aws, 1)
    compliance_job = job(aws, Lookup({tweet: "deleted"}), clock)

    compliance_job.run("@Example_User")

    assert (item(aws, tweet) or {})["tombstone"] == "1"


class FailSecond(Lookup):
    """Answers the first batch, then raises."""

    def __call__(self, ids: list[str]) -> TweetLookup:
        if self.calls:
            raise XApiError("boom")
        return super().__call__(ids)


def store_many(aws: Aws, count: int) -> list[str]:
    """``count`` bare Tweets, oldest first in the GSI."""
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    ids = []
    for n in range(count):
        ids.append(tid(n))
        created = f"2026-01-01T{n // 3600:02d}:{n // 60 % 60:02d}:{n % 60:02d}.000Z"
        store.put_tweet(make_message(tid(n), tweet={"created_at": created}), [])
    return ids


def test_lookup_failure_keeps_progress_still_purges_and_then_raises(
    aws: Aws, clock: Clock
) -> None:
    ids = store_many(aws, 230)
    old = ids[0]
    assert store_for(aws).tombstone(old, "deleted", "2026-10-01T00:00:00Z")
    failing_job = job(aws, FailSecond(), clock, max_lookups=200)

    with pytest.raises(ComplianceError, match="lookup walk: XApiError"):
        failing_job.run(HANDLE)

    assert failing_job.checked == 100
    assert store_for(aws).load_cursor(HANDLE) is not None  # progress kept
    assert_purged(aws, old)  # purged despite the failing lookup
    assert failing_job.purged == 1


def test_auth_failure_is_flagged(aws: Aws, clock: Clock) -> None:
    put(aws, 1)

    def lookup(_ids: list[str]) -> TweetLookup:
        raise AuthError("nope", status_code=401)

    auth_job = job(aws, lookup, clock)
    with pytest.raises(ComplianceError):
        auth_job.run(HANDLE)

    assert auth_job.auth_failed


def test_purge_failure_leaves_the_item_for_a_retry_and_continues(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    bad, good = put(aws, 1, with_media=True), put(aws, 2, with_media=True)
    lookup = Lookup({bad: "deleted", good: "deleted"})
    run(aws, lookup, clock, grace=timedelta(0))  # purges both... so redo with grace
    # (re-create) both, tombstone with a grace, then break the first one's S3 delete
    bad, good = put(aws, 3, with_media=True), put(aws, 4, with_media=True)
    lookup = Lookup({bad: "deleted", good: "deleted"})
    run(aws, lookup, clock)
    clock.now += GRACE + timedelta(seconds=1)
    store = store_for(aws)
    real = store.delete_object_versions

    def flaky(key: str) -> int:
        if bad in key:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject")
        return real(key)

    store.delete_object_versions = flaky  # type: ignore[method-assign]
    failing_job = Compliance(store, lookup, max_lookups=10, grace=GRACE, now=clock)

    with pytest.raises(ComplianceError, match=f"purge {bad}: ClientError"):
        failing_job.run(HANDLE)

    assert item(aws, bad) is not None  # still names its keys
    assert_purged(aws, good)
    assert failing_job.purged == 1


def test_purge_of_an_item_that_is_already_gone_is_fine(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    tweet = put(aws, 1)
    run(aws, Lookup({tweet: "deleted"}), clock)
    aws.tweets_table.delete_item(Key={"tweet_id": tweet})  # e.g. TTL got there first
    store = store_for(aws)
    stale = {tweet: "2026-10-01T00:00:00Z"}
    caplog.set_level(logging.INFO)

    Compliance(store, Lookup(), max_lookups=1, grace=GRACE, now=clock)._purge_due(stale)

    assert "already removed" in caplog.text


def test_keys_outside_the_allowed_prefixes_are_never_deleted(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet},
        UpdateExpression="SET text_s3_key = :k",
        ExpressionAttributeValues={":k": "raw/page.json"},
    )
    aws.s3.put_object(Bucket=aws.bucket, Key="raw/page.json", Body=b"{}")

    with pytest.raises(ComplianceError, match="purge"):
        run(aws, Lookup({tweet: "deleted"}), clock, grace=timedelta(0))

    assert aws.s3.head_object(Bucket=aws.bucket, Key="raw/page.json")
    assert item(aws, tweet) is not None


def test_s3_listing_is_paginated(versioned: Aws) -> None:
    aws = versioned
    key = "media/1/many.jpg"
    for _ in range(5):
        aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b"x")
    real = aws.s3.list_object_versions

    class SmallPages:
        def __getattr__(self, name: str) -> Any:
            return getattr(aws.s3, name)

        def list_object_versions(self, **kwargs: Any) -> Any:
            return real(MaxKeys=2, **kwargs)

    store = ComplianceStore(
        aws.tweets_table, aws.state_table, cast(Any, SmallPages()), aws.bucket
    )

    assert store.delete_object_versions(key) == 5
    assert versions(aws, key) == []


def test_out_of_time_stops_the_purge_for_the_next_run(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    tweet = put(aws, 1)
    lookup = Lookup({tweet: "deleted"})
    caplog.set_level(logging.INFO)

    result = run(aws, lookup, clock, grace=timedelta(0), remaining_ms=lambda: 1_000)

    assert result.purged == 0
    assert item(aws, tweet) is not None
    assert "out of time" in caplog.text


def test_s3_keys_collects_every_pointer() -> None:
    keys = ComplianceStore.s3_keys(
        {
            "text_s3_key": "text/1/a.json",
            "display_text_s3_key": "text/1/a.display_text.json",
            "media": [
                {"s3_key": "media/1/a.jpg", "preview_s3_key": "media/1/a-p.jpg"},
                {"status": "unavailable"},
                "junk",
            ],
        }
    )

    assert keys == [
        "media/1/a-p.jpg",
        "media/1/a.jpg",
        "text/1/a.display_text.json",
        "text/1/a.json",
    ]


# --- handler ------------------------------------------------------------------


@pytest.fixture
def handler_env(monkeypatch: pytest.MonkeyPatch, pipeline_env: Aws) -> Aws:
    monkeypatch.setenv("DELETION_GRACE_HOURS", "0")
    monkeypatch.setenv("COMPLIANCE_MAX_LOOKUPS", "50")
    compliance._store.cache_clear()
    return pipeline_env


def api_handler(gone: set[str]) -> Callable[[Any], Any]:
    import httpx

    def handle(request: httpx.Request) -> httpx.Response:
        ids = request.url.params["ids"].split(",")
        data = [{"id": i} for i in ids if i not in gone]
        errors = [
            {
                "resource_id": i,
                "title": "Not Found Error",
                "type": "https://api.twitter.com/2/problems/resource-not-found",
            }
            for i in ids
            if i in gone
        ]
        return httpx.Response(200, json={"data": data, "errors": errors})

    return handle


class Context:
    def get_remaining_time_in_millis(self) -> int:
        return 200_000


def test_handler_runs_the_job_end_to_end(
    monkeypatch: pytest.MonkeyPatch, handler_env: Aws
) -> None:
    aws = handler_env
    gone, kept = put(aws, 1, with_media=True), put(aws, 2)
    constructed = install_x_api(monkeypatch, compliance, api_handler({gone}))

    result = compliance.handler({}, Context())

    assert result == {
        "checked": 2,
        "tombstoned": 1,
        "cleared": 0,
        "purged": 1,
        "unknown": 0,
        "wrapped": True,
        "raw_pages_redacted": 0,
    }
    assert_purged(aws, gone)
    assert item(aws, kept) is not None
    assert constructed[0]["token"] == TOKEN
    assert constructed[0]["max_rate_limit_wait"] == 60.0


def test_handler_uses_the_events_username_and_needs_no_context(
    monkeypatch: pytest.MonkeyPatch, handler_env: Aws
) -> None:
    other = make_message(tid(7), username="other_user")
    TweetStore(handler_env.tweets_table, handler_env.s3, handler_env.bucket).put_tweet(
        other, []
    )
    install_x_api(monkeypatch, compliance, api_handler({tid(7)}))

    result = compliance.handler({"username": "Other_User"}, object())

    assert result["tombstoned"] == 1


def test_handler_clears_the_token_cache_after_an_auth_error(
    monkeypatch: pytest.MonkeyPatch, handler_env: Aws
) -> None:
    import httpx

    put(handler_env, 1)
    install_x_api(
        monkeypatch, compliance, lambda _r: httpx.Response(401, json={"title": "x"})
    )
    credentials._cache["x"] = ("t", 1e18)

    with pytest.raises(ComplianceError):
        compliance.handler({}, Context())

    assert credentials._cache == {}


def test_handler_requires_its_settings_and_a_username(
    monkeypatch: pytest.MonkeyPatch, handler_env: Aws
) -> None:
    monkeypatch.delenv("TWITTER_USERNAME")
    with pytest.raises(RuntimeError, match="no username"):
        compliance.handler({}, Context())
    monkeypatch.delenv("MEDIA_BUCKET")
    with pytest.raises(ConfigError, match="MEDIA_BUCKET"):
        compliance.handler({}, Context())


def test_store_is_cached_per_configuration(handler_env: Aws) -> None:
    first = compliance._store(TWEETS_TABLE, STATE_TABLE, handler_env.bucket)

    assert compliance._store(TWEETS_TABLE, STATE_TABLE, handler_env.bucket) is first
    assert boto3  # the resource and client come from the default session


class ThrottledTable:
    """A tweets table whose writes fail, and whose queries return one item a page."""

    def __init__(self, table: Any) -> None:
        self._table = table

    def __getattr__(self, name: str) -> Any:
        return getattr(self._table, name)

    def update_item(self, **_: Any) -> Any:
        raise ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException"}}, "UpdateItem"
        )

    def query(self, **kwargs: Any) -> Any:
        return self._table.query(Limit=1, **kwargs)


def test_other_dynamodb_errors_propagate_from_tombstone_and_clear(aws: Aws) -> None:
    store = ComplianceStore(
        cast(Any, ThrottledTable(aws.tweets_table)), aws.state_table, aws.s3, aws.bucket
    )

    with pytest.raises(ClientError):
        store.tombstone(tid(1), "deleted", "2026-10-26T12:00:00Z")
    with pytest.raises(ClientError):
        store.clear_tombstone(tid(1), "2026-10-26T12:00:00Z")


def test_clear_tombstone_ignores_an_item_that_is_not_tombstoned(aws: Aws) -> None:
    tweet = put(aws, 1)

    assert store_for(aws).clear_tombstone(tweet, "2026-10-26T12:00:00Z") is False


def test_tombstones_are_listed_across_pages(aws: Aws) -> None:
    ids = [put(aws, n) for n in range(1, 4)]
    store = store_for(aws)
    for n, tweet in enumerate(ids):
        store.tombstone(tweet, "deleted", f"2026-10-2{n}T00:00:00Z")
    paged = ComplianceStore(
        cast(Any, ThrottledTable(aws.tweets_table)), aws.state_table, aws.s3, aws.bucket
    )

    assert paged.tombstones() == {
        ids[0]: "2026-10-20T00:00:00Z",
        ids[1]: "2026-10-21T00:00:00Z",
        ids[2]: "2026-10-22T00:00:00Z",
    }
