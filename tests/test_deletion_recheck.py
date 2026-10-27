"""Regression tests for the #21 re-check: the batches of the resumable redaction (a
cleared Tweet, copy scrubs over several runs, a failure after the redacted put, holder
messages in quarantine, shared media) and the limits of a run."""

import json
import logging
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError
from conftest import Aws
from helpers import USER_ID, make_message
from test_deletion_copies import B, C, entry, put_quote
from test_deletion_raw_pages import api_tweet, envelope, land, read, response
from test_handlers_compliance import (
    HANDLE,
    Clock,
    Lookup,
    item,
    job,
    put,
    store_for,
    tid,
    versions,
)

from twitter_timeline_data_pipeline.config import ConfigError, Settings
from twitter_timeline_data_pipeline.handlers.compliance import (
    BATCH_CAP,
    MAX_CONFIRM_REQUESTS,
    REDACT_PREFIX,
    ComplianceError,
    ComplianceStore,
    _strip_message_copies,
)
from twitter_timeline_data_pipeline.models import ReferencedPost
from twitter_timeline_data_pipeline.quarantine import QuarantineStore
from twitter_timeline_data_pipeline.raw import Envelope, redact_envelope
from twitter_timeline_data_pipeline.storage import TweetStore
from twitter_timeline_data_pipeline.x_api import TweetLookup

D = "1800000000000000003"
_pages = iter(range(20_000, 30_000))


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def versioned(aws: Aws) -> Aws:
    aws.s3.put_bucket_versioning(
        Bucket=aws.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    return aws


class Gate:
    """``_time_low`` replacement: ``fine`` calls say time is fine, then it is up."""

    def __init__(self, fine: int) -> None:
        self.fine = fine

    def __call__(self) -> bool:
        self.fine -= 1
        return self.fine < 0


def gated(aws: Aws, lookup: Any, clock: Clock, fine: int, **kwargs: Any) -> Any:
    step = job(aws, lookup, clock, **kwargs)
    step._time_low = Gate(fine)  # type: ignore[method-assign]
    return step


def ids_in(aws: Aws, key: str, where: str = "data") -> list[str]:
    page = read(aws, key).response
    if where == "data":
        return [t["id"] for t in page.get("data", [])]
    return [t["id"] for t in page.get("includes", {}).get(where, [])]


def quarantine(aws: Aws, kind: str, payload: Any) -> str:
    return QuarantineStore(aws.s3, aws.bucket, run_id="q").put(
        kind=kind,
        item=payload,
        error="boom",
        raw_key=None,
        user_id=USER_ID,
        fetched_at="2026-10-12T00:00:00Z",
        page_no=next(_pages),
    )


# --- N1: once a purge has begun it completes ---


def seed_pages(aws: Aws, doomed: str, count: int) -> list[str]:
    keys = []
    for day in range(count):
        key = f"raw/{USER_ID}/dt=2026-10-{13 + day:02d}/p{day}-1.json.gz"
        land(aws, key, envelope(response(api_tweet(doomed), api_tweet(tid(900 + day)))))
        keys.append(key)
    return keys


def test_a_purge_that_has_begun_is_not_undone_by_a_later_present_answer(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    a = put(aws, 1)
    keys = seed_pages(aws, a, 10)
    first = gated(aws, Lookup({a: "deleted"}), clock, 3 + 4, grace=timedelta(0))
    first.run(HANDLE)  # begins, then runs out of time mid-listing
    assert first.purged == 0
    assert "purging" in (item(aws, a) or {})
    assert 0 < sum(a not in ids_in(aws, k) for k in keys) < len(keys)

    present = Lookup()  # X now says A is present
    second = job(aws, present, clock, grace=timedelta(0))
    second.run(HANDLE)

    # The walk could not clear it, the purge was not asked about again, and it ended.
    assert second.cleared == 0 and second.purged == 1
    assert sum(a in c for c in present.calls) == 1  # the walk's; no confirmation
    assert all(a not in ids_in(aws, k) for k in keys)
    stored = item(aws, a) or {}
    assert "purged_at" in stored and "text" not in stored


def test_clearing_refuses_an_item_that_is_purging(aws: Aws) -> None:
    tweet = put(aws, 1)
    store = store_for(aws)
    store.tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")
    store.begin_purge(store.load_item(tweet) or {})

    assert store.clear_tombstone(tweet) is False
    assert "purging" in (item(aws, tweet) or {})


# --- N2: copy scrubs complete when a batch needs several runs ---


def test_copy_scrubs_complete_across_runs_even_if_the_walk_never_returns(
    aws: Aws, clock: Clock
) -> None:
    holders = [put_quote(aws, 1, B), put_quote(aws, 2, C), put_quote(aws, 3, D)]
    refs = (B, C, D)
    includes = {"tweets": [api_tweet(B), api_tweet(C), api_tweet(D)]}
    for day in range(10):
        key = f"raw/{USER_ID}/dt=2026-10-{14 + day:02d}/p{day}-1.json.gz"
        body = response(api_tweet(tid(900 + day)))
        body["includes"] = includes
        land(aws, key, envelope(body))
    lookup = Lookup({B: "deleted", C: "deleted", D: "deleted"})

    scrubbed = 0
    for _ in range(6):
        step = gated(aws, lookup, clock, 1 + 6, max_lookups=4, grace=timedelta(0))
        step.run(HANDLE)
        scrubbed += step.copies_scrubbed
        clock.now += timedelta(hours=6)

    assert scrubbed == 3
    assert all(
        "text" not in entry(aws, h, r) for h, r in zip(holders, refs, strict=True)
    )
    assert store_for(aws).active_units() == []


def test_a_batch_in_progress_makes_a_new_registration_wait(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    put_quote(aws, 1, B)
    land(
        aws,
        f"raw/{USER_ID}/dt=2026-10-14/p-1.json.gz",
        envelope(response(api_tweet(tid(900)))),
    )
    stalled = gated(aws, Lookup({B: "deleted"}), clock, 1, grace=timedelta(0))
    stalled.run(HANDLE)
    assert store_for(aws).active_units() == [f"copy#{USER_ID}"]
    caplog.set_level(logging.INFO)
    stalled_again = gated(aws, Lookup({B: "deleted"}), clock, 1, grace=timedelta(0))

    stalled_again.run(HANDLE)

    assert "redaction of deleted copies is in progress" in caplog.text


def test_a_failed_registration_is_reported(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    put_quote(aws, 1, B)

    def failing(self: ComplianceStore, *args: Any, **kwargs: Any) -> Any:
        raise ClientError({"Error": {"Code": "Throttled"}}, "PutItem")

    monkeypatch.setattr(ComplianceStore, "start_batch", failing)

    with pytest.raises(ComplianceError, match="raw pages of copies"):
        job(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0)).run(HANDLE)


def test_a_missing_batch_is_dropped_from_the_active_units(
    aws: Aws, clock: Clock
) -> None:
    store = store_for(aws)
    store._set_active("copy#gone", True)

    step = job(aws, Lookup(), clock)
    step._walk = lambda tombstones: None  # type: ignore[method-assign]
    step.run(HANDLE)

    assert store.active_units() == []
    assert store.advance("copy#nothing") is None


def test_a_failing_advance_is_reported_and_the_batch_stays(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = store_for(aws)
    store.start_batch("copy#x", mode="copy", author="x", ids={B}, first_day="")

    def failing(self: ComplianceStore, *args: Any, **kwargs: Any) -> Any:
        raise ClientError({"Error": {"Code": "SlowDown"}}, "ListObjectsV2")

    monkeypatch.setattr(ComplianceStore, "advance", failing)
    step = job(aws, Lookup(), clock)
    step._walk = lambda tombstones: None  # type: ignore[method-assign]

    with pytest.raises(ComplianceError, match="raw pages of copies"):
        step.run(HANDLE)

    assert store.active_units() == ["copy#x"]


def test_finishing_a_copy_batch_confirms_first_and_retries_after_a_failure(
    aws: Aws, clock: Clock
) -> None:
    holder = put_quote(aws, 1, B)
    store = store_for(aws)
    store.start_batch(
        "copy#z", mode="copy", author=None, ids={B}, first_day="", holders={holder, "9"}
    )

    def boom(ids: list[str]) -> TweetLookup:
        raise RuntimeError("503")

    failing = job(aws, boom, clock, grace=timedelta(0))
    failing._walk = lambda tombstones: None  # type: ignore[method-assign]
    with pytest.raises(ComplianceError, match="copy confirmation"):
        failing.run(HANDLE)
    assert store.active_units() == ["copy#z"]  # still there: retried
    assert "text" in entry(aws, holder)

    unknown = job(aws, Lookup(unknown={B}), clock, grace=timedelta(0))
    unknown._walk = lambda tombstones: None  # type: ignore[method-assign]
    unknown.run(HANDLE)  # an unknown answer scrubs nothing
    assert "text" in entry(aws, holder) and store.active_units() == []

    store.start_batch(
        "copy#z", mode="copy", author=None, ids={B}, first_day="", holders={holder}
    )
    ok = job(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0))
    ok._walk = lambda tombstones: None  # type: ignore[method-assign]
    ok._store.update_copies = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[method-assign]
        ClientError({"Error": {"Code": "Throttled"}}, "UpdateItem")
    )
    with pytest.raises(ComplianceError, match=f"copies {holder}"):
        ok.run(HANDLE)
    assert store.active_units() == ["copy#z"]


# --- N3: a holder's quarantined message ---


def message_document(holder: str, ref: str) -> dict[str, Any]:
    copy = ReferencedPost(
        id=ref,
        type="quoted",
        text="what the original said",
        author_id="77",
        author_username="someone",
        entities={"hashtags": [{"tag": "secret"}]},
    )
    message = make_message(
        holder,
        referenced=(copy,),
        tweet={
            "created_at": "2026-10-11T12:00:00.000Z",
            "referenced_tweets": [{"type": "quoted", "id": ref}],
        },
    )
    return json.loads(message.to_json())  # type: ignore[no-any-return]


def test_a_holders_quarantined_message_loses_the_copy_but_stays(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    holder = put_quote(aws, 1, B)
    key = quarantine(aws, "message", message_document(holder, B))
    elsewhere = quarantine(aws, "message", message_document(tid(50), C))

    job(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0)).run(HANDLE)

    body = aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()
    assert b"what the original said" not in body and b"someone" not in body
    document = json.loads(body)
    assert document["item"]["tweet"]["id"] == holder  # the holder's own data stays
    (ref,) = document["item"]["referenced"]
    assert ref == {"id": B, "type": "quoted", "status": "unavailable"}
    assert len(versions(aws, key)) == 1  # the unstripped version is gone
    kept = aws.s3.get_object(Bucket=aws.bucket, Key=elsewhere)["Body"].read()
    assert b"what the original said" in kept  # another post's copy: untouched


def test_a_purge_also_strips_the_copy_from_other_tweets_quarantined_messages(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    key = quarantine(aws, "message", message_document(tid(50), doomed))
    own = quarantine(aws, "message", message_document(doomed, B))

    run_purge(aws, clock, doomed)

    assert b"what the original said" not in (
        aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()
    )
    assert versions(aws, own) == []  # the doomed Tweet's own message: deleted


def run_purge(aws: Aws, clock: Clock, tweet: str) -> None:
    for _ in range(2):
        job(aws, Lookup({tweet: "deleted"}), clock).run(HANDLE)
        clock.now += timedelta(hours=25)


def test_stripping_a_message_edge_cases() -> None:
    plain = {"kind": "message", "item": {"referenced": [{"id": B, "type": "quoted"}]}}
    assert _strip_message_copies(plain, {B}) is None  # nothing left to strip
    assert _strip_message_copies({"kind": "tweet", "item": {}}, {B}) is None
    assert _strip_message_copies({"kind": "message", "item": []}, {B}) is None
    assert _strip_message_copies({"kind": "message", "item": {}}, {B}) is None
    mixed = {
        "kind": "message",
        "item": {
            "referenced": ["junk", {"id": C, "text": "other"}, {"id": B, "text": "x"}]
        },
    }
    stripped = _strip_message_copies(mixed, {B})
    assert stripped is not None
    assert stripped["item"]["referenced"][1] == {"id": C, "text": "other"}
    assert stripped["item"]["referenced"][2] == {
        "id": B,
        "status": "unavailable",
    }


def test_a_quarantined_message_that_changes_under_the_rewrite_is_given_up_on(
    versioned: Aws,
) -> None:
    aws = versioned
    key = quarantine(aws, "message", message_document(tid(1), B))
    real = aws.s3

    class Moving:
        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

        def put_object(self, **kw: Any) -> Any:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")

    broken = ComplianceStore(aws.tweets_table, aws.state_table, Moving(), aws.bucket)  # type: ignore[arg-type]

    with pytest.raises(ComplianceError, match="changed under"):
        broken._quarantined(key, {B}, False, set(), set, lambda k, p: None)


# --- N4: a failure after the redacted put ---


def test_a_failed_version_delete_is_finished_by_the_next_run(
    versioned: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    key = f"raw/{USER_ID}/dt=2026-10-12/p-1.json.gz"
    land(aws, key, envelope(response(api_tweet(doomed), api_tweet(tid(900)))))
    original = ComplianceStore._delete_older_versions
    calls = {"n": 0}

    def flaky(self: ComplianceStore, k: str, version: str) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ClientError({"Error": {"Code": "SlowDown"}}, "DeleteObject")
        original(self, k, version)

    monkeypatch.setattr(ComplianceStore, "_delete_older_versions", flaky)
    first = job(aws, Lookup({doomed: "deleted"}), clock, grace=timedelta(0))
    with pytest.raises(ComplianceError):
        first.run(HANDLE)
    assert len(versions(aws, key)) == 2  # the unredacted version is still there
    state = aws.state_table.get_item(Key={"username": f"{REDACT_PREFIX}own#{USER_ID}"})
    assert state["Item"]["pending_versions"] == [key]

    second = job(aws, Lookup({doomed: "deleted"}), clock, grace=timedelta(0))
    second.run(HANDLE)

    assert second.purged == 1
    assert len(versions(aws, key)) == 1  # only the redacted page is left
    assert doomed not in ids_in(aws, key)


def test_noncurrent_versions_are_deleted_but_never_under_a_delete_marker(
    versioned: Aws,
) -> None:
    aws = versioned
    store = store_for(aws)
    for body in (b"1", b"2", b"3"):
        aws.s3.put_object(Bucket=aws.bucket, Key="raw/9/a.json.gz", Body=body)
    store._delete_noncurrent_versions("raw/9/a.json.gz")
    assert len(versions(aws, "raw/9/a.json.gz")) == 1
    assert (
        aws.s3.get_object(Bucket=aws.bucket, Key="raw/9/a.json.gz")["Body"].read()
        == b"3"
    )

    aws.s3.put_object(Bucket=aws.bucket, Key="raw/9/b.json.gz", Body=b"1")
    aws.s3.put_object(Bucket=aws.bucket, Key="raw/9/b.json.gz", Body=b"2")
    aws.s3.delete_object(Bucket=aws.bucket, Key="raw/9/b.json.gz")  # a delete marker
    store._delete_noncurrent_versions("raw/9/b.json.gz")
    assert len(versions(aws, "raw/9/b.json.gz")) == 3  # someone removed it: keep all

    store._delete_noncurrent_versions("raw/9/none.json.gz")  # nothing there


def test_the_version_listing_of_a_cleanup_is_paginated(versioned: Aws) -> None:
    aws = versioned
    for n in range(5):
        aws.s3.put_object(
            Bucket=aws.bucket, Key="raw/9/c.json.gz", Body=str(n).encode()
        )
    real = aws.s3

    class SmallPages:
        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

        def list_object_versions(self, **kw: Any) -> Any:
            return real.list_object_versions(MaxKeys=2, **kw)

    store = ComplianceStore(aws.tweets_table, aws.state_table, SmallPages(), aws.bucket)  # type: ignore[arg-type]

    store._delete_noncurrent_versions("raw/9/c.json.gz")

    assert len(versions(aws, "raw/9/c.json.gz")) == 1


def test_only_older_versions_than_the_new_one_are_deleted(versioned: Aws) -> None:
    aws = versioned
    ids = [
        aws.s3.put_object(
            Bucket=aws.bucket, Key="raw/9/d.json.gz", Body=str(n).encode()
        )["VersionId"]
        for n in range(3)
    ]  # ids[1] is "ours"; ids[2] landed after it
    store_for(aws)._delete_older_versions("raw/9/d.json.gz", ids[1])

    kept = {
        v["VersionId"]
        for v in aws.s3.list_object_versions(
            Bucket=aws.bucket, Prefix="raw/9/d.json.gz"
        )["Versions"]
    }
    assert kept == {ids[1], ids[2]}  # the newer version is never touched


# --- N5: shared media ---


def test_a_copy_redaction_keeps_media_a_live_tweet_in_data_still_uses() -> None:
    retweet = {
        "id": "1700000000000000001",
        "text": "RT @b: look",
        "referenced_tweets": [{"type": "retweeted", "id": B}],
        "attachments": {"media_keys": ["3_1"]},
    }
    original = {"id": B, "text": "look", "attachments": {"media_keys": ["3_1", "3_2"]}}
    body = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at="2026-10-22T12:00:00Z",
        request={"endpoint": "/x", "params": {}},
        response={
            "data": [retweet],
            "includes": {
                "tweets": [original],
                "media": [
                    {"media_key": "3_1", "type": "photo"},
                    {"media_key": "3_2", "type": "photo"},
                ],
            },
        },
    ).to_bytes()

    redacted, gone = redact_envelope(body, [B], from_data=False)

    page = Envelope.from_bytes(redacted).response
    assert gone == {B}
    assert [m["media_key"] for m in page["includes"]["media"]] == ["3_1"]


# --- limits of a run ---


def test_the_budget_has_a_floor_of_four() -> None:
    with pytest.raises(ConfigError, match="COMPLIANCE_MAX_LOOKUPS must be >= 4"):
        Settings(compliance_max_lookups=3)
    assert Settings(compliance_max_lookups=4).compliance_max_lookups == 4


def test_the_confirmation_is_capped_per_run_and_the_rest_waits_without_failing(
    aws: Aws, clock: Clock
) -> None:
    due = [str(1900000000000000000 + n) for n in range(MAX_CONFIRM_REQUESTS * 100 + 50)]
    step = job(
        aws, lambda ids: TweetLookup(unavailable=dict.fromkeys(ids, "deleted")), clock
    )

    confirmed = step._confirm_due(due, {})

    assert len(confirmed) == MAX_CONFIRM_REQUESTS * 100
    assert step.deferred == 50 and step.failures == []


def test_the_confirmation_waits_when_time_is_up(aws: Aws, clock: Clock) -> None:
    step = gated(aws, Lookup(), clock, 0)

    assert step._confirm_due(["1", "2"], {}) == []
    assert step.deferred == 2 and step.failures == []


def test_a_scan_for_held_media_that_runs_out_of_time_defers_the_batch(
    aws: Aws,
) -> None:
    store = store_for(aws)
    doomed = put(aws, 1, with_media=True)
    own_key = (item(aws, doomed) or {})["media_keys"][0]
    quarantine(aws, "media", {"media_key": own_key, "type": "photo"})
    store.start_batch(
        "own#t",
        mode="own",
        author=USER_ID,
        ids={doomed},
        first_day="2026-10-11",
        media_keys={own_key},
    )
    gate = Gate(1)  # the quarantine listing's check is fine, the held scan's is not

    stalled = store.advance("own#t", stop=gate)

    assert stalled is not None and not stalled.done
    saved = store.load_batch("own#t") or {}
    assert saved["quar_after"].startswith("quarantine/")  # not past the object
    assert store.advance("own#t") is not None  # with time, it finishes
    assert (store.load_batch("own#t") or {})["complete"] is True


# --- retention edges ---


def test_a_cleared_tombstone_gets_its_expiry_back_from_the_first_store(
    aws: Aws,
) -> None:
    writer = TweetStore(aws.tweets_table, aws.s3, aws.bucket, retention_days=2)
    tweet = tid(1)
    writer.put_tweet(make_message(tweet), [])
    first = (item(aws, tweet) or {})["first_stored_at"]
    store = ComplianceStore(
        aws.tweets_table, aws.state_table, aws.s3, aws.bucket, retention_days=2
    )
    assert store.tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")
    assert "expires_at" not in (item(aws, tweet) or {})

    assert store.clear_tombstone(tweet)

    from datetime import datetime

    expected = int(datetime.fromisoformat(first).timestamp()) + 2 * 86_400
    assert int((item(aws, tweet) or {})["expires_at"]) == expected


def test_clearing_with_retention_handles_odd_items(aws: Aws) -> None:
    store = ComplianceStore(
        aws.tweets_table, aws.state_table, aws.s3, aws.bucket, retention_days=1
    )
    assert store.clear_tombstone(tid(404)) is False  # no such item
    tweet = put(aws, 1)
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet},
        UpdateExpression="REMOVE first_stored_at, stored_at",
    )
    store.tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")

    assert store.clear_tombstone(tweet)  # no stored time: no expiry is invented

    assert "expires_at" not in (item(aws, tweet) or {})


# --- the size of a batch ---


def test_a_batch_is_capped_and_its_state_stays_small(aws: Aws) -> None:
    store = store_for(aws)
    ids = [str(1700000000000000000 + n) for n in range(BATCH_CAP + 200)]

    state = store.start_batch(
        "own#big",
        mode="own",
        author=USER_ID,
        ids=ids,
        first_day="2026-01-01",
        holders=ids,
        explicit=[f"raw/{USER_ID}/dt=2026-01-01/{n}.json.gz" for n in range(2000)],
        media_keys=[f"3_{n}" for n in range(9000)],
    )

    assert len(state["ids"]) == len(state["holders"]) == BATCH_CAP
    assert len(state["explicit"]) == BATCH_CAP and len(state["media_keys"]) == 4000
    assert len(json.dumps(state, default=str)) < 250_000  # far below DynamoDB's 400 KB
    result = store.advance("own#big", stop=lambda: True)
    assert result is not None
    assert not result.covers(ids, "2026-01-01")  # the IDs past the cap wait


# --- the mutation gaps ---


def test_a_finished_batch_is_not_reused_for_an_earlier_start(
    aws: Aws,
) -> None:
    store = store_for(aws)
    doomed = put(aws, 1)
    stored = item(aws, doomed) or {}
    kwargs: dict[str, Any] = {"author": USER_ID}
    land(
        aws,
        f"raw/{USER_ID}/dt=2026-10-12/a-1.json.gz",
        envelope(response(api_tweet(doomed))),
    )
    first = store.redact_raw(
        "own#m", [stored], {doomed}, first_day="2026-10-12", **kwargs
    )
    assert first.done
    early = f"raw/{USER_ID}/dt=2026-10-05/b-1.json.gz"
    land(aws, early, envelope(response(api_tweet(doomed))))

    again = store.redact_raw(
        "own#m", [stored], {doomed}, first_day="2026-10-01", **kwargs
    )

    assert again.done and again.pages == 1  # a new batch, from the earlier day
    assert doomed not in ids_in(aws, early)


# --- the purge loop under time pressure and failures ---


def test_the_purge_does_not_begin_when_time_is_up_after_the_confirmation(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    store_for(aws).tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")
    # checks: walk, purge start, the read, the confirmation, then none left for begin
    step = gated(aws, Lookup({tweet: "deleted"}), clock, 4, max_lookups=4)

    step.run(HANDLE)

    assert "purging" not in (item(aws, tweet) or {}) and step.purged == 0


def test_the_purge_stops_before_finishing_when_time_is_up(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    store_for(aws).tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")
    step = gated(aws, Lookup({tweet: "deleted"}), clock, 5, max_lookups=4)

    step.run(HANDLE)  # begun, raw pages done (none), no time to finish

    assert "purging" in (item(aws, tweet) or {}) and step.purged == 0
    finishing = job(aws, Lookup(), clock, max_lookups=4)
    finishing.run(HANDLE)  # the next run finishes it, without asking X again
    assert finishing.purged == 1


def test_a_failure_to_begin_a_purge_is_reported(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    tweet = put(aws, 1)
    store_for(aws).tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")

    def failing(self: ComplianceStore, item: Any) -> Any:
        raise ClientError({"Error": {"Code": "Throttled"}}, "UpdateItem")

    monkeypatch.setattr(ComplianceStore, "begin_purge", failing)

    with pytest.raises(ComplianceError, match=f"purge {tweet}: ClientError"):
        job(aws, Lookup({tweet: "deleted"}), clock, max_lookups=4).run(HANDLE)
