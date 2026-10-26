"""Copies of deleted posts held in other items, the revision lock and the text purge."""

import json
import logging
from typing import Any

import pytest
from botocore.exceptions import ClientError
from conftest import Aws
from helpers import USER_ID, make_message
from test_deletion_raw_pages import (
    api_tweet,
    envelope,
    land,
    read,
    response,
    set_raw_key,
)
from test_handlers_compliance import (
    HANDLE,
    Clock,
    Lookup,
    assert_purged,
    item,
    job,
    put,
    run,
    store_for,
    tid,
    versions,
)

from twitter_timeline_data_pipeline.handlers.compliance import (
    REFERENCES_CURSOR,
    ComplianceError,
    ComplianceStore,
)
from twitter_timeline_data_pipeline.models import ReferencedPost
from twitter_timeline_data_pipeline.raw import Envelope, redact_envelope
from twitter_timeline_data_pipeline.replay import without_redactions
from twitter_timeline_data_pipeline.storage import TweetStore
from twitter_timeline_data_pipeline.x_api import TweetLookup

B = "1800000000000000001"  # a post by someone else, quoted by our Tweets
C = "1800000000000000002"
HUGE = "q" * 400_000


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def versioned(aws: Aws) -> Aws:
    aws.s3.put_bucket_versioning(
        Bucket=aws.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    return aws


def put_quote(
    aws: Aws,
    n: int,
    *refs: str,
    huge: bool = False,
    text: str = "what the original said",
) -> str:
    """Store our Tweet ``n`` quoting the posts ``refs`` (copies held in its item)."""
    tweet_id = tid(n)
    copies = tuple(
        ReferencedPost(
            id=ref,
            type="quoted",
            text=text,
            note_text=HUGE if huge else None,
            author_id="77",
            author_username="someone",
            created_at="2026-10-01T00:00:00.000Z",
            entities={"hashtags": [{"tag": "secret"}]},
        )
        for ref in refs
    )
    tweet: dict[str, Any] = {
        "created_at": f"2026-10-{10 + n:02d}T12:00:00.000Z",
        "referenced_tweets": [{"type": "quoted", "id": ref} for ref in refs],
    }
    if huge:
        tweet["note_text"] = "n" * 200_000
    message = make_message(tweet_id, tweet=tweet, referenced=copies)
    assert TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])
    return tweet_id


def entry(aws: Aws, tweet_id: str, ref: str = B) -> dict[str, Any]:
    stored = item(aws, tweet_id)
    assert stored is not None
    (found,) = (e for e in stored["referenced_tweets"] if e["id"] == ref)
    return dict(found)


def ref_keys(aws: Aws, tweet_id: str) -> list[str]:
    listed = aws.s3.list_objects_v2(
        Bucket=aws.bucket, Prefix=f"text/{USER_ID}/{tweet_id}.ref."
    )
    return [o["Key"] for o in listed.get("Contents", [])]


# --- the copies of a deleted post ----------


def test_first_sight_marks_the_copy_and_keeps_its_content(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    before = int(item(aws, quoting)["revision"])  # type: ignore[index]

    done = run(aws, Lookup(gone={B: "deleted"}), clock)

    stored = entry(aws, quoting)
    assert stored["status"] == "unavailable"
    assert stored["unavailable_reason"] == "not_found"
    assert stored["unavailable_since"] == "2026-10-26T12:00:00Z"
    assert stored["text"] == "what the original said"
    assert stored["author_username"] == "someone"
    assert int(item(aws, quoting)["revision"]) == before + 1  # type: ignore[index]
    assert (done.ref_checked, done.copies_marked, done.copies_scrubbed) == (1, 1, 0)


def test_the_copy_is_scrubbed_once_the_grace_period_has_passed(
    versioned: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    aws = versioned
    quoting = put_quote(aws, 1, B, C, huge=True)
    (key_b,) = [k for k in ref_keys(aws, quoting) if f".ref.{B}." in k]
    assert len(ref_keys(aws, quoting)) == 2
    raw = f"raw/{USER_ID}/dt=2026-10-21/aaa-0001.json.gz"
    page = response(api_tweet(quoting))
    page["includes"]["tweets"] = [api_tweet(B), api_tweet(C)]
    land(aws, raw, envelope(page))
    set_raw_key(aws, quoting, raw)
    lookup = Lookup(gone={B: "deleted"})
    run(aws, lookup, clock)
    assert versions(aws, key_b)
    clock.now += clock.step * 24 * 3600 + clock.step  # just past the grace period
    caplog.set_level(logging.INFO)

    done = run(aws, lookup, clock)

    assert entry(aws, quoting) == {
        "id": B,
        "type": "quoted",
        "status": "unavailable",
        "unavailable_reason": "not_found",
        "unavailable_since": "2026-10-26T12:00:00Z",
    }
    assert versions(aws, key_b) == []  # every version and delete marker
    assert len(ref_keys(aws, quoting)) == 1  # C's object stays
    assert "text_s3_key" in entry(aws, quoting, C)  # C's copy is untouched
    stored = item(aws, quoting)
    assert stored is not None and stored["text"].startswith("n")
    envelope_after = read(aws, raw)
    assert [t["id"] for t in envelope_after.response["includes"]["tweets"]] == [C]
    assert [t["id"] for t in envelope_after.response["data"]] == [quoting]
    assert envelope_after.redactions == (B,)
    assert (done.copies_scrubbed, done.ref_raw_pages) == (1, 1)
    assert "scrubbed 1 copy(ies) of deleted posts" in caplog.text
    assert "compliance references: checked=" in caplog.text


def test_zero_grace_scrubs_at_first_sight(aws: Aws, clock: Clock) -> None:
    quoting = put_quote(aws, 1, B)

    done = run(aws, Lookup(gone={B: "protected"}), clock, grace=clock.step * 0)

    assert entry(aws, quoting)["status"] == "unavailable"
    assert "text" not in entry(aws, quoting)
    assert done.copies_scrubbed == 1


def test_a_post_that_comes_back_clears_the_mark(aws: Aws, clock: Clock) -> None:
    quoting = put_quote(aws, 1, B)
    run(aws, Lookup(gone={B: "protected"}), clock)
    assert entry(aws, quoting).get("status") == "unavailable"

    done = run(aws, Lookup(), clock)

    assert entry(aws, quoting).get("status") is None
    assert entry(aws, quoting)["text"] == "what the original said"
    assert done.copies_unmarked == 1


def test_an_unknown_answer_changes_nothing(aws: Aws, clock: Clock) -> None:
    quoting = put_quote(aws, 1, B)
    run(aws, Lookup(gone={B: "deleted"}), clock)
    before = item(aws, quoting)

    done = run(aws, Lookup(unknown={B}), clock)

    assert item(aws, quoting) == before
    assert done.ref_checked == 1 and done.copies_scrubbed == 0


def test_a_failed_lookup_never_marks_or_scrubs_and_fails_the_run(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    before = item(aws, quoting)

    def failing(ids: list[str]) -> TweetLookup:
        raise RuntimeError("503")

    with pytest.raises(ComplianceError, match="references walk"):
        job(aws, failing, clock).run(HANDLE)

    assert item(aws, quoting) == before


def test_a_scrubbed_or_content_free_entry_is_not_looked_up_again(
    aws: Aws, clock: Clock
) -> None:
    put_quote(aws, 1, B)
    run(aws, Lookup(gone={B: "deleted"}), clock, grace=clock.step * 0)
    lookup = Lookup(gone={B: "deleted"})

    done = run(aws, lookup, clock)

    assert done.ref_checked == 0
    assert all(B not in call for call in lookup.calls)


def test_ids_are_deduplicated_and_counted_in_the_budget(aws: Aws, clock: Clock) -> None:
    put_quote(aws, 1, B, C)
    put_quote(aws, 2, B)
    lookup = Lookup()

    done = run(aws, lookup, clock, max_lookups=1000)

    refs = [i for call in lookup.calls for i in call if i in (B, C)]
    assert sorted(refs) == [B, C]  # B once, though two items hold it
    assert done.ref_checked == 2
    assert done.checked == 2  # plus the two stored Tweets: 4 lookups in all
    assert sum(len(c) for c in lookup.calls) == 4


def test_references_take_at_most_half_of_the_budget(aws: Aws, clock: Clock) -> None:
    for n in range(1, 4):
        put_quote(aws, n, str(1800000000000000100 + n))
    lookup = Lookup()

    done = run(aws, lookup, clock, max_lookups=6)

    assert done.ref_checked == 3  # half of 6
    assert done.checked == 3
    assert sum(len(c) for c in lookup.calls) == 6


def test_the_references_walk_stops_where_an_item_no_longer_fits_and_resumes(
    aws: Aws, clock: Clock
) -> None:
    ids = [str(1800000000000000100 + n) for n in range(4)]
    a = put_quote(aws, 1, ids[0], ids[1])
    b = put_quote(aws, 2, ids[2], ids[3])
    first = Lookup(gone=dict.fromkeys(ids, "deleted"))

    run(aws, first, clock, max_lookups=6)  # budget 3: item one fits (2), two doesn't

    assert entry(aws, a, ids[0]).get("status") == "unavailable"
    assert entry(aws, b, ids[2]).get("status") is None
    store = store_for(aws)
    assert store.load_cursor(REFERENCES_CURSOR) is not None

    run(aws, first, clock, max_lookups=6)

    assert entry(aws, b, ids[2]).get("status") == "unavailable"
    assert entry(aws, b, ids[3]).get("status") == "unavailable"


def test_the_walk_wraps_and_the_cursor_is_kept(aws: Aws, clock: Clock) -> None:
    put_quote(aws, 1, B)
    run(aws, Lookup(), clock)
    store = store_for(aws)

    assert store.load_cursor(REFERENCES_CURSOR) is None  # wrapped


def test_a_budget_below_six_skips_the_walk_with_a_warning(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    put_quote(aws, 1, B)

    done = run(aws, Lookup(gone={B: "deleted"}), clock, max_lookups=5)

    assert done.ref_checked == 0
    assert "leaves no room for the references walk" in caplog.text


def test_one_item_that_alone_exceeds_the_budget_is_reported(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    put_quote(aws, 1, B, C, "1800000000000000003", "1800000000000000004")

    done = run(aws, Lookup(), clock, max_lookups=6)

    assert done.ref_checked == 0
    assert "needs more lookups than left" in caplog.text


def test_a_stored_tweet_that_is_also_quoted_is_tombstoned_too(
    aws: Aws, clock: Clock
) -> None:
    original = put(aws, 1)
    put_quote(aws, 2, original)

    run(aws, Lookup(gone={original: "deleted"}), clock)

    stored = item(aws, original)
    assert stored is not None and stored["deleted_at"]


def test_the_item_changing_after_the_scan_is_read_again(aws: Aws, clock: Clock) -> None:
    quoting = put_quote(aws, 1, B)
    store = store_for(aws)
    ((snapshot, _),) = [(store.page_references(None, 10)[0][0], 0)]
    # a fetch lands between the scan and the update: the revision moves
    aws.tweets_table.update_item(
        Key={"tweet_id": quoting},
        UpdateExpression="SET fetched_at = :f ADD revision :one",
        ExpressionAttributeValues={":f": "2030-01-01T00:00:00Z", ":one": 1},
    )

    result = store.update_copies(
        snapshot,
        {B: "deleted"},
        now="2026-10-26T12:00:00Z",
        cutoff="2026-10-26T12:00:00Z",
    )

    assert result == (0, 0, 1)
    assert entry(aws, quoting)["status"] == "unavailable"
    assert item(aws, quoting)["fetched_at"] == "2030-01-01T00:00:00Z"  # type: ignore[index]


def test_copy_updates_give_up_after_three_lost_races(aws: Aws, clock: Clock) -> None:
    quoting = put_quote(aws, 1, B)
    store = store_for(aws)
    (snapshot,) = store.page_references(None, 10)[0]
    real = aws.tweets_table

    class Busy:
        def get_item(self, **kw: Any) -> Any:
            return real.get_item(**kw)

        def update_item(self, **kw: Any) -> Any:
            real.update_item(
                Key={"tweet_id": quoting},
                UpdateExpression="ADD revision :one",
                ExpressionAttributeValues={":one": 1},
            )
            return real.update_item(**kw)

    busy = ComplianceStore(Busy(), aws.state_table, aws.s3, aws.bucket)  # type: ignore[arg-type]

    with pytest.raises(ComplianceError, match="copy updates"):
        busy.update_copies(snapshot, {B: "deleted"}, now="n", cutoff="z")

    assert entry(aws, quoting).get("status") is None


def test_a_copy_update_leaves_a_tombstoned_or_vanished_item_alone(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    store = store_for(aws)
    (snapshot,) = store.page_references(None, 10)[0]
    store.tombstone(quoting, "deleted", "2026-10-26T12:00:00Z")

    assert store.update_copies(snapshot, {B: "deleted"}, now="n", cutoff="z") == (
        0,
        0,
        0,
    )
    assert entry(aws, quoting).get("status") is None
    aws.tweets_table.delete_item(Key={"tweet_id": quoting})
    store.update_copies(
        snapshot, {B: "deleted"}, now="n", cutoff="z"
    )  # lost race, gone


def test_a_failing_update_is_reported_and_the_rest_goes_on(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = put_quote(aws, 1, B)
    second = put_quote(aws, 2, C)
    real = ComplianceStore.update_copies

    def flaky(self: ComplianceStore, item: Any, *args: Any, **kwargs: Any) -> Any:
        if item["tweet_id"] == first:
            raise ClientError({"Error": {"Code": "Throttled"}}, "UpdateItem")
        return real(self, item, *args, **kwargs)

    monkeypatch.setattr(ComplianceStore, "update_copies", flaky)

    with pytest.raises(ComplianceError, match="copies"):
        job(aws, Lookup(gone={B: "deleted", C: "deleted"}), clock).run(HANDLE)

    assert entry(aws, second, C).get("status") == "unavailable"


def test_a_failing_scrub_is_reported_and_leaves_the_copy(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    quoting = put_quote(aws, 1, B)

    def failing(self: ComplianceStore, *args: Any, **kwargs: Any) -> Any:
        raise ClientError({"Error": {"Code": "Throttled"}}, "UpdateItem")

    monkeypatch.setattr(ComplianceStore, "update_copies", failing)

    with pytest.raises(ComplianceError, match="copies"):
        job(aws, Lookup(gone={B: "deleted"}), clock, grace=clock.step * 0).run(HANDLE)

    assert entry(aws, quoting)["text"] == "what the original said"


def test_a_raw_page_failure_blocks_the_scrub(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    quoting = put_quote(aws, 1, B)

    def failing(self: ComplianceStore, *args: Any, **kwargs: Any) -> Any:
        raise ClientError({"Error": {"Code": "SlowDown"}}, "ListObjectsV2")

    monkeypatch.setattr(ComplianceStore, "redact_raw", failing)

    with pytest.raises(ComplianceError, match="raw pages of copies"):
        job(aws, Lookup(gone={B: "deleted"}), clock, grace=clock.step * 0).run(HANDLE)

    assert entry(aws, quoting)["text"] == "what the original said"


def test_a_scrub_removes_only_the_copy_not_the_quoting_tweet(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    ours = item(aws, quoting)
    assert ours is not None

    run(aws, Lookup(gone={B: "deleted"}), clock, grace=clock.step * 0)

    after = item(aws, quoting)
    assert after is not None
    assert after["text"] == ours["text"]
    assert after["tweet_id"] == quoting


# --- raw pages ----------


def test_redact_envelope_removes_a_post_from_includes_and_its_media() -> None:
    page = response(api_tweet("1", "keep"))
    page["includes"]["tweets"] = [
        api_tweet("2", "only-2"),
        api_tweet("3"),
        "junk",
    ]
    page["includes"]["media"].append({"media_key": "only-2", "type": "photo"})
    body = envelope(page)

    redacted, gone = redact_envelope(body, ["2"])

    after = Envelope.from_bytes(redacted)
    assert gone == {"2"}
    assert [t["id"] for t in after.response["data"]] == ["1"]
    assert after.response["includes"]["tweets"] == [api_tweet("3"), "junk"]
    assert [m["media_key"] for m in after.response["includes"]["media"]] == ["keep"]
    assert after.redactions == ("2",)
    only_included = envelope({"includes": {"tweets": [api_tweet("2")]}})
    out = Envelope.from_bytes(redact_envelope(only_included, ["2"])[0])
    assert out.response == {"includes": {"tweets": []}}


def test_purging_a_stored_tweet_also_removes_it_from_includes(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    page = f"raw/{USER_ID}/dt=2026-10-27/zzz-0001.json.gz"
    body = response(api_tweet(tid(2)))
    body["includes"]["tweets"] = [api_tweet(doomed)]
    land(aws, page, envelope(body))
    set_raw_key(aws, doomed, f"raw/{USER_ID}/dt=2026-10-21/aaa-0001.json.gz")
    store = store_for(aws)
    store.tombstone(doomed, "deleted", "2026-10-24T00:00:00Z")

    run(aws, Lookup(gone={doomed: "deleted"}), clock)

    assert read(aws, page).response["includes"]["tweets"] == []
    assert read(aws, page).redactions == (doomed,)


def test_replay_shows_a_redacted_included_post_as_unavailable() -> None:
    from twitter_timeline_data_pipeline.models import TimelinePage

    page = TimelinePage(
        tweets=(),
        media={},
        included={B: ReferencedPost(id=B, text="secret")},
        requested={"expansions": True},
    )

    assert without_redactions(page, (B,)).included == {}
    assert without_redactions(page, ()).included == {B: page.included[B]}


# --- text objects of a purge ----------


def test_the_purge_deletes_every_text_object_of_the_tweet_and_only_its_own(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    gone = put(aws, 1)  # 1700000000000000001
    other = gone + "0"  # another Tweet whose ID merely starts with the same digits
    mine = [
        f"text/{USER_ID}/{gone}.json",
        f"text/{USER_ID}/{gone}.display_text.json",
        f"text/{USER_ID}/{gone}.ref.55.json",
        f"text/{USER_ID}/{gone}.ref.55.display_text.json",
    ]
    theirs = [f"text/{USER_ID}/{other}.json", f"text/{USER_ID}/{other}.ref.55.json"]
    for key in (*mine, *theirs, mine[0]):  # the first one has two versions
        aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b'"x"')
    store_for(aws).tombstone(gone, "deleted", "2026-10-24T00:00:00Z")

    run(aws, Lookup(gone={gone: "deleted"}), clock)

    assert_purged(aws, gone)
    for key in mine:
        assert versions(aws, key) == []
    for key in theirs:
        assert len(versions(aws, key)) == 1


def test_the_text_prefix_never_matches_a_longer_id(aws: Aws) -> None:
    store = store_for(aws)

    (prefix,) = store.text_prefixes({"author_id": "9"}, "123")

    assert prefix == "text/9/123."
    assert not "text/9/1234.json".startswith(prefix)
    assert store.text_prefixes(
        {
            "username": "Bob",
            "referenced_tweets": ["x", {"text_s3_key": "text/7/1.json"}],
        },
        "1",
    ) == {"text/bob/1.", "text/7/1."}


def test_the_prefix_delete_refuses_a_prefix_outside_text(aws: Aws) -> None:
    with pytest.raises(ComplianceError):
        store_for(aws).delete_prefix_versions("media/1/")


def test_the_prefix_listing_is_paginated(versioned: Aws) -> None:
    aws = versioned
    for n in range(1100):
        aws.s3.put_object(Bucket=aws.bucket, Key=f"text/9/5.ref.{n}.json", Body=b"1")

    objects, count = store_for(aws).delete_prefix_versions("text/9/5.")

    assert (objects, count) == (1100, 1100)


# --- the revision lock ----------


def test_the_tombstone_and_its_clearing_bump_the_revision(aws: Aws) -> None:
    tweet = put(aws, 1)
    store = store_for(aws)
    first = int(item(aws, tweet)["revision"])  # type: ignore[index]

    assert store.tombstone(tweet, "deleted", "2026-10-26T12:00:00Z")
    tombstoned = int(item(aws, tweet)["revision"])  # type: ignore[index]
    assert store.clear_tombstone(tweet, "2026-10-26T00:00:00Z")

    assert tombstoned == first + 1
    assert int(item(aws, tweet)["revision"]) == first + 2  # type: ignore[index]


def test_a_legacy_item_without_a_revision_gets_one(aws: Aws) -> None:
    tweet = put(aws, 1)
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet}, UpdateExpression="REMOVE revision"
    )

    assert store_for(aws).tombstone(tweet, "deleted", "2026-10-26T12:00:00Z")

    assert int(item(aws, tweet)["revision"]) == 1  # type: ignore[index]


def test_the_purge_is_retried_when_the_item_changes_under_it(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    tweet = put(aws, 1)
    store = store_for(aws)
    store.tombstone(tweet, "deleted", "2026-10-24T00:00:00Z")
    stale = store.load_item(tweet)
    assert stale is not None
    # a media retry resolves after the item was read: a new object and a new revision
    key = f"media/{USER_ID}/{tweet}/late.jpg"
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b"img")
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet},
        UpdateExpression="SET media = :m ADD revision :one",
        ExpressionAttributeValues={
            ":m": [{"media_key": "k", "status": "stored", "s3_key": key}],
            ":one": 1,
        },
    )

    objects, _ = store.purge(stale)

    assert objects == 1  # the late object, found on the second attempt
    assert versions(aws, key) == []
    assert_purged(aws, tweet)


def test_the_purge_gives_up_after_three_lost_races(aws: Aws) -> None:
    tweet = put(aws, 1)
    store = store_for(aws)
    store.tombstone(tweet, "deleted", "2026-10-24T00:00:00Z")
    stale = store.load_item(tweet)
    assert stale is not None
    real = aws.tweets_table

    class Busy:
        def get_item(self, **kw: Any) -> Any:
            return real.get_item(**kw)

        def update_item(self, **kw: Any) -> Any:
            real.update_item(
                Key={"tweet_id": tweet},
                UpdateExpression="ADD revision :one",
                ExpressionAttributeValues={":one": 1},
            )
            return real.update_item(**kw)

    busy = ComplianceStore(Busy(), aws.state_table, aws.s3, aws.bucket)  # type: ignore[arg-type]

    with pytest.raises(ComplianceError, match="purge attempts"):
        busy.purge(stale)

    assert "purged_at" not in (item(aws, tweet) or {})


def test_a_purge_that_finds_the_item_cleared_or_already_purged_stops(
    aws: Aws,
) -> None:
    tweet = put(aws, 1)
    store = store_for(aws)
    store.tombstone(tweet, "deleted", "2026-10-24T00:00:00Z")
    stale = store.load_item(tweet)
    assert stale is not None
    store.clear_tombstone(tweet, "2026-10-01T00:00:00Z")  # tombstone cleared meanwhile

    assert store.purge(stale)[0] == 0
    assert "purged_at" not in (item(aws, tweet) or {})
    aws.tweets_table.delete_item(Key={"tweet_id": tweet})
    assert store.purge(stale)[0] == 0  # vanished


def test_other_purge_errors_propagate(aws: Aws) -> None:
    tweet = put(aws, 1)
    store = store_for(aws)
    store.tombstone(tweet, "deleted", "2026-10-24T00:00:00Z")
    stale = store.load_item(tweet)
    assert stale is not None

    class Throttled:
        def update_item(self, **kw: Any) -> Any:
            raise ClientError({"Error": {"Code": "Throttled"}}, "UpdateItem")

    broken = ComplianceStore(Throttled(), aws.state_table, aws.s3, aws.bucket)  # type: ignore[arg-type]

    with pytest.raises(ClientError):
        broken.purge(stale)


def test_copy_updates_propagate_other_errors(aws: Aws) -> None:
    put_quote(aws, 1, B)
    (snapshot,) = store_for(aws).page_references(None, 10)[0]

    class Throttled:
        def update_item(self, **kw: Any) -> Any:
            raise ClientError({"Error": {"Code": "Throttled"}}, "UpdateItem")

    broken = ComplianceStore(Throttled(), aws.state_table, aws.s3, aws.bucket)  # type: ignore[arg-type]

    with pytest.raises(ClientError):
        broken.update_copies(snapshot, {B: "deleted"}, now="n", cutoff="z")


def test_an_entry_without_a_lookup_result_is_left_alone(aws: Aws) -> None:
    put_quote(aws, 1, B, C)
    (snapshot,) = store_for(aws).page_references(None, 10)[0]

    assert store_for(aws).update_copies(snapshot, {}, now="n", cutoff="z") == (0, 0, 0)
    assert json.dumps(snapshot["referenced_tweets"], default=str)


def test_a_legacy_item_without_a_revision_is_purged_and_gets_one(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet}, UpdateExpression="REMOVE revision"
    )

    run(aws, Lookup(gone={tweet: "deleted"}), clock, grace=clock.step * 0)

    assert_purged(aws, tweet)


def mark_unavailable_like_a_fetch(aws: Aws, tweet_id: str, since: str) -> None:
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet_id},
        UpdateExpression=(
            "SET referenced_tweets[0].#s = :s, referenced_tweets[0].unavailable_reason"
            " = :r, referenced_tweets[0].unavailable_since = :since ADD revision :one"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": "unavailable",
            ":r": "not_found",
            ":since": since,
            ":one": 1,
        },
    )


def test_a_fetchs_unavailable_mark_starts_the_clock_but_the_lookup_confirms(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    mark_unavailable_like_a_fetch(aws, quoting, "2026-10-20T00:00:00Z")

    # the compliance lookup says the post is there: nothing is scrubbed, marks cleared
    done = run(aws, Lookup(), clock)

    cleared = entry(aws, quoting)
    assert cleared["text"] == "what the original said"
    for name in ("status", "unavailable_reason", "unavailable_since"):
        assert name not in cleared
    assert done.copies_scrubbed == 0 and done.copies_unmarked == 1


def test_a_confirmed_unavailable_copy_is_scrubbed_on_the_fetchs_clock(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    mark_unavailable_like_a_fetch(aws, quoting, "2026-10-20T00:00:00Z")

    done = run(aws, Lookup(gone={B: "deleted"}), clock)

    assert entry(aws, quoting) == {
        "id": B,
        "type": "quoted",
        "status": "unavailable",
        "unavailable_reason": "not_found",
        "unavailable_since": "2026-10-20T00:00:00Z",
    }
    assert done.copies_scrubbed == 1


def test_an_unconfirmed_fetch_mark_scrubs_nothing(aws: Aws, clock: Clock) -> None:
    quoting = put_quote(aws, 1, B)
    mark_unavailable_like_a_fetch(aws, quoting, "2026-10-20T00:00:00Z")

    run(aws, Lookup(unknown={B}), clock)

    assert entry(aws, quoting)["text"] == "what the original said"


def test_a_legacy_item_without_a_revision_takes_copy_updates(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    aws.tweets_table.update_item(
        Key={"tweet_id": quoting}, UpdateExpression="REMOVE revision"
    )

    run(aws, Lookup(gone={B: "deleted"}), clock)

    assert entry(aws, quoting).get("status") == "unavailable"
    assert int(item(aws, quoting)["revision"]) == 1  # type: ignore[index]


def test_the_scrub_removes_the_copys_entities_objects_and_pointers(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    quoting = put_quote(aws, 1, B)
    base = f"text/{USER_ID}/{quoting}.ref.{B}."
    hashed = [
        base + "entities.0123456789abcdef.json",
        base + "entities.fedcba9876543210.json",  # an older version of the entities
        base + "display_entities.0123456789abcdef.json",
    ]
    other = f"text/{USER_ID}/{quoting}.ref.{C}.entities.0123456789abcdef.json"
    for key in (*hashed, other):
        aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b"{}")
    aws.tweets_table.update_item(
        Key={"tweet_id": quoting},
        UpdateExpression=(
            "SET referenced_tweets[0].entities_s3_key = :k "
            "REMOVE referenced_tweets[0].entities"
        ),
        ExpressionAttributeValues={":k": hashed[0]},
    )

    run(aws, Lookup(gone={B: "protected"}), clock, grace=clock.step * 0)

    assert entry(aws, quoting) == {
        "id": B,
        "type": "quoted",
        "status": "unavailable",
        "unavailable_reason": "not_authorized",
        "unavailable_since": "2026-10-26T12:00:00Z",
    }
    for key in hashed:
        assert versions(aws, key) == []
    assert len(versions(aws, other)) == 1


def test_the_purge_covers_the_entities_objects_of_the_tweet(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    gone = put(aws, 1)
    keys = [
        f"text/{USER_ID}/{gone}.entities.0123456789abcdef.json",
        f"text/{USER_ID}/{gone}.entities.fedcba9876543210.json",
        f"text/{USER_ID}/{gone}.display_entities.0123456789abcdef.json",
        f"text/{USER_ID}/{gone}.ref.7.entities.0123456789abcdef.json",
    ]
    longer = f"text/{USER_ID}/{gone}0.entities.0123456789abcdef.json"
    for key in (*keys, longer):
        aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=b"{}")

    run(aws, Lookup(gone={gone: "deleted"}), clock, grace=clock.step * 0)

    assert_purged(aws, gone)
    for key in keys:
        assert versions(aws, key) == []
    assert len(versions(aws, longer)) == 1
