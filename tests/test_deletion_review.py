"""Regression tests for the #21 review: each one is the inverse of a repro that passed
while the bug was there (quarantine copies, a stale purge, overlapping runs, media,
retention, unbounded work, rename, redelivery, unreadable pages)."""

import gzip
import json
import logging
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError
from conftest import Aws
from helpers import USER_ID, make_message, photo
from test_deletion_copies import B, entry, put_quote
from test_deletion_raw_pages import api_tweet, envelope, land, read, response
from test_handlers_compliance import (
    GRACE,
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

from twitter_timeline_data_pipeline.handlers import media_retry
from twitter_timeline_data_pipeline.handlers.compliance import (
    REDACT_PREFIX,
    ComplianceError,
    ComplianceStore,
)
from twitter_timeline_data_pipeline.models import MediaRetry, ReferencedPost
from twitter_timeline_data_pipeline.quarantine import QuarantineStore
from twitter_timeline_data_pipeline.raw import Envelope, redact_envelope
from twitter_timeline_data_pipeline.storage import (
    MediaStore,
    StoredMedia,
    TweetStore,
)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def versioned(aws: Aws) -> Aws:
    aws.s3.put_bucket_versioning(
        Bucket=aws.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    return aws


def purge(aws: Aws, clock: Clock, tweet: str, **kwargs: Any) -> Any:
    run(aws, Lookup({tweet: "deleted"}), clock, **kwargs)
    clock.now += GRACE + clock.step
    return run(aws, Lookup({tweet: "deleted"}), clock, **kwargs)


_pages = iter(range(1, 10_000))


def quarantine(aws: Aws, kind: str, payload: Any) -> str:
    """A real quarantine object (its own key: the page number is never reused)."""
    return QuarantineStore(aws.s3, aws.bucket, run_id="r").put(
        kind=kind,
        item=payload,
        error="ValueError: boom",
        raw_key=None,
        user_id=USER_ID,
        fetched_at="2026-10-12T00:00:00Z",
        page_no=next(_pages),
    )


# --- F1: quarantine copies are deleted ---


@pytest.mark.parametrize("kind", ["tweet", "message", "included_tweet"])
def test_real_quarantine_objects_of_a_purged_tweet_are_deleted(
    versioned: Aws, clock: Clock, kind: str
) -> None:
    aws = versioned
    doomed, other = put(aws, 1), put(aws, 2)
    payload: Any = (
        json.loads(make_message(doomed).to_json())
        if kind == "message"
        else api_tweet(doomed)
    )
    key = quarantine(aws, kind, payload)
    keep = quarantine(aws, kind, api_tweet(other))

    result = purge(aws, clock, doomed)

    assert result.purged == 1
    assert versions(aws, key) == []  # the deleted post is no longer in quarantine/
    assert len(versions(aws, keep)) == 1  # another Tweet's object stays


def test_quarantine_objects_without_a_top_level_tweet_id_are_matched_too(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    key = quarantine(aws, "tweet", api_tweet(doomed))
    document = json.loads(aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read())
    assert document["tweet_id"] == doomed  # new writers add it
    del document["tweet_id"]  # an object written before it existed
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=json.dumps(document).encode())

    purge(aws, clock, doomed)

    assert versions(aws, key) == []


def test_a_quarantined_media_object_goes_unless_another_item_lists_its_key(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1, with_media=True)
    shared = put(aws, 2, with_media=True)
    stored = item(aws, doomed) or {}
    own_key = stored["media_keys"][0]
    shared_key = (item(aws, shared) or {})["media_keys"][0]
    gone = quarantine(aws, "media", {"media_key": own_key, "type": "photo"})
    held = quarantine(aws, "media", {"media_key": shared_key, "type": "photo"})
    aws.tweets_table.update_item(  # the other Tweet also lists the doomed one's key
        Key={"tweet_id": shared},
        UpdateExpression="SET media_keys = :k",
        ExpressionAttributeValues={":k": [shared_key, own_key]},
    )
    gone_too = quarantine(aws, "media", {"media_key": "3_unrelated", "type": "photo"})

    purge(aws, clock, doomed)

    assert len(versions(aws, gone)) == 1  # another stored item still lists this key
    assert len(versions(aws, held)) == 1
    assert len(versions(aws, gone_too)) == 1  # not the doomed Tweet's media at all


def test_a_quarantined_media_object_of_the_doomed_tweet_alone_is_deleted(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1, with_media=True)
    own_key = (item(aws, doomed) or {})["media_keys"][0]
    key = quarantine(aws, "media", {"media_key": own_key, "type": "photo"})

    purge(aws, clock, doomed)

    assert versions(aws, key) == []


def test_a_copy_scrub_deletes_only_included_tweet_quarantine(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    quoting = put_quote(aws, 1, B)
    included = quarantine(aws, "included_tweet", api_tweet(B))
    own_page = quarantine(aws, "tweet", api_tweet(B))  # B's own object: B's own purge

    run(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0))

    assert entry(aws, quoting)["status"] == "unavailable" and "text" not in entry(
        aws, quoting
    )
    assert versions(aws, included) == []
    assert len(versions(aws, own_page)) == 1


def test_an_unparseable_quarantine_object_that_names_the_post_blocks_the_purge(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    key = f"quarantine/{USER_ID}/dt=2026-10-12/bad.json"
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=f"{{not json {doomed}".encode())
    other = f"quarantine/{USER_ID}/dt=2026-10-12/ok.json"
    aws.s3.put_object(Bucket=aws.bucket, Key=other, Body=b"{not json")

    with pytest.raises(ComplianceError, match="raw pages"):
        purge(aws, clock, doomed)

    assert "purged_at" not in (item(aws, doomed) or {})


# --- F2: a stale or transient answer never purges a present post ---


def test_a_tombstone_is_rechecked_before_its_purge_on_a_larger_account(
    aws: Aws, clock: Clock
) -> None:
    ids = [put(aws, n) for n in range(1, 31)]
    first = ids[0]
    assert store_for(aws).tombstone(first, "deleted", _iso(clock.now))  # one hiccup
    clock.now += GRACE + clock.step

    result = run(aws, Lookup(), clock, max_lookups=5)  # X says everything is there

    assert result.purged == 0
    stored = item(aws, first) or {}
    assert "text" in stored and "deleted_at" not in stored  # cleared, never purged


def test_a_present_answer_in_the_purge_run_itself_keeps_the_post(
    aws: Aws, clock: Clock
) -> None:
    tweet = put(aws, 1)
    run(aws, Lookup({tweet: "protected"}), clock)
    clock.now += GRACE  # exactly at the cutoff: due now
    lookup = Lookup()  # X: present

    result = run(aws, lookup, clock)

    assert (result.cleared, result.purged) == (1, 0)
    assert "text" in (item(aws, tweet) or {})


def _iso(moment: Any) -> str:
    return str(moment.isoformat(timespec="seconds")).replace("+00:00", "Z")


# --- F3: TTL can no longer remove a tombstone before its purge ---


def test_a_tombstone_has_no_expiry_and_retention_counts_from_the_first_store(
    aws: Aws, clock: Clock
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket, retention_days=1)
    doomed = tid(1)
    first = make_message(doomed, fetched_at="2026-10-20T00:00:00Z")
    store.put_tweet(first, [])
    stored = item(aws, doomed) or {}
    expires = int(stored["expires_at"])
    first_at = stored["first_stored_at"]
    aws.tweets_table.update_item(  # as if it was written long ago
        Key={"tweet_id": doomed},
        UpdateExpression="SET first_stored_at = :f, stored_at = :f",
        ExpressionAttributeValues={":f": "2026-01-01T00:00:00+00:00"},
    )
    store.put_tweet(make_message(doomed, fetched_at="2026-10-21T00:00:00Z"), [])
    rewritten = item(aws, doomed) or {}

    assert first_at <= rewritten["stored_at"]
    assert rewritten["first_stored_at"] == "2026-01-01T00:00:00+00:00"
    assert int(rewritten["expires_at"]) < expires  # from the first store, not the last

    run(aws, Lookup({doomed: "deleted"}), clock)

    tombstoned = item(aws, doomed) or {}
    assert "tombstone" in tombstoned and "expires_at" not in tombstoned


# --- F4: media ---


def test_media_retry_resolves_nothing_on_a_tombstoned_item(
    aws: Aws, clock: Clock
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    doomed = tid(1)
    store.put_tweet(
        make_message(doomed, media=(photo("3_9"),)),
        [StoredMedia(media_key="3_9", type="photo", status="retrying")],
    )
    run(aws, Lookup({doomed: "deleted"}), clock)
    key = f"media/{USER_ID}/{doomed}/3_9.jpg"
    entry_ = {"media_key": "3_9", "type": "photo", "status": "stored", "s3_key": key}

    assert store.resolve_media(doomed, 0, "3_9", entry_) is False  # deleted_at guard


def test_a_media_retry_for_a_tombstoned_tweet_downloads_nothing(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    doomed = tid(1)
    media = photo("3_9")
    store.put_tweet(
        make_message(doomed, media=(media,)),
        [StoredMedia(media_key="3_9", type="photo", status="retrying")],
    )
    run(aws, Lookup({doomed: "deleted"}), clock)
    retry = MediaRetry(
        tweet_id=doomed,
        username="example_user",
        author_id=USER_ID,
        media=media,
    )

    class NoDownload(MediaStore):
        def store(self, *args: Any, **kwargs: Any) -> Any:
            raise AssertionError("a tombstoned Tweet's media must not be downloaded")

    caplog.set_level(logging.INFO)
    media_retry._retry_one(
        {"body": retry.to_json()},
        media_store=NoDownload(aws.s3, aws.bucket, None),  # type: ignore[arg-type]
        tweet_store=store,
        remaining_ms=None,
    )

    assert "is tombstoned" in caplog.text


def test_media_uploaded_after_the_purge_is_swept_by_the_next_run(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    doomed = tid(1)
    store.put_tweet(
        make_message(doomed, media=(photo("3_9"),)),
        [StoredMedia(media_key="3_9", type="photo", status="retrying")],
    )
    run(aws, Lookup({doomed: "deleted"}), clock, grace=timedelta(0))  # purged now
    late = f"media/{USER_ID}/{doomed}/3_9.jpg"
    aws.s3.put_object(Bucket=aws.bucket, Key=late, Body=b"deleted post's photo")
    assert versions(aws, late)
    purged = item(aws, doomed) or {}
    assert purged["tombstone"] == "2" and purged["sweep_owners"]

    sweeping = run(aws, Lookup({doomed: "deleted"}), clock, grace=timedelta(0))

    assert versions(aws, late) == []  # swept
    assert sweeping.swept == 1
    final = item(aws, doomed) or {}
    assert set(final) == {  # the marker and the owners are gone: strictly content-free
        "tweet_id",
        "deleted_at",
        "deletion_reason",
        "purged_at",
        "revision",
    }


def test_the_purge_sweeps_the_media_prefix_not_only_the_named_keys(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1, with_media=True)
    stray = f"media/{USER_ID}/{doomed}/3_1.png"  # a key the item lost
    longer = f"media/{USER_ID}/{doomed}0/3_1.png"  # another Tweet whose ID extends it
    aws.s3.put_object(Bucket=aws.bucket, Key=stray, Body=b"old rendition")
    aws.s3.put_object(Bucket=aws.bucket, Key=longer, Body=b"not ours")

    purge(aws, clock, doomed)

    assert versions(aws, stray) == []
    assert len(versions(aws, longer)) == 1


# --- F5: overlapping runs cannot lose or resurrect page content ---


def page_ids(aws: Aws, key: str) -> list[str]:
    page = Envelope.from_bytes(
        aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()
    )
    return [t["id"] for t in page.response["data"]]


def test_overlapping_redactions_of_one_page_both_hold(
    versioned: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    aws = versioned
    a, b, c = tid(1), tid(2), tid(3)
    key = f"raw/{USER_ID}/dt=2026-10-22/aaa-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(a), api_tweet(b), api_tweet(c))))
    run1, run2 = store_for(aws), store_for(aws)
    original_read = ComplianceStore._read

    def interleaved_read(self: ComplianceStore, k: str) -> Any:
        body = original_read(self, k)
        if self is run1 and not interleaved_read.done:  # type: ignore[attr-defined]
            interleaved_read.done = True  # type: ignore[attr-defined]
            assert run2._redact_page(k, {b}, True) == 1  # run 2 completes in between
        return body

    interleaved_read.done = False  # type: ignore[attr-defined]
    monkeypatch.setattr(ComplianceStore, "_read", interleaved_read)
    assert run1._redact_page(key, {a}, True) == 1

    assert page_ids(aws, key) == [c]  # neither A nor B came back
    assert len(versions(aws, key)) == 1


def test_overlapping_runs_never_delete_every_version_of_a_page(
    versioned: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    aws = versioned
    a, c = tid(1), tid(3)
    key = f"raw/{USER_ID}/dt=2026-10-22/aaa-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(a), api_tweet(c))))
    run1, run2 = store_for(aws), store_for(aws)
    original_read = ComplianceStore._read
    body = original_read(run1, key)
    assert body is not None
    reads = iter([body, body, None])

    def stale_then_fresh(self: ComplianceStore, k: str) -> Any:
        stale = next(reads)
        return stale if stale is not None else original_read(self, k)

    monkeypatch.setattr(ComplianceStore, "_read", stale_then_fresh)
    run1._redact_page(key, {a}, True)  # both read the original ETag
    run2._redact_page(key, {a}, True)  # run 2's put is refused: it reads again

    assert page_ids(aws, key) == [c]
    assert len(versions(aws, key)) == 1  # the page, with live Tweet C, survives


def test_a_page_that_keeps_changing_is_given_up_on(versioned: Aws) -> None:
    aws = versioned
    a = tid(1)
    key = f"raw/{USER_ID}/dt=2026-10-22/aaa-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(a), api_tweet(tid(3)))))
    store = store_for(aws)
    real = aws.s3

    class Moving:
        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

        def put_object(self, **kw: Any) -> Any:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")

    broken = ComplianceStore(aws.tweets_table, aws.state_table, Moving(), aws.bucket)  # type: ignore[arg-type]

    with pytest.raises(ComplianceError, match="changed under"):
        broken._redact_page(key, {a}, True)
    other = ComplianceStore(aws.tweets_table, aws.state_table, Moving(), aws.bucket)  # type: ignore[arg-type]
    Moving.put_object = lambda self, **kw: (_ for _ in ()).throw(  # type: ignore[method-assign]
        ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
    )
    with pytest.raises(ClientError):
        other._redact_page(key, {a}, True)
    assert store  # (the plain store is untouched)


# --- F6: a redelivery cannot put a scrubbed copy back ---


def delivery(a: str, fetched_at: str, *, copy: bool = True) -> Any:
    refs = (
        (
            ReferencedPost(
                id=B,
                type="quoted",
                text="what the original said",
                author_id="77",
                author_username="someone",
                entities={"hashtags": [{"tag": "secret"}]},
            ),
        )
        if copy
        else ()
    )
    return make_message(
        a,
        tweet={
            "created_at": "2026-10-11T12:00:00.000Z",
            "referenced_tweets": [{"type": "quoted", "id": B}],
        },
        referenced=refs,
        fetched_at=fetched_at,
    )


def test_a_redelivery_after_the_scrub_does_not_restore_the_copy(
    aws: Aws, clock: Clock
) -> None:
    a = put_quote(aws, 1, B)
    run(aws, Lookup({B: "deleted"}), clock)
    clock.now += GRACE + clock.step
    run(aws, Lookup({B: "deleted"}), clock)
    scrubbed = entry(aws, a)
    assert "text" not in scrubbed and scrubbed["scrubbed_at"]
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    # the same SQS message again (duplicate delivery, DLQ redrive), then an older one
    for fetched_at in ("2026-10-26T12:00:00Z", "2026-10-01T00:00:00Z"):
        store.put_tweet(delivery(a, fetched_at), [])
        assert "text" not in entry(aws, a)
        assert "author_username" not in entry(aws, a)
    # unavailable in a later fetch: stays scrubbed too
    unavailable = delivery(a, "2030-01-01T00:00:00Z", copy=False)
    store.put_tweet(unavailable, [])
    assert "text" not in entry(aws, a)


def test_a_later_fetch_that_shows_the_post_available_restores_the_copy(
    aws: Aws, clock: Clock
) -> None:
    a = put_quote(aws, 1, B)
    run(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0))
    assert "text" not in entry(aws, a)

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(
        delivery(a, "2030-01-01T00:00:00Z"), []
    )

    back = entry(aws, a)
    assert back["text"] == "what the original said" and "scrubbed_at" not in back


# --- F7/F10: bounded, time-aware work ---


def seed_pages(aws: Aws, doomed: str, count: int) -> list[str]:
    keys = []
    for day in range(count):
        stamp = f"2026-{1 + day // 28:02d}-{1 + day % 28:02d}"
        key = f"raw/{USER_ID}/dt={stamp}/r{day}-1.json.gz"
        land(aws, key, envelope(response(api_tweet(tid(1000 + day)))))
        keys.append(key)
    return keys


def test_a_purge_with_many_pages_is_resumed_across_runs(aws: Aws, clock: Clock) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    old = tid(1)  # an old Tweet the user deletes
    store.put_tweet(
        make_message(old, tweet={"created_at": "2020-01-01T00:00:00.000Z"}), []
    )
    keys = seed_pages(aws, old, 90)
    land(aws, keys[70], envelope(response(api_tweet(old), api_tweet(tid(5000)))))
    reads = {"n": 0}
    original_read = ComplianceStore._read

    def counting(self: ComplianceStore, key: str) -> Any:
        reads["n"] += 1
        return original_read(self, key)

    ComplianceStore._read = counting  # type: ignore[method-assign]
    budget = {"left": 40}  # the run may read 40 pages, then time is up

    def remaining() -> int:
        budget["left"] -= 1
        return 10**6 if budget["left"] > 0 else 0

    try:
        first = job(aws, Lookup({old: "deleted"}), clock, grace=timedelta(0))
        first._remaining_ms = remaining
        first.run(HANDLE)
        assert first.purged == 0 and first.deferred == 1  # not finished: not purged
        assert reads["n"] < 90
        state = aws.state_table.get_item(
            Key={"username": f"{REDACT_PREFIX}own#{USER_ID}"}
        )
        assert state["Item"]["raw_after"] and not state["Item"].get("complete")
        assert "purged_at" not in (item(aws, old) or {})
        assert old in ids_of(aws, keys[70])  # not reached yet

        budget["left"] = 10**6  # enough time now
        second = run(aws, Lookup({old: "deleted"}), clock, grace=timedelta(0))
    finally:
        ComplianceStore._read = original_read  # type: ignore[method-assign]

    assert second.purged == 1
    assert_purged(aws, old)
    assert old not in ids_of(aws, keys[70])
    assert reads["n"] < 2 * 91  # the second run resumed, it did not start over


def ids_of(aws: Aws, key: str) -> list[str]:
    return [t["id"] for t in read(aws, key).response["data"]]


def test_a_finished_redaction_that_covers_a_request_is_not_repeated(
    aws: Aws,
) -> None:
    store = store_for(aws)
    doomed = put(aws, 1)
    stored = item(aws, doomed) or {}
    kwargs: dict[str, Any] = {"first_day": "2026-10-11", "author": USER_ID}
    land(
        aws,
        f"raw/{USER_ID}/dt=2026-10-12/a-1.json.gz",
        envelope(response(api_tweet(doomed))),
    )

    first = store.redact_raw("own#x", [stored], {doomed}, **kwargs)
    assert first.done and first.pages == 1
    again = store.redact_raw("own#x", [stored], {doomed}, **kwargs)

    assert again.done and again.pages == 0 and again.covers({doomed}, "2026-10-11")
    assert not again.covers({doomed}, "2026-10-01")  # an earlier start isn't covered
    assert not again.covers({"other"}, "2026-10-11")
    # a request it does not cover starts a new batch
    wider = store.redact_raw("own#x", [stored], {doomed, "9"}, **kwargs)
    assert wider.done and wider.ids == {doomed, "9"}


def test_a_batch_in_progress_is_finished_before_a_new_one_starts(aws: Aws) -> None:
    store = store_for(aws)
    first_item, second_item = put(aws, 1), put(aws, 2)
    for day in (12, 13, 14):
        land(
            aws,
            f"raw/{USER_ID}/dt=2026-10-{day}/a-1.json.gz",
            envelope(response(api_tweet(tid(900)))),
        )
    kwargs: dict[str, Any] = {"first_day": "2026-10-11", "author": USER_ID}
    calls = {"n": 0}

    def stop_after_one() -> bool:
        calls["n"] += 1
        return calls["n"] > 1

    stalled = store.redact_raw(
        "own#y",
        [item(aws, first_item) or {}],
        {first_item},
        stop=stop_after_one,
        **kwargs,
    )
    assert not stalled.done and stalled.ids == {first_item}

    other = store.redact_raw(
        "own#y", [item(aws, second_item) or {}], {second_item}, **kwargs
    )

    assert other.done and other.ids == {first_item}  # the batch in progress, first
    assert not other.covers({second_item}, "2026-10-11")  # the new one waits


# --- F8 (the walk covers every handle) is in test_handlers_compliance ---


# --- F9: includes.posts ---


def test_redact_envelope_redacts_includes_posts_too() -> None:
    body = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at="2026-10-22T12:00:00Z",
        request={"endpoint": "/x", "params": {}},
        response={
            "data": [{"id": "1", "text": "quote"}],
            "includes": {
                "posts": [{"id": B, "text": "DELETED POST TEXT"}, {"id": "7"}],
                "tweets": [{"id": B, "text": "DELETED POST TEXT"}],
            },
        },
    ).to_bytes()

    redacted, gone = redact_envelope(body, [B], from_data=False)

    assert gone == {B}
    after = Envelope.from_bytes(redacted)
    assert after.response["includes"] == {"posts": [{"id": "7"}], "tweets": []}
    assert after.redactions == ()  # includes only: not a redaction of the post's page
    assert b"DELETED POST TEXT" not in gzip.decompress(redacted)


# --- F11: a page the job can't parse blocks the purge ---


def test_an_unparseable_page_that_names_the_post_blocks_the_purge(
    aws: Aws, clock: Clock
) -> None:
    doomed = put(aws, 1)
    key = f"raw/{USER_ID}/dt=2026-10-12/v2-0001.json.gz"
    page = {
        "schema_version": 2,
        "username": "example_user",
        "user_id": USER_ID,
        "fetched_at": "2026-10-12T00:00:00Z",
        "request": {},
        "response": {"data": [{"id": doomed, "text": "DELETED POST TEXT"}]},
    }
    aws.s3.put_object(
        Bucket=aws.bucket, Key=key, Body=gzip.compress(json.dumps(page).encode())
    )
    unrelated = f"raw/{USER_ID}/dt=2026-10-13/v2-0002.json.gz"
    aws.s3.put_object(Bucket=aws.bucket, Key=unrelated, Body=b"not gzip at all")
    failing = job(aws, Lookup({doomed: "deleted"}), clock, grace=timedelta(0))

    with pytest.raises(ComplianceError, match="raw pages"):
        failing.run(HANDLE)

    assert failing.purged == 0 and "purged_at" not in (item(aws, doomed) or {})
    assert "text" in (item(aws, doomed) or {})  # retried every run until fixed


# --- P2-15: a copy scrub never touches a page's data ---


def test_a_copy_scrub_redacts_includes_only_never_the_posts_own_page_data(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    quoting = put_quote(aws, 1, B)
    holder = f"raw/{USER_ID}/dt=2026-10-12/h-1.json.gz"
    page = response(api_tweet(quoting), api_tweet(B))  # B also in this page's data
    page["includes"]["tweets"] = [api_tweet(B)]
    land(aws, holder, envelope(page))

    run(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0))

    after = read(aws, holder)
    assert after.response["includes"]["tweets"] == []
    assert [t["id"] for t in after.response["data"]] == [quoting, B]  # B's own purge
    assert after.redactions == ()


# --- the purge needs the item to be due when it is read ---


def test_a_tombstone_renewed_after_the_walk_is_not_purged(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    tweet = put(aws, 1)
    store = store_for(aws)
    store.tombstone(tweet, "deleted", _iso(clock.now))  # a fresh tombstone ...
    stale = {tweet: "2026-10-01T00:00:00Z"}  # ... that a stale listing calls old
    caplog.set_level(logging.INFO)
    compliance = job(aws, Lookup({tweet: "deleted"}), clock)

    compliance._purge_due(stale)

    assert "no longer due" in caplog.text
    assert "purged_at" not in (item(aws, tweet) or {})


# --- time, paging and failure paths ---


class CallsThenTimeOut:
    """``remaining_ms``-style gate: time is fine for ``fine`` calls, then it is up."""

    def __init__(self, fine: int) -> None:
        self.fine = fine

    def __call__(self) -> bool:
        self.fine -= 1
        return self.fine < 0


def test_the_quarantine_listing_stops_and_saves_its_position(
    aws: Aws,
) -> None:
    store = store_for(aws)
    doomed = put(aws, 1)
    quarantine(aws, "tweet", api_tweet(tid(77)))
    stalled = store.redact_raw(
        "own#q",
        [item(aws, doomed) or {}],
        {doomed},
        first_day="2026-10-11",
        author=USER_ID,
        stop=lambda: True,
    )

    assert not stalled.done
    saved = aws.state_table.get_item(Key={"username": f"{REDACT_PREFIX}own#q"})["Item"]
    assert "quar_after" in saved and saved["raw_done"] is True


def test_the_quarantine_progress_is_saved_every_few_objects(aws: Aws) -> None:
    store = store_for(aws)
    doomed = put(aws, 1)
    keys = [quarantine(aws, "tweet", api_tweet(tid(100 + n))) for n in range(30)]
    saves: list[str] = []
    original = store._state.put_item

    class Spy:
        def __getattr__(self, name: str) -> Any:
            return getattr(store._state, name)

        def put_item(self, **kw: Any) -> Any:
            saves.append(str(kw["Item"].get("quar_after", "")))
            return original(**kw)

    spied = ComplianceStore(aws.tweets_table, Spy(), aws.s3, aws.bucket)  # type: ignore[arg-type]

    result = spied.redact_raw(
        "own#r",
        [item(aws, doomed) or {}],
        {doomed},
        first_day="2026-10-11",
        author=USER_ID,
    )

    assert result.done
    assert any(s == keys[24] for s in saves)  # after the 25th object


def test_media_keys_held_by_others_pages_through_the_table(aws: Aws) -> None:
    ids = [put(aws, n, with_media=True) for n in range(1, 4)]
    mine = (item(aws, ids[0]) or {})["media_keys"][0]
    real = aws.tweets_table

    class OneAtATime:
        def scan(self, **kw: Any) -> Any:
            return real.scan(Limit=1, **kw)

    store = ComplianceStore(OneAtATime(), aws.state_table, aws.s3, aws.bucket)  # type: ignore[arg-type]
    real.update_item(
        Key={"tweet_id": ids[2]},
        UpdateExpression="SET media_keys = :k",
        ExpressionAttributeValues={":k": [mine]},
    )

    assert store.media_keys_held_by_others({mine}, {ids[0]}) == {mine}
    assert store.media_keys_held_by_others({mine}, {ids[0], ids[2]}) == set()
    assert store.media_keys_held_by_others(set(), set()) == set()


def test_a_sweep_of_a_tweet_that_is_not_waiting_for_one_does_nothing(
    aws: Aws,
) -> None:
    tweet = put(aws, 1)
    store = store_for(aws)

    assert store.sweep(tid(404)) == 0  # no such item
    assert store.sweep(tweet) == 0  # not a purged one
    # a sweep marker whose item lost purged_at: the marker write is refused, quietly
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet},
        UpdateExpression="SET tombstone = :s, sweep_owners = :o",
        ExpressionAttributeValues={":s": "2", ":o": [USER_ID]},
    )
    assert store.sweep(tweet) == 0
    assert (item(aws, tweet) or {})["tombstone"] == "2"

    class Throttled:
        def get_item(self, **kw: Any) -> Any:
            return aws.tweets_table.get_item(**kw)

        def update_item(self, **kw: Any) -> Any:
            raise ClientError({"Error": {"Code": "Throttled"}}, "UpdateItem")

    broken = ComplianceStore(Throttled(), aws.state_table, aws.s3, aws.bucket)  # type: ignore[arg-type]
    with pytest.raises(ClientError):
        broken.sweep(tweet)


def test_the_sweep_stops_when_time_is_up_and_reports_a_failure(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = put(aws, 1), put(aws, 2)
    run(aws, Lookup({first: "deleted", second: "deleted"}), clock, grace=timedelta(0))

    def failing(self: ComplianceStore, tweet_id: str) -> int:
        raise ClientError({"Error": {"Code": "Throttled"}}, "ListObjectVersions")

    monkeypatch.setattr(ComplianceStore, "sweep", failing)
    with pytest.raises(ComplianceError, match="sweep"):
        run(aws, Lookup(), clock)
    monkeypatch.undo()

    timed_out = job(aws, Lookup(), clock)
    timed_out._time_low = CallsThenTimeOut(2)  # type: ignore[method-assign]
    timed_out.run(HANDLE)  # time is up for the second sweep: it stays for the next run

    left = [t for t in (first, second) if (item(aws, t) or {}).get("tombstone") == "2"]
    assert len(left) == 1


def test_an_unfinished_raw_redaction_defers_the_copy_scrub_to_a_later_run(
    aws: Aws, clock: Clock
) -> None:
    quoting = put_quote(aws, 1, B)
    land(
        aws,
        f"raw/{USER_ID}/dt=2026-10-12/h-1.json.gz",
        envelope(response(api_tweet(quoting))),
    )
    deferred = job(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0))
    deferred._time_low = CallsThenTimeOut(1)  # type: ignore[method-assign]

    deferred.run(HANDLE)

    assert entry(aws, quoting)["text"] == "what the original said"  # kept, retried
    assert store_for(aws).active_units() == [f"copy#{USER_ID}"]

    # The next run goes on with the batch and scrubs the holder: it does not have to
    # wait for the walk to reach it again.
    nothing_to_walk = job(aws, Lookup({B: "deleted"}), clock, grace=timedelta(0))
    nothing_to_walk._walk = lambda tombstones: None  # type: ignore[method-assign]
    nothing_to_walk.run(HANDLE)

    assert nothing_to_walk.copies_scrubbed == 1
    assert "text" not in entry(aws, quoting)
    assert store_for(aws).active_units() == []


def test_a_present_answer_at_the_confirmation_clears_the_tombstones(
    aws: Aws, clock: Clock
) -> None:
    ids = [put(aws, n) for n in range(1, 6)]
    store = store_for(aws)
    for tweet in ids:
        store.tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")

    result = run(aws, Lookup(), clock, max_lookups=1)  # the walk reaches one of them

    assert result.purged == 0 and result.cleared == 5
    assert result.purge_checked == 4  # the rest were confirmed, and cleared there
    assert store.tombstones() == {}


def test_a_purge_that_cannot_begin_is_skipped(
    aws: Aws,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    tweet = put(aws, 1)
    store_for(aws).tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")
    monkeypatch.setattr(ComplianceStore, "begin_purge", lambda self, i: None)
    caplog.set_level(logging.INFO)

    result = run(aws, Lookup({tweet: "deleted"}), clock, max_lookups=1)

    assert result.purged == 0
    assert "was already removed or cleared" in caplog.text


def test_the_purge_stops_taking_items_when_time_is_up(aws: Aws, clock: Clock) -> None:
    ids = [put(aws, n) for n in range(1, 4)]
    store = store_for(aws)
    for tweet in ids:
        store.tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")
    lookup = Lookup(gone=dict.fromkeys(ids, "deleted"))

    early = job(aws, lookup, clock, max_lookups=1)
    early._time_low = CallsThenTimeOut(2)  # type: ignore[method-assign]
    early.run(HANDLE)  # walk, the confirmation, then no time to begin any purge

    assert early.purged == 0
    late = job(aws, lookup, clock, max_lookups=1)
    late._time_low = CallsThenTimeOut(5)  # type: ignore[method-assign]
    late.run(HANDLE)  # time for the first purge only
    assert late.purged <= 1


def test_a_failure_to_read_an_item_for_the_purge_is_reported(
    aws: Aws, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    tweet = put(aws, 1)
    store_for(aws).tombstone(tweet, "deleted", "2026-10-20T00:00:00Z")
    monkeypatch.setattr(
        ComplianceStore,
        "load_item",
        lambda self, t: (_ for _ in ()).throw(
            ClientError({"Error": {"Code": "Throttled"}}, "GetItem")
        ),
    )

    with pytest.raises(ComplianceError, match=f"purge {tweet}: ClientError"):
        run(aws, Lookup({tweet: "deleted"}), clock, max_lookups=1)


def test_the_time_comparison_of_a_scrub_falls_back_to_string_order() -> None:
    from twitter_timeline_data_pipeline.storage import _later

    assert _later("2026-10-27T00:00:00Z", "2026-10-26T00:00:00Z")
    assert _later("not a time", "also not")  # 'n' > 'a'
