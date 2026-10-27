"""Deletions reach raw pages and quarantine, and a purged Tweet can't come back."""

import gzip
import json
import logging
from typing import Any

import pytest
from botocore.exceptions import ClientError
from conftest import Aws
from helpers import CdnStub, make_message, photo, sqs_record
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

from twitter_timeline_data_pipeline.handlers.compliance import (
    Compliance,
    ComplianceError,
)
from twitter_timeline_data_pipeline.handlers.process import process_records
from twitter_timeline_data_pipeline.models import RawPage
from twitter_timeline_data_pipeline.raw import (
    Envelope,
    RawPageError,
    RawPageStore,
    redact_envelope,
)
from twitter_timeline_data_pipeline.replay import messages_from_raw, run_replay
from twitter_timeline_data_pipeline.storage import MediaStore, TweetStore

USER = "2244994945"
FETCHED = "2026-10-26T12:00:00Z"
PHOTO = "https://pbs.twimg.com/media/Doomed.jpg"


def api_tweet(tweet_id: str, *media: str) -> dict[str, Any]:
    tweet: dict[str, Any] = {
        "id": tweet_id,
        "text": f"tweet {tweet_id}",
        "created_at": "2026-10-20T12:00:00.000Z",
        "author_id": USER,
        "edit_history_tweet_ids": [tweet_id],
    }
    if media:
        tweet["attachments"] = {"media_keys": list(media)}
    return tweet


def response(*tweets: dict[str, Any]) -> dict[str, Any]:
    keys = {
        k for t in tweets for k in (t.get("attachments") or {}).get("media_keys", ())
    }
    return {
        "data": list(tweets),
        "includes": {
            "media": [
                {"media_key": k, "type": "photo", "url": PHOTO} for k in sorted(keys)
            ]
        },
        "meta": {"result_count": len(tweets)},
    }


def envelope(
    body: dict[str, Any], user: str = USER, redactions: tuple[str, ...] = ()
) -> bytes:
    return Envelope(
        username=HANDLE,
        user_id=user,
        fetched_at=FETCHED,
        request={"endpoint": f"/users/{user}/tweets", "params": {}},
        response=body,
        redactions=redactions,
    ).to_bytes()


def land(aws: Aws, key: str, body: bytes) -> None:
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=body)


def read(aws: Aws, key: str) -> Envelope:
    return Envelope.from_bytes(
        aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()
    )


def ids_in(page: Envelope) -> list[str]:
    return [t["id"] for t in page.response["data"]]


def set_raw_key(aws: Aws, tweet_id: str, key: str) -> None:
    aws.tweets_table.update_item(
        Key={"tweet_id": tweet_id},
        UpdateExpression="SET raw_key = :k",
        ExpressionAttributeValues={":k": key},
    )


# --- redact_envelope ------------------------------------------------------------


def test_redact_envelope_removes_the_tweet_and_only_its_own_media() -> None:
    body = envelope(
        response(api_tweet("1", "m1", "shared"), api_tweet("2", "shared", "m2")),
        redactions=("9",),
    )

    redacted, gone = redact_envelope(body, ["1", "404"])

    page = Envelope.from_bytes(redacted)
    assert gone == {"1"}
    assert ids_in(page) == ["2"]
    assert [m["media_key"] for m in page.response["includes"]["media"]] == [
        "m2",
        "shared",
    ]
    assert page.redactions == ("1", "9")
    assert page.response["meta"] == {"result_count": 2}


def test_redact_envelope_leaves_other_pages_and_odd_shapes_alone() -> None:
    other = envelope(response(api_tweet("2")))
    no_data = envelope({"meta": {"result_count": 0}})
    odd = envelope({"data": ["junk", {"no_id": 1}, api_tweet("2")]})

    for body in (other, no_data, odd):
        assert redact_envelope(body, ["1"]) == (body, set())
    only = envelope({"data": [api_tweet("1", "m1")]})  # no includes at all
    page = Envelope.from_bytes(redact_envelope(only, ["1"])[0])
    assert page.response["data"] == []
    assert page.redactions == ("1",)
    junk_media = envelope(
        {"data": [api_tweet("1", "m1")], "includes": {"media": ["junk"]}}
    )
    kept = Envelope.from_bytes(redact_envelope(junk_media, ["1"])[0])
    assert kept.response["includes"]["media"] == ["junk"]


def test_redact_envelope_rejects_what_is_not_an_envelope() -> None:
    with pytest.raises(RawPageError):
        redact_envelope(b"not gzip", ["1"])


# --- the purge rewrites raw pages --------------------------------------------------


def purge_with_pages(aws: Aws, clock: Clock, *, tweet: str) -> Any:
    run(aws, Lookup({tweet: "deleted"}), clock)
    clock.now += GRACE + clock.step
    return run(aws, Lookup({tweet: "deleted"}), clock)


@pytest.fixture(name="clock")
def make_clock() -> Clock:
    return Clock()


@pytest.fixture
def versioned(aws: Aws) -> Aws:
    aws.s3.put_bucket_versioning(
        Bucket=aws.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    return aws


def test_purge_rewrites_every_raw_page_holding_the_tweet(
    versioned: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    other = tid(2)
    first = f"raw/{USER}/dt=2026-10-21/aaa-0001.json.gz"  # a re-fetched duplicate
    second = f"raw/{USER}/dt=2026-10-22/bbb-0001.json.gz"  # the item's raw_key
    later = f"raw/{USER}/dt=2026-10-27/ccc-0001.json.gz"  # after the item's page
    elsewhere = "raw/999/dt=2026-10-22/ddd-0001.json.gz"
    for key in (first, second):
        land(aws, key, envelope(response(api_tweet(doomed, "m1"), api_tweet(other))))
    land(aws, first, envelope(response(api_tweet(doomed, "m1"), api_tweet(other))))
    land(aws, later, envelope(response(api_tweet(doomed))))
    unrelated = f"raw/{USER}/dt=2026-10-21/eee-0001.json.gz"
    land(aws, unrelated, envelope(response(api_tweet(other))))
    land(aws, elsewhere, envelope(response(api_tweet(doomed)), user="999"))
    set_raw_key(aws, doomed, second)
    assert len(versions(aws, first)) == 2
    caplog.set_level(logging.INFO)

    result = purge_with_pages(aws, clock, tweet=doomed)

    # a later page too: another Tweet quoting this one holds it in includes.tweets
    assert result.raw_pages_redacted == 3
    for key in (first, second):
        page = read(aws, key)
        assert ids_in(page) == [other]
        assert page.redactions == (doomed,)
        assert page.response["includes"]["media"] == []
        assert len(versions(aws, key)) == 1  # the originals are gone
    assert ids_in(read(aws, unrelated)) == [other]
    assert read(aws, unrelated).redactions == ()
    assert len(versions(aws, unrelated)) == 1  # not rewritten
    assert doomed not in ids_in(read(aws, later))  # no upper day limit
    assert doomed in ids_in(read(aws, elsewhere))  # another account's pages
    assert_purged(aws, doomed)
    assert f"redacted raw page {first}" in caplog.text


def test_replay_after_the_purge_does_not_bring_the_tweet_back(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    other = tid(2)
    key = f"raw/{USER}/dt=2026-10-22/bbb-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(doomed), api_tweet(other))))
    set_raw_key(aws, doomed, key)
    purge_with_pages(aws, clock, tweet=doomed)

    sent: list[str] = []

    def send(messages: list[Any]) -> int:
        sent.extend(m.tweet.id for m in messages)
        return len(messages)

    result = run_replay(
        s3=aws.s3,
        bucket=aws.bucket,
        prefix="raw/",
        send=send,
        quarantine=None,
    )

    assert sent == [other]
    assert result.tweets == 1
    ((_, messages),) = messages_from_raw(aws.s3, aws.bucket, "raw/")
    assert [m.tweet.id for m in messages] == [other]


def test_redaction_is_limited_to_the_days_between_creation_and_the_raw_page(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)  # created 2026-10-11
    too_early = f"raw/{USER}/dt=2026-10-10/x-0001.json.gz"
    in_range = f"raw/{USER}/dt=2026-10-12/x-0001.json.gz"
    for key in (too_early, in_range):
        land(aws, key, envelope(response(api_tweet(doomed))))
    set_raw_key(aws, doomed, f"raw/{USER}/dt=2026-10-13/gone-0001.json.gz")  # missing

    purge_with_pages(aws, clock, tweet=doomed)

    assert ids_in(read(aws, in_range)) == []
    assert ids_in(read(aws, too_early)) == [doomed]


def test_an_item_without_a_raw_key_is_searched_to_the_end(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    latest = f"raw/{USER}/dt=2027-03-01/x-0001.json.gz"
    land(aws, latest, envelope(response(api_tweet(doomed))))

    purge_with_pages(aws, clock, tweet=doomed)

    assert ids_in(read(aws, latest)) == []


def test_several_tweets_of_one_account_share_one_listing(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    a, b = put(aws, 1), put(aws, 3)
    key = f"raw/{USER}/dt=2026-10-14/x-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(a), api_tweet(b), api_tweet(tid(2)))))
    set_raw_key(aws, a, key)
    set_raw_key(aws, b, f"raw/{USER}/dt=2026-10-15/y-0001.json.gz")
    lookup = Lookup({a: "deleted", b: "protected"})
    run(aws, lookup, clock)
    clock.now += GRACE + clock.step

    result = run(aws, lookup, clock)

    assert result.purged == 2
    assert result.raw_pages_redacted == 1
    assert ids_in(read(aws, key)) == [tid(2)]
    assert read(aws, key).redactions == tuple(sorted((a, b)))


def test_items_without_an_author_id_only_use_their_raw_key(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    aws.tweets_table.update_item(
        Key={"tweet_id": doomed}, UpdateExpression="REMOVE author_id"
    )
    key = f"raw/{USER}/dt=2026-10-12/x-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(doomed))))
    set_raw_key(aws, doomed, key)

    purge_with_pages(aws, clock, tweet=doomed)

    assert ids_in(read(aws, key)) == []


def test_unreadable_raw_pages_are_left_alone(
    versioned: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    key = f"raw/{USER}/dt=2026-10-12/x-0001.json.gz"
    land(aws, key, b"not gzip")
    set_raw_key(aws, doomed, key)

    purge_with_pages(aws, clock, tweet=doomed)

    assert aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read() == b"not gzip"
    assert "unreadable" in caplog.text
    assert_purged(aws, doomed)


def test_unversioned_bucket_rewrite_keeps_the_new_page(aws: Aws, clock: Clock) -> None:
    doomed = put(aws, 1)
    key = f"raw/{USER}/dt=2026-10-12/x-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(doomed), api_tweet(tid(2)))))
    set_raw_key(aws, doomed, key)

    purge_with_pages(aws, clock, tweet=doomed)

    assert ids_in(read(aws, key)) == [tid(2)]


def test_a_raw_page_failure_blocks_the_purge_and_is_retried(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1, with_media=True)
    key = f"raw/{USER}/dt=2026-10-12/x-0001.json.gz"
    land(aws, key, envelope(response(api_tweet(doomed))))
    set_raw_key(aws, doomed, key)
    run(aws, Lookup({doomed: "deleted"}), clock)
    clock.now += GRACE + clock.step
    store = store_for(aws)
    real = store._s3.put_object

    def broken(**_: Any) -> Any:
        raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")

    store._s3.put_object = broken  # type: ignore[method-assign]
    failing = Compliance(
        store, Lookup({doomed: "deleted"}), max_lookups=10, grace=GRACE, now=clock
    )
    with pytest.raises(ComplianceError, match="raw pages: ClientError"):
        failing.run(HANDLE)
    stored = item(aws, doomed)
    assert stored is not None
    assert stored["media"]  # nothing was purged, so the retry still has its keys

    store._s3.put_object = real  # type: ignore[method-assign]
    Compliance(
        store, Lookup({doomed: "deleted"}), max_lookups=10, grace=GRACE, now=clock
    ).run(HANDLE)

    assert_purged(aws, doomed)
    assert ids_in(read(aws, key)) == []


def test_other_s3_errors_reading_a_page_propagate(versioned: Aws) -> None:
    store = store_for(versioned)

    def denied(**_: Any) -> Any:
        raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

    store._s3.get_object = denied  # type: ignore[method-assign]

    with pytest.raises(ClientError):
        store._read("raw/1/dt=2026-10-12/x.json.gz")


def test_purge_stops_before_the_item_step_when_time_runs_out(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    run(aws, Lookup({doomed: "deleted"}), clock)
    clock.now += GRACE + clock.step
    remaining = iter([200_000, 1_000])
    timed = job(
        aws, Lookup({doomed: "deleted"}), clock, remaining_ms=lambda: next(remaining)
    )

    timed.run(HANDLE)

    assert timed.purged == 0
    assert item(aws, doomed) is not None
    assert "tombstone" in (item(aws, doomed) or {})


# --- quarantine --------------------------------------------------------------


def test_purge_deletes_quarantined_objects_naming_the_tweet(
    versioned: Aws, clock: Clock
) -> None:
    aws = versioned
    doomed = put(aws, 1)
    base = f"quarantine/{USER}/dt=2026-10-22"
    mine = f"{base}/a.json"
    mine_gz = f"{base}/b.json.gz"
    keep = f"{base}/c.json"
    for key, body in {
        mine: json.dumps({"tweet_id": doomed, "error": "x"}).encode(),
        mine_gz: gzip.compress(json.dumps({"tweet_id": int(doomed)}).encode()),
        keep: json.dumps({"tweet_id": tid(2)}).encode(),
        f"{base}/d.json": b"not json",
        f"{base}/e.json.gz": b"\x1f\x8bbroken",
        f"{base}/f.json": b"[1]",
    }.items():
        land(aws, key, body)
    land(aws, mine, b'{"tweet_id": "%s", "again": 1}' % doomed.encode())

    result = purge_with_pages(aws, clock, tweet=doomed)

    assert result.purged == 1
    assert versions(aws, mine) == [] and versions(aws, mine_gz) == []
    assert versions(aws, keep) and versions(aws, f"{base}/d.json")
    assert versions(aws, f"{base}/f.json")


def test_quarantine_prefix_may_not_exist(aws: Aws, clock: Clock) -> None:
    doomed = put(aws, 1)

    result = purge_with_pages(aws, clock, tweet=doomed)

    assert (result.purged, result.raw_pages_redacted) == (1, 0)


# --- no resurrection ---------------------------------------------------------


def sync_media_store(aws: Aws, cdn: CdnStub) -> MediaStore:
    return MediaStore(aws.s3, aws.bucket, cdn.client())


def message_for(tweet_id: str) -> str:
    media = photo(f"3_{tweet_id}", PHOTO)
    return make_message(
        tweet_id, media=(media,), tweet={"media_keys": [media.media_key]}
    ).to_json()


def test_a_redelivered_message_after_the_purge_is_refused_without_downloads(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    doomed = put(aws, 1, with_media=True)
    purge_with_pages(aws, clock, tweet=doomed)
    cdn = CdnStub()
    cdn.serve(PHOTO, b"jpeg", "image/jpeg")
    caplog.set_level(logging.INFO)

    failed = process_records(
        [sqs_record("m1", message_for(doomed))],
        media_store=sync_media_store(aws, cdn),
        tweet_store=TweetStore(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []
    assert cdn.requests == []  # no download, no HEAD
    assert_purged(aws, doomed)
    keys = [
        o["Key"] for o in aws.s3.list_objects_v2(Bucket=aws.bucket).get("Contents", [])
    ]
    assert keys == []
    assert "is tombstoned" in caplog.text


def test_a_tombstone_inside_the_grace_period_also_refuses_writes(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    doomed = put(aws, 1)
    run(aws, Lookup({doomed: "deleted"}), clock)
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    caplog.set_level(logging.INFO)

    result = store.put_tweet(make_message(doomed), [])

    assert result.tombstoned and not result.written
    assert (item(aws, doomed) or {})["deletion_reason"] == "deleted"
    assert store.is_tombstoned(doomed)
    assert not store.is_tombstoned(tid(404))
    live = put(aws, 2)
    assert not store.is_tombstoned(live)


def test_a_write_racing_the_tombstone_is_refused_and_logged(
    aws: Aws, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    """The pre-check passed, then the Tweet was tombstoned before the write."""
    doomed = put(aws, 1)
    run(aws, Lookup({doomed: "deleted"}), clock)

    class Racing(TweetStore):
        def is_tombstoned(self, tweet_id: str) -> bool:
            return False

    caplog.set_level(logging.INFO)
    failed = process_records(
        [sqs_record("m1", make_message(doomed).to_json())],
        media_store=MediaStore(aws.s3, aws.bucket, CdnStub().client()),
        tweet_store=Racing(aws.tweets_table, aws.s3, aws.bucket),
    )

    assert failed == []
    assert "write refused for tombstoned tweet" in caplog.text
    assert (item(aws, doomed) or {})["deletion_reason"] == "deleted"


def test_a_reappearing_tweet_stays_blocked_until_an_operator_lifts_the_tombstone(
    aws: Aws, clock: Clock
) -> None:
    doomed = put(aws, 1)
    purge_with_pages(aws, clock, tweet=doomed)
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    # X reports it present again: the purged tombstone is past grace, so it stays.
    result = run(aws, Lookup(), clock)
    assert result.cleared == 0
    assert store.put_tweet(make_message(doomed), []).tombstoned

    aws.tweets_table.delete_item(Key={"tweet_id": doomed})  # the operator's step
    assert store.put_tweet(make_message(doomed), []).written


def test_raw_page_store_pages_round_trip_through_redaction(aws: Aws) -> None:
    """A page landed by the fetch path is redacted and still replays."""
    page = RawPage(
        endpoint=f"/users/{USER}/tweets",
        params={},
        body=response(api_tweet("1"), api_tweet("2")),
    )
    key = RawPageStore(aws.s3, aws.bucket, run_id="r").put(
        username=HANDLE, user_id=USER, fetched_at=FETCHED, page_no=1, page=page
    )
    body = aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()

    redacted, gone = redact_envelope(body, ["2"])

    assert gone == {"2"}
    assert ids_in(Envelope.from_bytes(redacted)) == ["1"]


def test_a_missing_quarantine_object_is_skipped(aws: Aws) -> None:
    assert (
        store_for(aws)._quarantined(
            "quarantine/1/dt=2026-10-12/x.json",
            {"1"},
            True,
            set(),
            set,
            lambda key, pending: None,
        )
        == 0
    )


def test_an_unreadable_item_is_reported_and_the_rest_still_purged(
    aws: Aws, clock: Clock
) -> None:
    bad, good = put(aws, 1), put(aws, 2)
    lookup = Lookup({bad: "deleted", good: "deleted"})
    run(aws, lookup, clock)
    clock.now += GRACE + clock.step
    store = store_for(aws)
    real = store.load_item

    def flaky(tweet_id: str) -> Any:
        if tweet_id == bad:
            raise ClientError({"Error": {"Code": "InternalServerError"}}, "GetItem")
        return real(tweet_id)

    store.load_item = flaky  # type: ignore[method-assign]
    failing = Compliance(store, lookup, max_lookups=10, grace=GRACE, now=clock)

    with pytest.raises(ComplianceError, match=f"purge {bad}: ClientError"):
        failing.run(HANDLE)

    assert_purged(aws, good)
    assert "tombstone" in (item(aws, bad) or {})
