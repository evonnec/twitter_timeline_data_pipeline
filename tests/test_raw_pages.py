"""Raw API pages: landing them on fetch, reading them back, and replaying them."""

import gzip
import json
from typing import Any

import pytest
from botocore.exceptions import ClientError
from conftest import BUCKET, Aws
from helpers import FIXTURES, USER_ID, FakeSqs, XApiStub, synthetic_timeline, x_client

from twitter_timeline_data_pipeline.config import ConfigError
from twitter_timeline_data_pipeline.handlers import fetch
from twitter_timeline_data_pipeline.handlers.fetch import run_fetch, send_messages
from twitter_timeline_data_pipeline.models import TweetMessage
from twitter_timeline_data_pipeline.raw import (
    Envelope,
    RawPageError,
    RawPageStore,
    iter_raw_objects,
    parse_s3_uri,
    raw_key,
)
from twitter_timeline_data_pipeline.replay import run_replay
from twitter_timeline_data_pipeline.storage import CheckpointStore, Gap, TweetStore

USERNAME = "example_user"
STAMP = "2026-10-25T08:30:00Z"
RUN_ID = "run0001"
KEY1 = f"raw/{USER_ID}/dt=2026-10-25/{RUN_ID}-0001.json.gz"
KEY2 = f"raw/{USER_ID}/dt=2026-10-25/{RUN_ID}-0002.json.gz"


class OrderedSqs(FakeSqs):
    """A ``FakeSqs`` that fails the test when a message is sent before the raw page
    it names is in S3."""

    def __init__(self, aws: Aws) -> None:
        super().__init__()
        self._aws = aws
        self.raw_keys: list[str] = []

    def send_message_batch(
        self, *, QueueUrl: str, Entries: list[dict[str, str]]
    ) -> dict[str, Any]:
        for entry in Entries:
            key = TweetMessage.from_json(entry["MessageBody"]).raw_key
            assert key is not None
            self._aws.s3.head_object(Bucket=BUCKET, Key=key)  # raises if absent
            self.raw_keys.append(key)
        return super().send_message_batch(QueueUrl=QueueUrl, Entries=Entries)


def store(aws: Aws) -> RawPageStore:
    return RawPageStore(aws.s3, BUCKET, run_id=RUN_ID)


def run(
    aws: Aws,
    api: Any,
    sqs: FakeSqs,
    raw_pages: RawPageStore | None,
    *,
    max_pages: int = 5,
    **kwargs: Any,
) -> Any:
    with x_client(api) as client:
        return run_fetch(
            username=USERNAME,
            x_client=client,
            sqs=sqs.client,
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            max_results=100,
            max_pages=max_pages,
            raw_pages=raw_pages,
            sleep=lambda _s: None,
            **kwargs,
        )


def keys(aws: Aws, prefix: str = "raw/") -> list[str]:
    listing = aws.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [o["Key"] for o in listing.get("Contents", [])]


def envelope_at(aws: Aws, key: str) -> Envelope:
    return Envelope.from_bytes(aws.s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def bodies(sqs: FakeSqs) -> list[str]:
    return [e["MessageBody"] for call in sqs.calls for e in call]


# ---- landing ---------------------------------------------------------------------


def test_every_page_is_in_s3_before_its_messages_are_enqueued(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fetch, "utc_now_iso", lambda: STAMP)
    sqs = OrderedSqs(aws)

    result = run(aws, XApiStub(), sqs, store(aws))

    assert result.pages == 2
    assert keys(aws) == [KEY1, KEY2]
    assert len(sqs.raw_keys) == 7  # OrderedSqs head-checked each message's object
    assert set(sqs.raw_keys) == {KEY1, KEY2}


def test_object_is_gzipped_json_with_the_envelope(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fetch, "utc_now_iso", lambda: STAMP)
    api = XApiStub()
    run(aws, api, FakeSqs(), store(aws))

    head = aws.s3.head_object(Bucket=BUCKET, Key=KEY2)
    stored = json.loads(
        gzip.decompress(aws.s3.get_object(Bucket=BUCKET, Key=KEY2)["Body"].read())
    )

    assert head["ContentType"] == "application/json"
    assert head["ContentEncoding"] == "gzip"
    second = api.timeline_requests[1]
    assert stored == {
        "schema_version": 1,
        "username": USERNAME,
        "user_id": USER_ID,
        "fetched_at": STAMP,
        "request": {
            "endpoint": f"/users/{USER_ID}/tweets",
            "params": {**dict(second.url.params), "max_results": 100},
        },
        "response": json.loads((FIXTURES / "timeline_page2.json").read_text()),
    }
    assert "pagination_token" in stored["request"]["params"]
    first = envelope_at(aws, KEY1)
    assert "pagination_token" not in first.request["params"]
    # the credential is a header, never part of what is landed
    assert "test-bearer-token-not-real" not in json.dumps(stored)


def test_empty_page_that_points_onward_is_landed_too(aws: Aws) -> None:
    run(
        aws,
        synthetic_timeline([], next_token="more"),
        FakeSqs(),
        store(aws),
        max_pages=1,
    )

    (key,) = keys(aws)
    assert envelope_at(aws, key).response["meta"]["next_token"] == "more"


def test_clean_empty_response_lands_nothing(aws: Aws) -> None:
    run(aws, synthetic_timeline([]), FakeSqs(), store(aws))

    assert keys(aws) == []


def test_upload_failure_enqueues_nothing_and_leaves_the_cursor(aws: Aws) -> None:
    sqs = FakeSqs()

    with pytest.raises(ClientError):
        run(aws, XApiStub(), sqs, RawPageStore(aws.s3, "no-such-bucket"))

    assert sqs.calls == []
    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id is None


def test_upload_failure_on_a_later_page_keeps_the_rest_of_the_range(aws: Aws) -> None:
    """Page 1 lands and is enqueued; page 2's upload fails: the gap below page 1 stays
    in the state, so the next run fetches it."""
    sqs = FakeSqs()
    real = store(aws)
    calls = 0

    class FlakyStore(RawPageStore):
        def put(self, **kwargs: Any) -> str:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("S3 down")
            return real.put(**kwargs)

    with pytest.raises(RuntimeError, match="S3 down"):
        run(aws, XApiStub(), sqs, FlakyStore(aws.s3, BUCKET))

    state = CheckpointStore(aws.state_table).load(USERNAME)
    assert len(sqs.calls) == 1  # page 1 only
    assert len(state.gaps) == 1
    assert state.gaps[0].until_id is not None


def test_without_a_store_nothing_is_landed_and_messages_have_no_raw_key(
    aws: Aws,
) -> None:
    sqs = FakeSqs()

    run(aws, XApiStub(), sqs, None)

    assert keys(aws) == []
    assert all("raw_key" not in json.loads(b) for b in bodies(sqs))


def test_page_numbers_continue_across_the_walks_of_one_run(aws: Aws) -> None:
    """The new-Tweets phase and a gap phase share the run's page counter, so keys
    never collide."""
    run(
        aws,
        XApiStub(),
        FakeSqs(),
        store(aws),
        backfill=Gap(until_id="1700000000000000003"),
    )

    found = keys(aws)
    assert len(found) == len(set(found)) >= 2


# ---- keys and envelope --------------------------------------------------------


def test_key_layout() -> None:
    assert raw_key("42", STAMP, "abc", 7) == "raw/42/dt=2026-10-25/abc-0007.json.gz"


def test_gzip_is_deterministic() -> None:
    envelope = Envelope(
        username="u", user_id="1", fetched_at=STAMP, request={}, response={"a": 1}
    )

    assert envelope.to_bytes() == envelope.to_bytes()


def test_lone_surrogate_in_the_body_survives_the_round_trip() -> None:
    body = json.loads('{"data": [{"text": "bad \\ud800 text"}]}')
    envelope = Envelope(
        username="u", user_id="1", fetched_at=STAMP, request={}, response=body
    )

    assert Envelope.from_bytes(envelope.to_bytes()).response == body


def test_default_run_id_is_unique(aws: Aws) -> None:
    a = RawPageStore(aws.s3, BUCKET).run_id
    b = RawPageStore(aws.s3, BUCKET).run_id

    assert a != b


@pytest.mark.parametrize(
    "data",
    [
        b"not gzip",
        gzip.compress(b"not json"),
        gzip.compress(b"[1]"),
        gzip.compress(b'{"schema_version": 2}'),
        gzip.compress(b'{"schema_version": 1}'),
        gzip.compress(
            b'{"schema_version":1,"username":"u","user_id":"1","fetched_at":"x",'
            b'"request":{},"response":[]}'
        ),
    ],
)
def test_malformed_envelopes_are_rejected(data: bytes) -> None:
    with pytest.raises(RawPageError):
        Envelope.from_bytes(data)


@pytest.mark.parametrize(
    "uri",
    ["", "http://b/raw/x", "s3://b/media/x", "s3:///raw/x", "s3://b", "s3://b/raw"],
)
def test_bad_replay_uris(uri: str) -> None:
    with pytest.raises(ConfigError, match="expected s3://"):
        parse_s3_uri(uri)


def test_good_replay_uri() -> None:
    assert parse_s3_uri("s3://b/raw/1/dt=2026-10-25/") == ("b", "raw/1/dt=2026-10-25/")


def test_listing_is_paginated(aws: Aws) -> None:
    for i in range(5):
        aws.s3.put_object(Bucket=BUCKET, Key=f"raw/1/{i}", Body=b"x")
    aws.s3.put_object(Bucket=BUCKET, Key="media/other", Body=b"x")

    found = [k for k, _ in iter_raw_objects(aws.s3, BUCKET, "raw/1/", page_size=2)]

    assert found == [f"raw/1/{i}" for i in range(5)]


# ---- lineage ---------------------------------------------------------------------


def test_messages_carry_raw_key_and_items_store_it(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fetch, "utc_now_iso", lambda: STAMP)
    sqs = FakeSqs()
    run(aws, XApiStub(), sqs, store(aws))

    messages = [TweetMessage.from_json(b) for b in bodies(sqs)]

    assert [m.raw_key for m in messages] == [KEY1] * 5 + [KEY2] * 2
    assert TweetStore.to_item(messages[0], [])["raw_key"] == KEY1


def test_item_without_raw_key_has_no_attribute() -> None:
    message = TweetMessage.from_json((FIXTURES / "message_v1.json").read_text())

    assert message.raw_key is None
    assert "raw_key" not in TweetStore.to_item(message, [])


# ---- replay ----------------------------------------------------------------------


def test_replay_rebuilds_identical_messages_without_calling_the_api(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fetch, "utc_now_iso", lambda: STAMP)
    fetched = FakeSqs()
    api = XApiStub()
    run(aws, api, fetched, store(aws))
    requests_after_fetch = len(api.requests)
    replayed = FakeSqs()

    result = run_replay(
        s3=aws.s3,
        bucket=BUCKET,
        prefix=f"raw/{USER_ID}/",
        send=lambda ms: send_messages(
            replayed.client, aws.queue_url, ms, lambda _s: None
        ),
    )

    assert len(api.requests) == requests_after_fetch  # replay has no HTTP client
    assert (result.objects, result.tweets, result.enqueued) == (2, 7, 7)
    assert bodies(replayed) == bodies(fetched)  # identical, fetched_at included


def test_replay_leaves_the_cursor_alone(aws: Aws) -> None:
    run(aws, XApiStub(), FakeSqs(), store(aws))
    before = CheckpointStore(aws.state_table).load(USERNAME)

    run_replay(s3=aws.s3, bucket=BUCKET, prefix="raw/", send=len)

    assert CheckpointStore(aws.state_table).load(USERNAME) == before


def test_replay_of_a_prefix_selects_only_matching_objects(aws: Aws) -> None:
    run(aws, XApiStub(), FakeSqs(), store(aws))

    result = run_replay(
        s3=aws.s3,
        bucket=BUCKET,
        prefix=f"raw/{USER_ID}/dt=1999-01-01/",
        send=len,
    )

    assert result.objects == 0


def test_replay_names_the_object_it_cannot_read(aws: Aws) -> None:
    aws.s3.put_object(Bucket=BUCKET, Key="raw/1/bad.json.gz", Body=b"junk")

    with pytest.raises(RawPageError, match=r"raw/1/bad\.json\.gz"):
        run_replay(s3=aws.s3, bucket=BUCKET, prefix="raw/", send=len)


def test_replay_of_a_response_that_cannot_be_parsed_names_the_key(aws: Aws) -> None:
    envelope = Envelope(
        username="u",
        user_id="1",
        fetched_at=STAMP,
        request={},
        response={"data": "oops"},
    )
    aws.s3.put_object(Bucket=BUCKET, Key="raw/1/x.json.gz", Body=envelope.to_bytes())

    with pytest.raises(RawPageError, match=r"raw/1/x\.json\.gz"):
        run_replay(s3=aws.s3, bucket=BUCKET, prefix="raw/", send=len)


# ---- hardening ---------------------------------------------------------------------


def test_a_raw_page_is_never_overwritten(aws: Aws) -> None:
    pages = store(aws)
    api = XApiStub()
    run(aws, api, FakeSqs(), pages)
    original = aws.s3.get_object(Bucket=BUCKET, Key=keys(aws)[0])["Body"].read()
    page = envelope_at(aws, keys(aws)[0])
    from twitter_timeline_data_pipeline.models import RawPage

    # the same key again (a retry of the same page) is treated as already landed
    pages.put(
        username=USERNAME,
        user_id=USER_ID,
        fetched_at="2026-10-26T00:00:00Z",
        page_no=1,
        page=RawPage(endpoint="/x", params={}, body={"other": 1}),
    )
    assert page.response  # sanity
    versions = aws.s3.list_object_versions(Bucket=BUCKET, Prefix="raw/")["Versions"]
    assert len(versions) == 2  # nothing added for the existing key
    assert aws.s3.get_object(Bucket=BUCKET, Key=keys(aws)[0])["Body"].read() == original


def test_other_upload_errors_propagate(aws: Aws) -> None:
    from twitter_timeline_data_pipeline.models import RawPage

    with pytest.raises(ClientError, match="NoSuchBucket"):
        RawPageStore(aws.s3, "nope-bucket").put(
            username="u",
            user_id="1",
            fetched_at=STAMP,
            page_no=1,
            page=RawPage(endpoint="/x", params={}, body={}),
        )


def test_upload_failure_in_the_gap_phase_enqueues_nothing_for_that_page(
    aws: Aws,
) -> None:
    sqs = FakeSqs()

    with pytest.raises(ClientError):
        run(
            aws,
            XApiStub(),
            sqs,
            RawPageStore(aws.s3, "nope-bucket"),
            since_id="1700000000000000005",  # nothing new: only the gap phase fetches
            backfill=Gap(until_id="1700000000000000005"),
        )

    assert sqs.calls == []
    state = CheckpointStore(aws.state_table).load(USERNAME)
    assert state.gaps  # the range is still owed


def test_page_the_parser_rejects_is_landed_before_the_run_fails(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    from twitter_timeline_data_pipeline import x_api

    def broken(body: Any, *, raw: Any = None) -> Any:
        raise ValueError("cannot parse")

    monkeypatch.setattr(x_api, "parse_timeline_page", broken)
    sqs = FakeSqs()

    with pytest.raises(x_api.PageParseError, match="cannot parse"):
        run(aws, XApiStub(), sqs, store(aws))

    (key,) = keys(aws)
    assert envelope_at(aws, key).response["data"]
    assert sqs.calls == []
    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id is None


def test_unparseable_page_without_a_store_just_fails(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    from twitter_timeline_data_pipeline import x_api

    def broken(body: Any, *, raw: Any = None) -> Any:
        raise ValueError("cannot parse")

    monkeypatch.setattr(x_api, "parse_timeline_page", broken)

    with pytest.raises(x_api.PageParseError):
        run(aws, XApiStub(), FakeSqs(), None)

    assert keys(aws) == []


def test_enqueue_failure_then_refetch_leaves_duplicate_pages_and_replay_is_newest(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raw landed, enqueue fails, the next run fetches the page again: two raw objects
    hold the same Tweets. Replaying both ends with the newest fetch's data."""
    stamps = iter(["2026-10-25T08:00:00Z", "2026-10-25T09:00:00Z"] * 4)
    monkeypatch.setattr(fetch, "utc_now_iso", lambda: next(stamps))
    failing = FakeSqs(lambda _call, entry_ids: set(entry_ids))
    with pytest.raises(fetch.EnqueueError):
        run(aws, XApiStub(), failing, RawPageStore(aws.s3, BUCKET, run_id="a"))
    good = FakeSqs()
    run(aws, XApiStub(), good, RawPageStore(aws.s3, BUCKET, run_id="b"))
    assert len(keys(aws)) == 3  # page 1 of run a, and both pages of run b

    replayed = FakeSqs()
    run_replay(
        s3=aws.s3,
        bucket=BUCKET,
        prefix="raw/",
        send=lambda ms: send_messages(
            replayed.client, aws.queue_url, ms, lambda _s: None
        ),
    )
    by_tweet: dict[str, str] = {}
    for body in bodies(replayed):  # key order: run a, then run b
        message = TweetMessage.from_json(body)
        by_tweet[message.tweet.id] = max(
            by_tweet.get(message.tweet.id, ""), message.fetched_at
        )
    newest = {TweetMessage.from_json(b).fetched_at for b in bodies(good)}
    assert len(by_tweet) == 7
    # every replayed Tweet's newest copy is as new as the fetch that succeeded
    assert set(by_tweet.values()) <= {*newest, "2026-10-25T08:00:00Z"}
    assert max(by_tweet.values()) == max(newest)
    # the duplicate copies carry different fetched_at, so the conditional write in the
    # processor keeps the newest whichever order they arrive in


def test_redactions_round_trip_and_are_omitted_when_empty() -> None:
    plain = Envelope(
        username="u", user_id="1", fetched_at=STAMP, request={}, response={}
    )
    redacted = Envelope(
        username="u",
        user_id="1",
        fetched_at=STAMP,
        request={},
        response={},
        redactions=("5",),
    )

    assert "redactions" not in plain.to_dict()
    assert Envelope.from_bytes(redacted.to_bytes()).redactions == ("5",)


@pytest.mark.parametrize("bad", ["5", [5], {"a": 1}])
def test_malformed_redactions_are_rejected(bad: Any) -> None:
    data = Envelope(
        username="u", user_id="1", fetched_at=STAMP, request={}, response={}
    ).to_dict()
    data["redactions"] = bad

    with pytest.raises(RawPageError):
        Envelope.from_bytes(gzip.compress(json.dumps(data).encode()))


def test_replay_skips_redacted_tweets_and_their_media(aws: Aws) -> None:
    body = json.loads((FIXTURES / "timeline_page1.json").read_text())
    with_media = next(
        t for t in body["data"] if t.get("attachments", {}).get("media_keys")
    )
    other = next(t for t in body["data"] if t["id"] != with_media["id"])
    envelope = Envelope(
        username=USERNAME,
        user_id=USER_ID,
        fetched_at=STAMP,
        request={},
        response=body,
        redactions=(with_media["id"], "999"),
    )
    aws.s3.put_object(Bucket=BUCKET, Key="raw/1/x.json.gz", Body=envelope.to_bytes())
    seen: list[list[TweetMessage]] = []

    def collect(messages: list[TweetMessage]) -> int:
        seen.append(messages)
        return 0

    run_replay(s3=aws.s3, bucket=BUCKET, prefix="raw/", send=collect)

    ids = {m.tweet.id for m in seen[0]}
    assert with_media["id"] not in ids
    assert other["id"] in ids
    assert len(ids) == len(body["data"]) - 1
    dropped_keys = set(with_media["attachments"]["media_keys"])
    assert not dropped_keys & {m.media_key for msg in seen[0] for m in msg.media}


def test_folder_markers_are_skipped_and_reported(aws: Aws) -> None:
    aws.s3.put_object(Bucket=BUCKET, Key="raw/1/", Body=b"")
    skipped: list[str] = []

    found = list(iter_raw_objects(aws.s3, BUCKET, "raw/", skipped=skipped))

    assert found == []
    assert skipped == ["raw/1/"]
    assert list(iter_raw_objects(aws.s3, BUCKET, "raw/")) == []  # no list needed


def test_start_after_resumes_the_listing(aws: Aws) -> None:
    for i in range(4):
        aws.s3.put_object(Bucket=BUCKET, Key=f"raw/1/{i}", Body=b"x")

    found = [
        k
        for k, _ in iter_raw_objects(
            aws.s3, BUCKET, "raw/1/", start_after="raw/1/1", page_size=1
        )
    ]

    assert found == ["raw/1/2", "raw/1/3"]


def test_replay_uri_gets_a_trailing_slash() -> None:
    assert parse_s3_uri("s3://b/raw/1") == ("b", "raw/1/")
