"""Malformed API data: a bad Tweet or media object or an unsendable message is
quarantined, never dropped, and never blocks the rest of the page or the cursor."""

import json
import logging
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from botocore.exceptions import ClientError
from conftest import BUCKET, Aws
from helpers import USER_ID, USERNAME, FakeSqs, XApiStub, install_x_api, x_client

from twitter_timeline_data_pipeline import cli, models
from twitter_timeline_data_pipeline.cli import EXIT_OK, main
from twitter_timeline_data_pipeline.handlers.fetch import (
    EnqueueError,
    run_fetch,
    send_messages,
)
from twitter_timeline_data_pipeline.models import TweetMessage
from twitter_timeline_data_pipeline.quarantine import (
    QUARANTINED_MESSAGE,
    QuarantineStore,
    QuarantineUnavailableError,
    quarantine_key,
)
from twitter_timeline_data_pipeline.raw import Envelope, RawPageStore
from twitter_timeline_data_pipeline.replay import run_replay
from twitter_timeline_data_pipeline.storage import CheckpointStore
from twitter_timeline_data_pipeline.x_api import (
    PARTIAL_ERRORS_MESSAGE,
    UNPARSEABLE_MESSAGE,
    AccessError,
    NotFoundError,
    XApiError,
    parse_timeline_page,
)

RUN_ID = "run0001"
STAMP = "2026-10-25T08:30:00Z"
GOOD1, BAD, GOOD2 = (f"17000000000000000{n:02d}" for n in (3, 2, 1))
PHOTO = "3_1700000000000000101"
PHOTO_URL = "https://pbs.twimg.com/media/FakePhotoId01.jpg"


def tweet(tweet_id: str, **extra: Any) -> dict[str, Any]:
    return {"id": tweet_id, "text": f"tweet {tweet_id}", "author_id": USER_ID, **extra}


def bad_tweet(tweet_id: str = BAD) -> dict[str, Any]:
    return tweet(tweet_id, public_metrics={"like_count": None})


def body_of(
    data: list[Any], media: list[Any] | None = None, **meta: Any
) -> dict[str, Any]:
    body: dict[str, Any] = {"data": data, "meta": {"result_count": len(data), **meta}}
    if media is not None:
        body["includes"] = {"media": media}
    return body


def serving(body: dict[str, Any]) -> Callable[[httpx.Request], httpx.Response]:
    stub = XApiStub()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != f"/2/users/{USER_ID}/tweets":
            return stub(request)
        return httpx.Response(200, json=body)

    return handler


def run(
    aws: Aws,
    body: dict[str, Any],
    sqs: FakeSqs,
    *,
    quarantine: QuarantineStore | str | None = "default",
    **kwargs: Any,
) -> Any:
    if quarantine == "default":
        quarantine = QuarantineStore(aws.s3, BUCKET, run_id=RUN_ID)
    with x_client(serving(body)) as client:
        return run_fetch(
            username=USERNAME,
            x_client=client,
            sqs=sqs.client,
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            max_results=100,
            max_pages=5,
            raw_pages=RawPageStore(aws.s3, BUCKET, run_id=RUN_ID),
            quarantine=quarantine,  # type: ignore[arg-type]
            sleep=lambda _s: None,
            **kwargs,
        )


def sent_ids(sqs: FakeSqs) -> list[str]:
    return [json.loads(e["MessageBody"])["tweet"]["id"] for c in sqs.calls for e in c]


def quarantined(aws: Aws) -> dict[str, dict[str, Any]]:
    listing = aws.s3.list_objects_v2(Bucket=BUCKET, Prefix="quarantine/")
    found = {}
    for o in listing.get("Contents", []):
        got = aws.s3.get_object(Bucket=BUCKET, Key=o["Key"])
        assert got["ContentType"] == "application/json"
        found[o["Key"]] = json.loads(got["Body"].read())
    return found


# ---- a Tweet that fails to parse -------------------------------------------------


def test_one_bad_tweet_is_quarantined_and_the_rest_enqueued(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    sqs = FakeSqs()

    with caplog.at_level(logging.WARNING):
        result = run(aws, body_of([tweet(GOOD1), bad_tweet(), tweet(GOOD2)]), sqs)

    assert sent_ids(sqs) == [GOOD1, GOOD2]
    assert result.tweets == result.enqueued == 2
    ((key, stored),) = quarantined(aws).items()
    assert (
        key
        == f"quarantine/{USER_ID}/dt={key.split('dt=')[1][:10]}/{RUN_ID}-0001-000.json"
    )
    raw_key = f"raw/{USER_ID}/dt={key.split('dt=')[1][:10]}/{RUN_ID}-0001.json.gz"
    assert stored == {
        "kind": "tweet",
        "raw_key": raw_key,
        "error": stored["error"],
        "item": bad_tweet(),
    }
    assert "TypeError" in stored["error"]
    assert raw_key in [
        o["Key"] for o in aws.s3.list_objects_v2(Bucket=BUCKET)["Contents"]
    ]
    # stable, greppable log lines
    assert any(r.msg == UNPARSEABLE_MESSAGE for r in caplog.records)
    assert any(r.msg == QUARANTINED_MESSAGE for r in caplog.records)


def test_the_cursor_moves_past_a_quarantined_tweet(aws: Aws) -> None:
    run(aws, body_of([bad_tweet(GOOD1), tweet(GOOD2)]), FakeSqs())

    state = CheckpointStore(aws.state_table).load(USERNAME)
    assert state.newest_id == GOOD1  # the bad (newest) Tweet counts as seen
    assert state.gaps == ()


def test_a_page_of_only_bad_tweets_still_advances(aws: Aws) -> None:
    sqs = FakeSqs()

    result = run(aws, body_of([bad_tweet(GOOD1), {"no": "id"}]), sqs)

    assert sqs.calls == []
    assert result.enqueued == 0
    assert len(quarantined(aws)) == 2
    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id == GOOD1
    items = sorted(q["item"].get("id", "-") for q in quarantined(aws).values())
    assert items == ["-", GOOD1]


def test_quarantine_write_failure_fails_the_page_with_nothing_enqueued(
    aws: Aws,
) -> None:
    sqs = FakeSqs()
    broken = QuarantineStore(aws.s3, "no-such-bucket", run_id=RUN_ID)

    with pytest.raises(ClientError):
        run(aws, body_of([tweet(GOOD1), bad_tweet()]), sqs, quarantine=broken)

    assert sqs.calls == []
    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id is None


def test_without_a_quarantine_store_the_page_fails_rather_than_drop(aws: Aws) -> None:
    sqs = FakeSqs()

    with pytest.raises(QuarantineUnavailableError, match="tweet needs quarantining"):
        run(aws, body_of([tweet(GOOD1), bad_tweet()]), sqs, quarantine=None)

    assert sqs.calls == []
    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id is None


def test_a_clean_page_writes_nothing_to_quarantine(aws: Aws) -> None:
    run(aws, body_of([tweet(GOOD1)]), FakeSqs())

    assert quarantined(aws) == {}


# ---- a media object that fails to parse ------------------------------------------


def test_bad_media_is_quarantined_and_the_tweet_kept_without_it(aws: Aws) -> None:
    good = {"media_key": "3_2", "type": "photo", "url": PHOTO_URL}
    bad_media = {"type": "photo", "url": PHOTO_URL}  # no media_key
    sqs = FakeSqs()
    attached = {"attachments": {"media_keys": [PHOTO, "3_2"]}}

    run(
        aws,
        body_of([tweet(GOOD1, **attached)], media=[bad_media, "junk", good]),
        sqs,
    )

    (message,) = [json.loads(e["MessageBody"]) for c in sqs.calls for e in c]
    assert message["tweet"]["media_keys"] == [PHOTO, "3_2"]
    assert [m["media_key"] for m in message["media"]] == ["3_2"]
    stored = [q for _, q in sorted(quarantined(aws).items())]
    assert [(q["kind"], q["item"]) for q in stored] == [
        ("media", bad_media),
        ("media", "junk"),
    ]


# ---- replay -----------------------------------------------------------------------


def land(aws: Aws, body: dict[str, Any], key: str = "raw/1/a.json.gz") -> None:
    envelope = Envelope(
        username="u", user_id="1", fetched_at=STAMP, request={}, response=body
    )
    aws.s3.put_object(Bucket=BUCKET, Key=key, Body=envelope.to_bytes())


def test_replay_quarantines_a_bad_tweet_and_enqueues_the_rest(aws: Aws) -> None:
    land(aws, body_of([tweet(GOOD1), bad_tweet(), tweet(GOOD2)]))
    sent: list[list[TweetMessage]] = []

    def collect(ms: list[TweetMessage]) -> int:
        sent.append(ms)
        return len(ms)

    result = run_replay(
        s3=aws.s3,
        bucket=BUCKET,
        prefix="raw/",
        send=collect,
        quarantine=QuarantineStore(aws.s3, BUCKET, run_id="replay1"),
    )

    assert (result.objects, result.tweets, result.enqueued) == (1, 2, 2)
    assert [m.tweet.id for m in sent[0]] == [GOOD1, GOOD2]
    ((key, stored),) = quarantined(aws).items()
    assert key == "quarantine/1/dt=2026-10-25/replay1-0001-000.json"
    assert stored["raw_key"] == "raw/1/a.json.gz"
    assert stored["item"] == bad_tweet()


def test_replay_without_a_store_only_logs(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    land(aws, body_of([tweet(GOOD1), bad_tweet()]))

    with caplog.at_level(logging.ERROR):
        result = run_replay(s3=aws.s3, bucket=BUCKET, prefix="raw/", send=len)

    assert result.tweets == 1
    assert quarantined(aws) == {}
    assert any(r.msg == UNPARSEABLE_MESSAGE for r in caplog.records)


def test_replay_after_a_parser_fix_recovers_the_quarantined_tweet(
    aws: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The documented recovery: fix the parser, then replay the raw prefix."""
    land(aws, body_of([tweet(GOOD1), bad_tweet()]))
    ids: list[str] = []

    def collect(ms: list[TweetMessage]) -> int:
        ids.extend(m.tweet.id for m in ms)
        return len(ms)

    run_replay(s3=aws.s3, bucket=BUCKET, prefix="raw/", send=collect)
    assert ids == [GOOD1]

    original = models.Tweet.from_api  # the parser fix: ignore null metrics
    monkeypatch.setattr(
        models.Tweet,
        "from_api",
        classmethod(lambda _cls, d: original({**d, "public_metrics": {}})),
    )
    ids.clear()
    run_replay(s3=aws.s3, bucket=BUCKET, prefix="raw/", send=collect)
    assert ids == [GOOD1, BAD]


# ---- errors-only responses --------------------------------------------------------

PROBLEMS = "https://api.twitter.com/2/problems/"


@pytest.mark.parametrize(
    ("problem_type", "expected"),
    [
        (PROBLEMS + "not-authorized-for-resource", AccessError),
        (PROBLEMS + "resource-not-found", NotFoundError),
        (PROBLEMS + "something-new", XApiError),
        (None, XApiError),
    ],
)
def test_errors_only_timeline_maps_the_problem_type(
    problem_type: str | None, expected: type[XApiError]
) -> None:
    error: dict[str, Any] = {"title": "Authorization Error", "detail": "protected"}
    if problem_type:
        error["type"] = problem_type
    handler = serving({"errors": [error], "meta": {"result_count": 0}})

    with x_client(handler) as client, pytest.raises(XApiError) as raised:
        list(client.iter_timeline(USER_ID))

    assert type(raised.value) is expected
    assert raised.value.status_code == 200
    assert "Authorization Error: protected" in str(raised.value)


def test_a_known_type_later_in_the_errors_still_picks_the_class() -> None:
    errors = [
        "junk",
        {"type": 5},
        {"type": PROBLEMS + "unknown"},
        {"type": PROBLEMS + "not-authorized-for-resource/"},
    ]
    handler = serving({"errors": errors})

    with x_client(handler) as client, pytest.raises(AccessError):
        list(client.iter_timeline(USER_ID))


def test_errors_only_response_fails_the_run_and_keeps_the_cursor(aws: Aws) -> None:
    error = {"type": PROBLEMS + "not-authorized-for-resource", "title": "x"}

    with pytest.raises(AccessError):
        run(aws, {"errors": [error]}, FakeSqs())

    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id is None


def test_partial_errors_are_logged_with_a_stable_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    body = body_of([tweet(GOOD1)])
    body["errors"] = [{"title": "Authorization Error", "detail": "no"}, {}]

    with x_client(serving(body)) as client, caplog.at_level(logging.WARNING):
        (page,) = client.iter_timeline(USER_ID)

    assert len(page.tweets) == 1
    (record,) = [r for r in caplog.records if r.msg == PARTIAL_ERRORS_MESSAGE]
    assert record.args == (1, 2, "Authorization Error: no")


def test_parse_timeline_page_reports_quarantined_items() -> None:
    page = parse_timeline_page(body_of([bad_tweet(), tweet(GOOD1)]))

    assert [t.id for t in page.tweets] == [GOOD1]
    (q,) = page.quarantined
    assert (q.kind, q.tweet_id, q.item) == ("tweet", BAD, bad_tweet())


@pytest.mark.parametrize("bad_id", [None, 5, "abc", "١٢٣"])
def test_a_quarantined_tweet_without_a_usable_id_has_no_tweet_id(bad_id: Any) -> None:
    page = parse_timeline_page(body_of([{"id": bad_id}]))

    (q,) = page.quarantined
    assert q.tweet_id is None


# ---- SenderFault ------------------------------------------------------------------


class FaultySqs(FakeSqs):
    """Rejects the entries of ``sender`` with ``SenderFault`` and those of ``other``
    as a server-side failure, on every attempt."""

    def __init__(self, *, sender: set[str], other: set[str] = frozenset()) -> None:  # type: ignore[assignment]
        super().__init__(lambda _call, _ids: set(sender) | set(other))
        self.sender = sender

    def send_message_batch(
        self, *, QueueUrl: str, Entries: list[dict[str, str]]
    ) -> dict[str, Any]:
        response = super().send_message_batch(QueueUrl=QueueUrl, Entries=Entries)
        for failure in response["Failed"]:
            failure["SenderFault"] = failure["Id"] in self.sender
        return response


def entry_ids(sqs: FakeSqs) -> list[list[str]]:
    return [[e["Id"] for e in call] for call in sqs.calls]


def test_a_sender_fault_entry_is_quarantined_not_retried(aws: Aws) -> None:
    sqs = FaultySqs(sender={"1"})

    result = run(aws, body_of([tweet(GOOD1), tweet(BAD), tweet(GOOD2)]), sqs)

    assert entry_ids(sqs) == [["0", "1", "2"]]  # one attempt, no retry
    assert result.enqueued == 2
    assert result.pages == 1
    ((_, stored),) = quarantined(aws).items()
    assert stored["kind"] == "message"
    assert stored["item"]["tweet"]["id"] == BAD
    assert stored["raw_key"].startswith(f"raw/{USER_ID}/")
    assert stored["error"] == "InternalError: boom"
    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id == GOOD1


def test_a_non_sender_fault_is_still_retried_and_raised_after_quarantining(
    aws: Aws,
) -> None:
    sqs = FaultySqs(sender={"1"}, other={"2"})

    with pytest.raises(EnqueueError, match="1 messages still failing"):
        run(aws, body_of([tweet(GOOD1), tweet(BAD), tweet(GOOD2)]), sqs)

    assert entry_ids(sqs) == [["0", "1", "2"], ["2"], ["2"]]
    assert len(quarantined(aws)) == 1
    assert CheckpointStore(aws.state_table).load(USERNAME).newest_id is None


def test_a_sender_fault_without_a_quarantine_raises_at_once(aws: Aws) -> None:
    sqs = FaultySqs(sender={"0"})

    with pytest.raises(EnqueueError, match="unsendable"):
        run(aws, body_of([tweet(GOOD1)]), sqs, quarantine=None)

    assert len(sqs.calls) == 1


def test_send_messages_does_not_count_rejected_ones(aws: Aws) -> None:
    sqs = FaultySqs(sender={"0"})
    rejected: list[tuple[str, str]] = []
    messages = [
        TweetMessage.from_json(
            json.dumps(
                {
                    "schema_version": 1,
                    "username": USERNAME,
                    "fetched_at": STAMP,
                    "tweet": tweet(i),
                    "media": [],
                }
            )
        )
        for i in (GOOD1, GOOD2)
    ]

    sent = send_messages(
        sqs.client,
        aws.queue_url,
        messages,
        lambda _s: None,
        lambda b, e: rejected.append((b, e)),
    )

    assert sent == 1
    assert len(rejected) == 1


# ---- the store and the replay CLI ---------------------------------------------------


def test_key_layout_and_per_page_index(aws: Aws) -> None:
    assert quarantine_key("42", STAMP, "abc", 7, 3) == (
        "quarantine/42/dt=2026-10-25/abc-0007-003.json"
    )
    store = QuarantineStore(aws.s3, BUCKET, run_id="abc")
    args: dict[str, Any] = {
        "kind": "tweet",
        "item": {"a": 1},
        "error": "e",
        "raw_key": None,
        "user_id": "42",
        "fetched_at": STAMP,
    }

    keys = [store.put(page_no=p, **args) for p in (1, 1, 2)]

    assert keys == [
        "quarantine/42/dt=2026-10-25/abc-0001-000.json",
        "quarantine/42/dt=2026-10-25/abc-0001-001.json",
        "quarantine/42/dt=2026-10-25/abc-0002-000.json",
    ]
    assert QuarantineStore(aws.s3, BUCKET).run_id != store.run_id


def test_cli_fetch_quarantines_with_the_bucket(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_x_api(monkeypatch, cli, serving(body_of([tweet(GOOD1), bad_tweet()])))

    assert main(["fetch", "--username", USERNAME]) == EXIT_OK

    assert len(quarantined(pipeline_env)) == 1


def test_cli_fetch_no_raw_without_a_bucket_fails_on_a_bad_tweet(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_x_api(monkeypatch, cli, serving(body_of([bad_tweet()])))
    monkeypatch.delenv("MEDIA_BUCKET")

    assert main(["fetch", "--username", USERNAME, "--no-raw"]) == cli.EXIT_FAILURE

    assert "needs quarantining" in capsys.readouterr().err


def test_cli_replay_quarantines_bad_tweets_and_unsendable_messages(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    land(
        pipeline_env,
        body_of([tweet(GOOD1), bad_tweet(), tweet(GOOD2)]),
        key=f"raw/{USER_ID}/dt=2026-10-25/x-0001.json.gz",
    )
    monkeypatch.setattr(cli, "sqs_client", lambda: FaultySqs(sender={"0"}).client)

    assert main(["replay", "--raw", f"s3://{BUCKET}/raw/"]) == EXIT_OK

    stored = sorted(quarantined(pipeline_env).values(), key=lambda q: q["kind"])
    assert [q["kind"] for q in stored] == ["message", "tweet"]
    assert stored[0]["item"]["tweet"]["id"] == GOOD1
    assert stored[0]["raw_key"] == f"raw/{USER_ID}/dt=2026-10-25/x-0001.json.gz"


def test_cli_replay_of_a_message_without_a_raw_key_files_it_under_unknown(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    land(pipeline_env, body_of([tweet(GOOD1)]))
    monkeypatch.setattr(cli, "sqs_client", lambda: FaultySqs(sender={"0"}).client)
    monkeypatch.setattr(
        TweetMessage, "to_json", lambda self: json.dumps({"tweet": {"id": "1"}})
    )

    assert main(["replay", "--raw", f"s3://{BUCKET}/raw/"]) == EXIT_OK

    (key,) = quarantined(pipeline_env)
    assert key.startswith("quarantine/unknown/dt=/")
