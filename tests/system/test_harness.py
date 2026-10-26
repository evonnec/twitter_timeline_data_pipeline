"""The simulation itself: does it behave like the AWS services and the X API?

These pin the fidelity the scenario tests rely on, so a harness regression shows up
here rather than as a puzzling scenario failure.
"""

import copy
import inspect
import itertools
import json
import time
from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest
from botocore.exceptions import ClientError

from system.clock import SimClock
from system.conftest import MakeSim
from system.fake_x import FakeXApi
from system.harness import TOKEN, USER_ID, Simulation
from system.runtime import (
    AwsCall,
    Invocation,
    LambdaRuntime,
    SqsEventSource,
    all_package_modules,
)
from system.stack import QueueSpec, SqsEventSpec, Template, load_template

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _props(template: Template, logical_id: str) -> dict[str, Any]:
    found: dict[str, Any] = template["Resources"][logical_id]["Properties"]
    return found


def test_resources_have_the_template_settings(sim: Simulation) -> None:
    """Queues, tables, bucket and environments come from template.yaml as written."""
    template = load_template()
    queue = _props(template, "TweetsQueue")
    attributes = sim.sqs.get_queue_attributes(
        QueueUrl=sim.queue.url, AttributeNames=["All"]
    )["Attributes"]
    assert int(attributes["VisibilityTimeout"]) == queue["VisibilityTimeout"]
    redrive = json.loads(attributes["RedrivePolicy"])
    assert redrive["deadLetterTargetArn"] == sim.dlq.arn
    assert redrive["maxReceiveCount"] == sim.queue.max_receive_count
    assert int(attributes["MaximumMessageSize"]) == queue.get(
        "MaximumMessageSize", 1_048_576
    )
    dlq_attributes = sim.sqs.get_queue_attributes(
        QueueUrl=sim.dlq.url, AttributeNames=["MessageRetentionPeriod"]
    )["Attributes"]
    assert (
        int(dlq_attributes["MessageRetentionPeriod"])
        == (_props(template, "TweetsDeadLetterQueue")["MessageRetentionPeriod"])
    )

    table = sim.tweets_table
    expected = _props(template, "TweetsTable")
    assert table.key_schema == expected["KeySchema"]
    (gsi,) = table.global_secondary_indexes or []
    (expected_gsi,) = expected["GlobalSecondaryIndexes"]
    assert gsi["IndexName"] == expected_gsi["IndexName"]
    assert gsi["KeySchema"] == expected_gsi["KeySchema"]
    assert gsi["Projection"] == expected_gsi["Projection"]
    assert sim.state_table.key_schema == _props(template, "StateTable")["KeySchema"]
    versioning = sim.s3.get_bucket_versioning(Bucket=sim.bucket)
    assert versioning["Status"] == "Enabled"

    fetch_env = sim.fetch.spec.environment
    assert fetch_env["QUEUE_URL"] == sim.queue.url
    assert fetch_env["STATE_TABLE"] == sim.state_table.name
    assert fetch_env["BEARER_TOKEN_SECRET_ARN"].startswith("arn:aws:secretsmanager:")
    assert fetch_env["MAX_PAGES"] == str(template["Parameters"]["MaxPages"]["Default"])
    process_env = sim.process.spec.environment
    assert process_env["TWEETS_TABLE"] == sim.tweets_table.name
    assert process_env["MEDIA_BUCKET"] == sim.bucket
    assert fetch_env["MEDIA_BUCKET"] == sim.bucket  # for the raw pages
    assert "TWEETS_TABLE" not in fetch_env
    (event,) = sim.process.spec.sqs_events
    assert (event.batch_size, event.report_batch_item_failures) == (10, True)
    (schedule,) = sim.fetch.spec.schedules
    assert schedule.every_seconds == 3600
    assert sim.fetch.spec.async_retries == 0


def test_max_receive_count_from_a_parameter_reference(make_sim: MakeSim) -> None:
    """``maxReceiveCount: !Ref MaxReceiveCount`` (with a default) resolves like a
    literal, and an explicit ``MaximumMessageSize`` is applied."""
    template = copy.deepcopy(load_template())
    template["Parameters"]["MaxReceiveCount"] = {"Type": "Number", "Default": 3}
    queue = _props(template, "TweetsQueue")
    queue["RedrivePolicy"]["maxReceiveCount"] = {"Ref": "MaxReceiveCount"}
    queue["MaximumMessageSize"] = 262_144

    sim = make_sim(template=template, MaxReceiveCount=4)

    assert sim.queue.max_receive_count == 4
    poison = sim.send_raw("not json")
    sim.settle()
    counts = [d.receive_count for d in sim.deliveries if d.message_id == poison]
    assert counts == [1, 2, 3, 4]
    with pytest.raises(ClientError, match="shorter than 262144 bytes"):
        sim.send_raw("é" * 140_000)  # 140,000 characters, 280,000 UTF-8 bytes


def test_failed_record_reappears_after_the_visibility_timeout(sim: Simulation) -> None:
    """A failing record is redelivered once its visibility timeout has run out, not
    before, and moves to the DLQ on the receive after ``maxReceiveCount``."""
    poison = sim.send_raw("not json")
    sim.settle()

    times = [d.at for d in sim.deliveries if d.message_id == poison]
    gaps = [later - earlier for earlier, later in itertools.pairwise(times)]
    timeout = sim.queue.visibility_timeout
    window = sim.esm.spec.batching_window
    assert all(timeout <= gap <= timeout + window + 1 for gap in gaps), gaps
    assert len(times) == sim.queue.max_receive_count
    assert sim.depth(sim.queue) == (0, 0)
    assert sim.dlq_bodies() == ["not json"]


def test_timeout_returns_the_whole_batch_and_resets_the_environment(
    sim: Simulation,
) -> None:
    """A DynamoDB write that hangs for 200 s (botocore's 60 s read timeout times its
    retries outlasts the function) makes the 120 s invocation time out: no record of
    that batch is deleted, all come back, and the next invocation starts cold."""
    x, user = sim.x, sim.username
    posted = [x.post(user), x.post(user, media=[x.photo()]), x.post(user)]

    def hang(_call: AwsCall) -> None:
        sim.clock.advance(200)

    sim.aws.inject(
        lambda call: call.operation == "PutItem" and call.function == sim.process.name,
        hang,
    )

    sim.run(1)

    first, second = sim.process.invocations
    assert first.outcome == "timeout"
    assert first.ended - first.started == sim.process.spec.timeout
    assert second.cold and second.outcome == "ok"
    assert len(sim.tweets()) == len(posted)
    assert sorted(d.receive_count for d in sim.deliveries) == [1, 1, 1, 2, 2, 2]


def test_a_stall_past_the_read_timeout_is_a_transient_media_error(
    sim: Simulation,
) -> None:
    """A CDN that holds a photo's headers for 200 s hits the client's 10 s read
    timeout, not the function timeout: only that record is retried."""
    x, user = sim.x, sim.username
    stalled = x.photo()
    assert stalled.url
    sim.cdn.stall(stalled.url, 200)
    posted = [x.post(user), x.post(user, media=[stalled])]

    sim.run(1)

    assert all(i.outcome == "ok" for i in sim.process.invocations)
    assert len(sim.tweets()) == len(posted)
    assert [d.receive_count for d in sim.deliveries_of(posted[1].id)] == [1, 2]
    assert [r.status for r in sim.cdn.requests if r.key == stalled.url] == [0, 200]


@pytest.mark.parametrize(
    ("result", "failed"),
    [
        ({"batchItemFailures": []}, set()),
        (None, set()),
        ({}, set()),
        ({"batchItemFailures": [{"itemIdentifier": "a"}]}, {"a"}),
        ({"batchItemFailures": [{"itemIdentifier": "zzz"}]}, {"a", "b"}),
        ({"batchItemFailures": [{"itemIdentifier": ""}]}, {"a", "b"}),
        ({"batchItemFailures": [{"itemIdentifier": None}]}, {"a", "b"}),
        ({"batchItemFailures": [{"wrong": "a"}]}, {"a", "b"}),
        ("not an object", {"a", "b"}),
    ],
)
def test_partial_batch_response_rules(result: object, failed: set[str]) -> None:
    """What the event source deletes, per the documented partial batch rules."""
    spec = SqsEventSpec(
        queue_arn="arn",
        batch_size=10,
        batching_window=0,
        report_batch_item_failures=True,
        maximum_concurrency=None,
    )
    source = SqsEventSource(
        runtime=cast("LambdaRuntime", None),
        spec=spec,
        queue=cast("QueueSpec", None),
        sqs=cast("SQSClient", None),
        clock=SimClock(0),
    )
    invocation = Invocation(
        function="f", request_id="r", event=None, started=0.0, result=result
    )
    assert source._failed_ids(invocation, {"a", "b"}) == failed
    invocation.outcome = "error"
    assert source._failed_ids(invocation, {"a", "b"}) == {"a", "b"}


def test_fake_x_api_paginates_like_the_real_one(sim: Simulation) -> None:
    """Newest first, exclusive ``since_id``/``until_id``, ``next_token`` paging, and
    nothing beyond the most recent ``window`` posts."""
    api = FakeXApi(sim.clock, token=TOKEN, window=25)
    api.add_user("someone", USER_ID)
    ids = [int(api.post("someone").id) for _ in range(30)]
    client = httpx.Client(
        base_url="https://api.x.com",
        transport=httpx.MockTransport(api),
        headers={"Authorization": f"Bearer {TOKEN}"},
    )

    def page(**params: Any) -> dict[str, Any]:
        response = client.get(f"/2/users/{USER_ID}/tweets", params=params)
        assert response.status_code == 200, response.text
        body: dict[str, Any] = response.json()
        return body

    seen: list[int] = []
    params: dict[str, Any] = {"max_results": 10}
    while True:
        body = page(**params)
        seen += [int(t["id"]) for t in body.get("data", [])]
        if "next_token" not in body["meta"]:
            break
        params["pagination_token"] = body["meta"]["next_token"]
    assert seen == sorted(ids, reverse=True)[:25]

    newer = [int(t["id"]) for t in page(max_results=100, since_id=ids[-6])["data"]]
    assert newer == sorted(ids[-5:], reverse=True)
    older = page(max_results=5, until_id=ids[-25]).get("data", [])
    assert older == []  # below the window
    between = page(max_results=100, since_id=ids[10], until_id=ids[14])["data"]
    assert [int(t["id"]) for t in between] == [ids[13], ids[12], ids[11]]
    assert (
        client.get(
            f"/2/users/{USER_ID}/tweets", params={"tweet.fields": "bogus"}
        ).status_code
        == 400
    )
    client.headers["Authorization"] = "Bearer wrong"
    assert client.get(f"/2/users/{USER_ID}/tweets").status_code == 401


def test_fake_x_api_serves_only_requested_fields(sim: Simulation) -> None:
    """``note_tweet`` and media come back only when asked for; withheld media is a
    partial error."""
    x = sim.x
    shown, withheld = x.photo(), x.photo(in_includes=False)
    long = x.post(sim.username, note_text="word " * 100, media=[shown, withheld])
    client = httpx.Client(
        base_url="https://api.x.com",
        transport=httpx.MockTransport(x),
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    plain = client.get(f"/2/users/{USER_ID}/tweets").json()
    (tweet,) = plain["data"]
    assert set(tweet) == {"id", "text", "edit_history_tweet_ids"}
    assert len(tweet["text"]) == 280 and tweet["text"].endswith("…")
    full = client.get(
        f"/2/users/{USER_ID}/tweets",
        params={
            "tweet.fields": "note_tweet,attachments",
            "expansions": "attachments.media_keys",
            "media.fields": "url",
        },
    ).json()
    assert full["data"][0]["note_tweet"]["text"] == long.note_text
    assert [m["media_key"] for m in full["includes"]["media"]] == [shown.media_key]
    assert full["errors"][0]["resource_id"] == withheld.media_key


def test_time_reaches_the_code_under_test(sim: Simulation) -> None:
    """No function of the package keeps a real-time default argument, and the
    handlers' clocks read the simulated time."""
    real = {id(f) for f in (time.time, time.monotonic, time.sleep, time.perf_counter)}
    for module in all_package_modules():
        for value in vars(module).values():
            members = vars(value).values() if inspect.isclass(value) else [value]
            for member in members:
                function = getattr(member, "__func__", member)
                if not inspect.isfunction(function):
                    continue
                defaults = [
                    *(function.__defaults__ or ()),
                    *(function.__kwdefaults__ or {}).values(),
                ]
                assert not any(id(d) in real for d in defaults), function
    fetch_time = vars(sim.fetch.module)["time"]
    assert fetch_time.time() == sim.clock.now
    sim.advance(3600)
    assert fetch_time.time() == sim.clock.now
