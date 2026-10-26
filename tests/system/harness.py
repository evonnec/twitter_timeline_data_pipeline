"""The whole system in one object: the stack in moto, the fakes, the AWS glue, time.

``Simulation`` deploys ``template.yaml`` into moto (``system.stack``), runs the real
handlers through ``system.runtime`` (Lambda, the SQS event-source mapping, the
schedule), serves the X API from ``system.fake_x`` and media from ``system.fake_cdn``,
and moves everything on one ``SimClock``. Tests drive it with ``run`` (scheduled
fetches with the queue processed in between), ``invoke_fetch``, ``settle`` and the
fakes' scripting, then observe the outcome only through DynamoDB, S3 and SQS.

What moto doesn't model faithfully is patched here, in the open:

- SQS reads the simulated clock (visibility timeouts, receive counts, retention).
- SQS sizes are UTF-8 bytes, as in the real service (moto counts characters), and a
  body with characters SQS refuses is rejected.
- S3 transfers run on the calling thread, so a simulated timeout or crash stops an
  upload where it stands instead of leaving s3transfer's worker threads waiting.
"""

import copy
import datetime as dt
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

import boto3
import boto3.s3.inject
import httpx
import moto.sqs.models as sqs_models
import pytest
from moto.sqs.exceptions import BatchRequestTooLong, InvalidParameterValue

from system.clock import SimClock, virtualize_time
from system.fake_cdn import FakeCdn
from system.fake_x import FakeXApi
from system.runtime import (
    PACKAGE,
    AwsCalls,
    Delivery,
    Invocation,
    LambdaRuntime,
    Schedule,
    SqsEventSource,
    all_package_modules,
    reset_module_state,
)
from system.stack import ACCOUNT_ID, REGION, STACK_NAME, QueueSpec, Template, deploy
from twitter_timeline_data_pipeline import USER_AGENT
from twitter_timeline_data_pipeline.x_api import XClient

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_secretsmanager import SecretsManagerClient
    from mypy_boto3_sqs import SQSClient

START = dt.datetime(2026, 10, 26, 12, 0, tzinfo=dt.UTC).timestamp()
TOKEN = "sim-bearer-token-not-real"
USER_ID = "1000000001"
SQS_BATCH_LIMIT = 1_048_576
# What SQS accepts in a message body (anything else is InvalidMessageContents).
_SQS_REFUSED = re.compile("[^\t\n\r\x20-퟿-�\U00010000-\U0010ffff]")


@dataclass(frozen=True, slots=True)
class StoredObject:
    key: str
    size: int
    content_type: str
    metadata: Mapping[str, str]
    versions: int


def _routed_httpx(handler: Callable[[httpx.Request], httpx.Response]) -> ModuleType:
    """A stand-in ``httpx`` module whose ``Client`` always uses ``handler``."""

    class RoutedClient(httpx.Client):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    shim = ModuleType("httpx")
    shim.__dict__.update(vars(httpx))
    shim.Client = RoutedClient  # type: ignore[attr-defined]
    return shim


class Simulation:
    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        parameters: Mapping[str, object] | None = None,
        template: Template | None = None,
        start: float = START,
    ) -> None:
        self.clock = SimClock(start)
        self._mp = monkeypatch
        self._patch_moto()
        virtualize_time(monkeypatch, self.clock, PACKAGE)
        reset_module_state(all_package_modules())
        monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
        boto3.setup_default_session(region_name=REGION)
        assert boto3.DEFAULT_SESSION is not None
        self.aws = AwsCalls(self.clock)
        self.aws.install(boto3.DEFAULT_SESSION)
        self._single_threaded_transfers()

        self.stack = deploy(parameters, template)
        session = boto3.Session(region_name=REGION)  # the harness's own: no faults
        self.sqs: SQSClient = session.client("sqs")
        self.s3: S3Client = session.client("s3")
        self.secrets: SecretsManagerClient = session.client("secretsmanager")
        dynamodb = session.resource("dynamodb")
        outputs = self.stack.outputs
        self.tweets_table: Table = dynamodb.Table(outputs["TweetsTableName"])
        self.state_table: Table = dynamodb.Table(outputs["StateTableName"])
        self.bucket = outputs["MediaBucketName"]

        self.x = FakeXApi(self.clock, token=TOKEN)
        self.cdn = FakeCdn(self.clock)
        self.username = self.stack.parameters["TwitterUsername"]
        self.x.add_user(self.username, USER_ID)
        self.put_secret(TOKEN)

        hidden = sorted(
            {k for f in self.stack.functions.values() for k in f.environment}
        )
        self.functions = {
            name: LambdaRuntime(
                spec,
                self.clock,
                self.aws,
                hidden_env=hidden,
                async_retries=spec.async_retries,
            )
            for name, spec in self.stack.functions.items()
        }
        for runtime in self.functions.values():
            self._install_fakes(runtime.module)
        self.event_sources = [
            SqsEventSource(
                runtime=runtime,
                spec=event,
                queue=self.stack.queue_by_arn(event.queue_arn),
                sqs=self.sqs,
                clock=self.clock,
            )
            for runtime in self.functions.values()
            for event in runtime.spec.sqs_events
        ]
        self.schedules = [
            Schedule(
                runtime=runtime,
                spec=schedule,
                clock=self.clock,
                next_at=self.clock.now,
                rule_arn=(
                    f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/"
                    f"{STACK_NAME}-{runtime.name}Schedule"
                ),
            )
            for runtime in self.functions.values()
            for schedule in runtime.spec.schedules
            if schedule.enabled
        ]
        (self.fetch,) = (f for f in self.functions.values() if f.spec.schedules)
        (self.process,) = self._functions_running("process")
        (self.media_retry,) = self._functions_running("media_retry")
        (self.esm,) = self._sources_of(self.process)
        (self.retry_esm,) = self._sources_of(self.media_retry)
        self.queue: QueueSpec = self.esm.queue
        assert self.queue.dead_letter_arn is not None
        self.dlq: QueueSpec = self.stack.queue_by_arn(self.queue.dead_letter_arn)
        self.retry_queue: QueueSpec = self.retry_esm.queue
        assert self.retry_queue.dead_letter_arn is not None
        self.retry_dlq: QueueSpec = self.stack.queue_by_arn(
            self.retry_queue.dead_letter_arn
        )

    def _functions_running(self, module: str) -> list[LambdaRuntime]:
        return [
            f
            for f in self.functions.values()
            if f.spec.handler.endswith(f".handlers.{module}.handler")
        ]

    def _sources_of(self, runtime: LambdaRuntime) -> list[SqsEventSource]:
        return [s for s in self.event_sources if s.runtime is runtime]

    # --- patches -------------------------------------------------------------------

    def _patch_moto(self) -> None:
        mp = self._mp
        mp.setattr(sqs_models, "unix_time", self.clock.seconds)
        mp.setattr(sqs_models, "unix_time_millis", self.clock.millis)
        backend = sqs_models.SQSBackend
        validate = backend._validate_message
        send_batch = backend.send_message_batch

        def validate_bytes(
            self: Any, queue: Any, message_body: str, *args: Any, **kwargs: Any
        ) -> None:
            if _SQS_REFUSED.search(message_body):
                raise InvalidParameterValue(
                    "Invalid characters found. Valid unicode characters are #x9 | #xA "
                    "| #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF"
                )
            size = len(message_body.encode())
            if size > queue.maximum_message_size:
                raise InvalidParameterValue(
                    "One or more parameters are invalid. Reason: Message must be "
                    f"shorter than {queue.maximum_message_size} bytes."
                )
            validate(self, queue, message_body, *args, **kwargs)

        def send_batch_bytes(
            self: Any, queue_name: str, entries: dict[str, dict[str, Any]]
        ) -> Any:
            total = sum(len(e["MessageBody"].encode()) for e in entries.values())
            if total > SQS_BATCH_LIMIT:
                raise BatchRequestTooLong(total)
            return send_batch(self, queue_name, entries)

        mp.setattr(backend, "_validate_message", validate_bytes)
        mp.setattr(backend, "send_message_batch", send_batch_bytes)

    def _single_threaded_transfers(self) -> None:
        create = boto3.s3.inject.create_transfer_manager

        def single_threaded(client: Any, config: Any, osutil: Any = None) -> Any:
            config = copy.copy(config)
            config.use_threads = False
            return create(client, config, osutil)

        self._mp.setattr(boto3.s3.inject, "create_transfer_manager", single_threaded)

    def _install_fakes(self, module: ModuleType) -> None:
        """Point a handler module's X API client and media downloads at the fakes."""
        if getattr(module, "XClient", None) is XClient:
            api, clock = self.x, self.clock

            class SimXClient(XClient):
                def __init__(self, bearer_token: str, **kwargs: Any) -> None:
                    self._sim_http = httpx.Client(
                        base_url=kwargs.pop("base_url", "https://api.x.com/2"),
                        timeout=kwargs.pop("timeout", 10.0),
                        headers={"User-Agent": USER_AGENT},
                        transport=httpx.MockTransport(api),
                    )
                    kwargs.pop("http_client", None)
                    kwargs.setdefault("sleep", clock.sleep)
                    kwargs.setdefault("clock", clock.time)
                    super().__init__(bearer_token, http_client=self._sim_http, **kwargs)

                def close(self) -> None:
                    super().close()
                    self._sim_http.close()

            self._mp.setattr(module, "XClient", SimXClient)
        if getattr(module, "httpx", None) is httpx:
            self._mp.setattr(module, "httpx", _routed_httpx(self.cdn))

    # --- driving -------------------------------------------------------------------

    @property
    def schedule(self) -> Schedule:
        (schedule,) = self.schedules
        return schedule

    def process_until(self, when: float) -> list[Invocation]:
        """Let the event source work (through visibility timeouts) until ``when``."""
        invocations = self._work(until=when)
        self.clock.advance_to(when)
        return invocations

    def _work(self, until: float) -> list[Invocation]:
        """Poll every event source until none has anything due (a Tweet message may
        enqueue a media retry, and the other way round), waiting out visibility
        timeouts and send delays, but never past ``until``."""
        invocations: list[Invocation] = []
        while True:
            progressed = False
            for source in self.event_sources:
                while done := source.poll():
                    invocations += done
                    progressed = True
            if progressed:
                continue
            due = [d for s in self.event_sources if (d := s.next_due()) is not None]
            if not due or min(due) > until:
                return invocations
            self.clock.advance_to(min(due))

    def run(self, runs: int = 1) -> list[Invocation]:
        """``runs`` scheduled fetches, one schedule interval apart. The queue is worked
        in between, and after the last run until the next one would be due."""
        fetches: list[Invocation] = []
        for _ in range(runs):
            self.process_until(self.schedule.next_at)
            fetches += self.schedule.fire()
        self.process_until(self.schedule.next_at - 0.001)
        return fetches

    def invoke_fetch(self, event: Mapping[str, Any] | None = None) -> Invocation:
        """A manual, synchronous invoke (``sam remote invoke``)."""
        return self.fetch.invoke(dict(event or {}))

    def settle(self, max_seconds: float = 2 * 86400) -> list[Invocation]:
        """Work the queues until nothing is visible, delayed or in flight (bounded)."""
        return self._work(until=self.clock.now + max_seconds)

    def advance(self, seconds: float) -> None:
        self.process_until(self.clock.now + seconds)

    # --- operator actions ------------------------------------------------------------

    def put_secret(self, token: str) -> None:
        """What ``scripts/put_secret.sh`` does."""
        self.secrets.put_secret_value(
            SecretId=self.stack.outputs["BearerTokenSecretArn"], SecretString=token
        )

    def send_raw(self, body: str) -> str:
        """Put a message on the tweets queue as some other producer would."""
        return self.sqs.send_message(QueueUrl=self.queue.url, MessageBody=body)[
            "MessageId"
        ]

    def redrive_dlq(self, *, media_retry: bool = False) -> int:
        """Move every DLQ message back to its source queue (a DLQ redrive): the tweets
        queue's, or with ``media_retry`` the media-retry queue's."""
        dlq, queue = (
            (self.retry_dlq, self.retry_queue)
            if media_retry
            else (self.dlq, self.queue)
        )
        moved = 0
        while messages := self.sqs.receive_message(
            QueueUrl=dlq.url, MaxNumberOfMessages=10, VisibilityTimeout=60
        ).get("Messages"):
            for m in messages:
                self.sqs.send_message(QueueUrl=queue.url, MessageBody=m["Body"])
                self.sqs.delete_message(
                    QueueUrl=dlq.url, ReceiptHandle=m["ReceiptHandle"]
                )
                moved += 1
        return moved

    def replay(self, prefix: str = "raw/") -> int:
        """``twitter-pipeline replay``: re-enqueue the messages of the raw pages under
        ``prefix`` (from the operator's own clients, as the CLI would)."""
        from twitter_timeline_data_pipeline.handlers.fetch import send_messages
        from twitter_timeline_data_pipeline.replay import run_replay

        url = self.queue.url
        return run_replay(
            s3=self.s3,
            bucket=self.bucket,
            prefix=prefix,
            send=lambda ms: send_messages(self.sqs, url, ms, self.clock.sleep),
        ).enqueued

    def wipe_tweets(self) -> int:
        """Delete every item of the tweets table (a disaster, or a bad migration)."""
        items = list(self.tweets())
        for tweet_id in items:
            self.tweets_table.delete_item(Key={"tweet_id": tweet_id})
        return len(items)

    # --- observation -----------------------------------------------------------------

    def tweets(self) -> dict[str, dict[str, Any]]:
        """Every item of the tweets table, by ``tweet_id``."""
        items: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {}
        while True:
            page = self.tweets_table.scan(**kwargs)
            items += cast("list[dict[str, Any]]", page["Items"])
            if "LastEvaluatedKey" not in page:
                break
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        by_id = {str(i["tweet_id"]): i for i in items}
        assert len(by_id) == len(items)
        return by_id

    def by_username(self, username: str) -> list[dict[str, Any]]:
        """The GSI partition of ``username``, newest first."""
        items: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {
            "IndexName": "username-created_at-index",
            "KeyConditionExpression": "username = :u",
            "ExpressionAttributeValues": {":u": username},
            "ScanIndexForward": False,
        }
        while True:
            page = self.tweets_table.query(**kwargs)
            items += cast("list[dict[str, Any]]", page["Items"])
            if "LastEvaluatedKey" not in page:
                return items
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def state_items(self) -> dict[str, dict[str, Any]]:
        items = cast("list[dict[str, Any]]", self.state_table.scan()["Items"])
        return {str(i["username"]): i for i in items}

    def checkpoint(self, username: str) -> str | None:
        """The stored high-water mark (``newest_id``) of ``username``'s state item."""
        item = self.state_items().get(username)
        if item is None or item.get("newest_id") is None:
            return None
        return str(int(item["newest_id"]))

    def objects(self) -> dict[str, StoredObject]:
        """The media and text objects (everything outside ``raw/``)."""
        return {
            k: o for k, o in self._all_objects().items() if not k.startswith("raw/")
        }

    def raw_objects(self) -> dict[str, StoredObject]:
        """The landed raw API pages."""
        return {k: o for k, o in self._all_objects().items() if k.startswith("raw/")}

    def _all_objects(self) -> dict[str, StoredObject]:
        found: dict[str, StoredObject] = {}
        versions = self.s3.list_object_versions(Bucket=self.bucket).get("Versions", [])
        counts: dict[str, int] = {}
        for v in versions:
            counts[v["Key"]] = counts.get(v["Key"], 0) + 1
        for key, count in counts.items():
            head = self.s3.head_object(Bucket=self.bucket, Key=key)
            found[key] = StoredObject(
                key=key,
                size=head["ContentLength"],
                content_type=head.get("ContentType", ""),
                metadata=head.get("Metadata", {}),
                versions=count,
            )
        return found

    def object_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def depth(self, queue: QueueSpec) -> tuple[int, int]:
        """(visible, in flight) messages of ``queue``."""
        attributes = self.sqs.get_queue_attributes(
            QueueUrl=queue.url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )["Attributes"]
        return (
            int(attributes["ApproximateNumberOfMessages"]),
            int(attributes["ApproximateNumberOfMessagesNotVisible"]),
        )

    def delayed(self, queue: QueueSpec) -> int:
        """Messages of ``queue`` still waiting out their ``DelaySeconds``."""
        attributes = self.sqs.get_queue_attributes(
            QueueUrl=queue.url, AttributeNames=["ApproximateNumberOfMessagesDelayed"]
        )["Attributes"]
        return int(attributes["ApproximateNumberOfMessagesDelayed"])

    def dlq_bodies(self, dlq: QueueSpec | None = None) -> list[str]:
        """Peek at a DLQ (the tweets queue's by default; messages stay there)."""
        url = (dlq or self.dlq).url
        bodies: list[str] = []
        # A 1 s hold instead of ChangeMessageVisibility(0) afterwards: SQS caps a
        # message's total visibility at 12 h from its first receive, and a message
        # that went through the media-retry queue's receives is already past that.
        while messages := self.sqs.receive_message(
            QueueUrl=url, MaxNumberOfMessages=10, VisibilityTimeout=1
        ).get("Messages"):
            bodies += [m["Body"] for m in messages]
        # moto makes a message visible once the clock passes its deadline.
        self.clock.advance(1.001)
        return bodies

    @property
    def deliveries(self) -> list[Delivery]:
        """Deliveries to the process function (media retries are in
        ``retry_deliveries``)."""
        return self.esm.deliveries

    @property
    def retry_deliveries(self) -> list[Delivery]:
        return self.retry_esm.deliveries

    def deliveries_of(self, tweet_id: str) -> list[Delivery]:
        """Deliveries of messages about ``tweet_id`` to the process function."""
        return [d for d in self.deliveries if message_tweet_id(d.body) == tweet_id]


def message_tweet_id(body: str) -> str | None:
    """The Tweet ID in a message body, or None if it isn't a Tweet message."""
    try:
        return str(json.loads(body)["tweet"]["id"])
    except ValueError, KeyError, TypeError:
        return None
