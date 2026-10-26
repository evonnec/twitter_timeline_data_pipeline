"""The AWS services around the handlers: Lambda, the SQS event source, the schedule.

- ``LambdaRuntime`` runs a function's real ``handler`` (looked up from the template's
  ``Handler``) with only that function's environment, a JSON round trip of the event
  and the result, and a context whose ``get_remaining_time_in_millis`` reads the
  simulated clock. Past the timeout the invocation is stopped where it stands. A
  timeout or crash resets the execution environment, which clears the module-level
  caches the handler's modules keep (clients, the bearer token), like a cold start.
- ``SqsEventSource`` is the event-source mapping: it receives up to ``BatchSize``
  messages (waiting out the batching window when the batch isn't full), keeps the
  invocation payload under Lambda's 6 MB limit, invokes the function, and deletes
  only what succeeded per the partial batch response rules. Visibility timeouts,
  receive counts and the move to the DLQ are moto's own, on simulated time.
- ``Schedule`` fires the EventBridge rule: an asynchronous invocation with the
  template's ``MaximumRetryAttempts``.
- ``AwsCalls`` hooks botocore in the handlers' session: every AWS call the handlers
  make is recorded with the function that made it, and faults (errors, partial
  ``SendMessageBatch`` failures, runtime crashes) can be injected per call.
"""

import importlib
import itertools
import json
import os
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, cast

import boto3
from botocore.awsrequest import AWSResponse
from botocore.compat import HTTPHeaders
from botocore.exceptions import ClientError
from moto.sqs.models import sqs_backends

from system.clock import FunctionCrashed, FunctionTimedOut, SimClock
from system.stack import (
    ACCOUNT_ID,
    REGION,
    FunctionSpec,
    QueueSpec,
    ScheduleSpec,
    SqsEventSpec,
)

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient
    from mypy_boto3_sqs.type_defs import (
        MessageTypeDef,
        SendMessageBatchRequestEntryTypeDef,
    )

PACKAGE = "twitter_timeline_data_pipeline"
INVOKE_PAYLOAD_LIMIT = 6 * 1024 * 1024
ASYNC_RETRY_DELAYS = (60.0, 120.0)

type Outcome = Literal["ok", "error", "timeout", "crash"]
type FakeResponse = tuple[AWSResponse, dict[str, Any]]


# --- AWS calls -----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AwsCall:
    service: str
    """botocore service name, e.g. ``dynamodb``."""
    operation: str
    """API operation, e.g. ``PutItem``."""
    params: Mapping[str, Any]
    function: str | None
    """Logical ID of the function making the call (None outside an invocation)."""
    now: float


@dataclass
class AwsFault:
    match: Callable[[AwsCall], bool]
    act: Callable[[AwsCall], FakeResponse | None]
    remaining: int | None
    hits: int = 0


def client_error(
    call: AwsCall, code: str, message: str = "injected fault"
) -> ClientError:
    response: Any = {"Error": {"Code": code, "Message": message}}
    return ClientError(response, call.operation)


def fake_response(url: str, parsed: dict[str, Any]) -> FakeResponse:
    """A 200 that botocore returns as ``parsed`` instead of calling the service."""
    return AWSResponse(url, 200, HTTPHeaders(), None), parsed


class AwsCalls:
    """Records the handlers' AWS calls and injects faults into them."""

    def __init__(self, clock: SimClock) -> None:
        self._clock = clock
        self.calls: list[AwsCall] = []
        self.faults: list[AwsFault] = []
        self.function: str | None = None

    def install(self, session: boto3.Session) -> None:
        session.events.register("before-parameter-build", self._stash)
        # A before-call handler may return a response; the stubs say None.
        before_call = cast("Callable[..., None]", self._before_call)
        session.events.register("before-call", before_call)

    @contextmanager
    def calling(self, function: str) -> Iterator[None]:
        outer, self.function = self.function, function
        try:
            yield
        finally:
            self.function = outer

    def _stash(self, params: dict[str, Any], context: dict[str, Any], **_: Any) -> None:
        context["sim_params"] = dict(params)

    def _before_call(
        self, model: Any, context: dict[str, Any], **_: Any
    ) -> FakeResponse | None:
        call = AwsCall(
            service=model.service_model.service_name,
            operation=model.name,
            params=context.get("sim_params", {}),
            function=self.function,
            now=self._clock.now,
        )
        self.calls.append(call)
        for fault in self.faults:
            if fault.remaining == 0 or not fault.match(call):
                continue
            fault.hits += 1
            if fault.remaining is not None:
                fault.remaining -= 1
            if (response := fault.act(call)) is not None:
                return response
        return None

    def inject(
        self,
        match: Callable[[AwsCall], bool],
        act: Callable[[AwsCall], FakeResponse | None],
        *,
        times: int | None = 1,
    ) -> AwsFault:
        fault = AwsFault(match, act, times)
        self.faults.append(fault)
        return fault

    def fail(
        self,
        operation: str,
        *,
        code: str = "InternalServerError",
        when: Callable[[AwsCall], bool] = lambda _call: True,
        times: int | None = 1,
    ) -> AwsFault:
        """Make matching calls raise ``code`` (as they would once the SDK's retries
        are spent)."""

        def act(call: AwsCall) -> None:
            raise client_error(call, code)

        return self.inject(
            lambda call: call.operation == operation and when(call), act, times=times
        )

    def crash(
        self,
        operation: str,
        *,
        when: Callable[[AwsCall], bool] = lambda _call: True,
        times: int | None = 1,
    ) -> AwsFault:
        """Kill the running invocation (out of memory, say) as the call is made."""

        def act(call: AwsCall) -> None:
            raise FunctionCrashed(f"Runtime exited during {call.operation}")

        return self.inject(
            lambda call: call.operation == operation and when(call), act, times=times
        )

    def made(self, operation: str, *, function: str | None = None) -> list[AwsCall]:
        return [
            c
            for c in self.calls
            if c.operation == operation and (function is None or c.function == function)
        ]


def sqs_partial_failure(
    sqs: SQSClient, fail: Callable[[Mapping[str, Any]], bool]
) -> Callable[[AwsCall], FakeResponse]:
    """A ``SendMessageBatch`` that sends the entries ``fail`` rejects nowhere and
    reports them as ``Failed`` (``SenderFault`` false: a server-side error), while the
    others really are sent."""

    def act(call: AwsCall) -> FakeResponse:
        entries = list(call.params["Entries"])
        good = [e for e in entries if not fail(e)]
        bad = [e for e in entries if fail(e)]
        successful: list[Any] = []
        if good:
            sent = sqs.send_message_batch(
                QueueUrl=call.params["QueueUrl"], Entries=good
            )
            successful = list(sent.get("Successful", []))
        parsed: dict[str, Any] = {
            "Successful": successful,
            "Failed": [
                {
                    "Id": e["Id"],
                    "SenderFault": False,
                    "Code": "InternalError",
                    "Message": "injected fault",
                }
                for e in bad
            ],
        }
        return fake_response(call.params["QueueUrl"], parsed)

    return act


def duplicating_send(sqs: SQSClient) -> Callable[[AwsCall], FakeResponse]:
    """A ``SendMessageBatch`` whose every entry is delivered twice, the copies in
    reverse order, as SQS's at-least-once delivery may."""

    def act(call: AwsCall) -> FakeResponse:
        url = call.params["QueueUrl"]
        entries = cast(
            "list[SendMessageBatchRequestEntryTypeDef]", list(call.params["Entries"])
        )
        sent = sqs.send_message_batch(QueueUrl=url, Entries=entries)
        copies = [{**e, "Id": f"copy-{e['Id']}"} for e in reversed(entries)]
        sqs.send_message_batch(
            QueueUrl=url,
            Entries=cast("list[SendMessageBatchRequestEntryTypeDef]", copies),
        )
        return fake_response(url, {"Successful": sent["Successful"], "Failed": []})

    return act


# --- Lambda --------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FakeContext:
    """The parts of the Lambda context object a handler may use."""

    function_name: str
    memory_limit_in_mb: int
    invoked_function_arn: str
    aws_request_id: str
    log_group_name: str
    log_stream_name: str
    function_version: str
    clock: SimClock

    def get_remaining_time_in_millis(self) -> int:
        return self.clock.remaining_ms()


@dataclass
class Invocation:
    function: str
    request_id: str
    event: Any
    started: float
    ended: float = 0.0
    outcome: Outcome = "ok"
    result: Any = None
    error: BaseException | None = None
    cold: bool = False
    attempt: int = 1


def package_modules(module: ModuleType, package: str = PACKAGE) -> list[ModuleType]:
    """``module`` and the package modules it takes names from (its run-time state)."""
    names = {module.__name__}
    for value in vars(module).values():
        source = value if isinstance(value, ModuleType) else None
        if source is None:
            source = sys.modules.get(getattr(value, "__module__", None) or "")
        if source is not None and source.__name__.startswith(f"{package}."):
            names.add(source.__name__)
    return [sys.modules[name] for name in sorted(names)]


def reset_module_state(modules: Sequence[ModuleType]) -> None:
    """Empty every ``functools.cache`` and call every ``clear_cache()`` they define."""
    for module in modules:
        for value in list(vars(module).values()):
            if getattr(value, "__module__", None) != module.__name__:
                continue
            if callable(clear := getattr(value, "cache_clear", None)):
                clear()
        clear_cache = vars(module).get("clear_cache")
        if callable(clear_cache) and clear_cache.__module__ == module.__name__:
            clear_cache()


def all_package_modules(package: str = PACKAGE) -> list[ModuleType]:
    return [
        m
        for name, m in sorted(sys.modules.items())
        if m is not None and (name == package or name.startswith(f"{package}."))
    ]


@contextmanager
def scoped_environment(
    variables: Mapping[str, str], hidden: Sequence[str]
) -> Iterator[None]:
    """Set ``variables`` and unset ``hidden`` for the duration; restore afterwards."""
    saved = {name: os.environ.get(name) for name in {*variables, *hidden}}
    for name in hidden:
        os.environ.pop(name, None)
    os.environ.update(variables)
    try:
        yield
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


class LambdaRuntime:
    """One function: its handler, its environment, its warm or cold state."""

    def __init__(
        self,
        spec: FunctionSpec,
        clock: SimClock,
        aws: AwsCalls,
        *,
        hidden_env: Sequence[str] = (),
        async_retries: int = 2,
    ) -> None:
        self.spec = spec
        self._clock = clock
        self._aws = aws
        module_name, _, self._attr = spec.handler.rpartition(".")
        self.module = importlib.import_module(module_name)
        self._hidden = [h for h in hidden_env if h not in spec.environment]
        self.async_retries = async_retries
        self.invocations: list[Invocation] = []
        self._warm = False
        self._ids = itertools.count(1)

    @property
    def name(self) -> str:
        return self.spec.logical_id

    def cold_start(self) -> None:
        reset_module_state(package_modules(self.module))
        self._warm = True

    def environment(self) -> dict[str, str]:
        return {
            **self.spec.environment,
            "AWS_REGION": REGION,
            "AWS_DEFAULT_REGION": REGION,
            "AWS_LAMBDA_FUNCTION_NAME": self.spec.name,
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": str(self.spec.memory_mb),
            "AWS_LAMBDA_FUNCTION_VERSION": "$LATEST",
        }

    def invoke(self, event: Any, *, attempt: int = 1) -> Invocation:
        """Run the handler once, synchronously, as the Lambda service would."""
        cold = not self._warm
        if cold:
            self.cold_start()
        request_id = f"{self.name}-{next(self._ids):05d}"
        invocation = Invocation(
            function=self.name,
            request_id=request_id,
            event=event,
            started=self._clock.now,
            cold=cold,
            attempt=attempt,
        )
        self.invocations.append(invocation)
        context = FakeContext(
            function_name=self.spec.name,
            memory_limit_in_mb=self.spec.memory_mb,
            invoked_function_arn=(
                f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{self.spec.name}"
            ),
            aws_request_id=request_id,
            log_group_name=f"/aws/lambda/{self.spec.name}",
            log_stream_name=f"2026/10/26/[$LATEST]{uuid.uuid4().hex}",
            function_version="$LATEST",
            clock=self._clock,
        )
        payload = json.loads(json.dumps(event))
        handler = getattr(self.module, self._attr)
        with (
            scoped_environment(self.environment(), self._hidden),
            self._aws.calling(self.name),
        ):
            try:
                with self._clock.deadline(invocation.started + self.spec.timeout):
                    result = handler(payload, context)
                invocation.result = json.loads(json.dumps(result))
            except FunctionTimedOut as exc:
                invocation.outcome, invocation.error = "timeout", exc
                self._warm = False
            except FunctionCrashed as exc:
                invocation.outcome, invocation.error = "crash", exc
                self._warm = False
            except Exception as exc:
                # An unhandled error keeps the execution environment warm.
                invocation.outcome, invocation.error = "error", exc
        invocation.ended = self._clock.now
        return invocation


# --- SQS event source mapping ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class Delivery:
    """One message handed to one invocation."""

    message_id: str
    body: str
    receive_count: int
    request_id: str
    at: float
    succeeded: bool


@dataclass
class SqsEventSource:
    runtime: LambdaRuntime
    spec: SqsEventSpec
    queue: QueueSpec
    sqs: SQSClient
    clock: SimClock
    deliveries: list[Delivery] = field(default_factory=list)
    _in_flight: dict[str, float] = field(default_factory=dict)
    """messageId -> when its visibility timeout runs out (receive time + timeout)."""

    def _receive(self, limit: int) -> list[MessageTypeDef]:
        messages = self.sqs.receive_message(
            QueueUrl=self.queue.url,
            MaxNumberOfMessages=min(10, limit),
            MessageSystemAttributeNames=["All"],
            MessageAttributeNames=["All"],
            WaitTimeSeconds=0,
        ).get("Messages", [])
        for m in messages:
            self._in_flight[m["MessageId"]] = (
                self.clock.now + self.queue.visibility_timeout
            )
        return messages

    def poll(self) -> list[Invocation]:
        """One batch: receive, invoke, delete what succeeded. [] when nothing is due."""
        batch = self._receive(self.spec.batch_size)
        if not batch:
            return []
        if len(batch) < self.spec.batch_size and self.spec.batching_window:
            # A short batch waits out the window; whatever is due by then joins it.
            self.clock.advance(self.spec.batching_window)
            batch += self._receive(self.spec.batch_size - len(batch))
        return [self._invoke(chunk) for chunk in self._fit_payload(batch)]

    def drain(self, until: float | None = None) -> list[Invocation]:
        """Poll until nothing is visible, waiting out visibility timeouts of messages
        in flight (failed ones reappear, and moto moves them to the DLQ past
        ``maxReceiveCount``), but never past ``until``."""
        invocations: list[Invocation] = []
        while True:
            if done := self.poll():
                invocations += done
                continue
            # Receive returned nothing, so whatever was due is gone (deleted or moved
            # to the DLQ by that receive); only later timeouts are left to wait for.
            self._in_flight = {
                k: t for k, t in self._in_flight.items() if t >= self.clock.now
            }
            if not self._in_flight:
                return invocations
            due = min(self._in_flight.values()) + 0.001  # moto: visible once now > t
            if until is not None and due > until:
                return invocations
            self.clock.advance_to(due)

    def next_due(self) -> float | None:
        """When something here becomes receivable again, or None if nothing will.

        Call it after ``poll`` found nothing: whatever was due is then gone (deleted or
        moved to the DLQ by that receive), so only later visibility timeouts of
        messages in flight and the delivery time of delayed messages (``DelaySeconds``)
        are left to wait for.
        """
        self._in_flight = {
            k: t for k, t in self._in_flight.items() if t >= self.clock.now
        }
        times = list(self._in_flight.values())
        # moto keeps a delayed message's delivery time on the message itself.
        backend = sqs_backends[ACCOUNT_ID][REGION]
        times += [
            m.delayed_until / 1000
            for m in backend.queues[self.queue.name]._messages
            if m.delayed_until / 1000 >= self.clock.now
        ]
        return min(times) + 0.001 if times else None  # moto: visible once now > t

    def _fit_payload(
        self, batch: list[MessageTypeDef]
    ) -> Iterator[list[MessageTypeDef]]:
        chunk: list[MessageTypeDef] = []
        size = len('{"Records":[]}')
        for message in batch:
            record_size = len(json.dumps(self._record(message)).encode()) + 1
            if chunk and size + record_size > INVOKE_PAYLOAD_LIMIT:
                yield chunk
                chunk, size = [], len('{"Records":[]}')
            chunk.append(message)
            size += record_size
        if chunk:
            yield chunk

    def _record(self, message: MessageTypeDef) -> dict[str, Any]:
        attributes = message.get("Attributes", {})
        return {
            "messageId": message["MessageId"],
            "receiptHandle": message["ReceiptHandle"],
            "body": message["Body"],
            "attributes": {
                k: attributes[k]
                for k in (
                    "ApproximateReceiveCount",
                    "SentTimestamp",
                    "SenderId",
                    "ApproximateFirstReceiveTimestamp",
                )
                if k in attributes
            },
            "messageAttributes": {},
            "md5OfBody": message["MD5OfBody"],
            "eventSource": "aws:sqs",
            "eventSourceARN": self.queue.arn,
            "awsRegion": REGION,
        }

    def _failed_ids(self, invocation: Invocation, ids: set[str]) -> set[str]:
        """Which records go back to the queue, per the partial batch response rules."""
        if invocation.outcome != "ok":
            return ids  # an error, timeout or crash returns the whole batch
        if not self.spec.report_batch_item_failures:
            return set()
        response = invocation.result
        if response is None or response == {}:
            return set()
        if not isinstance(response, dict):
            return ids
        failures = response.get("batchItemFailures")
        if failures is None:
            return set()
        if not isinstance(failures, list):
            return ids
        failed: set[str] = set()
        for failure in failures:
            identifier = (
                failure.get("itemIdentifier") if isinstance(failure, dict) else None
            )
            if not isinstance(identifier, str) or identifier not in ids:
                return ids  # a null, empty or unknown identifier fails the whole batch
            failed.add(identifier)
        return failed

    def _invoke(self, messages: list[MessageTypeDef]) -> Invocation:
        event = {"Records": [self._record(m) for m in messages]}
        invocation = self.runtime.invoke(event)
        ids = {m["MessageId"] for m in messages}
        failed = self._failed_ids(invocation, ids)
        done = [m for m in messages if m["MessageId"] not in failed]
        for start in range(0, len(done), 10):
            chunk = done[start : start + 10]
            self.sqs.delete_message_batch(
                QueueUrl=self.queue.url,
                Entries=[
                    {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
                    for i, m in enumerate(chunk)
                ],
            )
            for m in chunk:
                self._in_flight.pop(m["MessageId"], None)
        for m in messages:
            self.deliveries.append(
                Delivery(
                    message_id=m["MessageId"],
                    body=m["Body"],
                    receive_count=int(
                        m.get("Attributes", {})["ApproximateReceiveCount"]
                    ),
                    request_id=invocation.request_id,
                    at=invocation.started,
                    succeeded=m["MessageId"] not in failed,
                )
            )
        return invocation


# --- EventBridge schedule ----------------------------------------------------------


@dataclass
class Schedule:
    runtime: LambdaRuntime
    spec: ScheduleSpec
    clock: SimClock
    next_at: float
    rule_arn: str

    def event(self) -> dict[str, Any]:
        return {
            "version": "0",
            "id": str(uuid.uuid4()),
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "account": ACCOUNT_ID,
            "time": self.clock.iso().replace(".000Z", "Z"),
            "region": REGION,
            "resources": [self.rule_arn],
            "detail": {},
        }

    def fire(self) -> list[Invocation]:
        """One scheduled run, invoked asynchronously (with the configured retries)."""
        self.next_at += self.spec.every_seconds
        return invoke_async(self.runtime, self.event(), self.clock)


def invoke_async(
    runtime: LambdaRuntime, event: Any, clock: SimClock
) -> list[Invocation]:
    """An ``Event`` invocation: on error, Lambda retries after 1 and then 2 minutes."""
    invocations = [runtime.invoke(event)]
    for attempt, delay in enumerate(ASYNC_RETRY_DELAYS[: runtime.async_retries], 2):
        if invocations[-1].outcome == "ok":
            break
        clock.advance(delay)
        invocations.append(runtime.invoke(event, attempt=attempt))
    return invocations
