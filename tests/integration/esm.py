"""A small stand-in for Lambda's SQS event source mapping.

LocalStack's community edition can't run this project's Python 3.14 bundle itself, so
the Lambdas run in the real runtime image and this poller plays the event source
mapping: it receives up to a batch of messages, invokes the function with the SQS
event shape, and deletes only the records the function did not list in
``batchItemFailures`` (``ReportBatchItemFailures``). A failed invocation deletes
nothing. Everything else is the real service: the queue's visibility timeout, the
receive counts and the move to the dead-letter queue after ``maxReceiveCount``
receives are LocalStack's SQS.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from integration.rie import Invoke

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient


@dataclass
class Poller:
    sqs: SQSClient
    queue_url: str
    queue_arn: str
    region: str
    invoke: Invoke
    batch_size: int = 10
    deliveries: dict[str, list[int]] = field(default_factory=dict)
    """message id -> receive count of each delivery to the function, in order."""
    batches: list[list[str]] = field(default_factory=list)
    """The message ids of each invocation, in order."""
    failures: list[str] = field(default_factory=list)
    """Every id the function reported as failed, one entry per report."""

    def _record(self, message: Any) -> dict[str, Any]:
        return {
            "messageId": message["MessageId"],
            "receiptHandle": message["ReceiptHandle"],
            "body": message["Body"],
            "attributes": message["Attributes"],
            "messageAttributes": message.get("MessageAttributes", {}),
            "md5OfBody": message["MD5OfBody"],
            "eventSource": "aws:sqs",
            "eventSourceARN": self.queue_arn,
            "awsRegion": self.region,
        }

    def poll_once(self, wait_seconds: int = 1) -> list[str]:
        """One receive and (if anything arrived) one invocation.

        Returns the ids reported as failed; empty when the queue had nothing.
        """
        received = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.batch_size,
            WaitTimeSeconds=wait_seconds,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        ).get("Messages", [])
        if not received:
            return []
        records = [self._record(m) for m in received]
        for r in records:
            self.deliveries.setdefault(r["messageId"], []).append(
                int(r["attributes"]["ApproximateReceiveCount"])
            )
        self.batches.append([r["messageId"] for r in records])
        result = self.invoke({"Records": records}).payload
        failed = [f["itemIdentifier"] for f in result["batchItemFailures"]]
        self.failures += failed
        for r in records:
            if r["messageId"] not in failed:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url, ReceiptHandle=r["receiptHandle"]
                )
        return failed

    def run_until(
        self, done: Callable[[], bool], *, timeout: float = 120.0, wait_seconds: int = 1
    ) -> None:
        """Poll until ``done()`` is true; fail with the delivery history on timeout."""
        deadline = time.monotonic() + timeout
        while not done():
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"condition not met in {timeout}s; deliveries={self.deliveries}"
                )
            self.poll_once(wait_seconds)
