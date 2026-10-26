"""The integration poller's hand-built SQS event, checked without any stack."""

import json
from pathlib import Path
from typing import Any, cast

from integration.esm import Poller

EVENTS = Path(__file__).parents[1] / "events"


def test_the_poller_builds_the_keys_of_the_sample_sqs_event() -> None:
    """Guards the hand-built SQS event against drifting from events/sqs.json."""
    sample = json.loads((EVENTS / "sqs.json").read_text())
    poller = Poller(
        sqs=cast("Any", None),
        queue_url="",
        queue_arn="",
        region="us-east-1",
        invoke=lambda _event: cast("Any", None),
    )
    message = {
        "MessageId": "m",
        "ReceiptHandle": "r",
        "Body": "{}",
        "Attributes": {},
        "MD5OfBody": "x",
    }

    assert set(poller._record(message)) == set(sample["Records"][0])
