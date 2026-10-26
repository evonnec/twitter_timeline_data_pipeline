"""Fixtures for the integration tests.

These tests need the stack that ``scripts/integration_test.sh up`` starts (LocalStack,
the stub X API and CDN, and one Lambda runtime container per function), so they are
skipped from collection unless ``RUN_INTEGRATION=1``. The script sets that and the
``IT_*`` variables read below.
"""

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import httpx
import pytest

from integration.esm import Poller
from integration.provision import Queue, Resources
from integration.rie import Invoke, rie_invoker

# Not collected in a normal run: they would fail without the stack.
collect_ignore_glob = [] if os.environ.get("RUN_INTEGRATION") == "1" else ["test_*.py"]

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sqs import SQSClient


def endpoint() -> str | None:
    """LocalStack's URL from here; empty means the default AWS endpoints (moto)."""
    return os.environ.get("IT_AWS_ENDPOINT_URL") or None


def env(name: str) -> str:
    try:
        return os.environ[name]
    except KeyError:
        pytest.fail(f"{name} is not set; run these through scripts/integration_test.sh")


@pytest.fixture(scope="session")
def resources() -> Resources:
    data = json.loads((Path(env("IT_OUT_DIR")) / "resources.json").read_text())
    return Resources(
        **{
            **data,
            "tweets": Queue(**data["tweets"]),
            "media_retry": Queue(**data["media_retry"]),
        }
    )


@pytest.fixture(scope="session")
def sqs(resources: Resources) -> SQSClient:
    return boto3.client("sqs", region_name=resources.region, endpoint_url=endpoint())


@pytest.fixture(scope="session")
def s3(resources: Resources) -> S3Client:
    return boto3.client("s3", region_name=resources.region, endpoint_url=endpoint())


@pytest.fixture(scope="session")
def dynamodb(resources: Resources) -> Any:
    return boto3.resource(
        "dynamodb",
        region_name=resources.region,
        endpoint_url=endpoint(),
    )


@pytest.fixture(scope="session")
def tweets_table(dynamodb: Any, resources: Resources) -> Table:
    table: Table = dynamodb.Table(resources.tweets_table)
    return table


@pytest.fixture(scope="session")
def state_table(dynamodb: Any, resources: Resources) -> Table:
    table: Table = dynamodb.Table(resources.state_table)
    return table


@pytest.fixture(scope="session")
def fetch() -> Invoke:
    return rie_invoker(env("IT_FETCH_URL"))


@pytest.fixture(scope="session")
def process() -> Invoke:
    return rie_invoker(env("IT_PROCESS_URL"))


@pytest.fixture(scope="session")
def media_retry() -> Invoke:
    return rie_invoker(env("IT_MEDIA_RETRY_URL"))


class Stub:
    """The stub X API and CDN, as seen from the tests."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def reset(self) -> None:
        httpx.post(f"{self.base_url}/__reset").raise_for_status()

    def requests(self) -> list[dict[str, Any]]:
        response = httpx.get(f"{self.base_url}/__requests")
        response.raise_for_status()
        requests: list[dict[str, Any]] = response.json()
        return requests


@pytest.fixture
def stub() -> Stub:
    server = Stub(env("IT_STUB_URL"))
    server.reset()
    return server


def _empty(table: Table) -> None:
    keys = [k["AttributeName"] for k in table.key_schema]
    with table.batch_writer() as batch:
        for item in table.scan().get("Items", []):
            batch.delete_item(Key={k: item[k] for k in keys})


def _drain(sqs: SQSClient, url: str) -> None:
    """Receive and delete until the queue stays empty. (PurgeQueue is limited to one
    call a minute per queue, so it can't run before every test.)"""
    quiet = 0
    while quiet < 2:
        messages = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10).get(
            "Messages", []
        )
        quiet = 0 if messages else quiet + 1
        for message in messages:
            sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])


@pytest.fixture(autouse=True)
def clean_slate(
    sqs: SQSClient,
    s3: S3Client,
    tweets_table: Table,
    state_table: Table,
    resources: Resources,
) -> Iterator[None]:
    """Empty every queue, table and the bucket before each test."""
    for queue in (resources.tweets, resources.media_retry):
        for url in (queue.url, queue.dlq_url):
            _drain(sqs, url)
    _empty(tweets_table)
    _empty(state_table)
    for page in s3.get_paginator("list_object_versions").paginate(
        Bucket=resources.bucket
    ):
        for v in (*page.get("Versions", []), *page.get("DeleteMarkers", [])):
            s3.delete_object(
                Bucket=resources.bucket, Key=v["Key"], VersionId=v["VersionId"]
            )
    yield


@pytest.fixture
def tweets_poller(sqs: SQSClient, resources: Resources, process: Invoke) -> Poller:
    return Poller(
        sqs=sqs,
        queue_url=resources.tweets.url,
        queue_arn=resources.tweets.arn,
        region=resources.region,
        invoke=process,
        batch_size=resources.tweets.batch_size,
    )
