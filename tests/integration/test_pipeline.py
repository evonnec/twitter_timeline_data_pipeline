"""Fetch -> SQS -> process -> DynamoDB and S3, with the bundle in the Lambda runtime
image and the AWS services in LocalStack (see ``integration/esm.py`` and
``integration/provision.py`` for what is emulated)."""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from helpers import make_message

from integration.esm import Poller
from integration.provision import Resources
from integration.rie import Invoke
from integration.stub_server import (
    MISSING_KEY,
    MISSING_MEDIA_TWEET,
    PHOTO_BYTES,
    PHOTO_KEY,
    PHOTO_TWEET,
    TEXT_TWEET,
    TOKEN,
    TWEET_IDS,
    USER_ID,
    USERNAME,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sqs import SQSClient

    from integration.conftest import Stub


def stored_ids(table: Table) -> set[str]:
    return {str(item["tweet_id"]) for item in table.scan().get("Items", [])}


def queue_depth(sqs: SQSClient, url: str) -> int:
    attributes = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"])


def test_fetched_tweets_land_in_dynamodb_and_media_in_s3(
    fetch: Invoke,
    tweets_poller: Poller,
    tweets_table: Table,
    state_table: Table,
    s3: S3Client,
    resources: Resources,
    stub: Stub,
) -> None:
    fetched = fetch({"username": USERNAME}).payload
    assert fetched["enqueued"] == len(TWEET_IDS)

    tweets_poller.run_until(lambda: stored_ids(tweets_table) == set(TWEET_IDS))

    assert tweets_poller.failures == []
    items: dict[str, dict[str, Any]] = {
        str(i["tweet_id"]): cast("dict[str, Any]", i)
        for i in tweets_table.scan()["Items"]
    }
    assert items[TEXT_TWEET]["text"] == "Text only"
    media = {m["media_key"]: m for m in items[PHOTO_TWEET]["media"]}
    key = f"media/{USER_ID}/{PHOTO_TWEET}/{PHOTO_KEY}.jpg"
    assert media[PHOTO_KEY]["status"] == "stored"
    assert media[PHOTO_KEY]["s3_key"] == key
    body = s3.get_object(Bucket=resources.bucket, Key=key)["Body"].read()
    assert body == PHOTO_BYTES
    # Media that is gone at the source is recorded, not retried forever.
    (gone,) = items[MISSING_MEDIA_TWEET]["media"]
    assert (gone["media_key"], gone["status"]) == (MISSING_KEY, "unavailable")

    seen = stub.requests()
    api = [r for r in seen if r["path"].startswith("/2/")]
    assert api
    assert {r["authorization"] for r in api} == {f"Bearer {TOKEN}"}  # via the secret
    assert any(r["path"].startswith("/media/") for r in seen)

    # The checkpoint was saved, so the next run asks only for newer Tweets.
    assert state_table.scan()["Items"]
    again = fetch({"username": USERNAME}).payload
    assert again["enqueued"] == 0
    timeline = [r for r in stub.requests() if r["path"].endswith("/tweets")][-1]
    assert timeline["query"]["since_id"] == PHOTO_TWEET


def test_a_poison_message_is_redelivered_alone_and_ends_in_the_dlq(
    sqs: SQSClient,
    tweets_poller: Poller,
    tweets_table: Table,
    resources: Resources,
) -> None:
    good = make_message("1800000000000000001", username=USERNAME)
    poison = "{this is not json"
    sent = sqs.send_message_batch(
        QueueUrl=resources.tweets.url,
        Entries=[
            {"Id": "good", "MessageBody": good.to_json()},
            {"Id": "poison", "MessageBody": poison},
        ],
    )
    ids = {s["Id"]: s["MessageId"] for s in sent["Successful"]}
    dlq = resources.tweets.dlq_url

    tweets_poller.run_until(lambda: queue_depth(sqs, dlq) >= 1)

    # The good record was written and acknowledged on its first delivery. Only the
    # poison record came back, once per receive until maxReceiveCount.
    assert stored_ids(tweets_table) == {good.tweet.id}
    assert tweets_poller.deliveries[ids["good"]] == [1]
    assert tweets_poller.deliveries[ids["poison"]] == list(
        range(1, resources.max_receive_count + 1)
    )
    assert set(tweets_poller.failures) == {ids["poison"]}
    assert len(tweets_poller.failures) == resources.max_receive_count
    (moved,) = sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=10)["Messages"]
    assert moved["Body"] == poison
    assert queue_depth(sqs, resources.tweets.url) == 0


def test_the_poller_builds_the_keys_of_the_sample_sqs_event() -> None:
    """Guards the hand-built SQS event against drifting from events/sqs.json."""
    sample = json.loads((Path(__file__).parents[2] / "events" / "sqs.json").read_text())
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
