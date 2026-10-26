"""Fetch -> SQS -> process -> DynamoDB and S3, with the bundle in the Lambda runtime
image and the AWS services in LocalStack (see ``integration/esm.py`` and
``integration/provision.py`` for what is emulated)."""

from typing import TYPE_CHECKING, Any, cast

from helpers import make_message, photo

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
from twitter_timeline_data_pipeline.models import MediaRetry

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
    assert len(ids) == 2
    dlq = resources.tweets.dlq_url

    tweets_poller.run_until(lambda: queue_depth(sqs, dlq) >= 1)

    # Both records arrived in one batch, so the redelivery of the poison one alone shows
    # the partial batch response at work, not two separate receives.
    assert set(tweets_poller.batches[0]) == set(ids.values())
    assert len(tweets_poller.batches[0]) == 2
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


def retry_queue_counts(sqs: SQSClient, url: str) -> dict[str, int]:
    attributes = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])[
        "Attributes"
    ]
    return {
        "visible": int(attributes["ApproximateNumberOfMessages"]),
        "delayed": int(attributes["ApproximateNumberOfMessagesDelayed"]),
    }


def test_media_that_fails_until_the_last_receive_is_retried_and_then_stored(
    sqs: SQSClient,
    tweets_poller: Poller,
    media_retry_poller: Poller,
    tweets_table: Table,
    s3: S3Client,
    resources: Resources,
    stub: Stub,
) -> None:
    """The CDN answers 503, so the process function runs out of receives, stores the
    Tweet with the media as ``retrying`` and queues a media-retry message. Once the CDN
    is back, the media-retry function stores the file and replaces that entry."""
    media = photo(url=f"{stub.internal_url}/media/Flaky.jpg")
    message = make_message("1800000000000000002", username=USERNAME, media=(media,))
    sqs.send_message(QueueUrl=resources.tweets.url, MessageBody=message.to_json())

    tweets_poller.run_until(lambda: message.tweet.id in stored_ids(tweets_table))

    # Failed on receives 1 and 2, then rescued on the last one.
    (delivered,) = tweets_poller.deliveries.values()
    assert delivered == list(range(1, resources.max_receive_count + 1))
    item = tweets_table.get_item(Key={"tweet_id": message.tweet.id})["Item"]
    (entry,) = cast("list[dict[str, Any]]", item["media"])
    assert entry["status"] == "retrying"
    s3_key = str(entry["s3_key"])
    assert "Contents" not in s3.list_objects_v2(Bucket=resources.bucket, Prefix=s3_key)
    # The process function queued the retry, delayed (5 minutes in the template).
    delayed = retry_queue_counts(sqs, resources.media_retry.url)
    assert delayed == {"visible": 0, "delayed": 1}

    stub.flaky_media_up()
    # The queued copy won't be visible for minutes, so send an undelayed one with the
    # same body the process function built, and let the real function handle that.
    retry = MediaRetry(
        tweet_id=message.tweet.id,
        username=message.username,
        author_id=message.tweet.author_id,
        fetched_at=message.fetched_at,
        media=media,
    )
    sqs.send_message(QueueUrl=resources.media_retry.url, MessageBody=retry.to_json())

    def stored() -> bool:
        current = tweets_table.get_item(Key={"tweet_id": message.tweet.id})["Item"]
        (now,) = cast("list[dict[str, Any]]", current["media"])
        return bool(now["status"] == "stored")

    media_retry_poller.run_until(stored)

    assert media_retry_poller.failures == []
    body = s3.get_object(Bucket=resources.bucket, Key=s3_key)["Body"].read()
    assert body == PHOTO_BYTES
