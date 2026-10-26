"""Shared fixtures: fake AWS credentials, moto resources, cache resets."""

from collections.abc import Iterator
from dataclasses import dataclass

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from twitter_timeline_data_pipeline import credentials
from twitter_timeline_data_pipeline.config import ENV_VARS
from twitter_timeline_data_pipeline.handlers import fetch, process

REGION = "us-east-1"
BUCKET = "test-media-bucket"
TWEETS_TABLE = "test-tweets"
STATE_TABLE = "test-state"


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake credentials and region so no test can reach real AWS, and a clean env."""
    for name in ENV_VARS.values():
        monkeypatch.delenv(name, raising=False)
    for name in ("AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")


@pytest.fixture(autouse=True)
def clear_caches() -> Iterator[None]:
    """Module-level caches would otherwise carry moto clients between tests."""

    def clear() -> None:
        fetch.sqs_client.cache_clear()
        fetch.checkpoint_store.cache_clear()
        process._s3_client.cache_clear()
        process._sqs_client.cache_clear()
        process._http_client.cache_clear()
        process._tweet_store.cache_clear()
        credentials.clear_cache()

    clear()
    yield
    clear()


@dataclass(frozen=True, slots=True)
class Aws:
    s3: S3Client
    sqs: SQSClient
    dynamodb: DynamoDBServiceResource
    tweets_table: Table
    state_table: Table
    queue_url: str
    retry_queue_url: str
    bucket: str = BUCKET


@pytest.fixture
def aws() -> Iterator[Aws]:
    """Moto-backed bucket, queue and tables with the key schemas of template.yaml."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        tweets = dynamodb.create_table(
            TableName=TWEETS_TABLE,
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "tweet_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "tweet_id", "AttributeType": "S"},
                {"AttributeName": "username", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
                {"AttributeName": "tombstone", "AttributeType": "S"},
                {"AttributeName": "deleted_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "username-created_at-index",
                    "KeySchema": [
                        {"AttributeName": "username", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "tombstone-deleted_at-index",
                    "KeySchema": [
                        {"AttributeName": "tombstone", "KeyType": "HASH"},
                        {"AttributeName": "deleted_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                },
            ],
        )
        state = dynamodb.create_table(
            TableName=STATE_TABLE,
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "username", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "username", "AttributeType": "S"}],
        )
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        sqs = boto3.client("sqs")
        queue_url = sqs.create_queue(QueueName="test-tweets")["QueueUrl"]
        retry_queue_url = sqs.create_queue(QueueName="test-media-retry")["QueueUrl"]
        yield Aws(
            s3=s3,
            sqs=sqs,
            dynamodb=dynamodb,
            tweets_table=tweets,
            state_table=state,
            queue_url=queue_url,
            retry_queue_url=retry_queue_url,
        )


@pytest.fixture
def pipeline_env(monkeypatch: pytest.MonkeyPatch, aws: Aws) -> Aws:
    """Environment variables the handlers read, pointing at the moto resources."""
    monkeypatch.setenv("QUEUE_URL", aws.queue_url)
    monkeypatch.setenv("MEDIA_RETRY_QUEUE_URL", aws.retry_queue_url)
    monkeypatch.setenv("STATE_TABLE", STATE_TABLE)
    monkeypatch.setenv("TWEETS_TABLE", TWEETS_TABLE)
    monkeypatch.setenv("MEDIA_BUCKET", BUCKET)
    monkeypatch.setenv("TWITTER_BEARER_TOKEN", "test-bearer-token-not-real")
    monkeypatch.setenv("TWITTER_USERNAME", "example_user")
    return aws
