"""Create the pipeline's AWS resources in LocalStack, from ``template.yaml``.

LocalStack's community edition can't deploy this template end to end (the stack's
Python 3.14 functions have no emulated runtime there), so this creates the same tables,
bucket, queues and secret from the template's own properties, and works out each
function's environment from the template's ``Environment`` blocks. The functions then
run in the real Lambda runtime image (see ``scripts/integration_test.sh``).

What differs from a deployment, on purpose: queue names carry a test prefix, the
visibility timeouts are seconds not minutes, and ``maxReceiveCount`` is small, so a
poison message reaches the dead-letter queue quickly. Encryption, point-in-time
recovery, lifecycle rules, alarms, IAM and the schedule are not emulated.

Run as a script, it writes ``resources.json`` and one ``<function>.env`` per function
(docker ``--env-file`` format) into ``--out-dir``.
"""

import argparse
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import boto3
from cfnlint.decode import cfn_yaml  # type: ignore[import-untyped]

from integration.stub_server import TOKEN, USERNAME

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sqs import SQSClient

TEMPLATE = Path(__file__).resolve().parents[2] / "template.yaml"
ACCOUNT_ID = "000000000000"  # LocalStack's default account
STACK_NAME = "it"  # what ${AWS::StackName} stands for in the functions' variables
FUNCTIONS = ("FetchFunction", "ProcessFunction", "MediaRetryFunction")
MEDIA_HOST_OVERRIDE = "host.docker.internal"
VISIBILITY_TIMEOUT_SECONDS = 2

type Template = dict[str, Any]


@dataclass(frozen=True, slots=True)
class Queue:
    name: str
    url: str
    arn: str
    dlq_name: str
    dlq_url: str
    batch_size: int


@dataclass(frozen=True, slots=True)
class Resources:
    region: str
    max_receive_count: int
    tweets_table: str
    state_table: str
    bucket: str
    secret_arn: str
    tweets: Queue
    media_retry: Queue


def load_template() -> Template:
    loaded: Template = cfn_yaml.load(str(TEMPLATE))
    return loaded


def plain(value: Any) -> Any:
    """The cfn-lint loader's dict/list subclasses as plain JSON types."""
    return json.loads(json.dumps(value))


def _create_table(ddb: DynamoDBClient, name: str, props: Mapping[str, Any]) -> None:
    kwargs: dict[str, Any] = {
        key: plain(props[key])
        for key in (
            "AttributeDefinitions",
            "KeySchema",
            "BillingMode",
            "GlobalSecondaryIndexes",
        )
        if key in props
    }
    ddb.create_table(TableName=name, **kwargs)
    ddb.get_waiter("table_exists").wait(TableName=name)


def _create_bucket(
    s3: S3Client, name: str, props: Mapping[str, Any], region: str
) -> None:
    if region == "us-east-1":
        s3.create_bucket(Bucket=name)
    else:
        s3.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={"LocationConstraint": cast("Any", region)},
        )
    status = props["VersioningConfiguration"]["Status"]
    s3.put_bucket_versioning(
        Bucket=name, VersioningConfiguration={"Status": cast("Any", status)}
    )


def _queue_arn(sqs: SQSClient, url: str) -> str:
    attributes = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return attributes["Attributes"]["QueueArn"]


def _create_queue(
    sqs: SQSClient,
    template: Template,
    *,
    queue: str,
    dlq: str,
    name: str,
    batch_size: int,
    max_receive_count: int,
) -> Queue:
    def size(logical: str) -> dict[Any, str]:
        props = template["Resources"][logical]["Properties"]
        return {"MaximumMessageSize": str(props["MaximumMessageSize"])}

    dlq_name = f"{name}-dlq"
    dlq_url = sqs.create_queue(QueueName=dlq_name, Attributes=size(dlq))["QueueUrl"]
    url = sqs.create_queue(
        QueueName=name,
        Attributes={
            **size(queue),
            "VisibilityTimeout": str(VISIBILITY_TIMEOUT_SECONDS),
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": _queue_arn(sqs, dlq_url),
                    "maxReceiveCount": max_receive_count,
                }
            ),
        },
    )["QueueUrl"]
    return Queue(name, url, _queue_arn(sqs, url), dlq_name, dlq_url, batch_size)


def provision(
    *,
    endpoint_url: str | None,
    region: str,
    prefix: str,
    max_receive_count: int,
    template: Template | None = None,
) -> Resources:
    """Create everything in the service at ``endpoint_url`` (None: the default one,
    e.g. under moto) and return what was created."""
    template = template or load_template()
    res = template["Resources"]
    kwargs: dict[str, Any] = {"region_name": region, "endpoint_url": endpoint_url}
    ddb = boto3.client("dynamodb", **kwargs)
    s3 = boto3.client("s3", **kwargs)
    sqs = boto3.client("sqs", **kwargs)
    secrets = boto3.client("secretsmanager", **kwargs)

    tweets_table, state_table = f"{prefix}-tweets", f"{prefix}-state"
    _create_table(ddb, tweets_table, res["TweetsTable"]["Properties"])
    _create_table(ddb, state_table, res["StateTable"]["Properties"])
    bucket = f"{prefix}-media"
    _create_bucket(s3, bucket, res["MediaBucket"]["Properties"], region)
    secret = secrets.create_secret(Name=f"{prefix}-bearer-token", SecretString=TOKEN)

    def batch_size(function: str, event: str) -> int:
        events = res[function]["Properties"]["Events"]
        return int(events[event]["Properties"]["BatchSize"])

    return Resources(
        region=region,
        max_receive_count=max_receive_count,
        tweets_table=tweets_table,
        state_table=state_table,
        bucket=bucket,
        secret_arn=secret["ARN"],
        tweets=_create_queue(
            sqs,
            template,
            queue="TweetsQueue",
            dlq="TweetsDeadLetterQueue",
            name=f"{prefix}-tweets",
            batch_size=batch_size("ProcessFunction", "TweetsQueueEvent"),
            max_receive_count=max_receive_count,
        ),
        media_retry=_create_queue(
            sqs,
            template,
            queue="MediaRetryQueue",
            dlq="MediaRetryDeadLetterQueue",
            name=f"{prefix}-media-retry",
            batch_size=batch_size("MediaRetryFunction", "MediaRetryQueueEvent"),
            max_receive_count=max_receive_count,
        ),
    )


def function_env(
    template: Template,
    function: str,
    resources: Resources,
    *,
    internal_endpoint: str,
    stub_url_internal: str,
) -> dict[str, str]:
    """The function's environment: its template variables with every ``!Ref``,
    ``!If`` and ``!Sub`` resolved against ``resources`` and the parameter defaults,
    plus the test wiring (endpoint, fake credentials, stub X API, media host override).

    Raises ``KeyError`` for a reference or intrinsic function this module can't
    resolve, so a new variable in the template can't silently go missing from the
    integration run.
    """

    def queue_url(queue: Queue) -> str:
        # Path-style URL that works from inside the docker network.
        return f"{internal_endpoint}/{ACCOUNT_ID}/{queue.name}"

    refs: dict[str, str] = {
        name: str(parameter["Default"])
        for name, parameter in template["Parameters"].items()
    }
    refs.update(
        {
            "TweetsQueue": queue_url(resources.tweets),
            "MediaRetryQueue": queue_url(resources.media_retry),
            "TweetsTable": resources.tweets_table,
            "StateTable": resources.state_table,
            "MediaBucket": resources.bucket,
            "BearerTokenSecret": resources.secret_arn,
            # Parameters that a test sets to another value than the default.
            "TwitterUsername": USERNAME,
            "MaxReceiveCount": str(resources.max_receive_count),
            "AWS::AccountId": ACCOUNT_ID,
            "AWS::Region": resources.region,
            "AWS::StackName": STACK_NAME,
        }
    )
    conditions: Mapping[str, Any] = template.get("Conditions", {})

    def resolve(value: Any) -> str:
        if not isinstance(value, Mapping):
            return str(value)
        ((intrinsic, argument),) = value.items()
        match intrinsic:
            case "Ref":
                return refs[argument]
            case "Fn::If":
                condition, if_true, if_false = argument
                return resolve(if_true if holds(conditions[condition]) else if_false)
            case "Fn::Sub" if isinstance(argument, str):
                return re.sub(r"\$\{([^!}][^}]*)\}", lambda m: refs[m[1]], argument)
        raise KeyError(intrinsic)

    def holds(condition: Mapping[str, Any]) -> bool:
        ((intrinsic, argument),) = condition.items()
        match intrinsic:
            case "Fn::Equals":
                left, right = argument
                return resolve(left) == resolve(right)
            case "Fn::Not":
                return not holds(argument[0])
            case "Fn::And":
                return all(holds(c) for c in argument)
            case "Fn::Or":
                return any(holds(c) for c in argument)
            case "Condition":
                return holds(conditions[argument])
        raise KeyError(intrinsic)

    variables = template["Resources"][function]["Properties"]["Environment"][
        "Variables"
    ]
    env = {key: resolve(value) for key, value in variables.items()}
    env.update(
        {
            "AWS_ENDPOINT_URL": internal_endpoint,
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_REGION": resources.region,
            "AWS_DEFAULT_REGION": resources.region,
            "X_API_BASE_URL": f"{stub_url_internal}/2",
            "HTTP_TIMEOUT_SECONDS": "10",
            "MEDIA_ALLOWED_HOSTS_OVERRIDE": MEDIA_HOST_OVERRIDE,
        }
    )
    return env


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--endpoint-url", required=True, help="LocalStack, from here")
    parser.add_argument(
        "--internal-endpoint", required=True, help="LocalStack, from the containers"
    )
    parser.add_argument(
        "--stub-url-internal",
        required=True,
        help="the stub server, from the containers",
    )
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--prefix", default="it")
    parser.add_argument("--max-receive-count", type=int, default=3)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    resources = provision(
        endpoint_url=args.endpoint_url,
        region=args.region,
        prefix=args.prefix,
        max_receive_count=args.max_receive_count,
    )
    template = load_template()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "resources.json").write_text(
        json.dumps(asdict(resources), indent=2) + "\n"
    )
    for function in FUNCTIONS:
        env = function_env(
            template,
            function,
            resources,
            internal_endpoint=args.internal_endpoint,
            stub_url_internal=args.stub_url_internal,
        )
        lines = "".join(f"{k}={v}\n" for k, v in env.items())
        (args.out_dir / f"{function}.env").write_text(lines)
    print(f"provisioned {resources.tweets.name} and friends; wrote {args.out_dir}")


if __name__ == "__main__":
    main()
