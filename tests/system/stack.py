"""``template.yaml`` deployed into moto, with the settings the template gives.

The template is parsed with cfn-lint's decoder, intrinsic functions are resolved the
way CloudFormation would (parameters fall back to their ``Default``), and every
resource the handlers touch is created in moto: the secret, both tables with their
key schemas and GSI, the bucket with versioning and encryption, and both queues with
their visibility timeout, retention, size limit and redrive policy. Serverless
functions become ``FunctionSpec`` objects (handler, timeout, environment, events) for
``system.runtime`` to run. Logs, alarms and the bucket policy have no effect on what
the handlers can observe and are skipped; anything else unknown is an error, so a new
resource or property can't be silently ignored.
"""

import json
import re
import string
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import boto3
from cfnlint.decode import cfn_yaml  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_secretsmanager import SecretsManagerClient
    from mypy_boto3_sqs import SQSClient

ROOT = Path(__file__).parents[2]
TEMPLATE = ROOT / "template.yaml"

STACK_NAME = "sim-pipeline"
REGION = "us-east-1"
ACCOUNT_ID = "123456789012"  # moto's default account
PARTITION = "aws"

type Template = dict[str, Any]

SKIPPED_TYPES = frozenset(
    {"AWS::Logs::LogGroup", "AWS::CloudWatch::Alarm", "AWS::S3::BucketPolicy"}
)
_QUEUE_ATTRIBUTES = (
    "DelaySeconds",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    "ReceiveMessageWaitTimeSeconds",
    "VisibilityTimeout",
    "SqsManagedSseEnabled",
    "KmsMasterKeyId",
    "KmsDataKeyReusePeriodSeconds",
)
_JSON_QUEUE_ATTRIBUTES = ("RedrivePolicy", "RedriveAllowPolicy")
_RATE = re.compile(r"rate\((?P<n>\d+) (?P<unit>minutes?|hours?|days?)\)")
_UNIT_SECONDS = {"minute": 60, "hour": 3600, "day": 86400}


class TemplateError(RuntimeError):
    """The template uses something this harness does not know how to deploy."""


class _NoValue:
    """``AWS::NoValue``: the property is removed."""


NO_VALUE = _NoValue()


def load_template(path: Path = TEMPLATE) -> Template:
    # cfn-lint's loader turns !Ref/!GetAtt/!Sub into {"Ref": ...} and friends.
    loaded: Template = cfn_yaml.load(str(path))
    return loaded


@dataclass(frozen=True, slots=True)
class Physical:
    """What ``Ref`` and ``Fn::GetAtt`` return for one created resource."""

    ref: str
    attributes: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class QueueSpec:
    logical_id: str
    url: str
    arn: str
    name: str
    visibility_timeout: int
    max_receive_count: int | None
    dead_letter_arn: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqsEventSpec:
    queue_arn: str
    batch_size: int
    batching_window: int
    report_batch_item_failures: bool
    maximum_concurrency: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleSpec:
    every_seconds: int
    enabled: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class FunctionSpec:
    logical_id: str
    name: str
    handler: str
    timeout: int
    memory_mb: int
    environment: Mapping[str, str]
    sqs_events: tuple[SqsEventSpec, ...] = ()
    schedules: tuple[ScheduleSpec, ...] = ()
    async_retries: int = 2
    """``EventInvokeConfig.MaximumRetryAttempts``: retries of an asynchronous call."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Stack:
    template: Template
    parameters: Mapping[str, str]
    resources: Mapping[str, Physical]
    queues: Mapping[str, QueueSpec]
    functions: Mapping[str, FunctionSpec]
    outputs: Mapping[str, str]

    def queue_by_arn(self, arn: str) -> QueueSpec:
        return next(q for q in self.queues.values() if q.arn == arn)


class Resolver:
    """Evaluates ``Ref``, ``Fn::GetAtt``, ``Fn::Sub``, ``Fn::If`` and conditions."""

    def __init__(
        self,
        template: Template,
        parameters: Mapping[str, str],
        resources: Mapping[str, Physical],
    ) -> None:
        self._template = template
        self._parameters = parameters
        self._resources = resources

    def pseudo(self, name: str) -> str | _NoValue:
        values: dict[str, str | _NoValue] = {
            "AWS::StackName": STACK_NAME,
            "AWS::Region": REGION,
            "AWS::AccountId": ACCOUNT_ID,
            "AWS::Partition": PARTITION,
            "AWS::URLSuffix": "amazonaws.com",
            "AWS::NoValue": NO_VALUE,
        }
        if name not in values:
            raise TemplateError(f"unknown pseudo parameter {name}")
        return values[name]

    def ref(self, name: str) -> str | _NoValue:
        if name.startswith("AWS::"):
            return self.pseudo(name)
        if name in self._parameters:
            return self._parameters[name]
        if name in self._resources:
            return self._resources[name].ref
        raise _Unresolved(name)

    def get_att(self, logical_id: str, attribute: str) -> str:
        if logical_id not in self._resources:
            raise _Unresolved(logical_id)
        attributes = self._resources[logical_id].attributes
        if attribute not in attributes:
            raise TemplateError(f"{logical_id} has no attribute {attribute} here")
        return attributes[attribute]

    def condition(self, name: str) -> bool:
        return bool(self.resolve(self._template["Conditions"][name]))

    def resolve(self, value: Any) -> Any:
        if isinstance(value, list):
            items = [self.resolve(v) for v in value]
            return [i for i in items if i is not NO_VALUE]
        if not isinstance(value, dict):
            return value
        if len(value) == 1:
            ((key, arg),) = value.items()
            match key:
                case "Ref":
                    return self.ref(str(arg))
                case "Fn::GetAtt":
                    logical_id, attribute = (
                        str(arg).split(".", 1) if isinstance(arg, str) else arg
                    )
                    return self.get_att(str(logical_id), str(attribute))
                case "Fn::Sub":
                    return self._sub(arg)
                case "Fn::If":
                    name, if_true, if_false = arg
                    return self.resolve(if_true if self.condition(name) else if_false)
                case "Fn::Equals":
                    left, right = (self.resolve(a) for a in arg)
                    return str(left) == str(right)
                case "Fn::Not":
                    return not self.resolve(arg[0])
                case "Condition":
                    return self.condition(str(arg))
                case "Fn::Join":
                    separator, parts = arg
                    return str(separator).join(str(p) for p in self.resolve(parts))
        resolved = {str(k): self.resolve(v) for k, v in value.items()}
        return {k: v for k, v in resolved.items() if v is not NO_VALUE}

    def _sub(self, arg: Any) -> str:
        text, variables = (arg, {}) if isinstance(arg, str) else arg
        local = {str(k): self.resolve(v) for k, v in variables.items()}

        def substitute(match: re.Match[str]) -> str:
            name = match["name"]
            if name in local:
                return str(local[name])
            if "." in name and not name.startswith("AWS::"):
                logical_id, attribute = name.split(".", 1)
                return self.get_att(logical_id, attribute)
            return str(self.ref(name))

        return re.sub(r"\$\{(?P<name>[^}!]+)\}", substitute, str(text))


class _Unresolved(Exception):
    """A reference to a resource that hasn't been created yet."""


def parameter_values(
    template: Template, overrides: Mapping[str, object] | None = None
) -> dict[str, str]:
    """Every parameter's value: the override if given, else its ``Default``."""
    overrides = dict(overrides or {})
    declared = template.get("Parameters", {})
    if unknown := set(overrides) - set(declared):
        raise TemplateError(f"unknown parameters: {sorted(unknown)}")
    values: dict[str, str] = {}
    for name, spec in declared.items():
        value = overrides.get(name, spec.get("Default"))
        if value is None:
            raise TemplateError(f"parameter {name} has no default and no value")
        if "AllowedValues" in spec and str(value) not in map(
            str, spec["AllowedValues"]
        ):
            raise TemplateError(f"{name}={value!r} is not an allowed value")
        if spec.get("Type") == "Number":
            number = float(str(value))
            if "MinValue" in spec and number < float(spec["MinValue"]):
                raise TemplateError(f"{name}={value} is below MinValue")
            if "MaxValue" in spec and number > float(spec["MaxValue"]):
                raise TemplateError(f"{name}={value} is above MaxValue")
        values[str(name)] = str(value)
    return values


def _physical_name(logical_id: str, *, lower: bool = False) -> str:
    """A CloudFormation-style generated name: ``<stack>-<LogicalId>-<suffix>``."""
    suffix = "".join(
        string.ascii_uppercase[(ord(c) * 7 + i) % 26] for i, c in enumerate(logical_id)
    )[:12]
    name = f"{STACK_NAME}-{logical_id}-{suffix}"
    return name.lower() if lower else name


class _Deployer:
    def __init__(self, template: Template, parameters: Mapping[str, str]) -> None:
        self.template = template
        self.parameters = parameters
        self.resources: dict[str, Physical] = {}
        self.queues: dict[str, QueueSpec] = {}
        self.functions: dict[str, FunctionSpec] = {}
        self.resolver = Resolver(template, parameters, self.resources)
        session = boto3.Session(region_name=REGION)
        self.sqs: SQSClient = session.client("sqs")
        self.dynamodb: DynamoDBClient = session.client("dynamodb")
        self.s3: S3Client = session.client("s3")
        self.secrets: SecretsManagerClient = session.client("secretsmanager")
        self._creators: dict[str, Callable[[str, dict[str, Any]], Physical]] = {
            "AWS::SQS::Queue": self._queue,
            "AWS::DynamoDB::Table": self._table,
            "AWS::S3::Bucket": self._bucket,
            "AWS::SecretsManager::Secret": self._secret,
            "AWS::Serverless::Function": self._function,
        }

    def deploy(self) -> None:
        """Create resources in dependency order (retrying ones with dangling refs)."""
        pending: dict[str, Any] = {}
        for name, resource in self.template["Resources"].items():
            if resource["Type"] in SKIPPED_TYPES:
                # Not created, but others may name it (a function's log group).
                self.resources[str(name)] = Physical(_physical_name(str(name)))
            else:
                pending[str(name)] = resource
        if unknown := {r["Type"] for r in pending.values()} - set(self._creators):
            raise TemplateError(f"no simulation for resource types {sorted(unknown)}")
        while pending:
            progressed = False
            for name, resource in list(pending.items()):
                if "Condition" in resource and not self.resolver.condition(
                    resource["Condition"]
                ):
                    del pending[name]
                    continue
                try:
                    props = self.resolver.resolve(resource.get("Properties", {}))
                except _Unresolved:
                    continue
                self.resources[name] = self._creators[resource["Type"]](name, props)
                del pending[name]
                progressed = True
            if not progressed:
                raise TemplateError(
                    f"circular or dangling references: {sorted(pending)}"
                )

    def _queue(self, logical_id: str, props: dict[str, Any]) -> Physical:
        props = dict(props)
        name = str(props.pop("QueueName", None) or _physical_name(logical_id))
        props.pop("Tags", None)
        attributes: dict[str, str] = {}
        for key in _QUEUE_ATTRIBUTES:
            if key in props:
                value = props.pop(key)
                attributes[key] = (
                    str(value).lower() if isinstance(value, bool) else str(value)
                )
        for key in _JSON_QUEUE_ATTRIBUTES:
            if key in props:
                policy = props.pop(key)
                if "maxReceiveCount" in policy:
                    policy["maxReceiveCount"] = int(policy["maxReceiveCount"])
                attributes[key] = json.dumps(policy)
        if props:
            raise TemplateError(f"{logical_id}: unsupported properties {sorted(props)}")
        url = self.sqs.create_queue(QueueName=name, Attributes=cast(Any, attributes))[
            "QueueUrl"
        ]
        arn = f"arn:{PARTITION}:sqs:{REGION}:{ACCOUNT_ID}:{name}"
        redrive = json.loads(attributes.get("RedrivePolicy", "null")) or {}
        self.queues[logical_id] = QueueSpec(
            logical_id=logical_id,
            url=url,
            arn=arn,
            name=name,
            visibility_timeout=int(attributes.get("VisibilityTimeout", 30)),
            max_receive_count=redrive.get("maxReceiveCount"),
            dead_letter_arn=redrive.get("deadLetterTargetArn"),
        )
        return Physical(url, {"Arn": arn, "QueueName": name, "QueueUrl": url})

    def _table(self, logical_id: str, props: dict[str, Any]) -> Physical:
        props = dict(props)
        name = str(props.pop("TableName", None) or _physical_name(logical_id))
        request: dict[str, Any] = {
            "TableName": name,
            "BillingMode": props.pop("BillingMode", "PROVISIONED"),
            "AttributeDefinitions": props.pop("AttributeDefinitions"),
            "KeySchema": props.pop("KeySchema"),
        }
        for key in ("GlobalSecondaryIndexes", "LocalSecondaryIndexes"):
            if key in props:
                request[key] = props.pop(key)
        if "ProvisionedThroughput" in props:
            request["ProvisionedThroughput"] = props.pop("ProvisionedThroughput")
        if "SSESpecification" in props:
            sse = props.pop("SSESpecification")
            request["SSESpecification"] = {"Enabled": bool(sse.get("SSEEnabled"))}
        if "StreamSpecification" in props:
            stream = props.pop("StreamSpecification")
            request["StreamSpecification"] = {"StreamEnabled": True, **stream}
        ttl = props.pop("TimeToLiveSpecification", None)
        props.pop("PointInTimeRecoverySpecification", None)  # no effect on behavior
        props.pop("Tags", None)
        if props:
            raise TemplateError(f"{logical_id}: unsupported properties {sorted(props)}")
        self.dynamodb.create_table(**request)
        if ttl:
            self.dynamodb.update_time_to_live(
                TableName=name, TimeToLiveSpecification=ttl
            )
        arn = f"arn:{PARTITION}:dynamodb:{REGION}:{ACCOUNT_ID}:table/{name}"
        return Physical(name, {"Arn": arn})

    def _bucket(self, logical_id: str, props: dict[str, Any]) -> Physical:
        props = dict(props)
        name = str(
            props.pop("BucketName", None) or _physical_name(logical_id, lower=True)
        )
        self.s3.create_bucket(Bucket=name)
        if versioning := props.pop("VersioningConfiguration", None):
            self.s3.put_bucket_versioning(
                Bucket=name, VersioningConfiguration={"Status": versioning["Status"]}
            )
        if encryption := props.pop("BucketEncryption", None):
            # CloudFormation's ServerSideEncryptionByDefault is the API's
            # ApplyServerSideEncryptionByDefault.
            rules = [
                {
                    "ApplyServerSideEncryptionByDefault": rule[
                        "ServerSideEncryptionByDefault"
                    ],
                    **(
                        {"BucketKeyEnabled": rule["BucketKeyEnabled"]}
                        if "BucketKeyEnabled" in rule
                        else {}
                    ),
                }
                for rule in encryption["ServerSideEncryptionConfiguration"]
            ]
            configuration: Any = {"Rules": rules}
            self.s3.put_bucket_encryption(
                Bucket=name, ServerSideEncryptionConfiguration=configuration
            )
        if block := props.pop("PublicAccessBlockConfiguration", None):
            self.s3.put_public_access_block(
                Bucket=name, PublicAccessBlockConfiguration=block
            )
        if ownership := props.pop("OwnershipControls", None):
            self.s3.put_bucket_ownership_controls(
                Bucket=name, OwnershipControls=ownership
            )
        # moto doesn't run lifecycle rules; they act over days, outside any scenario.
        props.pop("LifecycleConfiguration", None)
        props.pop("Tags", None)
        if props:
            raise TemplateError(f"{logical_id}: unsupported properties {sorted(props)}")
        return Physical(name, {"Arn": f"arn:{PARTITION}:s3:::{name}"})

    def _secret(self, logical_id: str, props: dict[str, Any]) -> Physical:
        name = _physical_name(logical_id)
        response = self.secrets.create_secret(
            Name=name, Description=str(props.get("Description", ""))
        )
        return Physical(response["ARN"], {"Id": response["ARN"]})

    def _function(self, logical_id: str, props: dict[str, Any]) -> Physical:
        globals_ = self.resolver.resolve(
            self.template.get("Globals", {}).get("Function", {})
        )
        merged = {**globals_, **props}
        environment = {
            **globals_.get("Environment", {}).get("Variables", {}),
            **props.get("Environment", {}).get("Variables", {}),
        }
        name = _physical_name(logical_id)
        sqs_events: list[SqsEventSpec] = []
        schedules: list[ScheduleSpec] = []
        for event_name, event in merged.get("Events", {}).items():
            event_props = event.get("Properties", {})
            match event["Type"]:
                case "SQS":
                    scaling = event_props.get("ScalingConfig", {})
                    sqs_events.append(
                        SqsEventSpec(
                            queue_arn=str(event_props["Queue"]),
                            batch_size=int(event_props.get("BatchSize", 10)),
                            batching_window=int(
                                event_props.get("MaximumBatchingWindowInSeconds", 0)
                            ),
                            report_batch_item_failures="ReportBatchItemFailures"
                            in event_props.get("FunctionResponseTypes", []),
                            maximum_concurrency=scaling.get("MaximumConcurrency"),
                        )
                    )
                case "Schedule":
                    schedules.append(
                        ScheduleSpec(
                            every_seconds=rate_seconds(str(event_props["Schedule"])),
                            enabled=event_props.get("State", "ENABLED") == "ENABLED"
                            and event_props.get("Enabled", True) is not False,
                        )
                    )
                case other:
                    raise TemplateError(f"{logical_id}.{event_name}: no {other} events")
        spec = FunctionSpec(
            logical_id=logical_id,
            name=name,
            handler=str(merged["Handler"]),
            timeout=int(merged.get("Timeout", 3)),
            memory_mb=int(merged.get("MemorySize", 128)),
            environment={str(k): str(v) for k, v in environment.items()},
            sqs_events=tuple(sqs_events),
            schedules=tuple(schedules),
            async_retries=int(
                merged.get("EventInvokeConfig", {}).get("MaximumRetryAttempts", 2)
            ),
        )
        self.functions[logical_id] = spec
        arn = f"arn:{PARTITION}:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
        return Physical(name, {"Arn": arn})


def rate_seconds(expression: str) -> int:
    """Seconds between runs of an EventBridge ``rate(...)`` expression."""
    match = _RATE.fullmatch(expression.strip())
    if match is None:
        raise TemplateError(f"only rate() schedules are simulated, got {expression!r}")
    unit = match["unit"].removesuffix("s")
    return int(match["n"]) * _UNIT_SECONDS[unit]


def deploy(
    parameters: Mapping[str, object] | None = None,
    template: Template | None = None,
) -> Stack:
    """Create the stack's resources in the active moto mock; return what was made."""
    template = load_template() if template is None else template
    values = parameter_values(template, parameters)
    deployer = _Deployer(template, values)
    deployer.deploy()
    outputs = {
        str(name): str(deployer.resolver.resolve(output["Value"]))
        for name, output in template.get("Outputs", {}).items()
        if not set(_references(output["Value"])) - set(deployer.resources)
    }
    return Stack(
        template=template,
        parameters=values,
        resources=dict(deployer.resources),
        queues=dict(deployer.queues),
        functions=dict(deployer.functions),
        outputs=outputs,
    )


def _references(value: Any) -> list[str]:
    """Logical IDs a value refers to through ``Ref``/``Fn::GetAtt``."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, arg in value.items():
            if key == "Ref" and not str(arg).startswith("AWS::"):
                found.append(str(arg))
            elif key == "Fn::GetAtt":
                found.append(str(arg).split(".")[0] if isinstance(arg, str) else arg[0])
            else:
                found += _references(arg)
    elif isinstance(value, list):
        for item in value:
            found += _references(item)
    return found
