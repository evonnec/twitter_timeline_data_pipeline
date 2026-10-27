"""Invariants of template.yaml that the code depends on, plus a cfn-lint pass."""

import ast
import importlib
import inspect
import json
from pathlib import Path
from typing import Any

import pytest
from cfnlint.api import lint_all  # type: ignore[import-untyped]
from cfnlint.decode import cfn_yaml  # type: ignore[import-untyped]

from twitter_timeline_data_pipeline.config import ENV_VARS
from twitter_timeline_data_pipeline.storage import MEDIA_HOSTS_OVERRIDE_ENV

ROOT = Path(__file__).parent.parent
TEMPLATE = ROOT / "template.yaml"
SRC = ROOT / "src" / "twitter_timeline_data_pipeline"

type Template = dict[str, Any]


@pytest.fixture(scope="module")
def template() -> Template:
    # cfn-lint's loader turns !Ref/!GetAtt into {"Ref": ...}/{"Fn::GetAtt": ...}.
    loaded: Template = cfn_yaml.load(str(TEMPLATE))
    return loaded


def resource(template: Template, name: str) -> dict[str, Any]:
    found: dict[str, Any] = template["Resources"][name]
    return found


def properties(template: Template, name: str) -> dict[str, Any]:
    found: dict[str, Any] = resource(template, name)["Properties"]
    return found


def env_of(template: Template, function: str) -> dict[str, Any]:
    found: dict[str, Any] = properties(template, function)["Environment"]["Variables"]
    return found


def required_env_vars(module_file: str) -> set[str]:
    """Env vars named by ``settings.require(...)`` calls in a handler module."""
    tree = ast.parse((SRC / "handlers" / module_file).read_text())
    attrs = {
        arg.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "require"
        for arg in node.args
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
    }
    return {ENV_VARS[a] for a in attrs}


def lifecycle_rules(bucket: dict[str, Any]) -> list[dict[str, Any]]:
    """The always-on rules (the retention ones sit behind a condition)."""
    rules = bucket["LifecycleConfiguration"]["Rules"]
    return [r for r in rules if "Fn::If" not in r]


def functions(template: Template) -> dict[str, dict[str, Any]]:
    return {
        name: res["Properties"]
        for name, res in template["Resources"].items()
        if res["Type"] == "AWS::Serverless::Function"
    }


def test_cfn_lint_reports_no_errors_or_warnings() -> None:
    matches = lint_all(TEMPLATE.read_text())

    problems = [
        f"{m.rule.id} line {m.linenumber}: {m.message}"
        for m in matches
        if m.rule.id.startswith(("E", "W"))
    ]
    assert problems == []


def test_runtime_is_python_3_14(template: Template) -> None:
    assert template["Globals"]["Function"]["Runtime"] == "python3.14"
    assert all("Runtime" not in props for props in functions(template).values())


def test_there_are_exactly_three_functions(template: Template) -> None:
    assert set(functions(template)) == {
        "FetchFunction",
        "ProcessFunction",
        "MediaRetryFunction",
        "ComplianceFunction",
    }


@pytest.mark.parametrize(
    ("function", "module"),
    [
        ("FetchFunction", "twitter_timeline_data_pipeline.handlers.fetch"),
        ("ProcessFunction", "twitter_timeline_data_pipeline.handlers.process"),
        ("MediaRetryFunction", "twitter_timeline_data_pipeline.handlers.media_retry"),
        ("ComplianceFunction", "twitter_timeline_data_pipeline.handlers.compliance"),
    ],
)
def test_handler_path_resolves_to_a_callable(
    template: Template, function: str, module: str
) -> None:
    handler_path: str = properties(template, function)["Handler"]

    module_name, _, attr = handler_path.rpartition(".")
    target = getattr(importlib.import_module(module_name), attr)

    assert module_name == module
    assert callable(target)
    assert list(inspect.signature(target).parameters) == ["event", "context"]


def test_queue_visibility_timeout_is_six_times_process_timeout_plus_batch_window(
    template: Template,
) -> None:
    process_timeout = properties(template, "ProcessFunction")["Timeout"]
    window = sqs_event(template)["MaximumBatchingWindowInSeconds"]

    visibility = properties(template, "TweetsQueue")["VisibilityTimeout"]

    assert visibility >= 6 * process_timeout + window


def test_dead_letter_queue_accepts_redrives_only_from_the_tweets_queue(
    template: Template,
) -> None:
    allow = properties(template, "TweetsDeadLetterQueue")["RedriveAllowPolicy"]
    queue_name = properties(template, "TweetsQueue")["QueueName"]

    assert allow["redrivePermission"] == "byQueue"
    (arn,) = allow["sourceQueueArns"]
    assert arn["Fn::Sub"].endswith(":" + queue_name["Fn::Sub"])


def test_media_bucket_is_versioned_and_expires_old_versions(
    template: Template,
) -> None:
    bucket = properties(template, "MediaBucket")
    rules = {r["Id"]: r for r in lifecycle_rules(bucket)}

    assert bucket["VersioningConfiguration"]["Status"] == "Enabled"
    expire = rules["expire-noncurrent-versions"]
    assert expire["Status"] == "Enabled"
    assert expire["NoncurrentVersionExpiration"]["NoncurrentDays"] >= 1


def test_media_moves_to_intelligent_tiering(template: Template) -> None:
    rules = lifecycle_rules(properties(template, "MediaBucket"))
    (rule,) = (r for r in rules if r["Id"] == "tier-media")

    assert rule["Prefix"] == "media/"
    assert {t["StorageClass"] for t in rule["Transitions"]} == {"INTELLIGENT_TIERING"}


def test_username_index_projects_only_listing_attributes(template: Template) -> None:
    indexes = properties(template, "TweetsTable")["GlobalSecondaryIndexes"]
    (index,) = (i for i in indexes if i["IndexName"] == "username-created_at-index")

    assert index["Projection"] == {
        "ProjectionType": "INCLUDE",
        # text may be a prefix of an offloaded text; the markers say so.
        "NonKeyAttributes": [
            "text",
            "text_truncated",
            "text_s3_key",
            "media_count",
            "public_metrics",
        ],
    }


def test_queue_redrives_to_the_dead_letter_queue(template: Template) -> None:
    policy = properties(template, "TweetsQueue")["RedrivePolicy"]

    assert policy["deadLetterTargetArn"] == {
        "Fn::GetAtt": ["TweetsDeadLetterQueue", "Arn"]
    }
    assert policy["maxReceiveCount"] == {"Ref": "MaxReceiveCount"}


def test_max_receive_count_parameter_feeds_the_queue_and_the_process_function(
    template: Template,
) -> None:
    parameter = template["Parameters"]["MaxReceiveCount"]

    assert parameter["Default"] == 5
    assert parameter["MinValue"] >= 1
    assert env_of(template, "ProcessFunction")["MAX_RECEIVE_COUNT"] == {
        "Ref": "MaxReceiveCount"
    }


def test_max_media_bytes_parameter_feeds_the_process_function(
    template: Template,
) -> None:
    parameter = template["Parameters"]["MaxMediaBytes"]

    assert parameter["Default"] == 512 * 1024 * 1024
    assert parameter["MinValue"] >= 1
    assert env_of(template, "ProcessFunction")["MAX_MEDIA_BYTES"] == {
        "Ref": "MaxMediaBytes"
    }
    assert "MAX_MEDIA_BYTES" not in env_of(template, "FetchFunction")


def test_expansion_parameters_feed_the_fetch_function_only(template: Template) -> None:
    for parameter, variable in (
        ("ExpandReferencedTweets", "EXPAND_REFERENCED_TWEETS"),
        ("RequestEntities", "REQUEST_ENTITIES"),
    ):
        assert template["Parameters"][parameter]["Default"] == "true"
        assert template["Parameters"][parameter]["AllowedValues"] == ["true", "false"]
        assert env_of(template, "FetchFunction")[variable] == {"Ref": parameter}
        assert variable not in env_of(template, "ProcessFunction")


def test_dead_letter_queue_keeps_messages_longer_than_the_source(
    template: Template,
) -> None:
    dlq = properties(template, "TweetsDeadLetterQueue")["MessageRetentionPeriod"]

    assert dlq > 345_600  # longer than the 4-day default the source queue keeps


def sqs_event(template: Template, function: str = "ProcessFunction") -> dict[str, Any]:
    events = properties(template, function)["Events"]
    (event,) = (e for e in events.values() if e["Type"] == "SQS")
    found: dict[str, Any] = event["Properties"]
    return found


def test_process_function_is_fed_by_the_main_queue(template: Template) -> None:
    assert sqs_event(template)["Queue"] == {"Fn::GetAtt": ["TweetsQueue", "Arn"]}


def test_partial_batch_responses_are_enabled(template: Template) -> None:
    assert "ReportBatchItemFailures" in sqs_event(template)["FunctionResponseTypes"]


def test_batch_size_fits_a_single_send_batch(template: Template) -> None:
    assert sqs_event(template)["BatchSize"] <= 10


def test_fetch_function_has_no_reserved_concurrency(template: Template) -> None:
    # The checkpoint write is conditional, so overlapping runs only duplicate work.
    assert "ReservedConcurrentExecutions" not in properties(template, "FetchFunction")


def test_fetch_function_is_not_retried_by_lambda(template: Template) -> None:
    config = properties(template, "FetchFunction")["EventInvokeConfig"]

    assert config["MaximumRetryAttempts"] == 0


def test_fetch_function_runs_on_a_schedule(template: Template) -> None:
    events = properties(template, "FetchFunction")["Events"]

    assert [e["Type"] for e in events.values()] == ["Schedule"]


@pytest.mark.parametrize(
    ("function", "module_file"),
    [
        ("FetchFunction", "fetch.py"),
        ("ProcessFunction", "process.py"),
        ("MediaRetryFunction", "media_retry.py"),
        ("ComplianceFunction", "compliance.py"),
    ],
)
def test_every_required_env_var_is_set_on_its_function(
    template: Template, function: str, module_file: str
) -> None:
    required = required_env_vars(module_file)

    assert required  # the AST scan found the require() call
    assert required <= set(env_of(template, function))


def test_fetch_function_has_a_token_source(template: Template) -> None:
    env = env_of(template, "FetchFunction")

    assert {"BEARER_TOKEN_SECRET_ARN", "TWITTER_BEARER_TOKEN"} & set(env)
    assert "TWITTER_BEARER_TOKEN" not in env  # never put the token itself in env
    assert env["BEARER_TOKEN_SECRET_ARN"] == {"Ref": "BearerTokenSecret"}


@pytest.mark.parametrize(
    "function",
    ["FetchFunction", "ProcessFunction", "MediaRetryFunction", "ComplianceFunction"],
)
def test_env_vars_are_all_known_to_settings(template: Template, function: str) -> None:
    assert set(env_of(template, function)) <= set(ENV_VARS.values())


def test_env_vars_point_at_the_right_resources(template: Template) -> None:
    fetch_env = env_of(template, "FetchFunction")
    process_env = env_of(template, "ProcessFunction")

    assert fetch_env["QUEUE_URL"] == {"Ref": "TweetsQueue"}
    assert fetch_env["STATE_TABLE"] == {"Ref": "StateTable"}
    assert process_env["TWEETS_TABLE"] == {"Ref": "TweetsTable"}
    assert process_env["MEDIA_BUCKET"] == {"Ref": "MediaBucket"}


@pytest.mark.parametrize("name", ["TweetsTable", "MediaBucket"])
def test_collected_data_stores_are_retained(template: Template, name: str) -> None:
    res = resource(template, name)

    assert res["DeletionPolicy"] == "Retain"
    assert res["UpdateReplacePolicy"] == "Retain"


def test_secret_has_no_value_in_the_template(template: Template) -> None:
    props = properties(template, "BearerTokenSecret")

    assert "SecretString" not in props
    assert "GenerateSecretString" not in props


def test_tweets_table_schema_matches_the_code(template: Template) -> None:
    props = properties(template, "TweetsTable")

    assert props["KeySchema"] == [{"AttributeName": "tweet_id", "KeyType": "HASH"}]
    gsi = next(
        i
        for i in props["GlobalSecondaryIndexes"]
        if i["IndexName"] == "username-created_at-index"
    )
    assert [(k["AttributeName"], k["KeyType"]) for k in gsi["KeySchema"]] == [
        ("username", "HASH"),
        ("created_at", "RANGE"),
    ]
    assert {
        a["AttributeName"]: a["AttributeType"] for a in props["AttributeDefinitions"]
    } == {
        "tweet_id": "S",
        "username": "S",
        "created_at": "S",
        "tombstone": "S",
        "deleted_at": "S",
    }


def test_state_table_schema_matches_the_code(template: Template) -> None:
    props = properties(template, "StateTable")

    assert props["KeySchema"] == [{"AttributeName": "username", "KeyType": "HASH"}]
    assert props["AttributeDefinitions"] == [
        {"AttributeName": "username", "AttributeType": "S"}
    ]


def test_media_bucket_blocks_public_access_and_requires_tls(template: Template) -> None:
    block = properties(template, "MediaBucket")["PublicAccessBlockConfiguration"]
    statements = properties(template, "MediaBucketPolicy")["PolicyDocument"][
        "Statement"
    ]

    assert all(block.values()) and len(block) == 4
    assert any(
        s["Effect"] == "Deny"
        and s["Condition"]["Bool"]["aws:SecureTransport"] == "false"
        for s in statements
    )


def test_process_function_can_head_missing_media_without_403(
    template: Template,
) -> None:
    """HeadObject on a missing key needs s3:ListBucket, else it returns 403, not 404."""
    statements = [
        s
        for policy in properties(template, "ProcessFunction")["Policies"]
        if isinstance(policy, dict) and "Statement" in policy
        for s in policy["Statement"]
    ]
    actions = {a for s in statements for a in _as_list(s["Action"])}

    assert {"s3:PutObject", "s3:GetObject", "s3:ListBucket"} <= actions


def _as_list(value: str | list[str]) -> list[str]:
    return [value] if isinstance(value, str) else value


def test_schedule_state_follows_the_enabled_parameter(template: Template) -> None:
    events = properties(template, "FetchFunction")["Events"]
    (event,) = (e for e in events.values() if e["Type"] == "Schedule")
    schedule = event["Properties"]

    assert template["Conditions"]["ScheduleIsEnabled"] == {
        "Fn::Equals": [{"Ref": "ScheduleEnabled"}, "true"]
    }
    assert schedule["State"] == {"Fn::If": ["ScheduleIsEnabled", "ENABLED", "DISABLED"]}
    assert "Enabled" not in schedule  # SAM would treat any non-empty string as true


def test_log_level_parameter_only_allows_lambda_levels(template: Template) -> None:
    allowed = template["Parameters"]["LogLevel"]["AllowedValues"]

    assert allowed == ["DEBUG", "INFO", "WARN", "ERROR"]
    assert template["Parameters"]["LogLevel"]["Default"] in allowed


def test_process_function_can_abort_multipart_uploads(template: Template) -> None:
    statements = [
        s
        for policy in properties(template, "ProcessFunction")["Policies"]
        if isinstance(policy, dict) and "Statement" in policy
        for s in policy["Statement"]
    ]
    actions = {a for s in statements for a in _as_list(s["Action"])}

    assert "s3:AbortMultipartUpload" in actions


@pytest.mark.parametrize("name", ["TweetsQueue", "TweetsDeadLetterQueue"])
def test_queues_accept_messages_up_to_the_size_the_fetch_guard_allows(
    template: Template, name: str
) -> None:
    from twitter_timeline_data_pipeline.handlers.fetch import SQS_MAX_MESSAGE_BYTES

    assert properties(template, name)["MaximumMessageSize"] == SQS_MAX_MESSAGE_BYTES


def test_fetch_is_deployed_after_the_processor(template: Template) -> None:
    # A fetcher that sends long posts must not go live before a processor that
    # understands them.
    assert resource(template, "FetchFunction")["DependsOn"] == "ProcessFunction"


def policy_statements(template: Template, function: str) -> list[dict[str, Any]]:
    return [
        s
        for policy in properties(template, function)["Policies"]
        if isinstance(policy, dict) and "Statement" in policy
        for s in policy["Statement"]
    ]


def test_media_retry_queue_redrives_to_its_own_dead_letter_queue(
    template: Template,
) -> None:
    queue = properties(template, "MediaRetryQueue")
    policy = queue["RedrivePolicy"]
    parameter = template["Parameters"]["MediaRetryMaxReceiveCount"]

    assert policy["deadLetterTargetArn"] == {
        "Fn::GetAtt": ["MediaRetryDeadLetterQueue", "Arn"]
    }
    assert policy["maxReceiveCount"] == {"Ref": "MediaRetryMaxReceiveCount"}
    assert parameter["Default"] == 10
    assert parameter["MinValue"] >= 1
    assert queue["SqsManagedSseEnabled"] is True
    assert queue["MessageRetentionPeriod"] == 14 * 86400


def test_media_retry_queue_visibility_is_six_times_the_consumer_timeout(
    template: Template,
) -> None:
    timeout = properties(template, "MediaRetryFunction")["Timeout"]
    event = sqs_event(template, "MediaRetryFunction")

    visibility = properties(template, "MediaRetryQueue")["VisibilityTimeout"]

    assert visibility == 5400
    assert visibility >= 6 * timeout + event.get("MaximumBatchingWindowInSeconds", 0)


def test_media_retry_dead_letter_queue_is_retained_and_redrive_restricted(
    template: Template,
) -> None:
    dlq = properties(template, "MediaRetryDeadLetterQueue")
    queue_name = properties(template, "MediaRetryQueue")["QueueName"]

    assert dlq["MessageRetentionPeriod"] == 14 * 86400
    assert dlq["SqsManagedSseEnabled"] is True
    assert dlq["RedriveAllowPolicy"]["redrivePermission"] == "byQueue"
    (arn,) = dlq["RedriveAllowPolicy"]["sourceQueueArns"]
    assert arn["Fn::Sub"].endswith(":" + queue_name["Fn::Sub"])


@pytest.mark.parametrize("name", ["MediaRetryQueue", "MediaRetryDeadLetterQueue"])
def test_media_retry_queues_take_the_same_message_size_as_the_main_queue(
    template: Template, name: str
) -> None:
    main = properties(template, "TweetsQueue")["MaximumMessageSize"]

    assert properties(template, name)["MaximumMessageSize"] == main


def test_media_retry_function_is_fed_one_message_at_a_time(template: Template) -> None:
    event = sqs_event(template, "MediaRetryFunction")

    assert event["Queue"] == {"Fn::GetAtt": ["MediaRetryQueue", "Arn"]}
    assert event["BatchSize"] == 1
    assert "ReportBatchItemFailures" in event["FunctionResponseTypes"]
    props = properties(template, "MediaRetryFunction")
    assert props["Timeout"] == 900
    assert props["MemorySize"] == properties(template, "ProcessFunction")["MemorySize"]
    assert props["LoggingConfig"]["LogGroup"] == {"Ref": "MediaRetryFunctionLogGroup"}
    assert (
        resource(template, "MediaRetryFunctionLogGroup")["Properties"][
            "RetentionInDays"
        ]
        == 30
    )


def test_media_retry_function_environment(template: Template) -> None:
    env = env_of(template, "MediaRetryFunction")

    assert env == {
        "TWEETS_TABLE": {"Ref": "TweetsTable"},
        "MEDIA_BUCKET": {"Ref": "MediaBucket"},
        "MAX_MEDIA_BYTES": {"Ref": "MaxMediaBytes"},
        "METRICS_NAMESPACE": NAMESPACE,
        "LOG_LEVEL": {"Ref": "LogLevel"},
    }


def test_process_function_sends_to_the_retry_queue_only(template: Template) -> None:
    env = env_of(template, "ProcessFunction")
    sends = [
        p
        for p in properties(template, "ProcessFunction")["Policies"]
        if isinstance(p, dict) and "SQSSendMessagePolicy" in p
    ]

    assert env["MEDIA_RETRY_QUEUE_URL"] == {"Ref": "MediaRetryQueue"}
    assert sends == [
        {
            "SQSSendMessagePolicy": {
                "QueueName": {"Fn::GetAtt": ["MediaRetryQueue", "QueueName"]}
            }
        }
    ]
    assert "MEDIA_RETRY_QUEUE_URL" not in env_of(template, "FetchFunction")


def test_media_retry_function_permissions(template: Template) -> None:
    statements = policy_statements(template, "MediaRetryFunction")
    by_resource = {
        json_key(s["Resource"]): set(_as_list(s["Action"])) for s in statements
    }

    assert by_resource[json_key({"Fn::GetAtt": ["TweetsTable", "Arn"]})] == {
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
    }
    actions = {a for s in statements for a in _as_list(s["Action"])}
    assert {
        "s3:PutObject",
        "s3:GetObject",
        "s3:AbortMultipartUpload",
        "s3:ListBucket",
    } <= actions
    assert not any(a.startswith("sqs:") for a in actions)
    assert "dynamodb:PutItem" not in actions


def json_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def test_media_retry_dead_letter_queue_has_an_alarm(template: Template) -> None:
    alarm = properties(template, "MediaRetryDeadLetterQueueNotEmptyAlarm")

    assert alarm["Dimensions"] == [
        {
            "Name": "QueueName",
            "Value": {"Fn::GetAtt": ["MediaRetryDeadLetterQueue", "QueueName"]},
        }
    ]
    assert alarm["MetricName"] == "ApproximateNumberOfMessagesVisible"
    assert alarm["Threshold"] == 0
    assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
    assert (
        alarm["AlarmActions"]
        == properties(template, "DeadLetterQueueNotEmptyAlarm")["AlarmActions"]
    )


def test_media_retry_function_writes_only_under_media(template: Template) -> None:
    statements = policy_statements(template, "MediaRetryFunction")
    object_actions = {"s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"}
    (objects,) = (s for s in statements if object_actions <= set(_as_list(s["Action"])))
    (listing,) = (s for s in statements if s["Action"] == "s3:ListBucket")

    assert objects["Resource"] == {"Fn::Sub": "${MediaBucket.Arn}/media/*"}
    assert listing["Resource"] == {"Fn::GetAtt": ["MediaBucket", "Arn"]}


def test_media_retry_function_has_an_errors_alarm(template: Template) -> None:
    alarm = properties(template, "MediaRetryFunctionErrorsAlarm")

    assert alarm["Namespace"] == "AWS/Lambda"
    assert alarm["MetricName"] == "Errors"
    assert alarm["Dimensions"] == [
        {"Name": "FunctionName", "Value": {"Ref": "MediaRetryFunction"}}
    ]
    assert (
        alarm["AlarmActions"]
        == properties(template, "ProcessFunctionErrorsAlarm")["AlarmActions"]
    )


# --- deletions and retention (#21) ----------------------------------------------


def test_tweets_table_has_ttl_on_expires_at_only_with_retention(
    template: Template,
) -> None:
    assert properties(template, "TweetsTable")["TimeToLiveSpecification"] == {
        "AttributeName": "expires_at",
        "Enabled": {"Fn::If": ["HasRetention", True, False]},
    }


def test_retention_is_off_by_default_and_feeds_the_process_function(
    template: Template,
) -> None:
    parameter = template["Parameters"]["RetentionDays"]

    assert parameter["Default"] == 0
    assert parameter["MinValue"] == 0
    assert env_of(template, "ProcessFunction")["RETENTION_DAYS"] == {
        "Ref": "RetentionDays"
    }
    assert template["Conditions"]["HasRetention"] == {
        "Fn::Not": [{"Fn::Equals": [{"Ref": "RetentionDays"}, "0"]}]
    }


def test_retention_lifecycle_rules_exist_only_with_retention(
    template: Template,
) -> None:
    rules = properties(template, "MediaBucket")["LifecycleConfiguration"]["Rules"]
    conditional = [r["Fn::If"] for r in rules if "Fn::If" in r]

    assert len(conditional) == 2
    prefixes = set()
    for name, enabled, disabled in conditional:
        assert name == "HasRetention"
        assert disabled == {"Ref": "AWS::NoValue"}
        assert enabled["Status"] == "Enabled"
        assert enabled["ExpirationInDays"] == {"Ref": "RetentionDays"}
        prefixes.add(enabled["Prefix"])
    assert prefixes == {"media/", "text/"}


def test_tombstone_index_is_sparse_and_keys_only(template: Template) -> None:
    props = properties(template, "TweetsTable")
    (index,) = (
        i
        for i in props["GlobalSecondaryIndexes"]
        if i["IndexName"] == "tombstone-deleted_at-index"
    )

    assert [(k["AttributeName"], k["KeyType"]) for k in index["KeySchema"]] == [
        ("tombstone", "HASH"),
        ("deleted_at", "RANGE"),
    ]
    assert index["Projection"] == {"ProjectionType": "KEYS_ONLY"}


def test_compliance_function_has_its_own_schedule_and_settings(
    template: Template,
) -> None:
    props = properties(template, "ComplianceFunction")
    parameters = template["Parameters"]

    assert parameters["ComplianceSchedule"]["Default"] == "rate(6 hours)"
    assert parameters["ComplianceMaxLookups"]["Default"] == 1000
    assert parameters["DeletionGraceHours"]["Default"] == 24
    assert props["Timeout"] == 300
    (event,) = props["Events"].values()
    assert event["Type"] == "Schedule"
    assert event["Properties"]["Schedule"] == {"Ref": "ComplianceSchedule"}
    assert props["EventInvokeConfig"]["MaximumRetryAttempts"] == 0
    env = env_of(template, "ComplianceFunction")
    assert env["COMPLIANCE_MAX_LOOKUPS"] == {"Ref": "ComplianceMaxLookups"}
    assert env["RETENTION_DAYS"] == {"Ref": "RetentionDays"}
    assert parameters["ComplianceMaxLookups"]["MinValue"] == 4
    assert env["DELETION_GRACE_HOURS"] == {"Ref": "DeletionGraceHours"}
    assert env["MEDIA_BUCKET"] == {"Ref": "MediaBucket"}
    log_group = resource(template, "ComplianceFunctionLogGroup")
    assert props["LoggingConfig"]["LogGroup"] == {"Ref": "ComplianceFunctionLogGroup"}
    assert log_group["Properties"]["RetentionInDays"] == 30


def test_compliance_function_permissions(template: Template) -> None:
    statements = policy_statements(template, "ComplianceFunction")
    by_actions = {frozenset(_as_list(s["Action"])): s for s in statements}

    assert set(by_actions) == {
        frozenset({"dynamodb:Query"}),
        frozenset({"dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Scan"}),
        frozenset({"dynamodb:GetItem", "dynamodb:PutItem"}),
        frozenset({"s3:ListBucketVersions", "s3:ListBucket"}),
        frozenset({"s3:DeleteObjectVersion"}),
        frozenset({"s3:GetObject"}),
        frozenset({"s3:PutObject"}),
    }
    assert all(s["Effect"] == "Allow" for s in statements)
    state = by_actions[frozenset({"dynamodb:GetItem", "dynamodb:PutItem"})]
    assert state["Resource"] == {"Fn::GetAtt": ["StateTable", "Arn"]}
    assert state["Condition"] == {
        "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["compliance#*"]}
    }
    deletes = by_actions[frozenset({"s3:DeleteObjectVersion"})]
    assert [json_key(r) for r in deletes["Resource"]] == [
        json_key({"Fn::Sub": f"${{MediaBucket.Arn}}/{prefix}/*"})
        for prefix in ("media", "text", "raw", "quarantine")
    ]
    reads = by_actions[frozenset({"s3:GetObject"})]
    assert [json_key(r) for r in reads["Resource"]] == [
        json_key({"Fn::Sub": f"${{MediaBucket.Arn}}/{prefix}/*"})
        for prefix in ("raw", "quarantine")
    ]
    # the only writes it has are the rewrites without a deleted post's copy
    writes = by_actions[frozenset({"s3:PutObject"})]
    assert writes["Resource"] == [
        {"Fn::Sub": "${MediaBucket.Arn}/raw/*"},
        {"Fn::Sub": "${MediaBucket.Arn}/quarantine/*"},
    ]
    listing = by_actions[frozenset({"s3:ListBucketVersions", "s3:ListBucket"})]
    assert listing["Condition"]["StringLike"]["s3:prefix"] == [
        "media/*",
        "text/*",
        "raw/*",
        "quarantine/*",
    ]
    query = by_actions[frozenset({"dynamodb:Query"})]
    assert query["Resource"] == {"Fn::Sub": "${TweetsTable.Arn}/index/*"}


def test_only_the_compliance_function_can_delete(template: Template) -> None:
    deleting = {"dynamodb:DeleteItem", "dynamodb:BatchWriteItem", "s3:DeleteObject"}
    deleting_prefixes = ("s3:DeleteObject", "dynamodb:DeleteItem", "s3:PutLifecycle")
    for name in functions(template):
        statements = policy_statements(template, name)
        actions = {a for s in statements for a in _as_list(s["Action"])}
        managed = [
            p
            for p in properties(template, name)["Policies"]
            if isinstance(p, dict) and "Statement" not in p
        ]
        has_delete = any(a.startswith(deleting_prefixes) for a in actions)
        assert has_delete == (name == "ComplianceFunction"), name
        assert not actions & deleting or name == "ComplianceFunction"
        # SAM policy templates in use: none that writes DynamoDB (the process function
        # has inline GetItem and PutItem only, pinned by its own test).
        for policy in managed:
            assert set(policy) <= {
                "SQSSendMessagePolicy",
                "AWSSecretsManagerGetSecretValuePolicy",
            }


def test_compliance_function_never_runs_twice_at_once(template: Template) -> None:
    assert (
        properties(template, "ComplianceFunction")["ReservedConcurrentExecutions"] == 1
    )


def test_compliance_function_has_an_errors_alarm(template: Template) -> None:
    props = properties(template, "ComplianceFunctionErrorsAlarm")

    assert props["Dimensions"] == [
        {"Name": "FunctionName", "Value": {"Ref": "ComplianceFunction"}}
    ]
    assert props["MetricName"] == "Errors"


def test_fetch_function_writes_only_under_raw_and_quarantine(
    template: Template,
) -> None:
    statements = policy_statements(template, "FetchFunction")
    puts = [s for s in statements if "s3:PutObject" in _as_list(s["Action"])]

    assert all(s["Effect"] == "Allow" and s["Action"] == "s3:PutObject" for s in puts)
    # one statement per prefix, each scoped to exactly that prefix
    assert [s["Resource"] for s in puts] == [
        {"Fn::Sub": "${MediaBucket.Arn}/raw/*"},
        {"Fn::Sub": "${MediaBucket.Arn}/quarantine/*"},
    ]
    # nothing else of S3: no reads, no listing, no other prefix
    other = [
        s
        for s in statements
        if s not in puts and any(a.startswith("s3:") for a in _as_list(s["Action"]))
    ]
    assert other == []


def test_fetch_function_gets_the_bucket_name(template: Template) -> None:
    assert env_of(template, "FetchFunction")["MEDIA_BUCKET"] == {"Ref": "MediaBucket"}


def test_no_lifecycle_rule_can_reach_raw_pages(template: Template) -> None:
    rules = properties(template, "MediaBucket")["LifecycleConfiguration"]["Rules"]
    # the retention rules sit behind HasRetention: look inside the condition
    rules = [r["Fn::If"][1] if "Fn::If" in r else r for r in rules]
    acting = {
        "ExpirationInDays",
        "ExpirationDate",
        "ExpiredObjectDeleteMarker",
        "NoncurrentVersionExpiration",
        "NoncurrentVersionTransitions",
        "Transitions",
    }

    for rule in rules:
        if acting & rule.keys():
            # every expiring or transitioning rule names its (non-raw) prefix
            assert rule.get("Prefix") in {"media/", "text/"}, rule["Id"]
    noncurrent = {r["Prefix"] for r in rules if "NoncurrentVersionExpiration" in r}
    assert noncurrent == {"media/", "text/"}


def test_process_function_cannot_write_raw_pages(template: Template) -> None:
    statements = policy_statements(template, "ProcessFunction")
    (objects,) = (s for s in statements if "s3:PutObject" in _as_list(s["Action"]))

    assert objects["Resource"] == [
        {"Fn::Sub": "${MediaBucket.Arn}/media/*"},
        {"Fn::Sub": "${MediaBucket.Arn}/text/*"},
    ]


def test_process_function_s3_access_is_exactly_this_and_never_deletes(
    template: Template,
) -> None:
    statements = [
        s
        for s in policy_statements(template, "ProcessFunction")
        if any(a.startswith("s3:") for a in _as_list(s["Action"]))
    ]

    assert [
        (
            s["Effect"],
            sorted(_as_list(s["Action"])),
            s["Resource"] if isinstance(s["Resource"], list) else [s["Resource"]],
        )
        for s in statements
    ] == [
        (
            "Allow",
            ["s3:AbortMultipartUpload", "s3:GetObject", "s3:PutObject"],
            [
                {"Fn::Sub": "${MediaBucket.Arn}/media/*"},
                {"Fn::Sub": "${MediaBucket.Arn}/text/*"},
            ],
        ),
        ("Allow", ["s3:GetObject"], [{"Fn::Sub": "${MediaBucket.Arn}/raw/*"}]),
        ("Allow", ["s3:ListBucket"], [{"Fn::GetAtt": ["MediaBucket", "Arn"]}]),
    ]
    # (ComplianceFunction is the only one with a delete action; see
    # test_only_the_compliance_function_can_delete)


def test_process_function_table_access_is_get_and_put_only(template: Template) -> None:
    # put_tweet reads before it writes. moto and LocalStack don't enforce IAM, so a
    # missing GetItem would only show up in AWS, as every message failing.
    statements = [
        s
        for s in policy_statements(template, "ProcessFunction")
        if any(a.startswith("dynamodb:") for a in _as_list(s["Action"]))
    ]
    policies = properties(template, "ProcessFunction")["Policies"]

    assert [
        (s["Effect"], sorted(_as_list(s["Action"])), s["Resource"]) for s in statements
    ] == [
        (
            "Allow",
            ["dynamodb:GetItem", "dynamodb:PutItem"],
            {"Fn::GetAtt": ["TweetsTable", "Arn"]},
        )
    ]
    assert not [
        p for p in policies if isinstance(p, dict) and any("DynamoDB" in k for k in p)
    ]


def test_no_function_sets_the_test_only_media_host_override(template: Template) -> None:
    for name, props in functions(template).items():
        assert MEDIA_HOSTS_OVERRIDE_ENV not in props.get("Environment", {}).get(
            "Variables", {}
        ), name
    assert MEDIA_HOSTS_OVERRIDE_ENV not in TEMPLATE.read_text()


# --- queue retention (#37) and alarms on metrics (#19) ---------------------------

FOURTEEN_DAYS = 1_209_600
SEVEN_DAYS = 604_800
NAMESPACE = {
    "Fn::If": [
        "HasMetricsNamespace",
        {"Ref": "MetricsNamespace"},
        {"Fn::Sub": "TwitterTimelinePipeline/${AWS::StackName}"},
    ]
}
ALARM_ACTIONS = {
    "Fn::If": ["HasAlarmTopic", [{"Ref": "AlarmTopicArn"}], {"Ref": "AWS::NoValue"}]
}


@pytest.mark.parametrize(
    "queue",
    [
        "TweetsQueue",
        "TweetsDeadLetterQueue",
        "MediaRetryQueue",
        "MediaRetryDeadLetterQueue",
    ],
)
def test_every_queue_keeps_messages_for_the_sqs_maximum(
    template: Template, queue: str
) -> None:
    assert properties(template, queue)["MessageRetentionPeriod"] == FOURTEEN_DAYS


def alarms(template: Template) -> dict[str, dict[str, Any]]:
    return {
        name: res
        for name, res in template["Resources"].items()
        if res["Type"] == "AWS::CloudWatch::Alarm"
    }


def alarm_props(template: Template, name: str) -> dict[str, Any]:
    found: dict[str, Any] = alarms(template)[name]["Properties"]
    return found


def test_every_alarm_notifies_the_alarm_topic(template: Template) -> None:
    for name, res in alarms(template).items():
        assert res["Properties"]["AlarmActions"] == ALARM_ACTIONS, name


def test_every_alarm_chooses_its_missing_data_treatment(template: Template) -> None:
    for name, res in alarms(template).items():
        assert res["Properties"]["TreatMissingData"] in {
            "breaching",
            "notBreaching",
        }, name


def queue_dimension(queue: str) -> list[dict[str, Any]]:
    return [{"Name": "QueueName", "Value": {"Fn::GetAtt": [queue, "QueueName"]}}]


def test_the_main_queue_age_alarm_is_a_parameter_defaulting_to_an_hour(
    template: Template,
) -> None:
    alarm = alarm_props(template, "TweetsQueueAgeAlarm")

    assert alarm["Namespace"] == "AWS/SQS"
    assert alarm["MetricName"] == "ApproximateAgeOfOldestMessage"
    assert alarm["Dimensions"] == queue_dimension("TweetsQueue")
    assert alarm["Threshold"] == {"Ref": "QueueAgeAlarmSeconds"}
    assert template["Parameters"]["QueueAgeAlarmSeconds"]["Default"] == 3600
    assert (alarm["Statistic"], alarm["Period"], alarm["EvaluationPeriods"]) == (
        "Maximum",
        300,
        1,
    )
    assert alarm["ComparisonOperator"] == "GreaterThanThreshold"
    assert alarm["TreatMissingData"] == "notBreaching"  # an empty queue has no metric


@pytest.mark.parametrize(
    ("alarm", "queue"),
    [
        ("DeadLetterQueueAgeAlarm", "TweetsDeadLetterQueue"),
        ("MediaRetryDeadLetterQueueAgeAlarm", "MediaRetryDeadLetterQueue"),
    ],
)
def test_dead_letter_queue_age_alarms_fire_seven_days_in(
    template: Template, alarm: str, queue: str
) -> None:
    props = alarm_props(template, alarm)

    assert (props["Namespace"], props["MetricName"]) == (
        "AWS/SQS",
        "ApproximateAgeOfOldestMessage",
    )
    assert props["Dimensions"] == queue_dimension(queue)
    # A DLQ message's age counts from the move, its expiry from the first send: 7 days
    # in the DLQ plus the longest the receive-count caps allow upstream stays under 14.
    assert props["Threshold"] == SEVEN_DAYS < FOURTEEN_DAYS
    assert props["ComparisonOperator"] == "GreaterThanThreshold"
    assert props["Statistic"] == "Maximum"
    assert (props["Period"], props["EvaluationPeriods"]) == (300, 1)
    assert props["TreatMissingData"] == "notBreaching"


@pytest.mark.parametrize(
    "function",
    ["FetchFunction", "ProcessFunction", "MediaRetryFunction", "ComplianceFunction"],
)
def test_every_function_has_a_throttles_alarm(
    template: Template, function: str
) -> None:
    props = alarm_props(template, f"{function}ThrottlesAlarm")

    assert (props["Namespace"], props["MetricName"]) == ("AWS/Lambda", "Throttles")
    assert props["Dimensions"] == [{"Name": "FunctionName", "Value": {"Ref": function}}]
    assert (props["Statistic"], props["Period"], props["Threshold"]) == (
        "Sum",
        300,
        1,
    )
    assert props["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
    assert props["TreatMissingData"] == "notBreaching"


# (alarm, metric, statistic, period, evaluation periods, threshold, comparison,
#  missing data). The metrics are in the stack's MetricsNamespace, dimensionless.
METRIC_ALARMS = [
    (
        "RecordFailuresAlarm",
        "RecordFailures",
        "Sum",
        300,
        1,
        0,
        "GreaterThan",
        "notBreaching",
    ),
    (
        "IdentityChangedAlarm",
        "IdentityChanged",
        "Sum",
        3600,
        1,
        0,
        "GreaterThan",
        "notBreaching",
    ),
    (
        "QuarantinedItemsAlarm",
        "QuarantinedItems",
        "Sum",
        3600,
        1,
        0,
        "GreaterThan",
        "notBreaching",
    ),
    (
        "WriteContentionAlarm",
        "WriteContentionErrors",
        "Sum",
        300,
        1,
        0,
        "GreaterThan",
        "notBreaching",
    ),
    (
        "GapsDroppedAlarm",
        "GapsDropped",
        "Sum",
        3600,
        1,
        0,
        "GreaterThan",
        "notBreaching",
    ),
    (
        "CompliancePurgeDeferredAlarm",
        "CompliancePurgeDeferred",
        "Sum",
        21600,
        2,
        0,
        "GreaterThan",
        "notBreaching",
    ),
]


@pytest.mark.parametrize(
    (
        "name",
        "metric",
        "statistic",
        "period",
        "evaluations",
        "threshold",
        "op",
        "missing",
    ),
    METRIC_ALARMS,
)
def test_metric_alarms_watch_the_emitted_metric_with_the_documented_settings(
    template: Template,
    name: str,
    metric: str,
    statistic: str,
    period: int,
    evaluations: int,
    threshold: int,
    op: str,
    missing: str,
) -> None:
    props = alarm_props(template, name)

    assert props["Namespace"] == NAMESPACE
    assert props["MetricName"] == metric
    assert "Dimensions" not in props  # the dimensionless (total) metric
    assert props["Statistic"] == statistic
    assert props["Period"] == period
    assert props["EvaluationPeriods"] == evaluations
    assert props.get("DatapointsToAlarm", evaluations) == evaluations
    assert props["Threshold"] == threshold
    assert props["ComparisonOperator"] == f"{op}Threshold"
    assert props["TreatMissingData"] == missing


@pytest.mark.parametrize(
    ("name", "metric", "hours"),
    [
        ("FetchFreshnessAlarm", "FetchRunSucceeded", "FetchFreshnessHours"),
        (
            "ComplianceFreshnessAlarm",
            "ComplianceRunSucceeded",
            "ComplianceFreshnessHours",
        ),
    ],
)
def test_freshness_alarms_breach_on_missing_heartbeats(
    template: Template, name: str, metric: str, hours: str
) -> None:
    resource_ = alarms(template)[name]
    props = resource_["Properties"]

    assert props["Namespace"] == NAMESPACE
    assert props["MetricName"] == metric
    assert "Dimensions" not in props
    assert (props["Statistic"], props["Period"]) == ("Sum", 3600)
    assert props["ComparisonOperator"] == "LessThanThreshold"
    assert props["Threshold"] == 1
    assert props["TreatMissingData"] == "breaching"
    assert props["EvaluationPeriods"] == {"Ref": hours}
    assert props["DatapointsToAlarm"] == {"Ref": hours}
    # a paused schedule (the first deploy, before the secret is set) must not alarm
    assert resource_["Condition"] == "ScheduleIsEnabled"


def test_freshness_windows_default_to_twice_the_default_schedules(
    template: Template,
) -> None:
    parameters = template["Parameters"]

    assert parameters["ScheduleExpression"]["Default"] == "rate(1 hour)"
    assert parameters["FetchFreshnessHours"]["Default"] == 2
    assert parameters["ComplianceSchedule"]["Default"] == "rate(6 hours)"
    assert parameters["ComplianceFreshnessHours"]["Default"] == 12
    # the window is hourly periods, at most a day
    assert parameters["FetchFreshnessHours"]["MaxValue"] <= 24
    assert parameters["ComplianceFreshnessHours"]["MaxValue"] <= 24


def test_sustained_rate_limiting_needs_consecutive_hours(template: Template) -> None:
    props = alarm_props(template, "RateLimitedRunsAlarm")

    assert props["Namespace"] == NAMESPACE
    assert props["MetricName"] == "RateLimitedRuns"
    assert (props["Statistic"], props["Period"], props["Threshold"]) == ("Sum", 3600, 1)
    assert props["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
    assert props["EvaluationPeriods"] == {"Ref": "RateLimitedRunsToAlarm"}
    assert props["DatapointsToAlarm"] == {"Ref": "RateLimitedRunsToAlarm"}
    assert template["Parameters"]["RateLimitedRunsToAlarm"]["Default"] == 3
    assert props["TreatMissingData"] == "notBreaching"  # freshness covers a silence


@pytest.mark.parametrize(
    "function",
    ["FetchFunction", "ProcessFunction", "MediaRetryFunction", "ComplianceFunction"],
)
def test_every_function_emits_into_the_stacks_namespace(
    template: Template, function: str
) -> None:
    assert env_of(template, function)["METRICS_NAMESPACE"] == NAMESPACE


def test_no_function_may_put_metric_data(template: Template) -> None:
    # EMF goes through the log group, so the functions need no CloudWatch permission
    assert "cloudwatch:PutMetricData" not in TEMPLATE.read_text()


def emitted_metric_names() -> set[str]:
    """Metric names written in modules that call ``metrics.emit``."""
    from twitter_timeline_data_pipeline import metrics

    names: set[str] = set()
    for path in SRC.rglob("*.py"):
        if path.name == "metrics.py":
            continue
        tree = ast.parse(path.read_text())
        calls_emit = any(
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "emit"
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "metrics"
            for n in ast.walk(tree)
        )
        if calls_emit:
            names |= {
                n.value
                for n in ast.walk(tree)
                if isinstance(n, ast.Constant)
                and isinstance(n.value, str)
                and n.value in metrics.UNITS
            }
    return names


def test_an_alarm_never_watches_a_metric_nobody_emits(template: Template) -> None:
    from twitter_timeline_data_pipeline import metrics

    watched = set()
    for res in alarms(template).values():
        props = res["Properties"]
        if props.get("Namespace") == NAMESPACE:
            watched.add(props["MetricName"])
        for m in props.get("Metrics", []):
            stat = m.get("MetricStat")
            if stat and stat["Metric"]["Namespace"] == NAMESPACE:
                watched.add(stat["Metric"]["MetricName"])

    assert watched
    assert watched <= emitted_metric_names()
    assert watched <= set(metrics.UNITS)


def test_every_declared_metric_is_emitted_somewhere() -> None:
    from twitter_timeline_data_pipeline import metrics

    assert set(metrics.UNITS) == emitted_metric_names()


def test_receive_count_caps_keep_a_message_from_expiring_before_its_dlq(
    template: Template,
) -> None:
    parameters = template["Parameters"]
    main_visibility = properties(template, "TweetsQueue")["VisibilityTimeout"]
    retry_visibility = properties(template, "MediaRetryQueue")["VisibilityTimeout"]

    upstream = parameters["MaxReceiveCount"]["MaxValue"] * main_visibility
    retry = parameters["MediaRetryMaxReceiveCount"]["MaxValue"] * retry_visibility

    assert upstream + SEVEN_DAYS < FOURTEEN_DAYS
    assert retry + SEVEN_DAYS < FOURTEEN_DAYS


def test_the_queue_age_threshold_cannot_be_set_past_the_retention(
    template: Template,
) -> None:
    parameter = template["Parameters"]["QueueAgeAlarmSeconds"]

    assert parameter["MaxValue"] <= SEVEN_DAYS < FOURTEEN_DAYS


def test_the_media_retry_queue_has_an_age_alarm(template: Template) -> None:
    props = alarm_props(template, "MediaRetryQueueAgeAlarm")

    assert (props["Namespace"], props["MetricName"]) == (
        "AWS/SQS",
        "ApproximateAgeOfOldestMessage",
    )
    assert props["Dimensions"] == queue_dimension("MediaRetryQueue")
    assert (props["Statistic"], props["Period"], props["EvaluationPeriods"]) == (
        "Maximum",
        300,
        1,
    )
    assert props["Threshold"] == 86_400
    assert props["ComparisonOperator"] == "GreaterThanThreshold"
    assert props["TreatMissingData"] == "notBreaching"


def test_media_retry_record_failures_alarm_adds_only_the_reasons_that_do_not_heal(
    template: Template,
) -> None:
    props = alarm_props(template, "MediaRetryRecordFailuresAlarm")

    by_id = {m["Id"]: m for m in props["Metrics"]}
    # plain arithmetic: FILL with one evaluation period can miss a late datapoint
    assert by_id["bad"]["Expression"] == "malformed + error"
    for reason in ("malformed", "error"):
        stat = by_id[reason]["MetricStat"]
        assert stat["Metric"]["Namespace"] == NAMESPACE
        assert stat["Metric"]["MetricName"] == "MediaRetryFailures"
        assert stat["Metric"]["Dimensions"] == [{"Name": "Reason", "Value": reason}]
        assert (stat["Stat"], stat["Period"]) == ("Sum", 300)
        assert by_id[reason]["ReturnData"] is False
    assert (props["Threshold"], props["EvaluationPeriods"]) == (0, 1)
    assert props["ComparisonOperator"] == "GreaterThanThreshold"
    assert props["TreatMissingData"] == "notBreaching"


def test_the_application_log_level_never_exceeds_info(template: Template) -> None:
    # Lambda's JSON log format files unlevelled stdout lines (the EMF metrics) under
    # INFO, so WARN or ERROR there would drop every metric.
    level = template["Globals"]["Function"]["LoggingConfig"]["ApplicationLogLevel"]

    assert level == {"Fn::If": ["LogLevelIsDebug", "DEBUG", "INFO"]}
    assert template["Conditions"]["LogLevelIsDebug"] == {
        "Fn::Equals": [{"Ref": "LogLevel"}, "DEBUG"]
    }
    assert not {"WARN", "ERROR"} & set(level["Fn::If"][1:])


@pytest.mark.parametrize(
    "function",
    ["FetchFunction", "ProcessFunction", "MediaRetryFunction", "ComplianceFunction"],
)
def test_the_requested_log_level_is_applied_in_code(
    template: Template, function: str
) -> None:
    assert env_of(template, function)["LOG_LEVEL"] == {"Ref": "LogLevel"}


def test_per_account_metrics_are_opt_in_and_only_the_fetch_function_needs_them(
    template: Template,
) -> None:
    parameter = template["Parameters"]["MetricsPerAccount"]

    assert parameter["Default"] == "false"
    assert env_of(template, "FetchFunction")["METRICS_PER_ACCOUNT"] == {
        "Ref": "MetricsPerAccount"
    }


def test_the_default_namespace_is_per_stack_and_never_aws(template: Template) -> None:
    import re

    parameter = template["Parameters"]["MetricsNamespace"]
    pattern = re.compile(parameter["AllowedPattern"])

    assert parameter["Default"] == ""
    assert template["Conditions"]["HasMetricsNamespace"] == {
        "Fn::Not": [{"Fn::Equals": [{"Ref": "MetricsNamespace"}, ""]}]
    }
    assert pattern.fullmatch("")
    assert pattern.fullmatch("Team/Twitter Pipeline-1.0_x#y:z")
    assert not pattern.fullmatch("AWS/Lambda")
    assert not pattern.fullmatch("   ")  # only spaces is not a name
    assert pattern.fullmatch("a b")
    assert not pattern.fullmatch("bad*char")
    assert not pattern.fullmatch("x" * 256)
    assert pattern.fullmatch("AWSome")  # only the AWS/ prefix is reserved


def test_every_alarm_in_the_metrics_namespace_uses_the_total(
    template: Template,
) -> None:
    custom = [
        (name, res["Properties"])
        for name, res in alarms(template).items()
        if res["Properties"].get("Namespace") == NAMESPACE
    ]

    assert len(custom) >= 9
    for name, props in custom:
        assert "Dimensions" not in props, name  # the dimensionless set only


def test_the_freshness_alarms_are_stack_outputs_for_the_deploy_script(
    template: Template,
) -> None:
    outputs = template["Outputs"]

    assert outputs["FetchFreshnessAlarmName"] == {
        "Condition": "ScheduleIsEnabled",
        "Value": {"Ref": "FetchFreshnessAlarm"},
    }
    assert outputs["ComplianceFreshnessAlarmName"] == {
        "Condition": "ScheduleIsEnabled",
        "Value": {"Ref": "ComplianceFreshnessAlarm"},
    }
    assert alarms(template)["FetchFreshnessAlarm"]["Condition"] == "ScheduleIsEnabled"


def test_the_purge_backlog_is_a_metric_without_an_alarm(template: Template) -> None:
    watched = {a["Properties"].get("MetricName") for a in alarms(template).values()}

    assert "CompliancePurgeBacklog" not in watched
    assert "CompliancePurgeBacklog" in emitted_metric_names()
