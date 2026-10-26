"""Invariants of template.yaml that the code depends on, plus a cfn-lint pass."""

import ast
import importlib
import inspect
from pathlib import Path
from typing import Any

import pytest
from cfnlint.api import lint_all  # type: ignore[import-untyped]
from cfnlint.decode import cfn_yaml  # type: ignore[import-untyped]

from twitter_timeline_data_pipeline.config import ENV_VARS

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


def test_there_are_exactly_two_functions(template: Template) -> None:
    assert set(functions(template)) == {"FetchFunction", "ProcessFunction"}


@pytest.mark.parametrize(
    ("function", "module"),
    [
        ("FetchFunction", "twitter_timeline_data_pipeline.handlers.fetch"),
        ("ProcessFunction", "twitter_timeline_data_pipeline.handlers.process"),
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
    rules = {r["Id"]: r for r in bucket["LifecycleConfiguration"]["Rules"]}

    assert bucket["VersioningConfiguration"]["Status"] == "Enabled"
    expire = rules["expire-noncurrent-versions"]
    assert expire["Status"] == "Enabled"
    assert expire["NoncurrentVersionExpiration"]["NoncurrentDays"] >= 1


def test_media_moves_to_intelligent_tiering(template: Template) -> None:
    rules = properties(template, "MediaBucket")["LifecycleConfiguration"]["Rules"]
    (rule,) = (r for r in rules if r["Id"] == "tier-media")

    assert rule["Prefix"] == "media/"
    assert {t["StorageClass"] for t in rule["Transitions"]} == {"INTELLIGENT_TIERING"}


def test_username_index_projects_only_listing_attributes(template: Template) -> None:
    (index,) = properties(template, "TweetsTable")["GlobalSecondaryIndexes"]

    assert index["Projection"] == {
        "ProjectionType": "INCLUDE",
        "NonKeyAttributes": ["text", "media_count", "public_metrics"],
    }


def test_queue_redrives_to_the_dead_letter_queue(template: Template) -> None:
    policy = properties(template, "TweetsQueue")["RedrivePolicy"]

    assert policy["deadLetterTargetArn"] == {
        "Fn::GetAtt": ["TweetsDeadLetterQueue", "Arn"]
    }
    assert policy["maxReceiveCount"] >= 1


def test_dead_letter_queue_keeps_messages_longer_than_the_source(
    template: Template,
) -> None:
    dlq = properties(template, "TweetsDeadLetterQueue")["MessageRetentionPeriod"]

    assert dlq > 345_600  # longer than the 4-day default the source queue keeps


def sqs_event(template: Template) -> dict[str, Any]:
    events = properties(template, "ProcessFunction")["Events"]
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
    [("FetchFunction", "fetch.py"), ("ProcessFunction", "process.py")],
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


@pytest.mark.parametrize("function", ["FetchFunction", "ProcessFunction"])
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
    (gsi,) = props["GlobalSecondaryIndexes"]
    assert gsi["IndexName"] == "username-created_at-index"
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
