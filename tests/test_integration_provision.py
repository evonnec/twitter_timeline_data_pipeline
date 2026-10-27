"""The integration run's function environments, worked out without any stack.

The integration job builds each function's environment from ``template.yaml`` before
anything runs; these catch a template variable it can't resolve in a normal test run
instead of only in the docker job.
"""

import copy

import pytest
from integration.provision import (
    FUNCTIONS,
    STACK_NAME,
    Queue,
    Resources,
    Template,
    function_env,
    load_template,
)


def _queue(name: str) -> Queue:
    dlq = f"{name}-dlq"
    return Queue(name, f"url/{name}", f"arn/{name}", dlq, f"url/{dlq}", 1)


RESOURCES = Resources(
    region="us-east-1",
    max_receive_count=3,
    tweets_table="it-tweets",
    state_table="it-state",
    bucket="it-media",
    secret_arn="arn/secret",
    tweets=_queue("it-tweets"),
    media_retry=_queue("it-media-retry"),
)


def _env(template: Template, function: str) -> dict[str, str]:
    return function_env(
        template,
        function,
        RESOURCES,
        internal_endpoint="http://localstack:4566",
        stub_url_internal="http://stub:8099",
    )


@pytest.fixture(scope="module")
def template() -> Template:
    return load_template()


@pytest.mark.parametrize("function", FUNCTIONS)
def test_every_template_variable_resolves(template: Template, function: str) -> None:
    variables = template["Resources"][function]["Properties"]["Environment"][
        "Variables"
    ]

    env = _env(template, function)

    assert set(variables) <= set(env)
    assert all(isinstance(value, str) and "${" not in value for value in env.values())


@pytest.mark.parametrize("function", FUNCTIONS)
def test_the_metrics_namespace_defaults_to_the_stack_name(
    template: Template, function: str
) -> None:
    """``MetricsNamespace`` defaults to empty, so ``!If`` takes the ``!Sub`` branch."""
    assert _env(template, function)["METRICS_NAMESPACE"] == (
        f"TwitterTimelinePipeline/{STACK_NAME}"
    )


def test_a_set_metrics_namespace_is_used_as_is(template: Template) -> None:
    changed = copy.deepcopy(template)
    changed["Parameters"]["MetricsNamespace"]["Default"] = "Custom/Space"

    assert _env(changed, "ProcessFunction")["METRICS_NAMESPACE"] == "Custom/Space"


@pytest.mark.parametrize(
    "value",
    [
        {"Ref": "NoSuchThing"},
        {"Fn::GetAtt": ["TweetsTable", "Arn"]},
        {"Fn::Sub": "${NoSuchThing}"},
        {"Fn::If": ["NoSuchCondition", "a", "b"]},
    ],
)
def test_a_variable_it_cannot_resolve_raises(template: Template, value: object) -> None:
    """So a new variable can't silently go missing from the integration run."""
    changed = copy.deepcopy(template)
    variables = changed["Resources"]["ProcessFunction"]["Properties"]["Environment"][
        "Variables"
    ]
    variables["NEW_VARIABLE"] = value

    with pytest.raises(KeyError):
        _env(changed, "ProcessFunction")
