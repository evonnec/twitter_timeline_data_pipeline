"""The EMF writer: document shape, units, switches, and that it never raises."""

import json
import logging
import sys
from typing import Any

import pytest
from conftest import Emf

from twitter_timeline_data_pipeline import metrics
from twitter_timeline_data_pipeline.config import ENV_VARS, Settings


def test_a_line_is_valid_emf_with_unit_namespace_and_both_dimension_sets(
    emf: Emf, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("METRICS_NAMESPACE", "Custom")

    metrics.emit(
        {"TweetsFetched": 3, "ProcessingLagSeconds": [1.5, 2]}, {"Username": "u"}
    )

    (doc,) = emf.read()
    assert doc["_aws"]["Timestamp"] > 0
    assert doc["_aws"]["CloudWatchMetrics"] == [
        {
            "Namespace": "Custom",
            "Dimensions": [["Username"], []],
            "Metrics": [
                {"Name": "TweetsFetched", "Unit": "Count"},
                {"Name": "ProcessingLagSeconds", "Unit": "Seconds"},
            ],
        }
    ]
    assert (doc["Username"], doc["TweetsFetched"]) == ("u", 3)
    assert doc["ProcessingLagSeconds"] == [1.5, 2]


def test_without_dimensions_the_only_set_is_empty_and_properties_are_fields(
    emf: Emf,
) -> None:
    metrics.emit({"MediaBytes": 9}, properties={"run_id": "r1", "skipped": None})

    (doc,) = emf.read()
    (block,) = doc["_aws"]["CloudWatchMetrics"]
    assert block["Namespace"] == metrics.DEFAULT_NAMESPACE == "TwitterTimelinePipeline"
    assert block["Dimensions"] == [[]]
    assert block["Metrics"] == [{"Name": "MediaBytes", "Unit": "Bytes"}]
    assert doc["run_id"] == "r1"
    assert "skipped" not in doc


def test_a_long_distribution_is_cut_to_the_cloudwatch_limit() -> None:
    doc = metrics.build({"ProcessingLagSeconds": list(range(250))})

    assert doc["ProcessingLagSeconds"] == list(range(metrics.MAX_VALUES))


def test_nothing_is_written_outside_lambda_unless_forced(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    monkeypatch.delenv("METRICS_EMIT", raising=False)

    metrics.emit({"TweetsFetched": 1})
    assert capsys.readouterr().out == ""

    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
    metrics.emit({"TweetsFetched": 1})
    assert json.loads(capsys.readouterr().out)["TweetsFetched"] == 1


def test_an_empty_batch_writes_nothing(
    emf: Emf, capsys: pytest.CaptureFixture[str]
) -> None:
    metrics.emit({})

    assert capsys.readouterr().out == ""


def test_an_undeclared_name_is_swallowed_and_logged(
    emf: Emf, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    with caplog.at_level(logging.WARNING):
        metrics.emit({"NotDeclared": 1})  # must not raise

    assert capsys.readouterr().out == ""
    assert "could not emit metrics ['NotDeclared']" in caplog.text


def test_a_write_failure_is_swallowed_and_logged(
    emf: Emf, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class Broken:
        def write(self, _text: str) -> None:
            raise OSError("stdout closed")

        def flush(self) -> None: ...

    monkeypatch.setattr(sys, "stdout", Broken())

    with caplog.at_level(logging.WARNING):
        metrics.emit({"TweetsFetched": 1})  # must not raise

    assert "could not emit metrics ['TweetsFetched']" in caplog.text


def test_an_unserializable_property_is_swallowed(emf: Emf) -> None:
    metrics.emit({"TweetsFetched": 1}, properties={"bad": {1, 2}})  # a set: not JSON

    assert emf.read() == []


def test_the_namespace_variable_is_a_setting_too() -> None:
    assert ENV_VARS["metrics_namespace"] == metrics.NAMESPACE_ENV
    assert Settings.from_env({"METRICS_NAMESPACE": "X"}).metrics_namespace == "X"
    assert Settings.from_env({}).metrics_namespace == metrics.DEFAULT_NAMESPACE


def test_every_declared_unit_is_a_cloudwatch_unit() -> None:
    assert set(metrics.UNITS.values()) <= {"Count", "Seconds", "Bytes"}


def test_documents_are_json_lines(emf: Emf, capsys: pytest.CaptureFixture[str]) -> None:
    metrics.emit({"TweetsFetched": 1})
    metrics.emit({"TweetsEnqueued": 2})

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    parsed: list[dict[str, Any]] = [json.loads(line) for line in lines]
    assert [next(k for k in d if k.startswith("Tweets")) for d in parsed] == [
        "TweetsFetched",
        "TweetsEnqueued",
    ]


def test_the_emf_validator_rejects_a_timestamp_in_seconds() -> None:
    from conftest import emf_problems

    doc = metrics.build({"TweetsFetched": 1})
    assert emf_problems(doc) == []

    doc["_aws"]["Timestamp"] = int(doc["_aws"]["Timestamp"] / 1000)  # type: ignore[index]
    assert any("Timestamp" in p for p in emf_problems(doc))
    assert emf_problems({"x": 1}) == ["no _aws object"]


def test_account_dimensions_follow_the_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("METRICS_PER_ACCOUNT", raising=False)
    assert metrics.account_dimensions("nasa") == {}
    monkeypatch.setenv("METRICS_PER_ACCOUNT", "false")
    assert metrics.account_dimensions("nasa") == {}
    monkeypatch.setenv("METRICS_PER_ACCOUNT", "true")
    assert metrics.account_dimensions("nasa") == {"Username": "nasa"}


def test_the_new_settings_are_read() -> None:
    settings = Settings.from_env({"METRICS_PER_ACCOUNT": "true", "LOG_LEVEL": "warn"})

    assert (settings.metrics_per_account, settings.log_level) == (True, "WARN")
    assert Settings.from_env({}).metrics_per_account is False
    assert Settings.from_env({}).log_level == "INFO"
