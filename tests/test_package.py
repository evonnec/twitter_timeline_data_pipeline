import importlib
from importlib.metadata import PackageNotFoundError

import pytest

import twitter_timeline_data_pipeline as package


def test_user_agent_carries_the_version() -> None:
    assert f"twitter-timeline-data-pipeline/{package.__version__}" == package.USER_AGENT


def test_version_is_a_string() -> None:
    assert isinstance(package.__version__, str)


def test_version_falls_back_when_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(_name: str) -> str:
        raise PackageNotFoundError

    monkeypatch.setattr("importlib.metadata.version", missing)
    try:
        assert importlib.reload(package).__version__ == "0.0.0"
    finally:
        monkeypatch.undo()
        importlib.reload(package)
