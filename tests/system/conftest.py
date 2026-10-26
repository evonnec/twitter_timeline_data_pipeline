"""Fixtures for the system simulation: one moto world and one simulation per test."""

from collections.abc import Callable, Iterator

import pytest
from moto import mock_aws

from system.harness import Simulation
from system.runtime import all_package_modules, reset_module_state
from system.stack import Template

type MakeSim = Callable[..., Simulation]


@pytest.fixture
def make_sim(monkeypatch: pytest.MonkeyPatch) -> Iterator[MakeSim]:
    """Build the simulation, optionally with template parameter overrides (or a
    modified template, passed as ``template=``)."""
    with mock_aws():
        built: list[Simulation] = []

        def make(template: Template | None = None, **parameters: object) -> Simulation:
            assert not built, "one simulation per test"
            built.append(
                Simulation(monkeypatch, parameters=parameters, template=template)
            )
            return built[0]

        yield make
    reset_module_state(all_package_modules())


@pytest.fixture
def sim(make_sim: MakeSim) -> Simulation:
    """The stack with the template's default parameters."""
    return make_sim()
