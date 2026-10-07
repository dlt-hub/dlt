"""Checks that manifest values dlt emits are accepted by the runtime API models."""

from typing import Set, get_args

import pytest

from dlt._workspace.deployment.typing import TInterfaceType


def _runtime_enum_values(name: str) -> Set[str]:
    # runtime models ship only with the `hub` extra, so workspace tests cannot catch a mismatch
    models = pytest.importorskip("dlt_runtime.runtime_clients.api.models")
    return {member.value for member in getattr(models, name)}


def test_expose_interface_is_accepted_by_the_runtime() -> None:
    assert set(get_args(TInterfaceType)) <= _runtime_enum_values("TExposeSpecInterface")
