"""Manifest values dlt emits must be accepted by the runtime that receives them.

The runtime validates an uploaded manifest against its generated API models, which dlt
cannot see: `dlthub-client` ships only with the `hub` extra, so `make test-workspace` never
loads it and a value dlt invents looks valid until `dlthub deploy` rejects it.
"""

from typing import Set, get_args

import pytest

from dlt._workspace.deployment.typing import TInterfaceType


def _runtime_enum_values(name: str) -> Set[str]:
    models = pytest.importorskip("dlt_runtime.runtime_clients.api.models")
    return {member.value for member in getattr(models, name)}


def test_expose_interface_is_accepted_by_the_runtime() -> None:
    assert set(get_args(TInterfaceType)) <= _runtime_enum_values("TExposeSpecInterface")
