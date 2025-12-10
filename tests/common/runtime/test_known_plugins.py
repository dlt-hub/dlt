"""Tests behavior of know plugins when they are not installed"""
import pytest

from dlt.common.exceptions import MissingDependencyException


def test_hub_fallback() -> None:
    import dlt.hub

    if dlt.hub.__found__ or not isinstance(dlt.hub.__exception__, ModuleNotFoundError):
        pytest.skip(
            "Skip test due to hub being present or partially loaded: " + str(dlt.hub.__exception__)
        )

    assert isinstance(dlt.hub.__exception__, ModuleNotFoundError)

    # accessing attributes generates import error

    with pytest.raises(MissingDependencyException) as missing_ex:
        dlt.hub.transformation

    assert missing_ex.value.dependencies[0] == "dlt[hub]"
