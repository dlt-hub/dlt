import pytest

import dlt


def test_direct_module_import():
    """It's currently not possible to import the module directly"""
    with pytest.raises(ModuleNotFoundError):
        import dlt.hub.data_quality  # type: ignore[import-not-found]


def test_from_module_import():
    """Can import the registered `dlthub` submodule from `dlt.hub`."""
    from dlt.hub import data_quality


def test_data_quality_entrypoints():
    import dlthub.data_quality as dq

    # access a single check
    assert dlt.hub.data_quality is dq
    assert dlt.hub.data_quality.checks is dq.checks
    assert dlt.hub.data_quality.checks.is_not_null is dq.checks.is_not_null
    assert dlt.hub.data_quality.CheckSuite is dq.CheckSuite
    assert dlt.hub.data_quality.prepare_checks is dq.prepare_checks
