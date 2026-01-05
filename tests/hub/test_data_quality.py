import dlt


def test_direct_module_import():
    # NOTE: this is still re-import so submodule structure is not importable
    from dlt.hub import data_quality as dq

    dq.checks.is_in("payment_methods", ["card", "cash", "voucher"])  # type: ignore[attr-defined,unused-ignore]


def test_from_module_import():
    """Can import the registered `dlthub` submodule from `dlt.hub`."""
    from dlt.hub import data_quality


def test_data_quality_entrypoints():
    import dlthub.data_quality as dq

    # access a single check
    assert dlt.hub.data_quality is not dq
    assert dlt.hub.data_quality.checks is dq.checks  # type: ignore[attr-defined,unused-ignore]
    assert dlt.hub.data_quality.checks.is_not_null is dq.checks.is_not_null  # type: ignore[attr-defined,unused-ignore]
    assert dlt.hub.data_quality.CheckSuite is dq.CheckSuite  # type: ignore[attr-defined,unused-ignore]
    assert dlt.hub.data_quality.prepare_checks is dq.prepare_checks  # type: ignore[attr-defined,unused-ignore]
