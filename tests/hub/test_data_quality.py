import dlt
from dlt.extract import DltResource


def test_direct_module_import():
    # NOTE: this is still re-import so submodule structure is not importable
    from dlt.hub import data_quality as dq

    dq.checks.is_in("payment_methods", ["card", "cash", "voucher"])  # type: ignore[attr-defined,unused-ignore]


def test_from_module_import():
    """Can import the registered `dlthub` submodule from `dlt.hub`."""
    from dlt.hub import data_quality as dq_hub
    from dlthub import data_quality as dq_dlthub


def test_data_quality_entrypoints():
    from dlt.hub import data_quality as dq

    # access a single check
    assert dq.checks.is_not_null is not None  # type: ignore[attr-defined,unused-ignore]
    assert dq.CheckSuite is not None  # type: ignore[attr-defined,unused-ignore]
    assert dq.prepare_checks is not None  # type: ignore[attr-defined,unused-ignore]

    from dlt.hub.data_quality import with_checks  # type: ignore[attr-defined,unused-ignore]
    from dlt.hub.data_quality import with_metrics  # type: ignore[attr-defined,unused-ignore]

    @with_checks(
        dq.checks.is_not_null("foo"),  # type: ignore[attr-defined,unused-ignore]
        dq.checks.is_unique("value"),  # type: ignore[attr-defined,unused-ignore]
    )
    @with_metrics(dq.metrics.table.row_count())  # type: ignore[attr-defined,unused-ignore]
    @dlt.resource
    def checked_resource():
        pass

    assert type(checked_resource()) is DltResource
