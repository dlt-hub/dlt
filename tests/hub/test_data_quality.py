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
    assert dq.checks.is_not_null is not None
    assert dq.CheckSuite is not None
    assert dq.prepare_checks is not None

    from dlt.hub.data_quality import with_checks
    from dlt.hub.data_quality import with_metrics

    @with_checks(
        dq.checks.is_not_null("foo"),
        dq.checks.is_unique("value"),
    )
    @with_metrics(dq.metrics.table.row_count())
    @dlt.resource
    def checked_resource():
        pass

    assert type(checked_resource()) is DltResource
