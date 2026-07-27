import os
from typing import Any, Dict, List, cast

import dlt
from dlt.common.metrics import TDatasetDataLocation, data_location_version
from dlt.common.utils import uniq_id

from tests.pipeline.utils import assert_load_info


def _outputs(info: Any) -> List[TDatasetDataLocation]:
    """All output locations of a load info, across load packages"""
    return [location for metrics in info.metrics.values() for location in metrics[0]["outputs"]]


def _by_resource(locations: List[TDatasetDataLocation]) -> Dict[str, TDatasetDataLocation]:
    return {location["resource_name"]: location for location in locations}


def _inputs(info: Any) -> List[Any]:
    """All input locations of an extract info"""
    return info.metrics[info.loads_ids[0]][0]["inputs"]


@dlt.resource(primary_key="id", name="orders")
def orders_with_nested() -> Any:
    yield [
        {"id": 1, "items": [{"sku": "a"}, {"sku": "b"}]},
        {"id": 2, "items": [{"sku": "c"}]},
    ]


@dlt.source(name="sales")
def sales_source() -> Any:
    return dlt.resource([{"id": 1}], name="orders")


@dlt.source(name="crm")
def crm_source() -> Any:
    return dlt.resource([{"id": 1}], name="contacts")


def test_output_locations_identify_the_dataset() -> None:
    """One entry per resource, naming the dataset the way the input side of another run will"""
    # rotate the writer so a table really is spread over several load jobs
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "1"

    pipeline = dlt.pipeline(
        pipeline_name="out_" + uniq_id(),
        destination="duckdb",
        dataset_name="My_DataSet",
        dev_mode=True,
    )
    info = pipeline.run([orders_with_nested(), dlt.resource([{"id": 1}], name="customers")])
    assert_load_info(info)

    locations = _by_resource(_outputs(info))
    # nested tables belong to the resource owning their root table, each listed once even though
    # the writer split them over several jobs
    assert locations["orders"]["tables"] == ["orders", "orders__items"]
    assert locations["customers"]["tables"] == ["customers"]
    orders_jobs = [
        job
        for metrics in info.metrics.values()
        for job in metrics[0]["job_metrics"].values()
        if job.table_name == "orders"
    ]
    assert len(orders_jobs) > 1

    location = locations["orders"]
    assert location["kind"] == "dataset"
    assert location["destination_type"] == "dlt.destinations.duckdb"
    assert location["destination_name"] == "duckdb"
    assert location["case_sensitive"] is False
    # duckdb does not casefold, the identity function is still recorded
    assert location["casefold"] == "str"
    # duckdb has no fingerprint, the column is present and empty
    assert location["destination_fingerprint"] == ""
    # the logical name is kept for display, the normalized one is the join key between sides
    assert location["dataset_name"] == pipeline.dataset_name
    assert location["physical_dataset_name"].startswith("my_data_set")
    assert location["physical_dataset_name"] == info.dataset_name
    assert location["version"] == data_location_version(location["schemas"])

    # a new column changes the schema hash and with it the version of the location
    info = pipeline.run(dlt.resource([{"id": 2, "extra": "x"}], name="customers"))
    assert _by_resource(_outputs(info))["customers"]["version"] != location["version"]


def test_output_locations_of_multiple_datasets() -> None:
    """A single load step writes several datasets when each schema resolves to its own"""
    pipeline = dlt.pipeline(
        pipeline_name="out_multi_" + uniq_id(),
        destination="duckdb",
        dataset_name="marts",
        dev_mode=True,
    )
    pipeline.config.use_single_dataset = False

    info = pipeline.run([sales_source(), crm_source()])
    assert_load_info(info, expected_load_packages=2)
    other_schema = next(s for s in pipeline.schema_names if s != pipeline.default_schema_name)

    # a location carries only the schema of its own package, versioned by that schema alone
    locations = _outputs(info)
    by_schema = {
        location["schemas"][0]["name"]: location
        for location in locations
        if location["resource_name"] in ("orders", "contacts")
    }
    assert set(by_schema) == {"sales", "crm"}
    assert by_schema["sales"]["version"] != by_schema["crm"]["version"]

    # the default schema keeps the configured dataset, the other is suffixed with its schema name
    physical_of = {name: location["physical_dataset_name"] for name, location in by_schema.items()}
    assert physical_of[pipeline.default_schema_name] == pipeline.dataset_name
    assert physical_of[other_schema] == f"{pipeline.dataset_name}_{other_schema}"

    # `LoadInfo.dataset_name` can only name one of them, the output locations record both
    assert len(set(physical_of.values())) == 2
    assert info.dataset_name in set(physical_of.values())

    # pipeline state is written into every dataset, so a resource name alone does not identify a
    # location - each row is self contained and carries the dataset it was written to
    state_datasets = {
        location["physical_dataset_name"]
        for location in locations
        if location["resource_name"] == "_dlt_pipeline_state"
    }
    assert state_datasets == set(physical_of.values())

    # flattened rows stay attributable to their package
    rows = info.asdict()["outputs"]
    assert len({row["load_id"] for row in rows}) == 2
    assert {row["physical_dataset_name"] for row in rows} == set(physical_of.values())

    # a schema loaded by a later run resolves to its own dataset, leaving the first ones alone
    info = pipeline.run(dlt.resource([{"id": 1}], name="c_table"), schema=dlt.Schema("other"))
    later = _by_resource(_outputs(info))["c_table"]
    assert later["physical_dataset_name"] == f"{pipeline.dataset_name}_other"
    assert [s["name"] for s in later["schemas"]] == ["other"]


def test_input_locations_replaced_on_each_run() -> None:
    """A reused resource instance re-reads its location every run, `replace` drops the stale one"""
    bucket = {"url": "file:///first"}

    @dlt.resource(name="files")
    def files() -> Any:
        resource = dlt.current.resource()
        resource.add_input(
            {"kind": "filesystem", "resource_name": resource.name, "location": bucket["url"]},
            replace=True,
        )
        yield [{"id": 1}]

    resource = files()
    pipeline = dlt.pipeline(
        pipeline_name="inputs_replaced_" + uniq_id(), destination="duckdb", dev_mode=True
    )
    assert [location["location"] for location in _inputs(pipeline.extract(resource))] == [
        "file:///first"
    ]

    bucket["url"] = "file:///second"
    assert [location["location"] for location in _inputs(pipeline.extract(resource))] == [
        "file:///second"
    ]


def test_input_and_output_locations_join_on_physical_dataset_name() -> None:
    """The read side of a run and the write side of the run before it compare field for field"""
    source_pipeline = dlt.pipeline(
        pipeline_name="lineage_src_" + uniq_id(),
        destination="duckdb",
        dataset_name="Raw_Data",
        dev_mode=True,
    )
    load_info = source_pipeline.run(orders_with_nested())
    assert_load_info(load_info)
    written = _by_resource(_outputs(load_info))["orders"]

    dataset = source_pipeline.dataset()

    @dlt.resource(name="orders_copy")
    def copy_orders() -> Any:
        yield list(dataset.table("orders").fetchall())

    resource = copy_orders()
    resource.add_input(
        TDatasetDataLocation(  # type: ignore[typeddict-item]
            kind="dataset",
            resource_name="orders_copy",
            location=written["location"],
            dataset_name=dataset.dataset_name,
            physical_dataset_name=written["physical_dataset_name"],
            tables=["orders"],
        )
    )

    target_pipeline = dlt.pipeline(
        pipeline_name="lineage_tgt_" + uniq_id(), destination="duckdb", dev_mode=True
    )
    extract_info = target_pipeline.extract(resource)

    read = cast(
        TDatasetDataLocation, extract_info.metrics[extract_info.loads_ids[0]][0]["inputs"][0]
    )
    assert read["physical_dataset_name"] == written["physical_dataset_name"]
    assert read["location"] == written["location"]
    assert read["resource_name"] == "orders_copy"
