from typing import List, Tuple, Dict, Union, cast

import dlt
import pytest
import pytest
import os

from copy import deepcopy
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.data_writers.writers import TLoaderFileFormat
from dlt.common.destination.reference import Destination
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.common.utils import uniq_id
from dlt.common.exceptions import InvalidDestinationReference
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs import ConnectionStringCredentials

from tests.load.utils import (
    TABLE_ROW_ALL_DATA_TYPES,
    TABLE_UPDATE_COLUMNS_SCHEMA,
    assert_all_data_types_row,
)

SUPPORTED_LOADER_FORMATS = ["parquet", "puae-jsonl"]


def _run_through_sink(
    items: TDataItems,
    loader_file_format: TLoaderFileFormat,
    columns=None,
    batch_size: int = 10,
) -> List[Tuple[TDataItems, TTableSchema]]:
    """
    runs a list of items through the sink destination and returns colleceted calls
    """
    calls: List[Tuple[TDataItems, TTableSchema]] = []

    @dlt.destination(loader_file_format=loader_file_format, batch_size=batch_size)
    def test_sink(items: TDataItems, table: TTableSchema) -> None:
        nonlocal calls
        # convert pyarrow table to dict list here to make tests more simple downstream
        if loader_file_format == "parquet":
            items = items.to_pylist()  # type: ignore
        calls.append((items, table))

    @dlt.resource(columns=columns, table_name="items")
    def items_resource() -> TDataItems:
        nonlocal items
        yield items

    p = dlt.pipeline("sink_test", destination=test_sink, full_refresh=True)
    p.run([items_resource()])

    return calls


@pytest.mark.parametrize("loader_file_format", SUPPORTED_LOADER_FORMATS)
def test_all_datatypes(loader_file_format: TLoaderFileFormat) -> None:
    data_types = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    column_schemas = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    sink_calls = _run_through_sink(
        [data_types, data_types, data_types],
        loader_file_format,
        columns=column_schemas,
        batch_size=1,
    )

    # inspect result
    assert len(sink_calls) == 3

    item = sink_calls[0][0][0]

    # filter out _dlt columns
    item = {k: v for k, v in item.items() if not k.startswith("_dlt")}

    # null values are not emitted
    data_types = {k: v for k, v in data_types.items() if v is not None}

    assert_all_data_types_row(item, expect_filtered_null_columns=True)


@pytest.mark.parametrize("loader_file_format", SUPPORTED_LOADER_FORMATS)
@pytest.mark.parametrize("batch_size", [1, 10, 23])
def test_batch_size(loader_file_format: TLoaderFileFormat, batch_size: int) -> None:
    items = [{"id": i, "value": str(i)} for i in range(100)]

    sink_calls = _run_through_sink(items, loader_file_format, batch_size=batch_size)

    if batch_size == 1:
        assert len(sink_calls) == 100
        # one item per call
        assert sink_calls[0][0][0].items() == {"id": 0, "value": "0"}.items()
    elif batch_size == 10:
        assert len(sink_calls) == 10
        # ten items in first call
        assert len(sink_calls[0][0]) == 10
        assert sink_calls[0][0][0].items() == {"id": 0, "value": "0"}.items()
    elif batch_size == 23:
        assert len(sink_calls) == 5
        # 23 items in first call
        assert len(sink_calls[0][0]) == 23
        assert sink_calls[0][0][0].items() == {"id": 0, "value": "0"}.items()

    # check all items are present
    all_items = set()
    for call in sink_calls:
        item = call[0]
        for entry in item:
            all_items.add(entry["value"])

    assert len(all_items) == 100
    for i in range(100):
        assert str(i) in all_items


global_calls: List[Tuple[TDataItems, TTableSchema]] = []


def global_sink_func(items: TDataItems, table: TTableSchema) -> None:
    global global_calls
    if table["name"].startswith("_dlt"):
        return
    global_calls.append((items, table))


def test_instantiation() -> None:
    calls: List[Tuple[TDataItems, TTableSchema]] = []

    # NOTE: we also test injection of config vars here
    def local_sink_func(items: TDataItems, table: TTableSchema, my_val=dlt.config.value, /) -> None:
        nonlocal calls
        if table["name"].startswith("_dlt"):
            return
        assert my_val == "something"

        calls.append((items, table))

    os.environ["DESTINATION__MY_VAL"] = "something"

    # test decorator
    calls = []
    p = dlt.pipeline("sink_test", destination=dlt.destination()(local_sink_func), full_refresh=True)  # type: ignore
    p.run([1, 2, 3], table_name="items")
    assert len(calls) == 1

    # test passing via from_reference
    calls = []
    p = dlt.pipeline(
        "sink_test",
        destination=Destination.from_reference("destination", destination_callable=local_sink_func),
        full_refresh=True,
    )
    p.run([1, 2, 3], table_name="items")
    assert len(calls) == 1

    # test passing string reference
    global global_calls
    global_calls = []
    p = dlt.pipeline(
        "sink_test",
        destination=Destination.from_reference(
            "destination",
            destination_callable="tests.destinations.test_custom_destination.global_sink_func",
        ),
        full_refresh=True,
    )
    p.run([1, 2, 3], table_name="items")
    assert len(global_calls) == 1

    # pass None credentials reference
    with pytest.raises(InvalidDestinationReference):
        p = dlt.pipeline(
            "sink_test",
            destination=Destination.from_reference("destination", destination_callable=None),
            full_refresh=True,
        )
        p.run([1, 2, 3], table_name="items")

    # pass invalid credentials module
    with pytest.raises(InvalidDestinationReference):
        p = dlt.pipeline(
            "sink_test",
            destination=Destination.from_reference(
                "destination", destination_callable="does.not.exist"
            ),
            full_refresh=True,
        )
        p.run([1, 2, 3], table_name="items")


@pytest.mark.parametrize("loader_file_format", SUPPORTED_LOADER_FORMATS)
@pytest.mark.parametrize("batch_size", [1, 10, 23])
def test_batched_transactions(loader_file_format: TLoaderFileFormat, batch_size: int) -> None:
    calls: Dict[str, List[TDataItems]] = {}
    # provoke errors on resources
    provoke_error: Dict[str, int] = {}

    @dlt.destination(
        loader_file_format=loader_file_format,
        batch_size=batch_size,
        skip_dlt_columns_and_tables=False,
    )
    def test_sink(items: TDataItems, table: TTableSchema) -> None:
        nonlocal calls
        table_name = table["name"]
        if table_name.startswith("_dlt"):
            return

        # convert pyarrow table to dict list here to make tests more simple downstream
        if loader_file_format == "parquet":
            items = items.to_pylist()  # type: ignore

        # provoke error if configured
        if table_name in provoke_error:
            for item in items:
                if provoke_error[table_name] == item["id"]:
                    raise AssertionError("Oh no!")

        calls.setdefault(table_name, []).append(items)

    @dlt.resource()
    def items() -> TDataItems:
        for i in range(100):
            yield {"id": i, "value": str(i)}

    @dlt.resource()
    def items2() -> TDataItems:
        for i in range(100):
            yield {"id": i, "value": str(i)}

    def assert_items_in_range(c: List[TDataItems], start: int, end: int) -> None:
        """
        Ensure all items where called and no duplicates are present
        """
        collected_items = set()
        for call in c:
            for item in call:
                assert item["value"] not in collected_items
                collected_items.add(item["value"])
        assert len(collected_items) == end - start
        for i in range(start, end):
            assert str(i) in collected_items

    # no errors are set, all items should be processed
    p = dlt.pipeline("sink_test", destination=test_sink, full_refresh=True)
    load_id = p.run([items(), items2()]).loads_ids[0]
    assert_items_in_range(calls["items"], 0, 100)
    assert_items_in_range(calls["items2"], 0, 100)

    # destination state should have all items
    destination_state = p.get_load_package_state(load_id)["destination_state"]
    values = {k.split(".")[0]: v for k, v in destination_state.items()}
    assert values == {"_dlt_pipeline_state": 1, "items": 100, "items2": 100}

    # provoke errors
    calls = {}
    provoke_error = {"items": 25, "items2": 45}
    p = dlt.pipeline("sink_test", destination=test_sink, full_refresh=True)
    with pytest.raises(PipelineStepFailed):
        p.run([items(), items2()])

    # we should have data for one load id saved here
    load_id = p.list_normalized_load_packages()[0]
    destination_state = p.get_load_package_state(load_id)["destination_state"]

    # get saved indexes mapped to table (this test will only work for one job per table)
    values = {k.split(".")[0]: v for k, v in destination_state.items()}

    # partly loaded, pointers in state should be right
    if batch_size == 1:
        assert_items_in_range(calls["items"], 0, 25)
        assert_items_in_range(calls["items2"], 0, 45)
        # one pointer for state, one for items, one for items2...
        assert values == {"_dlt_pipeline_state": 1, "items": 25, "items2": 45}
    elif batch_size == 10:
        assert_items_in_range(calls["items"], 0, 20)
        assert_items_in_range(calls["items2"], 0, 40)
        assert values == {"_dlt_pipeline_state": 1, "items": 20, "items2": 40}
    elif batch_size == 23:
        assert_items_in_range(calls["items"], 0, 23)
        assert_items_in_range(calls["items2"], 0, 23)
        assert values == {"_dlt_pipeline_state": 1, "items": 23, "items2": 23}
    else:
        raise AssertionError("Unknown batch size")

    # load the rest
    first_calls = deepcopy(calls)
    provoke_error = {}
    calls = {}
    p.load()

    # destination state should have all items
    destination_state = p.get_load_package_state(load_id)["destination_state"]
    values = {k.split(".")[0]: v for k, v in destination_state.items()}
    assert values == {"_dlt_pipeline_state": 1, "items": 100, "items2": 100}

    # both calls combined should have every item called just once
    assert_items_in_range(calls["items"] + first_calls["items"], 0, 100)
    assert_items_in_range(calls["items2"] + first_calls["items2"], 0, 100)


def test_naming_convention() -> None:
    @dlt.resource(table_name="PErson")
    def resource():
        yield [{"UpperCase": 1, "snake_case": 1, "camelCase": 1}]

    # check snake case
    @dlt.destination(naming_convention="snake_case")
    def snake_sink(items, table):
        if table["name"].startswith("_dlt"):
            return
        assert table["name"] == "p_erson"
        assert table["columns"]["upper_case"]["name"] == "upper_case"
        assert table["columns"]["snake_case"]["name"] == "snake_case"
        assert table["columns"]["camel_case"]["name"] == "camel_case"

    dlt.pipeline("sink_test", destination=snake_sink, full_refresh=True).run(resource())

    # check default (which is direct)
    @dlt.destination()
    def direct_sink(items, table):
        if table["name"].startswith("_dlt"):
            return
        assert table["name"] == "PErson"
        assert table["columns"]["UpperCase"]["name"] == "UpperCase"
        assert table["columns"]["snake_case"]["name"] == "snake_case"
        assert table["columns"]["camelCase"]["name"] == "camelCase"

    dlt.pipeline("sink_test", destination=direct_sink, full_refresh=True).run(resource())


def test_file_batch() -> None:
    @dlt.resource(table_name="person")
    def resource1():
        for i in range(100):
            yield [{"id": i, "name": f"Name {i}"}]

    @dlt.resource(table_name="address")
    def resource2():
        for i in range(50):
            yield [{"id": i, "city": f"City {i}"}]

    @dlt.destination(batch_size=0, loader_file_format="parquet")
    def direct_sink(file_path, table):
        if table["name"].startswith("_dlt"):
            return
        from dlt.common.libs.pyarrow import pyarrow

        assert table["name"] in ["person", "address"]

        with pyarrow.parquet.ParquetFile(file_path) as reader:
            assert reader.metadata.num_rows == (100 if table["name"] == "person" else 50)

    dlt.pipeline("sink_test", destination=direct_sink, full_refresh=True).run(
        [resource1(), resource2()]
    )


def test_config_spec() -> None:
    # NOTE: define the destination before the env var to test env vars are evaluated
    # at runtime
    @dlt.destination()
    def my_sink(file_path, table, my_val=dlt.config.value):
        assert my_val == "something"

    print(my_sink)

    # if no value is present, it should raise
    with pytest.raises(ConfigFieldMissingException):
        dlt.pipeline("sink_test", destination=my_sink, full_refresh=True).run(
            [1, 2, 3], table_name="items"
        )

    # we may give the value via __callable__ function
    dlt.pipeline("sink_test", destination=my_sink(my_val="something"), full_refresh=True).run(
        [1, 2, 3], table_name="items"
    )

    # right value will pass
    os.environ["DESTINATION__MY_SINK__MY_VAL"] = "something"
    dlt.pipeline("sink_test", destination=my_sink, full_refresh=True).run(
        [1, 2, 3], table_name="items"
    )

    # wrong value will raise
    os.environ["DESTINATION__MY_SINK__MY_VAL"] = "wrong"
    with pytest.raises(PipelineStepFailed):
        dlt.pipeline("sink_test", destination=my_sink, full_refresh=True).run(
            [1, 2, 3], table_name="items"
        )

    # will respect given name
    @dlt.destination(name="some_name")
    def other_sink(file_path, table, my_val=dlt.config.value):
        assert my_val == "something"

    # if no value is present, it should raise
    with pytest.raises(ConfigFieldMissingException):
        dlt.pipeline("sink_test", destination=other_sink, full_refresh=True).run(
            [1, 2, 3], table_name="items"
        )

    # right value will pass
    os.environ["DESTINATION__SOME_NAME__MY_VAL"] = "something"
    dlt.pipeline("sink_test", destination=other_sink, full_refresh=True).run(
        [1, 2, 3], table_name="items"
    )

    # test nested spec

    @dlt.destination()
    def my_gcp_sink(
        file_path,
        table,
        credentials: ConnectionStringCredentials = dlt.secrets.value,
    ):
        assert credentials.drivername == "my_driver"
        assert credentials.database == "my_database"
        assert credentials.username == "my_user_name"

    # missing spec
    with pytest.raises(ConfigFieldMissingException):
        dlt.pipeline("sink_test", destination=my_gcp_sink, full_refresh=True).run(
            [1, 2, 3], table_name="items"
        )

    # add gcp vars (in different sections for testing)
    os.environ["SINK_TEST__DESTINATION__CREDENTIALS__DRIVERNAME"] = "my_driver"
    os.environ["DESTINATION__CREDENTIALS__DATABASE"] = "my_database"
    os.environ["CREDENTIALS__USERNAME"] = "my_user_name"

    # now it will run
    dlt.pipeline("sink_test", destination=my_gcp_sink, full_refresh=True).run(
        [1, 2, 3], table_name="items"
    )


@pytest.mark.parametrize("loader_file_format", SUPPORTED_LOADER_FORMATS)
@pytest.mark.parametrize("remove_stuff", [True, False])
def test_remove_internal_tables_and_columns(loader_file_format, remove_stuff) -> None:
    found_dlt_table = False
    found_dlt_column = False
    found_dlt_column_value = False

    @dlt.destination(
        skip_dlt_columns_and_tables=remove_stuff, loader_file_format=loader_file_format
    )
    def test_sink(items, table):
        nonlocal found_dlt_table, found_dlt_column, found_dlt_column_value
        if table["name"].startswith("_dlt"):
            found_dlt_table = True
        for column in table["columns"].keys():
            if column.startswith("_dlt"):
                found_dlt_column = True

        # check actual data items
        if loader_file_format == "puae-jsonl":
            for item in items:
                for key in item.keys():
                    if key.startswith("_dlt"):
                        found_dlt_column_value = True
        else:
            for column in items.column_names:
                if column.startswith("_dlt"):
                    found_dlt_column_value = True

    # test with and without removing
    p = dlt.pipeline("sink_test", destination=test_sink, full_refresh=True)
    p.run([{"id": 1, "value": "1"}], table_name="some_table")

    assert found_dlt_column != remove_stuff
    assert found_dlt_table != remove_stuff
    assert found_dlt_column_value != remove_stuff
