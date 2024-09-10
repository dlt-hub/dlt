import asyncio
import inspect
import os
import random
from datetime import datetime  # noqa: I251
from itertools import chain, count
from time import sleep
from typing import Any, Optional
from unittest import mock

import duckdb
import pyarrow as pa
import pytest

import dlt
from dlt.common import Decimal
from dlt.common.configuration import ConfigurationValueError
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import InvalidNativeValue
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    configspec,
)
from dlt.common.json import json
from dlt.common.pendulum import pendulum, timedelta
from dlt.common.pipeline import NormalizeInfo, StateInjectableContext, resource_state
from dlt.common.schema.schema import Schema
from dlt.common.utils import chunks, digest128, uniq_id
from dlt.extract import DltSource
from dlt.extract.incremental import Incremental, IncrementalResourceWrapper
from dlt.extract.incremental.exceptions import (
    IncrementalCursorInvalidCoercion,
    IncrementalCursorPathHasValueNone,
    IncrementalCursorPathMissing,
    IncrementalPrimaryKeyMissing,
)
from dlt.extract.items import ValidateItem
from dlt.extract.resource import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.helpers.transform import take_first
from tests.extract.utils import AssertItems, data_item_to_list
from tests.pipeline.utils import assert_query_data
from tests.utils import (
    ALL_TEST_DATA_ITEM_FORMATS,
    TestDataItemFormat,
    data_item_length,
    data_to_item_format,
)


@pytest.fixture(autouse=True)
def switch_to_fifo():
    """most of the following tests rely on the old default fifo next item mode"""
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"
    yield
    del os.environ["EXTRACT__NEXT_ITEM_MODE"]


def test_detect_incremental_arg() -> None:
    def incr_1(incremental: dlt.sources.incremental):  # type: ignore[type-arg]
        pass

    assert (
        IncrementalResourceWrapper.get_incremental_arg(inspect.signature(incr_1)).name
        == "incremental"
    )

    def incr_2(incremental: Incremental[str]):
        pass

    assert (
        IncrementalResourceWrapper.get_incremental_arg(inspect.signature(incr_2)).name
        == "incremental"
    )

    def incr_3(incremental=dlt.sources.incremental[str]("updated_at")):  # noqa
        pass

    assert (
        IncrementalResourceWrapper.get_incremental_arg(inspect.signature(incr_3)).name
        == "incremental"
    )

    def incr_4(incremental=Incremental[str]("updated_at")):  # noqa
        pass

    assert (
        IncrementalResourceWrapper.get_incremental_arg(inspect.signature(incr_4)).name
        == "incremental"
    )

    def incr_5(incremental: IncrementalResourceWrapper):
        pass

    assert IncrementalResourceWrapper.get_incremental_arg(inspect.signature(incr_5)) is None


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_single_items_last_value_state_is_updated(item_type: TestDataItemFormat) -> None:
    data = [
        {"created_at": 425},
        {"created_at": 426},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at")):
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())
    s = some_data.state["incremental"]["created_at"]
    assert s["last_value"] == 426


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_single_items_last_value_state_is_updated_transformer(
    item_type: TestDataItemFormat,
) -> None:
    data = [
        {"created_at": 425},
        {"created_at": 426},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.transformer
    def some_data(item, created_at=dlt.sources.incremental("created_at")):
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(dlt.resource([1, 2, 3], name="table") | some_data())

    s = some_data().state["incremental"]["created_at"]
    assert s["last_value"] == 426


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_batch_items_last_value_state_is_updated(item_type: TestDataItemFormat) -> None:
    data1 = [{"created_at": i} for i in range(5)]
    data2 = [{"created_at": i} for i in range(5, 10)]

    source_items1 = data_to_item_format(item_type, data1)
    source_items2 = data_to_item_format(item_type, data2)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at")):
        yield source_items1
        yield source_items2

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 9


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_last_value_access_in_resource(item_type: TestDataItemFormat) -> None:
    values = []

    data = [{"created_at": i} for i in range(6)]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at")):
        values.append(created_at.last_value)
        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())
    assert values == [None]

    p.extract(some_data())
    assert values == [None, 5]


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_unique_keys_are_deduplicated(item_type: TestDataItemFormat) -> None:
    data1 = [
        {"created_at": 1, "id": "a"},
        {"created_at": 2, "id": "b"},
        {"created_at": 3, "id": "c"},
        {"created_at": 3, "id": "d"},
        {"created_at": 3, "id": "e"},
    ]
    data2 = [
        {"created_at": 4, "id": "g"},
        {"created_at": 3, "id": "c"},
        {"created_at": 3, "id": "d"},
        {"created_at": 3, "id": "e"},
        {"created_at": 3, "id": "f"},
    ]

    source_items1 = data_to_item_format(item_type, data1)
    source_items2 = data_to_item_format(item_type, data2)

    @dlt.resource(primary_key="id")
    def some_data(created_at=dlt.sources.incremental("created_at")):
        if created_at.last_value is None:
            yield from source_items1
        else:
            yield from source_items2

    p = dlt.pipeline(
        pipeline_name=uniq_id(),
        destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
    )
    p.run(some_data())
    p.run(some_data())

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, id FROM some_data order by created_at, id") as cur:
            rows = cur.fetchall()

    assert rows == [(1, "a"), (2, "b"), (3, "c"), (3, "d"), (3, "e"), (3, "f"), (4, "g")]


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_unique_rows_by_hash_are_deduplicated(item_type: TestDataItemFormat) -> None:
    data1 = [
        {"created_at": 1, "id": "a"},
        {"created_at": 2, "id": "b"},
        {"created_at": 3, "id": "c"},
        {"created_at": 3, "id": "d"},
        {"created_at": 3, "id": "e"},
    ]
    data2 = [
        {"created_at": 3, "id": "c"},
        {"created_at": 3, "id": "d"},
        {"created_at": 3, "id": "e"},
        {"created_at": 3, "id": "f"},
        {"created_at": 4, "id": "g"},
    ]

    source_items1 = data_to_item_format(item_type, data1)
    source_items2 = data_to_item_format(item_type, data2)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at")):
        if created_at.last_value is None:
            yield from source_items1
        else:
            yield from source_items2

    p = dlt.pipeline(
        pipeline_name=uniq_id(),
        destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
    )
    p.run(some_data())
    p.run(some_data())

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, id FROM some_data order by created_at, id") as cur:
            rows = cur.fetchall()

    assert rows == [(1, "a"), (2, "b"), (3, "c"), (3, "d"), (3, "e"), (3, "f"), (4, "g")]


def test_nested_cursor_path() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("data.items[0].created_at")):
        yield {"data": {"items": [{"created_at": 2}]}}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "data.items[0].created_at"
    ]
    assert s["last_value"] == 2


@pytest.mark.parametrize("item_type", ["arrow-table", "pandas"])
def test_nested_cursor_path_arrow_fails(item_type: TestDataItemFormat) -> None:
    data = [{"data": {"items": [{"created_at": 2}]}}]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("data.items[0].created_at")):
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.extract(some_data())

    ex: PipelineStepFailed = py_ex.value
    assert isinstance(ex.exception, IncrementalCursorPathMissing)
    assert ex.exception.json_path == "data.items[0].created_at"


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_explicit_initial_value(item_type: TestDataItemFormat) -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at")):
        data = [{"created_at": created_at.last_value}]
        yield from data_to_item_format(item_type, data)

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(created_at=4242))

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 4242


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_explicit_incremental_instance(item_type: TestDataItemFormat) -> None:
    data = [{"inserted_at": 242, "some_uq": 444}]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource(primary_key="some_uq")
    def some_data(incremental=dlt.sources.incremental("created_at", initial_value=0)):
        assert incremental.cursor_path == "inserted_at"
        assert incremental.initial_value == 241
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(incremental=dlt.sources.incremental("inserted_at", initial_value=241)))


@dlt.resource
def some_data_from_config(
    call_no: int,
    item_type: TestDataItemFormat,
    created_at: Optional[dlt.sources.incremental[str]] = dlt.secrets.value,
):
    assert created_at.cursor_path == "created_at"
    # start value will update to the last_value on next call
    if call_no == 1:
        assert created_at.initial_value == "2022-02-03T00:00:00Z"
        assert created_at.start_value == "2022-02-03T00:00:00Z"
    if call_no == 2:
        assert created_at.initial_value == "2022-02-03T00:00:00Z"
        assert created_at.start_value == "2022-02-03T00:00:01Z"
    data = [{"created_at": "2022-02-03T00:00:01Z"}]
    source_items = data_to_item_format(item_type, data)
    yield from source_items


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_optional_incremental_from_config(item_type: TestDataItemFormat) -> None:
    os.environ["SOURCES__TEST_INCREMENTAL__SOME_DATA_FROM_CONFIG__CREATED_AT__CURSOR_PATH"] = (
        "created_at"
    )
    os.environ["SOURCES__TEST_INCREMENTAL__SOME_DATA_FROM_CONFIG__CREATED_AT__INITIAL_VALUE"] = (
        "2022-02-03T00:00:00Z"
    )

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data_from_config(1, item_type))
    p.extract(some_data_from_config(2, item_type))


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_optional_incremental_not_passed(item_type: TestDataItemFormat) -> None:
    """Resource still runs when no incremental is passed"""
    data = [1, 2, 3]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at: Optional[dlt.sources.incremental[str]] = None):
        yield source_items

    result = list(some_data())
    assert result == source_items


@configspec
class OptionalIncrementalConfig(BaseConfiguration):
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]


@dlt.resource(spec=OptionalIncrementalConfig)
def optional_incremental_arg_resource(
    item_type: TestDataItemFormat, incremental: Optional[dlt.sources.incremental[Any]] = None
) -> Any:
    data = [1, 2, 3]
    source_items = data_to_item_format(item_type, data)
    assert incremental is None
    yield source_items


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_optional_arg_from_spec_not_passed(item_type: TestDataItemFormat) -> None:
    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(optional_incremental_arg_resource(item_type))


@configspec
class SomeDataOverrideConfiguration(BaseConfiguration):
    created_at: dlt.sources.incremental = dlt.sources.incremental("updated_at", initial_value="2022-02-03T00:00:00Z")  # type: ignore[type-arg]


# provide what to inject via spec. the spec contain the default
@dlt.resource(spec=SomeDataOverrideConfiguration)
def some_data_override_config(
    item_type: TestDataItemFormat, created_at: dlt.sources.incremental[str] = dlt.config.value
):
    assert created_at.cursor_path == "created_at"
    assert created_at.initial_value == "2000-02-03T00:00:00Z"
    data = [{"created_at": "2023-03-03T00:00:00Z"}]
    source_items = data_to_item_format(item_type, data)
    yield from source_items


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_override_initial_value_from_config(item_type: TestDataItemFormat) -> None:
    # use the shortest possible config version
    # os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA_OVERRIDE_CONFIG__CREATED_AT__INITIAL_VALUE'] = '2000-02-03T00:00:00Z'
    os.environ["CREATED_AT__CURSOR_PATH"] = "created_at"
    os.environ["CREATED_AT__INITIAL_VALUE"] = "2000-02-03T00:00:00Z"

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data_override_config(item_type))


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_override_primary_key_in_pipeline(item_type: TestDataItemFormat) -> None:
    """Primary key hint passed to pipeline is propagated through apply_hints"""
    data = [{"created_at": 22, "id": 2, "other_id": 5}, {"created_at": 22, "id": 2, "other_id": 6}]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource(primary_key="id")
    def some_data(created_at=dlt.sources.incremental("created_at")):
        # TODO: this only works because incremental instance is shared across many copies of the resource
        assert some_data.incremental.primary_key == ["id", "other_id"]

        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data, primary_key=["id", "other_id"])


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_composite_primary_key(item_type: TestDataItemFormat) -> None:
    data = [
        {"created_at": 1, "isrc": "AAA", "market": "DE"},
        {"created_at": 2, "isrc": "BBB", "market": "DE"},
        {"created_at": 2, "isrc": "CCC", "market": "US"},
        {"created_at": 2, "isrc": "AAA", "market": "DE"},
        {"created_at": 2, "isrc": "CCC", "market": "DE"},
        {"created_at": 2, "isrc": "DDD", "market": "DE"},
        {"created_at": 1, "isrc": "CCC", "market": "DE"},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource(primary_key=["isrc", "market"])
    def some_data(created_at=dlt.sources.incremental("created_at")):
        yield from source_items

    p = dlt.pipeline(
        pipeline_name=uniq_id(),
        destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
    )
    p.run(some_data())

    with p.sql_client() as c:
        with c.execute_query(
            "SELECT created_at, isrc, market FROM some_data order by created_at, isrc, market"
        ) as cur:
            rows = cur.fetchall()

    expected = {
        (1, "AAA", "DE"),
        (2, "AAA", "DE"),
        (2, "BBB", "DE"),
        (2, "CCC", "DE"),
        (2, "CCC", "US"),
        (2, "DDD", "DE"),
        (1, "CCC", "DE"),
    }
    assert set(rows) == expected


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_last_value_func_min(item_type: TestDataItemFormat) -> None:
    data = [
        {"created_at": 10},
        {"created_at": 11},
        {"created_at": 9},
        {"created_at": 10},
        {"created_at": 8},
        {"created_at": 22},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at", last_value_func=min)):
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]

    assert s["last_value"] == 8


def test_last_value_func_custom() -> None:
    def last_value(values):
        return max(values) + 1

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at", last_value_func=last_value)):
        yield {"created_at": 9}
        yield {"created_at": 10}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 11


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_datetime_type(item_type: TestDataItemFormat) -> None:
    initial_value = pendulum.now()
    data = [
        {"created_at": initial_value + timedelta(minutes=1)},
        {"created_at": initial_value + timedelta(minutes=3)},
        {"created_at": initial_value + timedelta(minutes=2)},
        {"created_at": initial_value + timedelta(minutes=4)},
        {"created_at": initial_value + timedelta(minutes=2)},
    ]

    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at", initial_value)):
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == initial_value + timedelta(minutes=4)


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_descending_order_unique_hashes(item_type: TestDataItemFormat) -> None:
    """Resource returns items in descending order but using `max` last value function.
    Only hash matching last_value are stored.
    """
    data = [{"created_at": i} for i in reversed(range(15, 25))]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at", 20)):
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]

    last_hash = digest128(json.dumps({"created_at": 24}))

    assert s["unique_hashes"] == [last_hash]

    # make sure nothing is returned on a next run, source will use state from the active pipeline
    assert list(some_data()) == []


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_unique_keys_json_identifiers(item_type: TestDataItemFormat) -> None:
    """Uses primary key name that is matching the name of the JSON element in the original namespace but gets converted into destination namespace"""

    @dlt.resource(primary_key="DelTa")
    def some_data(last_timestamp=dlt.sources.incremental("ts")):
        data = [{"DelTa": i, "ts": pendulum.now().add(days=i).timestamp()} for i in range(-10, 10)]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data, destination="duckdb")
    # check if default schema contains normalized PK
    assert p.default_schema.tables["some_data"]["columns"]["del_ta"]["primary_key"] is True
    with p.sql_client() as c:
        with c.execute_query("SELECT del_ta FROM some_data") as cur:
            rows = cur.fetchall()
    assert len(rows) == 20

    # get data again
    sleep(0.1)
    load_info = p.run(some_data, destination="duckdb")
    # something got loaded = wee create 20 elements starting from now. so one element will be in the future comparing to previous 20 elements
    assert len(load_info.loads_ids) == 1
    with p.sql_client() as c:
        # with c.execute_query("SELECT del_ta FROM some_data WHERE _dlt_load_id = %s", load_info.loads_ids[0]) as cur:
        #     rows = cur.fetchall()
        with c.execute_query("SELECT del_ta FROM some_data") as cur:
            rows2 = cur.fetchall()
    assert len(rows2) == 21
    assert rows2[-1][0] == 9


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_missing_primary_key(item_type: TestDataItemFormat) -> None:
    @dlt.resource(primary_key="DELTA")
    def some_data(last_timestamp=dlt.sources.incremental("ts")):
        data = [{"delta": i, "ts": pendulum.now().add(days=i).timestamp()} for i in range(-10, 10)]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    with pytest.raises(IncrementalPrimaryKeyMissing) as py_ex:
        list(some_data())
    assert py_ex.value.primary_key_column == "DELTA"


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_missing_cursor_field(item_type: TestDataItemFormat) -> None:
    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately

    @dlt.resource
    def some_data(last_timestamp=dlt.sources.incremental("item.timestamp")):
        data = [{"delta": i, "ts": pendulum.now().add(days=i).timestamp()} for i in range(-10, 10)]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    with pytest.raises(IncrementalCursorPathMissing) as py_ex:
        list(some_data())
    assert py_ex.value.json_path == "item.timestamp"

    # same thing when run in pipeline
    with pytest.raises(PipelineStepFailed) as pip_ex:
        dlt.run(some_data(), destination="dummy")
    assert isinstance(pip_ex.value.__context__, IncrementalCursorPathMissing)
    assert pip_ex.value.__context__.json_path == "item.timestamp"


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_includes_records_and_updates_incremental_cursor_1(
    item_type: TestDataItemFormat,
) -> None:
    data = [
        {"id": 1, "created_at": None},
        {"id": 2, "created_at": 1},
        {"id": 3, "created_at": 2},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="include")
    ):
        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")

    assert_query_data(p, "select count(id) from some_data", [3])
    assert_query_data(p, "select count(created_at) from some_data", [2])

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 2


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_does_not_include_overlapping_records(
    item_type: TestDataItemFormat,
) -> None:
    @dlt.resource
    def some_data(
        invocation: int,
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="include"),
    ):
        if invocation == 1:
            yield data_to_item_format(
                item_type,
                [
                    {"id": 1, "created_at": None},
                    {"id": 2, "created_at": 1},
                    {"id": 3, "created_at": 2},
                ],
            )
        elif invocation == 2:
            yield data_to_item_format(
                item_type,
                [
                    {"id": 4, "created_at": 1},
                    {"id": 5, "created_at": None},
                    {"id": 6, "created_at": 3},
                ],
            )

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(1), destination="duckdb")
    p.run(some_data(2), destination="duckdb")

    assert_query_data(p, "select id from some_data order by id", [1, 2, 3, 5, 6])
    assert_query_data(
        p, "select created_at from some_data order by created_at", [1, 2, 3, None, None]
    )

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 3


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_includes_records_and_updates_incremental_cursor_2(
    item_type: TestDataItemFormat,
) -> None:
    data = [
        {"id": 1, "created_at": 1},
        {"id": 2, "created_at": None},
        {"id": 3, "created_at": 2},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="include")
    ):
        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")

    assert_query_data(p, "select count(id) from some_data", [3])
    assert_query_data(p, "select count(created_at) from some_data", [2])

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 2


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_includes_records_and_updates_incremental_cursor_3(
    item_type: TestDataItemFormat,
) -> None:
    data = [
        {"id": 1, "created_at": 1},
        {"id": 2, "created_at": 2},
        {"id": 3, "created_at": None},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="include")
    ):
        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")
    assert_query_data(p, "select count(id) from some_data", [3])
    assert_query_data(p, "select count(created_at) from some_data", [2])

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 2


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_includes_records_without_cursor_path(
    item_type: TestDataItemFormat,
) -> None:
    data = [
        {"id": 1, "created_at": 1},
        {"id": 2},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="include")
    ):
        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")
    assert_query_data(p, "select count(id) from some_data", [2])
    assert_query_data(p, "select count(created_at) from some_data", [1])

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 1


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_excludes_records_and_updates_incremental_cursor(
    item_type: TestDataItemFormat,
) -> None:
    data = [
        {"id": 1, "created_at": 1},
        {"id": 2, "created_at": 2},
        {"id": 3, "created_at": None},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="exclude")
    ):
        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")
    assert_query_data(p, "select count(id) from some_data", [2])

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 2


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_can_raise_on_none_1(item_type: TestDataItemFormat) -> None:
    data = [
        {"id": 1, "created_at": 1},
        {"id": 2, "created_at": None},
        {"id": 3, "created_at": 2},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="raise")
    ):
        yield source_items

    with pytest.raises(IncrementalCursorPathHasValueNone) as py_ex:
        list(some_data())
    assert py_ex.value.json_path == "created_at"

    # same thing when run in pipeline
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p = dlt.pipeline(pipeline_name=uniq_id())
        p.extract(some_data())

    assert isinstance(pip_ex.value.__context__, IncrementalCursorPathHasValueNone)
    assert pip_ex.value.__context__.json_path == "created_at"


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_path_none_can_raise_on_none_2(item_type: TestDataItemFormat) -> None:
    data = [
        {"id": 1, "created_at": 1},
        {"id": 2},
        {"id": 3, "created_at": 2},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="raise")
    ):
        yield source_items

    # there is no fixed, error because cursor path is missing
    if item_type == "object":
        with pytest.raises(IncrementalCursorPathMissing) as ex:
            list(some_data())
        assert ex.value.json_path == "created_at"
    # there is a fixed schema, error because value is null
    else:
        with pytest.raises(IncrementalCursorPathHasValueNone) as e:
            list(some_data())
        assert e.value.json_path == "created_at"

    # same thing when run in pipeline
    with pytest.raises(PipelineStepFailed) as e:  # type: ignore[assignment]
        p = dlt.pipeline(pipeline_name=uniq_id())
        p.extract(some_data())
    if item_type == "object":
        assert isinstance(e.value.__context__, IncrementalCursorPathMissing)
    else:
        assert isinstance(e.value.__context__, IncrementalCursorPathHasValueNone)
    assert e.value.__context__.json_path == "created_at"  # type: ignore[attr-defined]


@pytest.mark.parametrize("item_type", ["arrow-table", "arrow-batch", "pandas"])
def test_cursor_path_none_can_raise_on_column_missing(item_type: TestDataItemFormat) -> None:
    data = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="raise")
    ):
        yield source_items

    with pytest.raises(IncrementalCursorPathMissing) as py_ex:
        list(some_data())
    assert py_ex.value.json_path == "created_at"

    # same thing when run in pipeline
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p = dlt.pipeline(pipeline_name=uniq_id())
        p.extract(some_data())
    assert pip_ex.value.__context__.json_path == "created_at"  # type: ignore[attr-defined]
    assert isinstance(pip_ex.value.__context__, IncrementalCursorPathMissing)


@pytest.mark.parametrize("item_type", ["arrow-table", "arrow-batch"])
def test_cursor_path_not_nullable_arrow(
    item_type: TestDataItemFormat,
) -> None:
    @dlt.resource
    def some_data(
        invocation: int,
        created_at=dlt.sources.incremental("created_at", on_cursor_value_missing="include"),
    ):
        if invocation == 1:
            data = [
                {"id": 1, "created_at": 1},
                {"id": 2, "created_at": 1},
                {"id": 3, "created_at": 2},
            ]
        elif invocation == 2:
            data = [
                {"id": 4, "created_at": 1},
                {"id": 5, "created_at": 2},
                {"id": 6, "created_at": 3},
            ]

        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("created_at", pa.int32(), nullable=False),
            ]
        )
        id_array = pa.array([item["id"] for item in data], type=pa.int32())
        created_at_array = pa.array([item["created_at"] for item in data], type=pa.int32())
        if item_type == "arrow-table":
            source_items = [pa.Table.from_arrays([id_array, created_at_array], schema=schema)]
        elif item_type == "arrow-batch":
            source_items = [pa.RecordBatch.from_arrays([id_array, created_at_array], schema=schema)]

        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(1), destination="duckdb")
    p.run(some_data(2), destination="duckdb")

    assert_query_data(p, "select id from some_data order by id", [1, 2, 3, 5, 6])
    assert_query_data(p, "select created_at from some_data order by id", [1, 1, 2, 2, 3])

    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "created_at"
    ]
    assert s["last_value"] == 3


def test_cursor_path_none_nested_can_raise_on_none_1() -> None:
    # No nested json path support for pandas and arrow. See test_nested_cursor_path_arrow_fails
    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental(
            "data.items[0].created_at", on_cursor_value_missing="raise"
        )
    ):
        yield {"data": {"items": [{"created_at": None}, {"created_at": 1}]}}

    with pytest.raises(IncrementalCursorPathHasValueNone) as e:
        list(some_data())
    assert e.value.json_path == "data.items[0].created_at"


def test_cursor_path_none_nested_can_raise_on_none_2() -> None:
    # No pandas and arrow. See test_nested_cursor_path_arrow_fails
    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental(
            "data.items[*].created_at", on_cursor_value_missing="raise"
        )
    ):
        yield {"data": {"items": [{"created_at": None}, {"created_at": 1}]}}

    with pytest.raises(IncrementalCursorPathHasValueNone) as e:
        list(some_data())
    assert e.value.json_path == "data.items[*].created_at"


def test_cursor_path_none_nested_can_include_on_none_1() -> None:
    # No nested json path support for pandas and arrow. See test_nested_cursor_path_arrow_fails
    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental(
            "data.items[*].created_at", on_cursor_value_missing="include"
        )
    ):
        yield {
            "data": {
                "items": [
                    {"created_at": None},
                    {"created_at": 1},
                ]
            }
        }

    results = list(some_data())
    assert results[0]["data"]["items"] == [
        {"created_at": None},
        {"created_at": 1},
    ]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")

    assert_query_data(p, "select count(*) from some_data__data__items", [2])


def test_cursor_path_none_nested_can_include_on_none_2() -> None:
    # No nested json path support for pandas and arrow. See test_nested_cursor_path_arrow_fails
    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental(
            "data.items[0].created_at", on_cursor_value_missing="include"
        )
    ):
        yield {
            "data": {
                "items": [
                    {"created_at": None},
                    {"created_at": 1},
                ]
            }
        }

    results = list(some_data())
    assert results[0]["data"]["items"] == [
        {"created_at": None},
        {"created_at": 1},
    ]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")

    assert_query_data(p, "select count(*) from some_data__data__items", [2])


def test_cursor_path_none_nested_includes_rows_without_cursor_path() -> None:
    # No nested json path support for pandas and arrow. See test_nested_cursor_path_arrow_fails
    @dlt.resource
    def some_data(
        created_at=dlt.sources.incremental(
            "data.items[*].created_at", on_cursor_value_missing="include"
        )
    ):
        yield {
            "data": {
                "items": [
                    {"id": 1},
                    {"id": 2, "created_at": 2},
                ]
            }
        }

    results = list(some_data())
    assert results[0]["data"]["items"] == [
        {"id": 1},
        {"id": 2, "created_at": 2},
    ]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination="duckdb")

    assert_query_data(p, "select count(*) from some_data__data__items", [2])


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_set_default_value_for_incremental_cursor(item_type: TestDataItemFormat) -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("updated_at")):
        yield data_to_item_format(
            item_type,
            [
                {"id": 1, "created_at": 1, "updated_at": 1},
                {"id": 2, "created_at": 4, "updated_at": None},
                {"id": 3, "created_at": 3, "updated_at": 3},
            ],
        )

    def set_default_updated_at(record):
        if record.get("updated_at") is None:
            record["updated_at"] = record.get("created_at", pendulum.now().int_timestamp)
        return record

    def set_default_updated_at_pandas(df):
        df["updated_at"] = df["updated_at"].fillna(df["created_at"])
        return df

    def set_default_updated_at_arrow(records):
        updated_at_is_null = pa.compute.is_null(records.column("updated_at"))
        updated_at_filled = pa.compute.if_else(
            updated_at_is_null, records.column("created_at"), records.column("updated_at")
        )
        if item_type == "arrow-table":
            records = records.set_column(
                records.schema.get_field_index("updated_at"),
                pa.field("updated_at", records.column("updated_at").type),
                updated_at_filled,
            )
        elif item_type == "arrow-batch":
            columns = [records.column(i) for i in range(records.num_columns)]
            columns[2] = updated_at_filled
            records = pa.RecordBatch.from_arrays(columns, schema=records.schema)
        return records

    if item_type == "object":
        func = set_default_updated_at
    elif item_type == "pandas":
        func = set_default_updated_at_pandas
    elif item_type in ["arrow-table", "arrow-batch"]:
        func = set_default_updated_at_arrow

    result = list(some_data().add_map(func, insert_at=1))
    values = data_item_to_list(item_type, result)
    assert data_item_length(values) == 3
    assert values[1]["updated_at"] == 4

    # same for pipeline run
    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data().add_map(func, insert_at=1))
    s = p.state["sources"][p.default_schema_name]["resources"]["some_data"]["incremental"][
        "updated_at"
    ]
    assert s["last_value"] == 4


def test_json_path_cursor() -> None:
    @dlt.resource
    def some_data(last_timestamp=dlt.sources.incremental("item.timestamp|modifiedAt")):
        yield [
            {"delta": i, "item": {"timestamp": pendulum.now().add(days=i).timestamp()}}
            for i in range(-10, 10)
        ]

        yield [
            {"delta": i, "item": {"modifiedAt": pendulum.now().add(days=i).timestamp()}}
            for i in range(-10, 10)
        ]

    # path should match both timestamp and modifiedAt in item
    list(some_data)


def test_remove_incremental_with_explicit_none() -> None:
    @dlt.resource(standalone=True)
    def some_data(
        last_timestamp: Optional[dlt.sources.incremental[float]] = dlt.sources.incremental(
            "id", initial_value=9
        ),
    ):
        first_idx = last_timestamp.start_value or 0
        for idx in range(first_idx, 10):
            yield {"id": idx}

    # keeps initial value
    assert list(some_data()) == [{"id": 9}]

    # removes any initial value
    assert len(list(some_data(last_timestamp=None))) == 10


def test_remove_incremental_with_incremental_empty() -> None:
    @dlt.resource
    def some_data_optional(
        last_timestamp: Optional[dlt.sources.incremental[float]] = dlt.sources.incremental(
            "item.timestamp"
        ),
    ):
        assert last_timestamp is None
        yield 1

    # can't use EMPTY to reset incremental
    with pytest.raises(ValueError):
        list(some_data_optional(last_timestamp=dlt.sources.incremental.EMPTY))

    @dlt.resource(standalone=True)
    def some_data(
        last_timestamp: dlt.sources.incremental[float] = dlt.sources.incremental("item.timestamp"),
    ):
        assert last_timestamp is None
        yield 1

    # we'll get the value error
    with pytest.raises(InvalidNativeValue):
        list(some_data(last_timestamp=dlt.sources.incremental.EMPTY))


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_filter_processed_items(item_type: TestDataItemFormat) -> None:
    """Checks if already processed items are filtered out"""

    @dlt.resource
    def standalone_some_data(
        item_type: TestDataItemFormat, now=None, last_timestamp=dlt.sources.incremental("timestamp")
    ):
        data = [
            {"delta": i, "timestamp": (now or pendulum.now()).add(days=i).timestamp()}
            for i in range(-10, 10)
        ]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    # we get all items (no initial - nothing filtered)
    values = list(standalone_some_data(item_type))
    values = data_item_to_list(item_type, values)
    assert len(values) == 20

    # provide initial value using max function
    values = list(standalone_some_data(item_type, last_timestamp=pendulum.now().timestamp()))
    values = data_item_to_list(item_type, values)
    assert len(values) == 10
    # only the future timestamps
    assert all(v["delta"] >= 0 for v in values)

    # provide the initial value, use min function
    values = list(
        standalone_some_data(
            item_type,
            last_timestamp=dlt.sources.incremental("timestamp", pendulum.now().timestamp(), min),
        )
    )
    values = data_item_to_list(item_type, values)
    assert len(values) == 10
    # the minimum element
    assert values[0]["delta"] == -10


def test_start_value_set_to_last_value() -> None:
    p = dlt.pipeline(pipeline_name=uniq_id())
    now = pendulum.now()

    @dlt.resource
    def some_data(step, last_timestamp=dlt.sources.incremental("ts")):
        expected_last = now.add(days=step - 1)

        if step == -10:
            assert last_timestamp.start_value is None
        else:
            # print(last_timestamp.initial_value)
            # print(now.add(days=step-1).timestamp())
            assert last_timestamp.start_value == last_timestamp.last_value == expected_last
        data = [{"delta": i, "ts": now.add(days=i)} for i in range(-10, 10)]
        yield from data
        # after all yielded
        if step == -10:
            assert last_timestamp.start_value is None
        else:
            assert last_timestamp.start_value == expected_last != last_timestamp.last_value

    for i in range(-10, 10):
        r = some_data(i)
        assert len(r._pipe) == 2
        r.add_filter(take_first(i + 11), 1)
        p.run(r, destination="duckdb")


@pytest.mark.parametrize("item_type", set(ALL_TEST_DATA_ITEM_FORMATS) - {"object"})
def test_start_value_set_to_last_value_arrow(item_type: TestDataItemFormat) -> None:
    p = dlt.pipeline(pipeline_name=uniq_id(), destination="duckdb")
    now = pendulum.now()

    data = [{"delta": i, "ts": now.add(days=i)} for i in range(-10, 10)]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(first: bool, last_timestamp=dlt.sources.incremental("ts")):
        if first:
            assert last_timestamp.start_value is None
        else:
            # print(last_timestamp.initial_value)
            # print(now.add(days=step-1).timestamp())
            assert last_timestamp.start_value == last_timestamp.last_value == data[-1]["ts"]
        yield from source_items
        # after all yielded
        if first:
            assert last_timestamp.start_value is None
        else:
            assert last_timestamp.start_value == data[-1]["ts"] == last_timestamp.last_value

    p.run(some_data(True))
    p.run(some_data(False))


@pytest.mark.parametrize("item_type", set(ALL_TEST_DATA_ITEM_FORMATS) - {"pandas"})
@pytest.mark.parametrize(
    "id_value",
    ("1231231231231271872", b"1231231231231271872", pendulum.now(), 1271.78, Decimal("1231.87")),
)
def test_primary_key_types(item_type: TestDataItemFormat, id_value: Any) -> None:
    """Case when deduplication filter is empty for an Arrow table."""
    p = dlt.pipeline(pipeline_name=uniq_id(), destination="duckdb")
    now = pendulum.now()

    data = [
        {
            "delta": str(i),
            "ts": now.add(days=i),
            "_id": id_value,
        }
        for i in range(-10, 10)
    ]
    source_items = data_to_item_format(item_type, data)
    start = now.add(days=-10)

    @dlt.resource
    def some_data(
        last_timestamp=dlt.sources.incremental("ts", initial_value=start, primary_key="_id"),
    ):
        yield from source_items

    p.run(some_data())
    norm_info = p.last_trace.last_normalize_info
    assert norm_info.row_counts["some_data"] == 20
    # load incrementally
    p.run(some_data())
    norm_info = p.last_trace.last_normalize_info
    assert "some_data" not in norm_info.row_counts


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_replace_resets_state(item_type: TestDataItemFormat) -> None:
    p = dlt.pipeline(pipeline_name=uniq_id(), destination="duckdb")
    now = pendulum.now()

    @dlt.resource
    def standalone_some_data(
        item_type: TestDataItemFormat, now=None, last_timestamp=dlt.sources.incremental("timestamp")
    ):
        data = [
            {"delta": i, "timestamp": (now or pendulum.now()).add(days=i).timestamp()}
            for i in range(-10, 10)
        ]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    info = p.run(standalone_some_data(item_type, now))
    print(p.last_trace.last_normalize_info)
    assert len(info.loads_ids) == 1
    info = p.run(standalone_some_data(item_type, now))
    print(p.last_trace.last_normalize_info)
    print(info)
    assert len(info.loads_ids) == 0
    info = p.run(standalone_some_data(item_type, now), write_disposition="replace")
    assert len(info.loads_ids) == 1

    parent_r = standalone_some_data(item_type, now)

    @dlt.transformer(data_from=parent_r, write_disposition="append")
    def child(item):
        state = resource_state("child")
        # print(f"CHILD: {state}")
        state["mark"] = f"mark:{item['delta']}"
        yield item

    print(parent_r._pipe._steps)
    print(child._pipe._steps)

    # also transformer will not receive new data
    info = p.run(child)
    assert len(info.loads_ids) == 0
    # now it will (as the parent resource also got reset)
    info = p.run(child, write_disposition="replace")
    # print(info.load_packages[0])
    assert len(info.loads_ids) == 1
    # pipeline applied hints to the child resource but it was placed into source first
    # so the original is still "append"
    assert child.write_disposition == "append"

    # create a source where we place only child
    child.write_disposition = "replace"
    s = DltSource(Schema("comp"), "section", [child])
    # but extracted resources will include its parent where it derives write disposition from child
    extracted = s.resources.extracted
    assert extracted[child.name].write_disposition == "replace"
    assert extracted[child._pipe.parent.name].write_disposition == "replace"

    # create a source where we place parent explicitly
    s = DltSource(Schema("comp"), "section", [parent_r, child])
    extracted = s.resources.extracted
    assert extracted[child.name].write_disposition == "replace"
    # now parent exists separately and has its own write disposition
    assert extracted[child._pipe.parent.name].write_disposition == "append"

    p = dlt.pipeline(pipeline_name=uniq_id(), destination="duckdb")
    info = p.run(s)
    # print(s.state)
    assert len(info.loads_ids) == 1
    info = p.run(s)
    # print(s.state)
    # state was reset (child is replace but parent is append! so it will not generate any more items due to incremental
    # so child will reset itself on replace and never set the state...)
    assert "child" not in s.state["resources"]
    # there will be a load package to reset the state but also a load package to update the child table
    assert len(info.load_packages[0].jobs["completed_jobs"]) == 2
    assert {
        job.job_file_info.table_name for job in info.load_packages[0].jobs["completed_jobs"]
    } == {"_dlt_pipeline_state", "child"}

    # now we add child that has parent_r as parent but we add another instance of standalone_some_data explicitly
    # so we have a resource with the same name as child parent but the pipe instance is different
    s = DltSource(Schema("comp"), "section", [standalone_some_data(now), child])
    assert extracted[child.name].write_disposition == "replace"
    # now parent exists separately and has its own write disposition - because we search by name to identify matching resource
    assert extracted[child._pipe.parent.name].write_disposition == "append"


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_incremental_as_transform(item_type: TestDataItemFormat) -> None:
    now = pendulum.now().timestamp()

    @dlt.resource
    def some_data():
        last_value: dlt.sources.incremental[float] = dlt.sources.incremental.from_existing_state(
            "some_data", "ts"
        )
        assert last_value.initial_value == now
        assert last_value.start_value == now
        assert last_value.cursor_path == "ts"
        assert last_value.last_value == now

        data = [{"delta": i, "ts": pendulum.now().add(days=i).timestamp()} for i in range(-10, 10)]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    r = some_data().add_step(dlt.sources.incremental("ts", initial_value=now, primary_key="delta"))
    p = dlt.pipeline(pipeline_name=uniq_id())
    info = p.run(r, destination="duckdb")
    assert len(info.loads_ids) == 1


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_incremental_explicit_disable_unique_check(item_type: TestDataItemFormat) -> None:
    @dlt.resource(primary_key="delta")
    def some_data(last_timestamp=dlt.sources.incremental("ts", primary_key=())):
        data = [{"delta": i, "ts": pendulum.now().timestamp()} for i in range(-10, 10)]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    with Container().injectable_context(StateInjectableContext(state={})):
        s = some_data()
        list(s)
        # no unique hashes at all
        assert s.state["incremental"]["ts"]["unique_hashes"] == []


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_apply_hints_incremental(item_type: TestDataItemFormat) -> None:
    p = dlt.pipeline(pipeline_name=uniq_id(), destination="dummy")
    data = [{"created_at": 1}, {"created_at": 2}, {"created_at": 3}]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at: Optional[dlt.sources.incremental[int]] = None):
        # make sure that incremental from apply_hints is here
        if created_at is not None:
            assert created_at.cursor_path == "created_at"
            assert created_at.last_value_func is max
        yield source_items

    # the incremental wrapper is created for a resource and the incremental value is provided via apply hints
    r = some_data()
    assert r is not some_data
    assert r.incremental is not None
    assert r.incremental.incremental is None
    r.apply_hints(incremental=dlt.sources.incremental("created_at", last_value_func=max))
    if item_type == "pandas":
        assert list(r)[0].equals(source_items[0])
    else:
        assert list(r) == source_items
    p.extract(r)
    assert "incremental" in r.state
    assert r.incremental.incremental is not None
    assert len(r._pipe) == 2
    # no more elements
    assert list(r) == []

    # same thing with explicit None
    r = some_data(created_at=None).with_name("copy")
    r.apply_hints(incremental=dlt.sources.incremental("created_at", last_value_func=max))
    if item_type == "pandas":
        assert list(r)[0].equals(source_items[0])
    else:
        assert list(r) == source_items
    p.extract(r)
    assert "incremental" in r.state
    assert list(r) == []

    # remove incremental
    r.apply_hints(incremental=dlt.sources.incremental.EMPTY)
    assert r.incremental is not None
    assert r.incremental.incremental is None
    if item_type == "pandas":
        assert list(r)[0].equals(source_items[0])
    else:
        assert list(r) == source_items

    # as above but we provide explicit incremental when creating resource
    p = p.drop()
    r = some_data(created_at=dlt.sources.incremental("created_at", last_value_func=min))
    # hints have precedence, as expected
    r.apply_hints(incremental=dlt.sources.incremental("created_at", last_value_func=max))
    p.extract(r)
    assert "incremental" in r.state
    # max value
    assert r.state["incremental"]["created_at"]["last_value"] == 3

    @dlt.resource
    def some_data_w_default(created_at=dlt.sources.incremental("created_at", last_value_func=min)):
        # make sure that incremental from apply_hints is here
        assert created_at is not None
        assert created_at.last_value_func is max
        yield source_items

    # default is overridden by apply hints
    p = p.drop()
    r = some_data_w_default()
    r.apply_hints(incremental=dlt.sources.incremental("created_at", last_value_func=max))
    p.extract(r)
    assert "incremental" in r.state
    # min value
    assert r.state["incremental"]["created_at"]["last_value"] == 3

    @dlt.resource
    def some_data_no_incremental():
        yield source_items

    # we add incremental as a step
    p = p.drop()
    r = some_data_no_incremental()
    print(r._pipe)
    incr_instance = dlt.sources.incremental("created_at", last_value_func=max)
    r.apply_hints(incremental=incr_instance)
    print(r._pipe)
    assert r.incremental is incr_instance
    p.extract(r)
    assert "incremental" in r.state
    info = p.normalize()
    assert info.row_counts["some_data_no_incremental"] == 3
    # make sure we can override incremental
    incr_instance = dlt.sources.incremental("created_at", last_value_func=max, row_order="desc")
    r.apply_hints(incremental=incr_instance)
    assert r.incremental is incr_instance
    p.extract(r)
    info = p.normalize()
    assert "some_data_no_incremental" not in info.row_counts
    # we switch last value func to min
    incr_instance = dlt.sources.incremental(
        "created_at", last_value_func=min, row_order="desc", primary_key=()
    )
    r.apply_hints(incremental=incr_instance)
    assert r.incremental is incr_instance
    p.extract(r)
    info = p.normalize()
    # we have three elements due to min function (equal element NOT is eliminated due to primary_key==())
    assert info.row_counts["some_data_no_incremental"] == 3

    # remove incremental
    r.apply_hints(incremental=dlt.sources.incremental.EMPTY)
    assert r.incremental is None


def test_incremental_wrapper_on_clone_standalone_incremental() -> None:
    @dlt.resource(standalone=True)
    def standalone_incremental(created_at: Optional[dlt.sources.incremental[int]] = None):
        yield [{"created_at": 1}, {"created_at": 2}, {"created_at": 3}]

    s_r_1 = standalone_incremental()
    s_r_i_1 = dlt.sources.incremental[int]("created_at")
    s_r_2 = standalone_incremental()
    s_r_i_2 = dlt.sources.incremental[int]("created_at", initial_value=3)
    s_r_i_3 = dlt.sources.incremental[int]("created_at", initial_value=1, last_value_func=min)
    s_r_3 = standalone_incremental(created_at=s_r_i_3)

    # different wrappers
    assert s_r_1.incremental is not s_r_2.incremental
    s_r_1.apply_hints(incremental=s_r_i_1)
    s_r_2.apply_hints(incremental=s_r_i_2)
    assert s_r_1.incremental.incremental is s_r_i_1
    assert s_r_2.incremental.incremental is s_r_i_2

    # evaluate s r 3
    assert list(s_r_3) == [{"created_at": 1}]
    # incremental is set after evaluation but the instance is different (wrapper is merging instances)
    assert s_r_3.incremental.incremental.last_value_func is min

    # standalone resources are bound so clone does not re-wrap
    s_r_3_clone = s_r_3._clone()
    assert s_r_3_clone.incremental is s_r_3.incremental
    assert s_r_3_clone.incremental.incremental is s_r_3.incremental.incremental

    # evaluate others
    assert len(list(s_r_1)) == 3
    assert len(list(s_r_2)) == 1


def test_incremental_wrapper_on_clone_standalone_no_incremental() -> None:
    @dlt.resource(standalone=True)
    def standalone():
        yield [{"created_at": 1}, {"created_at": 2}, {"created_at": 3}]

    s_r_1 = standalone()
    s_r_i_1 = dlt.sources.incremental[int]("created_at", row_order="desc")
    s_r_2 = standalone()
    s_r_i_2 = dlt.sources.incremental[int]("created_at", initial_value=3)

    # clone keeps the incremental step
    s_r_1.apply_hints(incremental=s_r_i_1)
    assert s_r_1.incremental is s_r_i_1

    s_r_1_clone = s_r_1._clone()
    assert s_r_1_clone.incremental is s_r_i_1

    assert len(list(s_r_1)) == 3
    s_r_2.apply_hints(incremental=s_r_i_2)
    assert len(list(s_r_2)) == 1


def test_incremental_wrapper_on_clone_incremental() -> None:
    @dlt.resource
    def regular_incremental(created_at: Optional[dlt.sources.incremental[int]] = None):
        yield [{"created_at": 1}, {"created_at": 2}, {"created_at": 3}]

    assert regular_incremental.incremental is not None
    assert regular_incremental.incremental.incremental is None

    # separate incremental
    r_1 = regular_incremental()
    assert r_1.args_bound is True
    r_2 = regular_incremental.with_name("cloned_regular")
    assert r_1.incremental is not None
    assert r_2.incremental is not None
    assert r_1.incremental is not r_2.incremental is not regular_incremental.incremental

    # evaluate and compare incrementals
    assert len(list(r_1)) == 3
    assert len(list(r_2)) == 3
    assert r_1.incremental.incremental is None
    assert r_2.incremental.incremental is None

    # now bind some real incrementals
    r_3 = regular_incremental(dlt.sources.incremental[int]("created_at", initial_value=3))
    r_4 = regular_incremental(
        dlt.sources.incremental[int]("created_at", initial_value=1, last_value_func=min)
    )
    r_4_clone = r_4._clone("r_4_clone")
    # evaluate
    assert len(list(r_3)) == 1
    assert len(list(r_4)) == 1
    assert r_3.incremental.incremental is not r_4.incremental.incremental
    # now the clone should share the incremental because it was done after parameters were bound
    assert r_4_clone.incremental is r_4.incremental


def test_last_value_func_on_dict() -> None:
    """Test last value which is a dictionary"""

    def by_event_type(event):
        last_value = None
        if len(event) == 1:
            (item,) = event
        else:
            item, last_value = event

        if last_value is None:
            last_value = {}
        else:
            last_value = dict(last_value)
        item_type = item["type"]
        last_value[item_type] = max(
            item["created_at"], last_value.get(item_type, "1970-01-01T00:00:00Z")
        )
        return last_value

    @dlt.resource(primary_key="id", table_name=lambda i: i["type"])
    def _get_shuffled_events(
        last_created_at=dlt.sources.incremental("$", last_value_func=by_event_type)
    ):
        with open(
            "tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8"
        ) as f:
            yield json.load(f)

    with Container().injectable_context(StateInjectableContext(state={})):
        r = _get_shuffled_events()
        all_events = list(r)
        assert len(all_events) == 100
        r = _get_shuffled_events()
        assert len(list(r)) == 0
        # remove one of keys from last value
        del r.state["incremental"]["$"]["last_value"]["WatchEvent"]
        r = _get_shuffled_events()
        watch_events = list(r)
        assert len(watch_events) > 0
        assert [e for e in all_events if e["type"] == "WatchEvent"] == watch_events


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_timezone_naive_datetime(item_type: TestDataItemFormat) -> None:
    """Resource has timezone naive datetime objects, but incremental stored state is
    converted to tz aware pendulum dates. Can happen when loading e.g. from sql database"""
    start_dt = datetime.now()
    pendulum_start_dt = pendulum.instance(start_dt)  # With timezone

    @dlt.resource(standalone=True, primary_key="hour")
    def some_data(
        updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
            "updated_at", initial_value=pendulum_start_dt
        ),
        max_hours: int = 2,
        tz: str = None,
    ):
        print("some_data", updated_at, dict(updated_at))
        data = [
            {"updated_at": start_dt + timedelta(hours=hour), "hour": hour}
            for hour in range(1, max_hours + 1)
        ]
        # make sure this is naive datetime
        assert data[0]["updated_at"].tzinfo is None  # type: ignore[attr-defined]
        if tz:
            data = [{**d, "updated_at": pendulum.instance(d["updated_at"])} for d in data]  # type: ignore[call-overload]

        yield data_to_item_format(item_type, data)

    pipeline = dlt.pipeline(pipeline_name=uniq_id())
    resource = some_data()
    # print(list(resource))
    extract_info = pipeline.extract(resource)
    # print(extract_info.asdict())
    assert (
        extract_info.metrics[extract_info.loads_ids[0]][0]["resource_metrics"][
            "some_data"
        ].items_count
        == 2
    )
    # last value has timezone added
    last_value = resource.state["incremental"]["updated_at"]["last_value"]
    assert isinstance(last_value, pendulum.DateTime)
    assert last_value.tzname() == "UTC"
    # try again with more records
    extract_info = pipeline.extract(some_data(max_hours=3))
    assert (
        extract_info.metrics[extract_info.loads_ids[0]][0]["resource_metrics"][
            "some_data"
        ].items_count
        == 1
    )

    # add end_value to incremental
    resource = some_data(max_hours=10)
    # it should be merged
    resource.apply_hints(
        incremental=dlt.sources.incremental(
            "updated_at", initial_value=pendulum_start_dt, end_value=pendulum_start_dt.add(hours=3)
        )
    )
    print(resource.incremental.incremental, dict(resource.incremental.incremental))
    pipeline = pipeline.drop()
    extract_info = pipeline.extract(resource)
    assert (
        extract_info.metrics[extract_info.loads_ids[0]][0]["resource_metrics"][
            "some_data"
        ].items_count
        == 2
    )

    # initial value is naive
    resource = some_data(max_hours=4).with_name("copy_1")  # also make new resource state
    resource.apply_hints(incremental=dlt.sources.incremental("updated_at", initial_value=start_dt))
    # and the data is naive. so it will work as expected with naive datetimes in the result set
    data = list(resource)
    if item_type == "object":
        # we do not convert data in arrow tables
        assert data[0]["updated_at"].tzinfo is None

    # end value is naive
    resource = some_data(max_hours=4).with_name("copy_2")  # also make new resource state
    resource.apply_hints(
        incremental=dlt.sources.incremental(
            "updated_at", initial_value=start_dt, end_value=start_dt + timedelta(hours=3)
        )
    )
    data = list(resource)
    if item_type == "object":
        assert data[0]["updated_at"].tzinfo is None

    # now use naive initial value but data is UTC
    resource = some_data(max_hours=4, tz="UTC").with_name("copy_3")  # also make new resource state
    resource.apply_hints(
        incremental=dlt.sources.incremental(
            "updated_at", initial_value=start_dt + timedelta(hours=3)
        )
    )
    # will cause invalid comparison
    if item_type == "object":
        with pytest.raises(IncrementalCursorInvalidCoercion):
            list(resource)
    else:
        data = data_item_to_list(item_type, list(resource))
        # we select two rows by adding 3 hours to start_dt. rows have hours:
        # 1, 2, 3, 4
        # and we select >=3
        assert len(data) == 2


@dlt.resource
def endless_sequence(
    item_type: TestDataItemFormat,
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
        "updated_at", initial_value=1
    ),
) -> Any:
    max_values = 20
    start = updated_at.last_value
    data = [{"updated_at": i} for i in range(start, start + max_values)]
    source_items = data_to_item_format(item_type, data)
    yield from source_items


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_chunked_ranges(item_type: TestDataItemFormat) -> None:
    """Load chunked ranges with end value along with incremental"""

    pipeline = dlt.pipeline(pipeline_name="incremental_" + uniq_id(), destination="duckdb")

    chunks = [
        # Load some start/end ranges in and out of order
        (40, 50),
        (50, 60),
        (60, 61),
        (62, 70),
        (20, 30),
        # # Do a couple of runs with incremental loading, starting from highest range end
        (70, None),
        # Load another chunk from the past
        (10, 20),
        # Incremental again
        (None, None),
    ]

    for start, end in chunks:
        pipeline.run(
            endless_sequence(
                item_type, updated_at=dlt.sources.incremental(initial_value=start, end_value=end)
            ),
            write_disposition="append",
        )

    expected_range = list(
        chain(
            range(10, 20),
            range(20, 30),
            range(40, 50),
            range(50, 60),
            range(60, 61),
            range(62, 70),
            range(70, 89),
            range(89, 109),
        )
    )

    with pipeline.sql_client() as client:
        items = [
            row[0]
            for row in client.execute_sql(
                "SELECT updated_at FROM endless_sequence ORDER BY updated_at"
            )
        ]

    assert items == expected_range


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_end_value_with_batches(item_type: TestDataItemFormat) -> None:
    """Ensure incremental with end_value works correctly when resource yields lists instead of single items"""

    @dlt.resource
    def batched_sequence(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=1
        )
    ) -> Any:
        start = updated_at.last_value
        data = [{"updated_at": i} for i in range(start, start + 12)]
        yield data_to_item_format(item_type, data)
        data = [{"updated_at": i} for i in range(start + 12, start + 20)]
        yield data_to_item_format(item_type, data)

    pipeline = dlt.pipeline(pipeline_name="incremental_" + uniq_id(), destination="duckdb")

    pipeline.run(
        batched_sequence(updated_at=dlt.sources.incremental(initial_value=1, end_value=10)),
        write_disposition="append",
    )

    with pipeline.sql_client() as client:
        items = [
            row[0]
            for row in client.execute_sql(
                "SELECT updated_at FROM batched_sequence ORDER BY updated_at"
            )
        ]

    assert items == list(range(1, 10))

    pipeline.run(
        batched_sequence(updated_at=dlt.sources.incremental(initial_value=10, end_value=14)),
        write_disposition="append",
    )

    with pipeline.sql_client() as client:
        items = [
            row[0]
            for row in client.execute_sql(
                "SELECT updated_at FROM batched_sequence ORDER BY updated_at"
            )
        ]

    assert items == list(range(1, 14))


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_load_with_end_value_does_not_write_state(item_type: TestDataItemFormat) -> None:
    """When loading chunk with initial/end value range. The resource state is untouched."""
    pipeline = dlt.pipeline(pipeline_name="incremental_" + uniq_id(), destination="duckdb")

    pipeline.extract(
        endless_sequence(
            item_type, updated_at=dlt.sources.incremental(initial_value=20, end_value=30)
        )
    )

    assert pipeline.state.get("sources") is None


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_end_value_initial_value_errors(item_type: TestDataItemFormat) -> None:
    @dlt.resource
    def some_data(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental("updated_at"),
    ) -> Any:
        yield {"updated_at": 1}

    # end_value without initial_value
    with pytest.raises(ConfigurationValueError) as ex:
        list(some_data(updated_at=dlt.sources.incremental(end_value=22)))

    assert str(ex.value).startswith("Incremental 'end_value' was specified without 'initial_value'")

    # max function and end_value lower than initial_value
    with pytest.raises(ConfigurationValueError) as ex:
        list(some_data(updated_at=dlt.sources.incremental(initial_value=42, end_value=22)))

    assert str(ex.value).startswith(
        "Incremental 'initial_value' (42) is higher than 'end_value` (22)"
    )

    # max function and end_value higher than initial_value
    with pytest.raises(ConfigurationValueError) as ex:
        list(
            some_data(
                updated_at=dlt.sources.incremental(
                    initial_value=22, end_value=42, last_value_func=min
                )
            )
        )

    assert str(ex.value).startswith(
        "Incremental 'initial_value' (22) is lower than 'end_value` (42)."
    )

    def custom_last_value(items):
        return max(items)

    # custom function which evaluates end_value lower than initial
    with pytest.raises(ConfigurationValueError) as ex:
        list(
            some_data(
                updated_at=dlt.sources.incremental(
                    initial_value=42, end_value=22, last_value_func=custom_last_value
                )
            )
        )

    assert (
        "The result of 'custom_last_value([end_value, initial_value])' must equal 'end_value'"
        in str(ex.value)
    )


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_out_of_range_flags(item_type: TestDataItemFormat) -> None:
    """Test incremental.start_out_of_range / end_out_of_range flags are set when items are filtered out"""

    @dlt.resource
    def descending(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=10
        )
    ) -> Any:
        for chunk in chunks(list(reversed(range(48))), 10):
            data = [{"updated_at": i} for i in chunk]
            yield data_to_item_format(item_type, data)
            # Assert flag is set only on the first item < initial_value
            if all(item > 9 for item in chunk):
                assert updated_at.start_out_of_range is False
            else:
                assert updated_at.start_out_of_range is True
                return

    @dlt.resource
    def ascending(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=22, end_value=45
        )
    ) -> Any:
        for chunk in chunks(list(range(22, 500)), 10):
            data = [{"updated_at": i} for i in chunk]
            yield data_to_item_format(item_type, data)
            # Flag is set only when end_value is reached
            if all(item < 45 for item in chunk):
                assert updated_at.end_out_of_range is False
            else:
                assert updated_at.end_out_of_range is True
                return

    @dlt.resource
    def descending_single_item(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=10
        )
    ) -> Any:
        for i in reversed(range(14)):
            data = [{"updated_at": i}]
            yield from data_to_item_format(item_type, data)
            if i >= 10:
                assert updated_at.start_out_of_range is False
            else:
                assert updated_at.start_out_of_range is True
                return

    @dlt.resource
    def ascending_single_item(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=10, end_value=22
        )
    ) -> Any:
        for i in range(10, 500):
            data = [{"updated_at": i}]
            yield from data_to_item_format(item_type, data)
            if i < 22:
                assert updated_at.end_out_of_range is False
            else:
                assert updated_at.end_out_of_range is True
                return

    pipeline = dlt.pipeline(pipeline_name="incremental_" + uniq_id(), destination="duckdb")

    pipeline.extract(descending())

    pipeline.extract(ascending())

    pipeline.extract(descending_single_item())

    pipeline.extract(ascending_single_item())


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_async_row_order_out_of_range(item_type: TestDataItemFormat) -> None:
    @dlt.resource
    async def descending(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=10, row_order="desc"
        )
    ) -> Any:
        for chunk in chunks(count(start=48, step=-1), 10):
            await asyncio.sleep(0.01)
            print(updated_at.start_value)
            data = [{"updated_at": i} for i in chunk]
            yield data_to_item_format(item_type, data)

    data = list(descending)
    assert data_item_length(data) == 48 - 10 + 1  # both bounds included


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_parallel_row_order_out_of_range(item_type: TestDataItemFormat) -> None:
    """Test automatic generator close for ordered rows"""

    @dlt.resource(parallelized=True)
    def descending(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=10, row_order="desc"
        )
    ) -> Any:
        for chunk in chunks(count(start=48, step=-1), 10):
            data = [{"updated_at": i} for i in chunk]
            yield data_to_item_format(item_type, data)

    data = list(descending)
    assert data_item_length(data) == 48 - 10 + 1  # both bounds included


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_transformer_row_order_out_of_range(item_type: TestDataItemFormat) -> None:
    out_of_range = []

    @dlt.transformer
    def descending(
        package: int,
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=10, row_order="desc", primary_key="updated_at"
        ),
    ) -> Any:
        for chunk in chunks(count(start=48, step=-1), 10):
            data = [{"updated_at": i, "package": package} for i in chunk]
            # print(data)
            yield data_to_item_format("object", data)
            if updated_at.can_close():
                out_of_range.append(package)
                return

    data = list([3, 2, 1] | descending)
    assert data_item_length(data) == 48 - 10 + 1
    # we take full package 3 and then nothing in 1 and 2
    assert len(out_of_range) == 3


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_row_order_out_of_range(item_type: TestDataItemFormat) -> None:
    """Test automatic generator close for ordered rows"""

    @dlt.resource
    def descending(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=10, row_order="desc"
        )
    ) -> Any:
        for chunk in chunks(count(start=48, step=-1), 10):
            data = [{"updated_at": i} for i in chunk]
            yield data_to_item_format(item_type, data)

    data = list(descending)
    assert data_item_length(data) == 48 - 10 + 1  # both bounds included

    @dlt.resource
    def ascending(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=22, end_value=45, row_order="asc"
        )
    ) -> Any:
        # use INFINITE sequence so this test wil not stop if closing logic is flawed
        for chunk in chunks(count(start=22), 10):
            data = [{"updated_at": i} for i in chunk]
            yield data_to_item_format(item_type, data)

    data = list(ascending)
    assert data_item_length(data) == 45 - 22

    # use wrong row order, this will prevent end value to close pipe

    @dlt.resource
    def ascending_desc(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", initial_value=22, end_value=45, row_order="desc"
        )
    ) -> Any:
        for chunk in chunks(range(22, 100), 10):
            data = [{"updated_at": i} for i in chunk]
            yield data_to_item_format(item_type, data)

    from dlt.extract import pipe

    with mock.patch.object(
        pipe.Pipe,
        "close",
        side_effect=RuntimeError("Close pipe should not be called"),
    ) as close_pipe:
        data = list(ascending_desc)
        assert close_pipe.assert_not_called
        assert data_item_length(data) == 45 - 22


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
@pytest.mark.parametrize("order", ["random", "desc", "asc"])
@pytest.mark.parametrize("primary_key", [[], None, "updated_at"])
@pytest.mark.parametrize(
    "deterministic", (True, False), ids=("deterministic-record", "non-deterministic-record")
)
def test_unique_values_unordered_rows(
    item_type: TestDataItemFormat, order: str, primary_key: Any, deterministic: bool
) -> None:
    @dlt.resource(primary_key=primary_key)
    def random_ascending_chunks(
        order: str,
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at",
            initial_value=10,
        ),
    ) -> Any:
        range_ = list(range(updated_at.start_value, updated_at.start_value + 121))
        if order == "random":
            random.shuffle(range_)
        if order == "desc":
            range_ = reversed(range_)  # type: ignore[assignment]

        for chunk in chunks(range_, 30):
            # make sure that overlapping element is the last one
            data = [
                {"updated_at": i, "rand": random.random() if not deterministic else 0}
                for i in chunk
            ]
            # random.shuffle(data)
            yield data_to_item_format(item_type, data)

    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    pipeline = dlt.pipeline("test_unique_values_unordered_rows", destination="dummy")
    pipeline.run(random_ascending_chunks(order))
    assert pipeline.last_trace.last_normalize_info.row_counts["random_ascending_chunks"] == 121

    # 120 rows (one overlap - incremental reacquires and deduplicates)
    pipeline.run(random_ascending_chunks(order))
    # overlapping element must be deduped when:
    # 1. we have primary key on just updated at
    # OR we have a key on full record but the record is deterministic so duplicate may be found
    rows = 120 if primary_key == "updated_at" or (deterministic and primary_key != []) else 121
    assert pipeline.last_trace.last_normalize_info.row_counts["random_ascending_chunks"] == rows


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
@pytest.mark.parametrize("primary_key", [[], None, "updated_at"])  # [], None,
@pytest.mark.parametrize(
    "deterministic", (True, False), ids=("deterministic-record", "non-deterministic-record")
)
def test_carry_unique_hashes(
    item_type: TestDataItemFormat, primary_key: Any, deterministic: bool
) -> None:
    # each day extends list of hashes and removes duplicates until the last day

    @dlt.resource(primary_key=primary_key)
    def random_ascending_chunks(
        # order: str,
        day: int,
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at",
            initial_value=10,
        ),
    ) -> Any:
        range_ = random.sample(
            range(updated_at.initial_value, updated_at.initial_value + 10), k=10
        )  # list(range(updated_at.initial_value, updated_at.initial_value + 10))
        range_ += [100]
        if day == 4:
            # on day 4 add an element that will reset all others
            range_ += [1000]

        for chunk in chunks(range_, 3):
            # make sure that overlapping element is the last one
            data = [
                {"updated_at": i, "rand": random.random() if not deterministic else 0}
                for i in chunk
            ]
            yield data_to_item_format(item_type, data)

    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    pipeline = dlt.pipeline("test_unique_values_unordered_rows", destination="dummy")

    def _assert_state(r_: DltResource, day: int, info: NormalizeInfo) -> None:
        uniq_hashes = r_.state["incremental"]["updated_at"]["unique_hashes"]
        row_count = info.row_counts.get("random_ascending_chunks", 0)
        if primary_key == "updated_at":
            # we keep only newest version of the record
            assert len(uniq_hashes) == 1
            if day == 1:
                # all records loaded
                assert row_count == 11
            elif day == 4:
                # new biggest item loaded
                assert row_count == 1
            else:
                # all deduplicated
                assert row_count == 0
        elif primary_key is None:
            # we deduplicate over full content
            if day == 4:
                assert len(uniq_hashes) == 1
                # both the 100 or 1000 are in if non deterministic content
                assert row_count == (2 if not deterministic else 1)
            else:
                # each day adds new hash if content non deterministic
                assert len(uniq_hashes) == (day if not deterministic else 1)
                if day == 1:
                    assert row_count == 11
                else:
                    assert row_count == (1 if not deterministic else 0)
        elif primary_key == []:
            # no deduplication
            assert len(uniq_hashes) == 0
            if day == 4:
                assert row_count == 2
            else:
                if day == 1:
                    assert row_count == 11
                else:
                    assert row_count == 1

    r_ = random_ascending_chunks(1)
    pipeline.run(r_)
    _assert_state(r_, 1, pipeline.last_trace.last_normalize_info)
    r_ = random_ascending_chunks(2)
    pipeline.run(r_)
    _assert_state(r_, 2, pipeline.last_trace.last_normalize_info)
    r_ = random_ascending_chunks(3)
    pipeline.run(r_)
    _assert_state(r_, 3, pipeline.last_trace.last_normalize_info)
    r_ = random_ascending_chunks(4)
    pipeline.run(r_)
    _assert_state(r_, 4, pipeline.last_trace.last_normalize_info)


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_get_incremental_value_type(item_type: TestDataItemFormat) -> None:
    assert dlt.sources.incremental("id").get_incremental_value_type() is Any
    assert dlt.sources.incremental("id", initial_value=0).get_incremental_value_type() is int
    assert dlt.sources.incremental("id", initial_value=None).get_incremental_value_type() is Any
    assert dlt.sources.incremental[int]("id").get_incremental_value_type() is int
    assert (
        dlt.sources.incremental[pendulum.DateTime]("id").get_incremental_value_type()
        is pendulum.DateTime
    )
    # typing has precedence
    assert dlt.sources.incremental[pendulum.DateTime]("id", initial_value=1).get_incremental_value_type() is pendulum.DateTime  # type: ignore[arg-type]

    # pass default value
    @dlt.resource
    def test_type(
        updated_at=dlt.sources.incremental[str](  # noqa: B008
            "updated_at", allow_external_schedulers=True
        )
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type()
    list(r)
    assert r.incremental.incremental.get_incremental_value_type() is str

    # use annotation
    @dlt.resource
    def test_type_2(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        )
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_2()
    list(r)
    assert r.incremental.incremental.get_incremental_value_type() is int

    # pass in explicit value
    @dlt.resource
    def test_type_3(updated_at: dlt.sources.incremental[int]):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_3(dlt.sources.incremental[float]("updated_at", allow_external_schedulers=True))
    list(r)
    assert r.incremental.incremental.get_incremental_value_type() is float

    # pass explicit value overriding default that is typed
    @dlt.resource
    def test_type_4(
        updated_at=dlt.sources.incremental("updated_at", allow_external_schedulers=True)
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_4(dlt.sources.incremental[str]("updated_at", allow_external_schedulers=True))
    list(r)
    assert r.incremental.incremental.get_incremental_value_type() is str

    # no generic type information
    @dlt.resource
    def test_type_5(
        updated_at=dlt.sources.incremental("updated_at", allow_external_schedulers=True)
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_5(dlt.sources.incremental("updated_at"))
    list(r)
    assert r.incremental.incremental.get_incremental_value_type() is Any


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_join_env_scheduler(item_type: TestDataItemFormat) -> None:
    @dlt.resource
    def test_type_2(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        )
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    result = list(test_type_2())
    assert data_item_to_list(item_type, result) == [
        {"updated_at": 1},
        {"updated_at": 2},
        {"updated_at": 3},
    ]

    # set start and end values
    os.environ["DLT_START_VALUE"] = "2"
    result = list(test_type_2())
    assert data_item_to_list(item_type, result) == [{"updated_at": 2}, {"updated_at": 3}]
    os.environ["DLT_END_VALUE"] = "3"
    result = list(test_type_2())
    assert data_item_to_list(item_type, result) == [{"updated_at": 2}]


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_join_env_scheduler_pipeline(item_type: TestDataItemFormat) -> None:
    @dlt.resource
    def test_type_2(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        )
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    pip_1_name = "incremental_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pip_1_name, destination="duckdb")
    r = test_type_2()
    r.add_step(AssertItems([{"updated_at": 2}, {"updated_at": 3}], item_type))
    os.environ["DLT_START_VALUE"] = "2"
    pipeline.extract(r)
    # state is saved next extract has no items
    r = test_type_2()
    r.add_step(AssertItems([]))
    pipeline.extract(r)

    # setting end value will stop using state
    os.environ["DLT_END_VALUE"] = "3"
    r = test_type_2()
    r.add_step(AssertItems([{"updated_at": 2}], item_type))
    pipeline.extract(r)
    r = test_type_2()
    os.environ["DLT_START_VALUE"] = "1"
    r.add_step(AssertItems([{"updated_at": 1}, {"updated_at": 2}], item_type))
    pipeline.extract(r)


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_allow_external_schedulers(item_type: TestDataItemFormat) -> None:
    @dlt.resource()
    def test_type_2(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental("updated_at"),
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    # does not participate
    os.environ["DLT_START_VALUE"] = "2"
    # r = test_type_2()
    # result = data_item_to_list(item_type, list(r))
    # assert len(result) == 3

    # # incremental not bound to the wrapper
    # assert test_type_2.incremental.allow_external_schedulers is None
    # assert test_type_2().incremental.allow_external_schedulers is None
    # # this one is bound
    # assert r.incremental.allow_external_schedulers is False

    # # allow scheduler in wrapper
    # r = test_type_2()
    # r.incremental.allow_external_schedulers = True
    # result = data_item_to_list(item_type, list(r))
    # assert len(result) == 2
    # assert r.incremental.allow_external_schedulers is True
    # assert r.incremental.incremental.allow_external_schedulers is True

    # add incremental dynamically
    @dlt.resource()
    def test_type_3():
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_3()
    r.add_step(dlt.sources.incremental[int]("updated_at"))
    r.incremental.allow_external_schedulers = True
    result = data_item_to_list(item_type, list(r))
    assert len(result) == 2

    # if type of incremental cannot be inferred, external scheduler will be ignored
    r = test_type_3()
    r.add_step(dlt.sources.incremental("updated_at"))
    r.incremental.allow_external_schedulers = True
    result = data_item_to_list(item_type, list(r))
    assert len(result) == 3


@pytest.mark.parametrize("yield_pydantic", (True, False))
def test_pydantic_columns_validator(yield_pydantic: bool) -> None:
    from pydantic import BaseModel, ConfigDict, Field

    # forbid extra fields so "id" in json is not a valid field BUT
    # add alias for id_ that will serde "id" correctly
    class TestRow(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")

        id_: int = Field(alias="id")
        example_string: str
        ts: datetime

    @dlt.resource(name="table_name", columns=TestRow, primary_key="id", write_disposition="replace")
    def generate_rows():
        for i in range(10):
            item = {"id": i, "example_string": "abc", "ts": datetime.now()}
            yield TestRow.model_validate(item) if yield_pydantic else item

    @dlt.resource(name="table_name", columns=TestRow, primary_key="id", write_disposition="replace")
    def generate_rows_incremental(
        ts: dlt.sources.incremental[datetime] = dlt.sources.incremental(cursor_path="ts"),
    ):
        for i in range(10):
            item = {"id": i, "example_string": "abc", "ts": datetime.now()}
            yield TestRow.model_validate(item) if yield_pydantic else item
            if ts.end_out_of_range:
                return

    @dlt.source
    def test_source_incremental():
        return generate_rows_incremental

    @dlt.source
    def test_source():
        return generate_rows

    pip_1_name = "test_pydantic_columns_validator_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pip_1_name, destination="duckdb")

    pipeline.run(test_source())
    pipeline.run(test_source_incremental())

    # verify that right steps are at right place
    steps = test_source().table_name._pipe._steps
    assert isinstance(steps[-1], ValidateItem)
    incremental_steps = test_source_incremental().table_name._pipe._steps
    assert isinstance(incremental_steps[-2], ValidateItem)
    assert isinstance(incremental_steps[-1], IncrementalResourceWrapper)


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_cursor_date_coercion(item_type: TestDataItemFormat) -> None:
    today = datetime.today().date()

    @dlt.resource()
    def updated_is_int(updated_at=dlt.sources.incremental("updated_at", initial_value=today)):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    pip_1_name = "test_pydantic_columns_validator_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pip_1_name, destination="duckdb")

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(updated_is_int())
    assert isinstance(pip_ex.value.__cause__, IncrementalCursorInvalidCoercion)
    assert pip_ex.value.__cause__.cursor_path == "updated_at"
