import os
from time import sleep
from typing import Optional, Any
from datetime import datetime  # noqa: I251
from itertools import chain

import duckdb
import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.base_configuration import configspec, BaseConfiguration
from dlt.common.configuration import ConfigurationValueError
from dlt.common.pendulum import pendulum, timedelta
from dlt.common.pipeline import StateInjectableContext, resource_state
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id, digest128, chunks
from dlt.common.json import json

from dlt.extract import DltSource
from dlt.sources.helpers.transform import take_first
from dlt.extract.incremental.exceptions import (
    IncrementalCursorPathMissing,
    IncrementalPrimaryKeyMissing,
)
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.extract.utils import AssertItems, data_item_to_list
from tests.utils import data_to_item_format, TDataItemFormat, ALL_DATA_ITEM_FORMATS


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_single_items_last_value_state_is_updated(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_single_items_last_value_state_is_updated_transformer(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_batch_items_last_value_state_is_updated(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_last_value_access_in_resource(item_type: TDataItemFormat) -> None:
    values = []

    data = [{"created_at": i} for i in range(6)]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental("created_at")):
        values.append(created_at.last_value)
        yield source_items

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())
    p.extract(some_data())

    assert values == [None, 5]


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_unique_keys_are_deduplicated(item_type: TDataItemFormat) -> None:
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

    @dlt.resource(primary_key="id")
    def some_data(created_at=dlt.sources.incremental("created_at")):
        if created_at.last_value is None:
            yield from source_items1
        else:
            yield from source_items2

    p = dlt.pipeline(
        pipeline_name=uniq_id(), destination="duckdb", credentials=duckdb.connect(":memory:")
    )
    p.run(some_data()).raise_on_failed_jobs()
    p.run(some_data()).raise_on_failed_jobs()

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, id FROM some_data order by created_at, id") as cur:
            rows = cur.fetchall()

    assert rows == [(1, "a"), (2, "b"), (3, "c"), (3, "d"), (3, "e"), (3, "f"), (4, "g")]


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_unique_rows_by_hash_are_deduplicated(item_type: TDataItemFormat) -> None:
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
        pipeline_name=uniq_id(), destination="duckdb", credentials=duckdb.connect(":memory:")
    )
    p.run(some_data()).raise_on_failed_jobs()
    p.run(some_data()).raise_on_failed_jobs()

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


@pytest.mark.parametrize("item_type", ["arrow", "pandas"])
def test_nested_cursor_path_arrow_fails(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_explicit_initial_value(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_explicit_incremental_instance(item_type: TDataItemFormat) -> None:
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
    item_type: TDataItemFormat,
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_optional_incremental_from_config(item_type: TDataItemFormat) -> None:
    os.environ["SOURCES__TEST_INCREMENTAL__SOME_DATA_FROM_CONFIG__CREATED_AT__CURSOR_PATH"] = (
        "created_at"
    )
    os.environ["SOURCES__TEST_INCREMENTAL__SOME_DATA_FROM_CONFIG__CREATED_AT__INITIAL_VALUE"] = (
        "2022-02-03T00:00:00Z"
    )

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data_from_config(1, item_type))
    p.extract(some_data_from_config(2, item_type))


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_optional_incremental_not_passed(item_type: TDataItemFormat) -> None:
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
    item_type: TDataItemFormat, incremental: Optional[dlt.sources.incremental[Any]] = None
) -> Any:
    data = [1, 2, 3]
    source_items = data_to_item_format(item_type, data)
    assert incremental is None
    yield source_items


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_optional_arg_from_spec_not_passed(item_type: TDataItemFormat) -> None:
    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(optional_incremental_arg_resource(item_type))


@configspec
class SomeDataOverrideConfiguration(BaseConfiguration):
    created_at: dlt.sources.incremental = dlt.sources.incremental("created_at", initial_value="2022-02-03T00:00:00Z")  # type: ignore[type-arg]


# provide what to inject via spec. the spec contain the default
@dlt.resource(spec=SomeDataOverrideConfiguration)
def some_data_override_config(
    item_type: TDataItemFormat, created_at: dlt.sources.incremental[str] = dlt.config.value
):
    assert created_at.cursor_path == "created_at"
    assert created_at.initial_value == "2000-02-03T00:00:00Z"
    data = [{"created_at": "2023-03-03T00:00:00Z"}]
    source_items = data_to_item_format(item_type, data)
    yield from source_items


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_override_initial_value_from_config(item_type: TDataItemFormat) -> None:
    # use the shortest possible config version
    # os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA_OVERRIDE_CONFIG__CREATED_AT__INITIAL_VALUE'] = '2000-02-03T00:00:00Z'
    os.environ["CREATED_AT__INITIAL_VALUE"] = "2000-02-03T00:00:00Z"

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data_override_config(item_type))


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_override_primary_key_in_pipeline(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_composite_primary_key(item_type: TDataItemFormat) -> None:
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
        pipeline_name=uniq_id(), destination="duckdb", credentials=duckdb.connect(":memory:")
    )
    p.run(some_data()).raise_on_failed_jobs()

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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_last_value_func_min(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_cursor_datetime_type(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_descending_order_unique_hashes(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_unique_keys_json_identifiers(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_missing_primary_key(item_type: TDataItemFormat) -> None:
    @dlt.resource(primary_key="DELTA")
    def some_data(last_timestamp=dlt.sources.incremental("ts")):
        data = [{"delta": i, "ts": pendulum.now().add(days=i).timestamp()} for i in range(-10, 10)]
        source_items = data_to_item_format(item_type, data)
        yield from source_items

    with pytest.raises(IncrementalPrimaryKeyMissing) as py_ex:
        list(some_data())
    assert py_ex.value.primary_key_column == "DELTA"


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_missing_cursor_field(item_type: TDataItemFormat) -> None:
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
        last_timestamp: dlt.sources.incremental[float] = dlt.sources.incremental(
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

    # we disable incremental by typing the argument as optional
    # if not disabled it would fail on "item.timestamp" not found
    assert list(some_data_optional(last_timestamp=dlt.sources.incremental.EMPTY)) == [1]

    @dlt.resource(standalone=True)
    def some_data(
        last_timestamp: dlt.sources.incremental[float] = dlt.sources.incremental("item.timestamp"),
    ):
        assert last_timestamp is None
        yield 1

    # we'll get the value error
    with pytest.raises(ValueError):
        assert list(some_data(last_timestamp=dlt.sources.incremental.EMPTY)) == [1]


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_filter_processed_items(item_type: TDataItemFormat) -> None:
    """Checks if already processed items are filtered out"""

    @dlt.resource
    def standalone_some_data(
        item_type: TDataItemFormat, now=None, last_timestamp=dlt.sources.incremental("timestamp")
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


@pytest.mark.parametrize("item_type", set(ALL_DATA_ITEM_FORMATS) - {"json"})
def test_start_value_set_to_last_value_arrow(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_replace_resets_state(item_type: TDataItemFormat) -> None:
    p = dlt.pipeline(pipeline_name=uniq_id(), destination="duckdb")
    now = pendulum.now()

    @dlt.resource
    def standalone_some_data(
        item_type: TDataItemFormat, now=None, last_timestamp=dlt.sources.incremental("timestamp")
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_incremental_as_transform(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_incremental_explicit_disable_unique_check(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_apply_hints_incremental(item_type: TDataItemFormat) -> None:
    p = dlt.pipeline(pipeline_name=uniq_id())
    data = [{"created_at": 1}, {"created_at": 2}, {"created_at": 3}]
    source_items = data_to_item_format(item_type, data)

    @dlt.resource
    def some_data(created_at: Optional[dlt.sources.incremental[int]] = None):
        yield source_items

    # the incremental wrapper is created for a resource and the incremental value is provided via apply hints
    r = some_data()
    assert list(r) == source_items
    r.apply_hints(incremental=dlt.sources.incremental("created_at"))
    p.extract(r)
    assert "incremental" in r.state
    assert list(r) == []

    # as above but we provide explicit incremental when creating resource
    p = p.drop()
    r = some_data(created_at=dlt.sources.incremental("created_at", last_value_func=min))
    # explicit has precedence here
    r.apply_hints(incremental=dlt.sources.incremental("created_at", last_value_func=max))
    p.extract(r)
    assert "incremental" in r.state
    # min value
    assert r.state["incremental"]["created_at"]["last_value"] == 1

    @dlt.resource
    def some_data_w_default(created_at=dlt.sources.incremental("created_at", last_value_func=min)):
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
    r.apply_hints(incremental=dlt.sources.incremental("created_at", last_value_func=max))
    assert r.incremental is not None
    p.extract(r)
    assert "incremental" in r.state


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


def test_timezone_naive_datetime() -> None:
    # TODO: arrow doesn't work with this
    """Resource has timezone naive datetime objects, but incremental stored state is
    converted to tz aware pendulum dates. Can happen when loading e.g. from sql database"""
    start_dt = datetime.now()
    pendulum_start_dt = pendulum.instance(start_dt)  # With timezone

    @dlt.resource
    def some_data(
        updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
            "updated_at", pendulum_start_dt
        )
    ):
        data = [
            {"updated_at": start_dt + timedelta(hours=1)},
            {"updated_at": start_dt + timedelta(hours=2)},
        ]
        yield data

    pipeline = dlt.pipeline(pipeline_name=uniq_id())
    resource = some_data()
    pipeline.extract(resource)
    # last value has timezone added
    last_value = resource.state["incremental"]["updated_at"]["last_value"]
    assert isinstance(last_value, pendulum.DateTime)
    assert last_value.tzname() == "UTC"


@dlt.resource
def endless_sequence(
    item_type: TDataItemFormat,
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
        "updated_at", initial_value=1
    ),
) -> Any:
    max_values = 20
    start = updated_at.last_value
    data = [{"updated_at": i} for i in range(start, start + max_values)]
    source_items = data_to_item_format(item_type, data)
    yield from source_items


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_chunked_ranges(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_end_value_with_batches(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_load_with_end_value_does_not_write_state(item_type: TDataItemFormat) -> None:
    """When loading chunk with initial/end value range. The resource state is untouched."""
    pipeline = dlt.pipeline(pipeline_name="incremental_" + uniq_id(), destination="duckdb")

    pipeline.extract(
        endless_sequence(
            item_type, updated_at=dlt.sources.incremental(initial_value=20, end_value=30)
        )
    )

    assert pipeline.state.get("sources") is None


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_end_value_initial_value_errors(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_out_of_range_flags(item_type: TDataItemFormat) -> None:
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
            yield {"updated_at": i}
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_get_incremental_value_type(item_type: TDataItemFormat) -> None:
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
    assert r.incremental._incremental.get_incremental_value_type() is str

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
    assert r.incremental._incremental.get_incremental_value_type() is int

    # pass in explicit value
    @dlt.resource
    def test_type_3(updated_at: dlt.sources.incremental[int]):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_3(dlt.sources.incremental[float]("updated_at", allow_external_schedulers=True))
    list(r)
    assert r.incremental._incremental.get_incremental_value_type() is float

    # pass explicit value overriding default that is typed
    @dlt.resource
    def test_type_4(
        updated_at=dlt.sources.incremental("updated_at", allow_external_schedulers=True)
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_4(dlt.sources.incremental[str]("updated_at", allow_external_schedulers=True))
    list(r)
    assert r.incremental._incremental.get_incremental_value_type() is str

    # no generic type information
    @dlt.resource
    def test_type_5(
        updated_at=dlt.sources.incremental("updated_at", allow_external_schedulers=True)
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    r = test_type_5(dlt.sources.incremental("updated_at"))
    list(r)
    assert r.incremental._incremental.get_incremental_value_type() is Any


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_join_env_scheduler(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_join_env_scheduler_pipeline(item_type: TDataItemFormat) -> None:
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


@pytest.mark.parametrize("item_type", ALL_DATA_ITEM_FORMATS)
def test_allow_external_schedulers(item_type: TDataItemFormat) -> None:
    @dlt.resource()
    def test_type_2(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental("updated_at"),
    ):
        data = [{"updated_at": d} for d in [1, 2, 3]]
        yield data_to_item_format(item_type, data)

    # does not participate
    os.environ["DLT_START_VALUE"] = "2"
    result = data_item_to_list(item_type, list(test_type_2()))
    assert len(result) == 3

    assert test_type_2.incremental.allow_external_schedulers is False
    assert test_type_2().incremental.allow_external_schedulers is False

    # allow scheduler in wrapper
    r = test_type_2()
    r.incremental.allow_external_schedulers = True
    result = data_item_to_list(item_type, list(test_type_2()))
    assert len(result) == 2
    assert r.incremental.allow_external_schedulers is True
    assert r.incremental._incremental.allow_external_schedulers is True

    # add incremental dynamically
    @dlt.resource()
    def test_type_3():
        yield [{"updated_at": d} for d in [1, 2, 3]]

    r = test_type_3()
    r.add_step(dlt.sources.incremental("updated_at"))
    r.incremental.allow_external_schedulers = True
    result = data_item_to_list(item_type, list(test_type_2()))
    assert len(result) == 2
