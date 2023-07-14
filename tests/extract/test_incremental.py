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
from dlt.common.utils import uniq_id, digest128
from dlt.common.json import json

from dlt.extract.source import DltSource
from dlt.sources.helpers.transform import take_first
from dlt.extract.incremental import IncrementalCursorPathMissing, IncrementalPrimaryKeyMissing

from tests.pipeline.utils import drop_pipeline
# from tests.load.pipeline.utils import load_table_counts
from tests.utils import preserve_environ, autouse_test_storage, patch_home_dir


def test_single_items_last_value_state_is_updated() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        yield {'created_at': 425}
        yield {'created_at': 426}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())
    s = some_data.state['incremental']['created_at']
    assert s['last_value'] == 426


def test_single_items_last_value_state_is_updated_transformer() -> None:
    @dlt.transformer
    def some_data(item, created_at=dlt.sources.incremental('created_at')):
        yield {'created_at': 425}
        yield {'created_at': 426}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(dlt.resource([1,2,3], name="table") | some_data())

    s = some_data().state['incremental']['created_at']
    assert s['last_value'] == 426


def test_batch_items_last_value_state_is_updated() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        yield [{'created_at': i} for i in range(5)]
        yield [{'created_at': i} for i in range(5, 10)]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 9


def test_last_value_access_in_resource() -> None:
    values = []

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        values.append(created_at.last_value)
        yield [{'created_at': i} for i in range(6)]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())
    p.extract(some_data())

    assert values == [None, 5]


def test_unique_keys_are_deduplicated() -> None:
    @dlt.resource(primary_key='id')
    def some_data(created_at=dlt.sources.incremental('created_at')):
        if created_at.last_value is None:
            yield {'created_at': 1, 'id': 'a'}
            yield {'created_at': 2, 'id': 'b'}
            yield {'created_at': 3, 'id': 'c'}
            yield {'created_at': 3, 'id': 'd'}
            yield {'created_at': 3, 'id': 'e'}
        else:
            yield {'created_at': 3, 'id': 'c'}
            yield {'created_at': 3, 'id': 'd'}
            yield {'created_at': 3, 'id': 'e'}
            yield {'created_at': 3, 'id': 'f'}
            yield {'created_at': 4, 'id': 'g'}

    p = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))

    p.run(some_data())
    p.run(some_data())

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, id FROM some_data order by created_at, id") as cur:
            rows = cur.fetchall()

    assert rows == [(1, 'a'), (2, 'b'), (3, 'c'), (3, 'd'), (3, 'e'), (3, 'f'), (4, 'g')]


def test_unique_rows_by_hash_are_deduplicated() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        if created_at.last_value is None:
            yield {'created_at': 1, 'id': 'a'}
            yield {'created_at': 2, 'id': 'b'}
            yield {'created_at': 3, 'id': 'c'}
            yield {'created_at': 3, 'id': 'd'}
            yield {'created_at': 3, 'id': 'e'}
        else:
            yield {'created_at': 3, 'id': 'c'}
            yield {'created_at': 3, 'id': 'd'}
            yield {'created_at': 3, 'id': 'e'}
            yield {'created_at': 3, 'id': 'f'}
            yield {'created_at': 4, 'id': 'g'}

    p = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))
    p.run(some_data())
    p.run(some_data())

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, id FROM some_data order by created_at, id") as cur:
            rows = cur.fetchall()

    assert rows == [(1, 'a'), (2, 'b'), (3, 'c'), (3, 'd'), (3, 'e'), (3, 'f'), (4, 'g')]


def test_nested_cursor_path() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('data.items[0].created_at')):
        yield {'data': {'items': [{'created_at': 2}]}}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]['resources']['some_data']['incremental']['data.items[0].created_at']
    assert s['last_value'] == 2


def test_explicit_initial_value() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        yield {'created_at': created_at.last_value}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(created_at=4242))

    s = p.state["sources"][p.default_schema_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 4242


def test_explicit_incremental_instance() -> None:
    @dlt.resource(primary_key='some_uq')
    def some_data(incremental=dlt.sources.incremental('created_at', initial_value=0)):
        assert incremental.cursor_path == 'inserted_at'
        assert incremental.initial_value == 241
        yield {'inserted_at': 242, 'some_uq': 444}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(incremental=dlt.sources.incremental('inserted_at', initial_value=241)))


@dlt.resource
def some_data_from_config(call_no: int, created_at: Optional[dlt.sources.incremental] = dlt.secrets.value):
    assert created_at.cursor_path == 'created_at'
    # start value will update to the last_value on next call
    if call_no == 1:
        assert created_at.initial_value == '2022-02-03T00:00:00Z'
        assert created_at.start_value == '2022-02-03T00:00:00Z'
    if call_no == 2:
        assert created_at.initial_value == '2022-02-03T00:00:00Z'
        assert created_at.start_value == '2022-02-03T00:00:01Z'
    yield {'created_at': '2022-02-03T00:00:01Z'}


def test_optional_incremental_from_config() -> None:

    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA_FROM_CONFIG__CREATED_AT__CURSOR_PATH'] = 'created_at'
    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA_FROM_CONFIG__CREATED_AT__INITIAL_VALUE'] = '2022-02-03T00:00:00Z'

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data_from_config(1))
    p.extract(some_data_from_config(2))


@configspec
class SomeDataOverrideConfiguration:
    created_at: dlt.sources.incremental = dlt.sources.incremental('created_at', initial_value='2022-02-03T00:00:00Z')


# provide what to inject via spec. the spec contain the default
@dlt.resource(spec=SomeDataOverrideConfiguration)
def some_data_override_config(created_at: dlt.sources.incremental = dlt.config.value):
    assert created_at.cursor_path == 'created_at'
    assert created_at.initial_value == '2000-02-03T00:00:00Z'
    yield {'created_at': '2023-03-03T00:00:00Z'}


def test_optional_incremental_not_passed() -> None:
    """Resource still runs when no incremental is passed"""

    @dlt.resource
    def some_data(created_at: Optional[dlt.sources.incremental] = None):
        yield [1,2,3]

    assert list(some_data()) == [1, 2, 3]


@configspec
class OptionalIncrementalConfig(BaseConfiguration):
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]


@dlt.resource(spec=OptionalIncrementalConfig)
def optional_incremental_arg_resource(incremental: Optional[dlt.sources.incremental[Any]] = None) -> Any:
    assert incremental is None
    yield [1, 2, 3]


def test_optional_arg_from_spec_not_passed() -> None:
    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(optional_incremental_arg_resource())


def test_override_initial_value_from_config() -> None:
    # use the shortest possible config version
    # os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA_OVERRIDE_CONFIG__CREATED_AT__INITIAL_VALUE'] = '2000-02-03T00:00:00Z'
    os.environ['CREATED_AT__INITIAL_VALUE'] = '2000-02-03T00:00:00Z'

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data_override_config())
    # p.extract(some_data_override_config())


def test_override_primary_key_in_pipeline() -> None:
    """Primary key hint passed to pipeline is propagated through apply_hints
    """
    @dlt.resource(primary_key='id')
    def some_data(created_at=dlt.sources.incremental('created_at')):
        # TODO: this only works because incremental instance is shared across many copies of the resource
        assert some_data.incremental.primary_key == ['id', 'other_id']

        yield {'created_at': 22, 'id': 2, 'other_id': 5}
        yield {'created_at': 22, 'id': 2, 'other_id': 6}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data, primary_key=['id', 'other_id'])


def test_composite_primary_key() -> None:
    @dlt.resource(primary_key=['isrc', 'market'])
    def some_data(created_at=dlt.sources.incremental('created_at')):
        yield {'created_at': 1, 'isrc': 'AAA', 'market': 'DE'}
        yield {'created_at': 2, 'isrc': 'BBB', 'market': 'DE'}
        yield {'created_at': 2, 'isrc': 'CCC', 'market': 'US'}
        yield {'created_at': 2, 'isrc': 'AAA', 'market': 'DE'}
        yield {'created_at': 2, 'isrc': 'CCC', 'market': 'DE'}
        yield {'created_at': 2, 'isrc': 'DDD', 'market': 'DE'}
        yield {'created_at': 2, 'isrc': 'CCC', 'market': 'DE'}

    p = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))
    p.run(some_data())

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, isrc, market FROM some_data order by created_at, isrc, market") as cur:
            rows = cur.fetchall()

    assert rows == [(1, 'AAA', 'DE'), (2, 'AAA', 'DE'), (2, 'BBB', 'DE'), (2, 'CCC', 'DE'), (2, 'CCC', 'US'), (2, 'DDD', 'DE')]


def test_last_value_func_min() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at', last_value_func=min)):
        yield {'created_at': 10}
        yield {'created_at': 11}
        yield {'created_at': 9}
        yield {'created_at': 10}
        yield {'created_at': 8}
        yield {'created_at': 22}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]['resources']['some_data']['incremental']['created_at']

    assert s['last_value'] == 8


def test_last_value_func_custom() -> None:
    def last_value(values):
        return max(values) + 1

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at', last_value_func=last_value)):
        yield {'created_at': 9}
        yield {'created_at': 10}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 11


def test_cursor_datetime_type() -> None:
    initial_value = pendulum.now()

    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at', initial_value)):
        yield {'created_at': initial_value + timedelta(minutes=1)}
        yield {'created_at': initial_value + timedelta(minutes=3)}
        yield {'created_at': initial_value + timedelta(minutes=2)}
        yield {'created_at': initial_value + timedelta(minutes=4)}
        yield {'created_at': initial_value + timedelta(minutes=2)}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == initial_value + timedelta(minutes=4)


def test_descending_order_unique_hashes() -> None:
    """Resource returns items in descending order but using `max` last value function.
    Only hash matching last_value are stored.
    """
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at', 20)):
        for i in reversed(range(15, 25)):
            yield {'created_at': i}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = p.state["sources"][p.default_schema_name]['resources']['some_data']['incremental']['created_at']

    last_hash = digest128(json.dumps({'created_at': 24}))

    assert s['unique_hashes'] == [last_hash]

    # make sure nothing is returned on a next run, source will use state from the active pipeline
    assert list(some_data()) == []


def test_unique_keys_json_identifiers() -> None:
    """Uses primary key name that is matching the name of the JSON element in the original namespace but gets converted into destination namespace"""
    @dlt.resource(primary_key="DelTa")
    def some_data(last_timestamp=dlt.sources.incremental("item.ts")):
        for i in range(-10, 10):
            yield {"DelTa": i, "item": {"ts": pendulum.now().add(days=i).timestamp()}}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data, destination="duckdb")
    # check if default schema contains normalized PK
    assert p.default_schema.tables["some_data"]['columns']["del_ta"]['primary_key'] is True
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
        with c.execute_query("SELECT del_ta FROM some_data WHERE _dlt_load_id = %s", load_info.loads_ids[0]) as cur:
            rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 9


def test_missing_primary_key() -> None:

    @dlt.resource(primary_key="DELTA")
    def some_data(last_timestamp=dlt.sources.incremental("item.ts")):
        for i in range(-10, 10):
            yield {"delta": i, "item": {"ts": pendulum.now().add(days=i).timestamp()}}

    with pytest.raises(IncrementalPrimaryKeyMissing) as py_ex:
        list(some_data())
    assert py_ex.value.primary_key_column == "DELTA"


def test_missing_cursor_field() -> None:

    @dlt.resource
    def some_data(last_timestamp=dlt.sources.incremental("item.timestamp")):
        for i in range(-10, 10):
            yield {"delta": i, "item": {"ts": pendulum.now().add(days=i).timestamp()}}

    with pytest.raises(IncrementalCursorPathMissing) as py_ex:
        list(some_data)
    assert py_ex.value.json_path == "item.timestamp"


@dlt.resource
def standalone_some_data(now=None, last_timestamp=dlt.sources.incremental("item.timestamp")):
    for i in range(-10, 10):
        print(i)
        yield {"delta": i, "item": {"timestamp": (now or pendulum.now()).add(days=i).timestamp()}}


def test_filter_processed_items() -> None:
    """Checks if already processed items are filtered out"""

    # we get all items (no initial - nothing filtered)
    assert len(list(standalone_some_data)) == 20

    # provide initial value using max function
    values = list(standalone_some_data(last_timestamp=pendulum.now().timestamp()))
    assert len(values) == 10
    # only the future timestamps
    assert all(v["delta"] >= 0 for v in values)

    # provide the initial value, use min function
    values = list(standalone_some_data(last_timestamp=dlt.sources.incremental("item.timestamp", pendulum.now().timestamp(), min)))
    assert len(values) == 10
    # the minimum element
    assert values[0]["delta"] == -10


def test_start_value_set_to_last_value() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"

    p = dlt.pipeline(pipeline_name=uniq_id())
    now = pendulum.now()

    @dlt.resource
    def some_data(step, last_timestamp=dlt.sources.incremental("item.ts")):
        if step == -10:
            assert last_timestamp.start_value is None
        else:
            # print(last_timestamp.initial_value)
            # print(now.add(days=step-1).timestamp())
            assert last_timestamp.start_value == last_timestamp.last_value == now.add(days=step-1).timestamp()
        for i in range(-10, 10):
            yield {"delta": i, "item": {"ts": now.add(days=i).timestamp()}}
        # after all yielded
        if step == -10:
            assert last_timestamp.start_value is None
        else:
            assert last_timestamp.start_value == now.add(days=step-1).timestamp() != last_timestamp.last_value

    for i in range(-10, 10):
        r = some_data(i)
        assert len(r._pipe) == 2
        r.add_filter(take_first(i + 11), 1)
        p.run(r, destination="dummy")


def test_replace_resets_state() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name=uniq_id(), destination="dummy")
    now = pendulum.now()

    info = p.run(standalone_some_data(now))
    assert len(info.loads_ids) == 1
    info = p.run(standalone_some_data(now))
    assert len(info.loads_ids) == 0
    info = p.run(standalone_some_data(now), write_disposition="replace")
    assert len(info.loads_ids) == 1

    parent_r = standalone_some_data(now)
    @dlt.transformer(data_from=parent_r, write_disposition="append")
    def child(item):
        state = resource_state("child")
        # print(f"CHILD: {state}")
        state["mark"] = f"mark:{item['delta']}"
        yield item

    # also transformer will not receive new data
    info = p.run(child)
    assert len(info.loads_ids) == 0
    # now it will
    info = p.run(child, write_disposition="replace")
    print(info.load_packages[0])
    assert len(info.loads_ids) == 1

    s = DltSource("comp", "section", Schema("comp"), [parent_r, child])

    p = dlt.pipeline(pipeline_name=uniq_id(), destination="duckdb")
    info = p.run(s)
    assert len(info.loads_ids) == 1
    info = p.run(s)
    # state was reset
    assert 'child' not in s.state['resources']
    # there's a load package but it contains 1 job to reset state
    assert len(info.load_packages[0].jobs['completed_jobs']) == 1
    assert info.load_packages[0].jobs['completed_jobs'][0].job_file_info.table_name == "_dlt_pipeline_state"


def test_incremental_as_transform() -> None:

    now = pendulum.now().timestamp()

    @dlt.resource
    def some_data():
        last_value = dlt.sources.incremental.from_existing_state("some_data", "item.ts")
        assert last_value.initial_value == now
        assert last_value.start_value == now
        assert last_value.cursor_path == "item.ts"
        assert last_value.last_value == now

        for i in range(-10, 10):
            yield {"delta": i, "item": {"ts": pendulum.now().add(days=i).timestamp()}}

    r = some_data().add_step(dlt.sources.incremental("item.ts", initial_value=now, primary_key="delta"))
    p = dlt.pipeline(pipeline_name=uniq_id())
    info = p.run(r, destination="duckdb")
    assert len(info.loads_ids) == 1


def test_incremental_explicit_primary_key() -> None:
    @dlt.resource(primary_key="delta")
    def some_data(last_timestamp=dlt.sources.incremental("item.ts", primary_key="DELTA")):
        for i in range(-10, 10):
            yield {"delta": i, "item": {"ts": pendulum.now().add(days=i).timestamp()}}

    with pytest.raises(IncrementalPrimaryKeyMissing) as py_ex:
        list(some_data())
    assert py_ex.value.primary_key_column == "DELTA"


def test_incremental_explicit_disable_unique_check() -> None:
    @dlt.resource(primary_key="delta")
    def some_data(last_timestamp=dlt.sources.incremental("item.ts", primary_key=())):
        for i in range(-10, 10):
            yield {"delta": i, "item": {"ts": pendulum.now().timestamp()}}

    with Container().injectable_context(StateInjectableContext(state={})):
        s = some_data()
        list(s)
        # no unique hashes at all
        assert s.state["incremental"]["item.ts"]["unique_hashes"] == []


def test_apply_hints_incremental() -> None:

    p = dlt.pipeline(pipeline_name=uniq_id())

    @dlt.resource
    def some_data(created_at: Optional[dlt.sources.incremental] = None):
        yield [1,2,3]

    # the incremental wrapper is created for a resource and the incremental value is provided via apply hints
    r = some_data()
    assert list(r) == [1, 2, 3]
    r.apply_hints(incremental=dlt.sources.incremental("$"))
    p.extract(r)
    assert "incremental" in r.state
    assert list(r) == []

    # as above but we provide explicit incremental when creating resource
    p = p.drop()
    r = some_data(created_at=dlt.sources.incremental("$", last_value_func=min))
    # explicit has precedence here
    r.apply_hints(incremental=dlt.sources.incremental("$", last_value_func=max))
    p.extract(r)
    assert "incremental" in r.state
    # min value
    assert r.state["incremental"]["$"]["last_value"] == 1

    @dlt.resource
    def some_data_w_default(created_at = dlt.sources.incremental("$", last_value_func=min)):
        yield [1,2,3]

    # default is overridden by apply hints
    p = p.drop()
    r = some_data_w_default()
    r.apply_hints(incremental=dlt.sources.incremental("$", last_value_func=max))
    p.extract(r)
    assert "incremental" in r.state
    # min value
    assert r.state["incremental"]["$"]["last_value"] == 3

    @dlt.resource
    def some_data_no_incremental():
        yield [1, 2, 3]

    # we add incremental as a step
    p = p.drop()
    r = some_data_no_incremental()
    r.apply_hints(incremental=dlt.sources.incremental("$", last_value_func=max))
    assert r.incremental is not None
    p.extract(r)
    assert "incremental" in r.state


def test_last_value_func_on_dict() -> None:

    """Test last value which is a dictionary"""
    def by_event_type(event):
        last_value = None
        if len(event) == 1:
            item, = event
        else:
            item, last_value = event

        if last_value is None:
            last_value = {}
        else:
            last_value = dict(last_value)
        item_type = item["type"]
        last_value[item_type] = max(item["created_at"], last_value.get(item_type, "1970-01-01T00:00:00Z"))
        return last_value

    @dlt.resource(primary_key="id", table_name=lambda i: i['type'])
    def _get_shuffled_events(last_created_at = dlt.sources.incremental("$", last_value_func=by_event_type)):
        with open("tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8") as f:
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
    """Resource has timezone naive datetime objects, but incremental stored state is
    converted to tz aware pendulum dates. Can happen when loading e.g. from sql database"""
    start_dt = datetime.now()
    pendulum_start_dt = pendulum.instance(start_dt)  # With timezone

    @dlt.resource
    def some_data(updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental('updated_at', pendulum_start_dt)):
        yield [{'updated_at': start_dt + timedelta(hours=1)}, {'updated_at': start_dt + timedelta(hours=2)}]

    pipeline = dlt.pipeline(pipeline_name=uniq_id())
    pipeline.extract(some_data())


@dlt.resource
def endless_sequence(
    updated_at: dlt.sources.incremental[int] = dlt.sources.incremental('updated_at', initial_value=1)
) -> Any:
    max_values = 20
    start = updated_at.last_value

    for i in range(start, start + max_values):
        yield {'updated_at': i}


def test_chunked_ranges() -> None:
    """Load chunked ranges with end value along with incremental"""

    pipeline = dlt.pipeline(pipeline_name='incremental_' + uniq_id(), destination='duckdb')

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
            endless_sequence(updated_at=dlt.sources.incremental(initial_value=start, end_value=end)),
            write_disposition='append'
        )

    expected_range = list(chain(
        range(10, 20),
        range(20, 30),
        range(40, 50),
        range(50, 60),
        range(60, 61),
        range(62, 70),
        range(70, 89),
        range(89, 109),
    ))

    with pipeline.sql_client() as client:
        items = [row[0] for row in client.execute_sql("SELECT updated_at FROM endless_sequence ORDER BY updated_at")]

    assert items == expected_range


def test_end_value_with_batches() -> None:
    """Ensure incremental with end_value works correctly when resource yields lists instead of single items"""
    @dlt.resource
    def batched_sequence(
            updated_at: dlt.sources.incremental[int] = dlt.sources.incremental('updated_at', initial_value=1)
    ) -> Any:
        start = updated_at.last_value
        yield [{'updated_at': i} for i in range(start, start + 12)]
        yield [{'updated_at': i} for i in range(start+12, start + 20)]

    pipeline = dlt.pipeline(pipeline_name='incremental_' + uniq_id(), destination='duckdb')

    pipeline.run(
        batched_sequence(updated_at=dlt.sources.incremental(initial_value=1, end_value=10)),
        write_disposition='append'
    )

    with pipeline.sql_client() as client:
        items = [row[0] for row in client.execute_sql("SELECT updated_at FROM batched_sequence ORDER BY updated_at")]

    assert items == list(range(1, 10))


def test_load_with_end_value_does_not_write_state() -> None:
    """When loading chunk with initial/end value range. The resource state is untouched.
    """
    pipeline = dlt.pipeline(pipeline_name='incremental_' + uniq_id(), destination='duckdb')

    pipeline.extract(endless_sequence(updated_at=dlt.sources.incremental(initial_value=20, end_value=30)))

    assert pipeline.state.get('sources') is None


def test_end_value_initial_value_errors() -> None:
    @dlt.resource
    def some_data(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental('updated_at')
    ) -> Any:
        yield {'updated_at': 1}

    # end_value without initial_value
    with pytest.raises(ConfigurationValueError) as ex:
        list(some_data(updated_at=dlt.sources.incremental(end_value=22)))

    assert str(ex.value).startswith("Incremental 'end_value' was specified without 'initial_value'")

    # max function and end_value lower than initial_value
    with pytest.raises(ConfigurationValueError) as ex:
        list(some_data(updated_at=dlt.sources.incremental(initial_value=42, end_value=22)))

    assert str(ex.value).startswith("Incremental 'initial_value' (42) is higher than 'end_value` (22)")

    # max function and end_value higher than initial_value
    with pytest.raises(ConfigurationValueError) as ex:
        list(some_data(updated_at=dlt.sources.incremental(initial_value=22, end_value=42, last_value_func=min)))

    assert str(ex.value).startswith("Incremental 'initial_value' (22) is lower than 'end_value` (42).")

    def custom_last_value(items):  # type: ignore[no-untyped-def]
        return max(items)

    # custom function which evaluates end_value lower than initial
    with pytest.raises(ConfigurationValueError) as ex:
        list(some_data(updated_at=dlt.sources.incremental(initial_value=42, end_value=22, last_value_func=custom_last_value)))

    assert "The result of 'custom_last_value([end_value, initial_value])' must equal 'end_value'" in str(ex.value)
