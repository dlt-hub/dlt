import os
from typing import Optional

import duckdb

import dlt
from dlt.common.pendulum import pendulum, timedelta
from dlt.common.utils import uniq_id, digest256
from dlt.common.json import json
from tests.utils import preserve_environ, autouse_test_storage


def test_single_items_last_value_state_is_updated() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        yield {'created_at': 425}
        yield {'created_at': 426}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 426


def test_batch_items_last_value_state_is_updated() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        yield [{'created_at': i} for i in range(5)]
        yield [{'created_at': i} for i in range(5, 10)]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
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


def test_nested_cursor_column() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('data.items[0].created_at')):
        yield {'data': {'items': [{'created_at': 2}]}}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['data.items[0].created_at']
    assert s['last_value'] == 2


def test_explicit_initial_value() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at')):
        yield {'created_at': created_at.last_value}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(created_at=4242))

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 4242


def test_explicit_incremental_instance() -> None:
    @dlt.resource(primary_key='some_uq')
    def some_data(incremental=dlt.sources.incremental('created_at', initial_value=0)):
        assert incremental.cursor_column == 'inserted_at'
        assert incremental.initial_value == 241
        yield {'inserted_at': 242, 'some_uq': 444}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(incremental=dlt.sources.incremental('inserted_at', initial_value=241)))


def test_optional_incremental_from_config() -> None:
    @dlt.resource
    def some_data(created_at: Optional[dlt.sources.incremental] = None):
        assert created_at.cursor_column == 'created_at'
        assert created_at.initial_value == '2022-02-03T00:00:00Z'
        yield {'created_at': '2022-02-03T00:00:01Z'}

    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA__CREATED_AT__CURSOR_COLUMN'] = 'created_at'
    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA__CREATED_AT__INITIAL_VALUE'] = '2022-02-03T00:00:00Z'

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())


def test_override_initial_value_from_config() -> None:
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at', initial_value='2022-02-03T00:00:00Z')):
        assert created_at.cursor_column == 'created_at'
        assert created_at.initial_value == '2000-02-03T00:00:00Z'
        yield {'created_at': '2023-03-03T00:00:00Z'}

    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA__CREATED_AT__INITIAL_VALUE'] = '2000-02-03T00:00:00Z'

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())


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

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']

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

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
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

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == json.loads(json.dumps(initial_value + timedelta(minutes=4)))


def test_descending_order_unique_hashes() -> None:
    """Resource returns items in descending order but using `max` last value function.
    Only hash matching last_value are stored.
    """
    @dlt.resource
    def some_data(created_at=dlt.sources.incremental('created_at', 20)):
        for i in reversed(range(created_at.last_value, created_at.last_value + 5)):
            yield {'created_at': i}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']

    last_hash = digest256(json.dumps({'created_at': 24}))

    assert s['unique_hashes'] == [last_hash]
