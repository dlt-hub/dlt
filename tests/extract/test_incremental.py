import os
from typing import Optional

import pytest

import dlt
from dlt.common.utils import uniq_id
from dlt.extract.incremental import Incremental


def test_single_items_last_value_state_is_updated() -> None:
    @dlt.resource
    def some_data(created_at=Incremental('created_at')):
        yield {'created_at': 425}
        yield {'created_at': 426}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 426


def test_batch_items_last_value_state_is_updated() -> None:
    @dlt.resource
    def some_data(created_at=Incremental('created_at')):
        yield [{'created_at': i} for i in range(5)]
        yield [{'created_at': i} for i in range(5, 10)]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 9


def test_last_value_access_in_resource() -> None:
    values = []

    @dlt.resource
    def some_data(created_at=Incremental('created_at')):
        values.append(created_at.last_value)
        yield [{'created_at': i} for i in range(6)]

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())
    p.extract(some_data())

    assert values == [None, 5]


def test_unique_keys_are_deduplicated() -> None:
    @dlt.resource
    def some_data(created_at=Incremental('created_at', unique_column='id')):
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

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination='duckdb')
    p.run(some_data(), destination='duckdb')

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, id FROM some_data order by created_at, id") as cur:
            rows = c.fetchall()

    assert rows == [(1, 'a'), (2, 'b'), (3, 'c'), (3, 'd'), (3, 'e'), (3, 'f'), (4, 'g')]


def test_unique_rows_by_hash_are_deduplicated() -> None:
    @dlt.resource
    def some_data(created_at=Incremental('created_at')):
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

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.run(some_data(), destination='duckdb')
    p.run(some_data(), destination='duckdb')

    with p.sql_client() as c:
        with c.execute_query("SELECT created_at, id FROM some_data order by created_at, id") as cur:
            rows = c.fetchall()

    assert rows == [(1, 'a'), (2, 'b'), (3, 'c'), (3, 'd'), (3, 'e'), (3, 'f'), (4, 'g')]


def test_nested_cursor_column() -> None:
    @dlt.resource
    def some_data(created_at=Incremental('data.items[0].created_at')):
        yield {'data': {'items': [{'created_at': 2}]}}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['data.items[0].created_at']
    assert s['last_value'] == 2


def test_explicit_initial_value() -> None:
    @dlt.resource
    def some_data(created_at=Incremental('created_at')):
        yield {'created_at': created_at.last_value}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(created_at=4242))

    s = dlt.state()[p.pipeline_name]['resources']['some_data']['incremental']['created_at']
    assert s['last_value'] == 4242


def test_explicit_incremental_instance() -> None:
    @dlt.resource
    def some_data(incremental=Incremental('created_at', unique_column='some_uq', initial_value=0)):
        assert incremental.cursor_column == 'inserted_at'
        assert incremental.initial_value == 241
        assert incremental.unique_column == 'other_uq'
        yield {'inserted_at': 242, 'other_uq': 444}

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data(incremental=Incremental('inserted_at', unique_column='other_uq', initial_value=241)))


def test_optional_incremental_from_config() -> None:
    @dlt.resource
    def some_data(created_at: Optional[Incremental] = None):
        assert created_at.cursor_column == 'created_at'
        assert created_at.initial_value == '2022-02-03T00:00:00Z'
        yield {'created_at': '2022-02-03T00:00:01Z'}

    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA__CREATED_AT__CURSOR_COLUMN'] = 'created_at'
    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA__CREATED_AT__INITIAL_VALUE'] = '2022-02-03T00:00:00Z'

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())


def test_override_initial_value_from_config() -> None:
    @dlt.resource
    def some_data(created_at=Incremental('created_at', initial_value='2022-02-03T00:00:00Z')):
        assert created_at.cursor_column == 'created_at'
        assert created_at.initial_value == '2000-02-03T00:00:00Z'
        yield {'created_at': '2023-03-03T00:00:00Z'}

    os.environ['SOURCES__TEST_INCREMENTAL__SOME_DATA__CREATED_AT__INITIAL_VALUE'] = '2000-02-03T00:00:00Z'

    p = dlt.pipeline(pipeline_name=uniq_id())
    p.extract(some_data())
