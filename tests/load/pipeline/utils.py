from typing import Any, Iterator, List, Sequence
import pytest

import dlt

from dlt.common.configuration.container import Container
from dlt.common.pipeline import LoadInfo, PipelineContext
from dlt.common.typing import DictStrAny
from dlt.pipeline.exceptions import SqlClientNotAvailable


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    yield
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()

        def _drop_dataset(schema_name: str) -> None:
            try:
                with p.sql_client(schema_name) as c:
                    try:
                        c.drop_dataset()
                        # print("dropped")
                    except Exception as exc:
                        print(exc)
                    with c.with_staging_dataset(staging=True):
                        try:
                            c.drop_dataset()
                            # print("dropped")
                        except Exception as exc:
                            print(exc)
            except SqlClientNotAvailable:
                pass

        # take all schemas and if destination was set
        if p.destination:
            if p.config.use_single_dataset:
                # drop just the dataset for default schema
                if p.default_schema_name:
                    _drop_dataset(p.default_schema_name)
            else:
                # for each schema, drop the dataset
                for schema_name in p.schema_names:
                    _drop_dataset(schema_name)

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()


def assert_table(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    assert_query_data(p, f"SELECT * FROM {table_name} ORDER BY 1 NULLS FIRST", table_data, schema_name, info)


def select_data(p: dlt.Pipeline, sql: str, schema_name: str = None) -> List[Sequence[Any]]:
    with p.sql_client(schema_name=schema_name) as c:
        with c.execute_query(sql) as cur:
            return list(cur.fetchall())


def assert_query_data(p: dlt.Pipeline, sql: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    """Asserts that query selecting single column of values matches `table_data`. If `info` is provided, second column must contain one of load_ids in `info`"""
    rows = select_data(p, sql, schema_name)
    assert len(rows) == len(table_data)
    for row, d in zip(rows, table_data):
        row = list(row)
        # first element comes from the data
        assert row[0] == d
        # the second is load id
        if info:
            assert row[1] in info.loads_ids


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(p.completed_at is not None for p in info.load_packages) is True
    # no failed jobs in any of the packages
    assert all(len(p.jobs["failed_jobs"]) == 0 for p in info.load_packages) is True


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict"""
    query = "\nUNION ALL\n".join([f"SELECT '{name}' as name, COUNT(1) as c FROM {name}" for name in table_names])
    with p.sql_client() as c:
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


def load_table_distinct_counts(p: dlt.Pipeline, distinct_column: str, *table_names: str) -> DictStrAny:
    """Returns counts of distinct values for column `distinct_column` for `table_names` as dict"""
    query = "\nUNION ALL\n".join([f"SELECT '{name}' as name, COUNT(DISTINCT {distinct_column}) as c FROM {name}" for name in table_names])
    with p.sql_client() as c:
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}