from typing import Any, Iterator, List
import pytest

import dlt

from dlt.common.configuration.container import Container
from dlt.common.pipeline import LoadInfo, PipelineContext
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
    assert_data(p, f"SELECT * FROM {table_name} ORDER BY 1 NULLS FIRST", table_data, schema_name, info)


def assert_data(p: dlt.Pipeline, sql: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    with p.sql_client(schema_name=schema_name) as c:
        with c.execute_query(sql) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == len(table_data)
            for row, d in zip(rows, table_data):
                row = list(row)
                # first element comes from the data
                assert row[0] == d
                # the second is load id
                if info:
                    assert row[1] in info.loads_ids
