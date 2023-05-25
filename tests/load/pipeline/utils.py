from typing import Any, Iterator, List, Sequence, TYPE_CHECKING
import pytest

import dlt

from dlt.common.configuration.container import Container
from dlt.common.pipeline import LoadInfo, PipelineContext
from dlt.common.typing import DictStrAny
from dlt.pipeline.exceptions import SqlClientNotAvailable
if TYPE_CHECKING:
    from dlt.destinations.filesystem.filesystem import FilesystemClient


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    yield
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()

        def _drop_dataset_fs(_: str) -> None:
            try:
                client: "FilesystemClient" = p._destination_client()  # type: ignore[assignment]
                client.fs_client.rm(str(client.dataset_path), recursive=True)
            except Exception as exc:
                print(exc)

        def _drop_dataset_sql(schema_name: str) -> None:
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

        drop_func = _drop_dataset_fs if _is_filesystem(p) else _drop_dataset_sql
        # take all schemas and if destination was set
        if p.destination:
            if p.config.use_single_dataset:
                # drop just the dataset for default schema
                if p.default_schema_name:
                    drop_func(p.default_schema_name)
            else:
                # for each schema, drop the dataset
                for schema_name in p.schema_names:
                    drop_func(schema_name)

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()


def _is_filesystem(p: dlt.Pipeline) -> bool:
    return p.destination.__name__.rsplit('.', 1)[-1] == 'filesystem'


def assert_table(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    func = _assert_table_fs if _is_filesystem(p) else _assert_table_sql
    func(p, table_name, table_data, schema_name, info)


def _assert_table_sql(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    assert_query_data(p, f"SELECT * FROM {table_name} ORDER BY 1 NULLS FIRST", table_data, schema_name, info)


def _assert_table_fs(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    """Assert table is loaded to filesystem destination"""
    client: "FilesystemClient" = p._destination_client(schema_name)  # type: ignore[assignment]
    glob =  client.fs_client.glob(str(client.dataset_path.joinpath(f'{client.schema.name}.{table_name}.*')))
    assert len(glob) == 1
    assert client.fs_client.isfile(glob[0])
    # TODO: may verify that filesize matches load package size
    assert client.fs_client.size(glob[0]) > 0


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
