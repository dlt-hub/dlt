from typing import Any, Iterator, List, Sequence, TYPE_CHECKING, Callable
import pytest

import dlt
from dlt.common.destination.reference import WithStagingDataset

from dlt.common.configuration.container import Container
from dlt.common.pipeline import LoadInfo, PipelineContext

from tests.pipeline.utils import (
    load_table_counts,
    load_data_table_counts,
    assert_data_table_counts,
    load_file,
    load_files,
    load_tables_to_dicts,
    load_table_distinct_counts,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs

if TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


@pytest.fixture(autouse=True)
def drop_pipeline(request) -> Iterator[None]:
    yield
    if "no_load" in request.keywords:
        return
    drop_active_pipeline_data()


def drop_active_pipeline_data() -> None:
    """Drops all the datasets for currently active pipeline, wipes the working folder and then deactivated it."""
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()

        def _drop_dataset(schema_name: str) -> None:
            with p.destination_client(schema_name) as client:
                try:
                    client.drop_storage()
                    print("dropped")
                except Exception as exc:
                    print(exc)
                if isinstance(client, WithStagingDataset):
                    with client.with_staging_dataset():
                        try:
                            client.drop_storage()
                            print("staging dropped")
                        except Exception as exc:
                            print(exc)

        # drop_func = _drop_dataset_fs if _is_filesystem(p) else _drop_dataset_sql
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


def _is_filesystem(p: dlt.Pipeline) -> bool:
    if not p.destination:
        return False
    return p.destination.destination_name == "filesystem"


def assert_table(
    p: dlt.Pipeline,
    table_name: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    func = _assert_table_fs if _is_filesystem(p) else _assert_table_sql
    func(p, table_name, table_data, schema_name, info)


def _assert_table_sql(
    p: dlt.Pipeline,
    table_name: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    with p.sql_client(schema_name=schema_name) as c:
        table_name = c.make_qualified_table_name(table_name)
    # Implement NULLS FIRST sort in python
    assert_query_data(
        p,
        f"SELECT * FROM {table_name} ORDER BY 1",
        table_data,
        schema_name,
        info,
        sort_key=lambda row: row[0] is not None,
    )


def _assert_table_fs(
    p: dlt.Pipeline,
    table_name: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    """Assert table is loaded to filesystem destination"""
    client: FilesystemClient = p.destination_client(schema_name)  # type: ignore[assignment]
    # get table directory
    table_dir = list(client._get_table_dirs([table_name]))[0]
    # assumes that each table has a folder
    files = client.fs_client.ls(table_dir, detail=False, refresh=True)
    # glob =  client.fs_client.glob(posixpath.join(client.dataset_path, f'{client.table_prefix_layout.format(schema_name=schema_name, table_name=table_name)}/*'))
    assert len(files) >= 1
    assert client.fs_client.isfile(files[0])
    # TODO: may verify that filesize matches load package size
    assert client.fs_client.size(files[0]) > 0


def select_data(p: dlt.Pipeline, sql: str, schema_name: str = None) -> List[Sequence[Any]]:
    with p.sql_client(schema_name=schema_name) as c:
        with c.execute_query(sql) as cur:
            return list(cur.fetchall())


def assert_query_data(
    p: dlt.Pipeline,
    sql: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
    sort_key: Callable[[Any], Any] = None,
) -> None:
    """Asserts that query selecting single column of values matches `table_data`. If `info` is provided, second column must contain one of load_ids in `info`

    Args:
        sort_key: Optional sort key function to sort the query result before comparing

    """
    rows = select_data(p, sql, schema_name)
    assert len(rows) == len(table_data)
    if sort_key is not None:
        rows = sorted(rows, key=sort_key)
    for row, d in zip(rows, table_data):
        row = list(row)
        # first element comes from the data
        assert row[0] == d
        # the second is load id
        if info:
            assert row[1] in info.loads_ids
