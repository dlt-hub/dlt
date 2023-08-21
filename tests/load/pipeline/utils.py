import posixpath, os
from typing import Any, Iterator, List, Sequence, TYPE_CHECKING, Optional, Tuple, Dict
import pytest

import dlt
from dlt.pipeline.pipeline import Pipeline

from dlt.common import json
from dlt.common.configuration.container import Container
from dlt.common.pipeline import LoadInfo, PipelineContext
from dlt.common.typing import DictStrAny
from dlt.pipeline.exceptions import SqlClientNotAvailable
from dlt.common.schema.typing import LOADS_TABLE_NAME
from dlt.common.destination.reference import WithStagingDataset

from tests.load.utils import DestinationTestConfiguration, destinations_configs

if TYPE_CHECKING:
    from dlt.destinations.filesystem.filesystem import FilesystemClient

@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def drop_active_pipeline_data() -> None:
    """Drops all the datasets for currently active pipeline, wipes the working folder and then deactivated it."""
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()

        def _drop_dataset_fs(_: str) -> None:
            try:
                client: "FilesystemClient" = p._destination_client()  # type: ignore[assignment]
                client.fs_client.rm(client.dataset_path, recursive=True)
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
                    if isinstance(c, WithStagingDataset):
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
    if not p.destination:
        return False
    return p.destination.__name__.rsplit('.', 1)[-1] == 'filesystem'


def assert_table(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    func = _assert_table_fs if _is_filesystem(p) else _assert_table_sql
    func(p, table_name, table_data, schema_name, info)


def _assert_table_sql(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    assert_query_data(p, f"SELECT * FROM {table_name} ORDER BY 1 NULLS FIRST", table_data, schema_name, info)


def _assert_table_fs(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    """Assert table is loaded to filesystem destination"""
    client: FilesystemClient = p._destination_client(schema_name)  # type: ignore[assignment]
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


def load_file(path: str, file: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    util function to load a filesystem destination file and return parsed content
    values may not be cast to the right type, especially for insert_values, please
    make sure to do conversions and casting if needed in your tests
    """
    result: List[dict, str] = []

    # check if this is a file we want to read
    file_name_items = file.split(".")
    ext = file_name_items[-1]
    if ext not in ["jsonl", "insert_values", "parquet"]:
        return "skip", []

    # table name will be last element of path
    table_name = path.split("/")[-1]

    # skip loads table
    if table_name == "_dlt_loads":
        return table_name, []

    full_path = posixpath.join(path, file)

    # load jsonl
    if ext == "jsonl":
        with open(full_path, "rU", encoding="utf-8") as f:
            for line in f:
                result.append(json.loads(line))

    # load insert_values (this is a bit volatile if the exact format of the source file changes)
    elif ext == "insert_values":
        with open(full_path, "rU", encoding="utf-8") as f:
            lines = f.readlines()
            # extract col names
            cols = lines[0][15:-2].split(",")
            for line in lines[2:]:
                values = line[1:-3].split(",")
                result.append(dict(zip(cols, values)))

    # load parquet
    elif ext == "parquet":
        import pyarrow.parquet as pq
        with open(full_path, "rb") as f:
            table = pq.read_table(f)
            cols = table.column_names
            count = 0
            for column in table:
                column_name = cols[count]
                item_count = 0
                for item in column.to_pylist():
                    if len(result) <= item_count:
                        result.append({column_name: item})
                    else:
                        result[item_count][column_name] = item
                    item_count += 1
                count += 1

    return table_name, result


def load_files(p: dlt.Pipeline, *table_names: str) -> Dict[str, List[Dict[str, Any]]]:
    """For now this will expect the standard layout in the filesystem destination, if changed the results will not be correct"""
    client: FilesystemClient = p._destination_client()  # type: ignore[assignment]
    result = {}
    for basedir, _dirs, files  in client.fs_client.walk(client.dataset_path, detail=False, refresh=True):
        for file in files:
            table_name, items = load_file(basedir, file)
            if table_name not in table_names:
                continue
            if table_name in result:
                result[table_name] = result[table_name] + items
            else:
                result[table_name] = items

            # loads file is special case
            if LOADS_TABLE_NAME in table_names and file.find(".{LOADS_TABLE_NAME}."):
                result[LOADS_TABLE_NAME] = []

    return result


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict"""

    # try sql, could be other destination though
    try:
        query = "\nUNION ALL\n".join([f"SELECT '{name}' as name, COUNT(1) as c FROM {name}" for name in table_names])
        with p.sql_client() as c:
            with c.execute_query(query) as cur:
                rows = list(cur.fetchall())
                return {r[0]: r[1] for r in rows}
    except SqlClientNotAvailable:
        pass

    # try filesystem
    file_tables = load_files(p, *table_names)
    result = {}
    for table_name, items in file_tables.items():
        result[table_name] = len(items)
    return result


def load_tables_to_dicts(p: dlt.Pipeline, *table_names: str) -> Dict[str, List[Dict[str, Any]]]:

    # try sql, could be other destination though
    try:
        result = {}
        for table_name in table_names:
            table_rows = []
            columns = p.default_schema.get_table_columns(table_name).keys()
            query_columns = ",".join(columns)

            query = f"SELECT {query_columns} FROM {table_name}"
            with p.sql_client() as c:
                with c.execute_query(query) as cur:
                    for row in list(cur.fetchall()):
                        table_rows.append(dict(zip(columns, row)))
            result[table_name] = table_rows
        return result

    except SqlClientNotAvailable:
        pass

    # try files
    return load_files(p, *table_names)

def load_table_distinct_counts(p: dlt.Pipeline, distinct_column: str, *table_names: str) -> DictStrAny:
    """Returns counts of distinct values for column `distinct_column` for `table_names` as dict"""
    query = "\nUNION ALL\n".join([f"SELECT '{name}' as name, COUNT(DISTINCT {distinct_column}) as c FROM {name}" for name in table_names])
    with p.sql_client() as c:
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}
