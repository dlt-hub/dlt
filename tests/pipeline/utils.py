import posixpath
from typing import Any, Dict, List, Tuple, Callable, Sequence
import pytest
import random
from os import environ
import io

import dlt
from dlt.common import json, sleep
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import LOADS_TABLE_NAME
from dlt.common.typing import DictStrAny
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.pipeline.exceptions import SqlClientNotAvailable

from tests.utils import TEST_STORAGE_ROOT

PIPELINE_TEST_CASES_PATH = "./tests/pipeline/cases/"


@pytest.fixture(autouse=True)
def drop_dataset_from_env() -> None:
    if "DATASET_NAME" in environ:
        del environ["DATASET_NAME"]


def json_case_path(name: str) -> str:
    return f"{PIPELINE_TEST_CASES_PATH}{name}.json"


def load_json_case(name: str) -> DictStrAny:
    with open(json_case_path(name), "rb") as f:
        return json.load(f)


@dlt.source
def airtable_emojis():
    @dlt.resource(name="📆 Schedule")
    def schedule():
        yield [1, 2, 3]

    @dlt.resource(name="💰Budget", primary_key=("🔑book_id", "asset_id"))
    def budget():
        # return empty
        yield

    @dlt.resource(name="🦚Peacock", selected=False, primary_key="🔑id")
    def peacock():
        dlt.current.resource_state()["🦚🦚🦚"] = "🦚"
        yield [{"peacock": [1, 2, 3], "🔑id": 1}]

    @dlt.resource(name="🦚WidePeacock", selected=False)
    def wide_peacock():
        yield [{"peacock": [1, 2, 3]}]

    return budget, schedule, peacock, wide_peacock


def run_deferred(iters):
    @dlt.defer
    def item(n):
        sleep(random.random() / 2)
        return n

    for n in range(iters):
        yield item(n)


@dlt.source
def many_delayed(many, iters):
    for n in range(many):
        yield dlt.resource(run_deferred(iters), name="resource_" + str(n))


#
# Utils for accessing data in pipelines
#


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(p.completed_at is not None for p in info.load_packages) is True
    # no failed jobs in any of the packages
    assert all(len(p.jobs["failed_jobs"]) == 0 for p in info.load_packages) is True


#
# Load utils
#


def _is_filesystem(p: dlt.Pipeline) -> bool:
    if not p.destination:
        return False
    return p.destination.destination_name == "filesystem"


def _load_file(fs_client, path: str, file: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    util function to load a filesystem destination file and return parsed content
    values may not be cast to the right type, especially for insert_values, please
    make sure to do conversions and casting if needed in your tests
    """
    result: List[Dict[str, Any]] = []

    # check if this is a file we want to read
    file_name_items = file.split(".")
    ext = file_name_items[-1]
    if ext not in ["jsonl", "insert_values", "parquet"]:
        return "skip", []

    # table name will be last element of path
    table_name = path.split("/")[-1]
    full_path = posixpath.join(path, file)

    # load jsonl
    if ext == "jsonl":
        file_text = fs_client.read_text(full_path)
        for line in file_text.split("\n"):
            if line:
                result.append(json.loads(line))

    # load insert_values (this is a bit volatile if the exact format of the source file changes)
    elif ext == "insert_values":
        file_text = fs_client.read_text(full_path)
        lines = file_text.split("\n")
        cols = lines[0][15:-2].split(",")
        for line in lines[2:]:
            if line:
                values = line[1:-3].split(",")
                result.append(dict(zip(cols, values)))

    # load parquet
    elif ext == "parquet":
        import pyarrow.parquet as pq

        file_bytes = fs_client.read_bytes(full_path)
        table = pq.read_table(io.BytesIO(file_bytes))
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


#
# Load table dicts
#
def _load_tables_to_dicts_fs(p: dlt.Pipeline, *table_names: str) -> Dict[str, List[Dict[str, Any]]]:
    """For now this will expect the standard layout in the filesystem destination, if changed the results will not be correct"""
    client: FilesystemClient = p.destination_client()  # type: ignore[assignment]
    result: Dict[str, Any] = {}
    for basedir, _dirs, files in client.fs_client.walk(
        client.dataset_path, detail=False, refresh=True
    ):
        for file in files:
            table_name, items = _load_file(client.fs_client, basedir, file)
            if table_name not in table_names:
                continue
            if table_name in result:
                result[table_name] = result[table_name] + items
            else:
                result[table_name] = items
    return result


def _load_tables_to_dicts_sql(
    p: dlt.Pipeline, *table_names: str
) -> Dict[str, List[Dict[str, Any]]]:
    result = {}
    for table_name in table_names:
        table_rows = []
        columns = p.default_schema.get_table_columns(table_name).keys()
        query_columns = ",".join(map(p.sql_client().capabilities.escape_identifier, columns))

        with p.sql_client() as c:
            query_columns = ",".join(map(c.escape_column_name, columns))
            f_q_table_name = c.make_qualified_table_name(table_name)
            query = f"SELECT {query_columns} FROM {f_q_table_name}"
            with c.execute_query(query) as cur:
                for row in list(cur.fetchall()):
                    table_rows.append(dict(zip(columns, row)))
        result[table_name] = table_rows
    return result


def load_tables_to_dicts(p: dlt.Pipeline, *table_names: str) -> Dict[str, List[Dict[str, Any]]]:
    func = _load_tables_to_dicts_fs if _is_filesystem(p) else _load_tables_to_dicts_sql
    return func(p, *table_names)


#
# Load table counts
#
def _load_table_counts_fs(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    file_tables = _load_tables_to_dicts_fs(p, *table_names)
    result = {}
    for table_name, items in file_tables.items():
        result[table_name] = len(items)
    return result


def _load_table_counts_sql(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    with p.sql_client() as c:
        qualified_names = [c.make_qualified_table_name(name) for name in table_names]
        query = "\nUNION ALL\n".join(
            [
                f"SELECT '{name}' as name, COUNT(1) as c FROM {q_name}"
                for name, q_name in zip(table_names, qualified_names)
            ]
        )
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict"""
    func = _load_table_counts_fs if _is_filesystem(p) else _load_table_counts_sql
    return func(p, *table_names)


def load_data_table_counts(p: dlt.Pipeline) -> DictStrAny:
    tables = [table["name"] for table in p.default_schema.data_tables()]
    return load_table_counts(p, *tables)


def assert_data_table_counts(p: dlt.Pipeline, expected_counts: DictStrAny) -> None:
    table_counts = load_data_table_counts(p)
    assert (
        table_counts == expected_counts
    ), f"Table counts do not match, expected {expected_counts}, got {table_counts}"


#
# TODO: migrate to be able to do full assertions on filesystem too, should be possible
#

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

def assert_table(
    p: dlt.Pipeline,
    table_name: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    func = _assert_table_fs if _is_filesystem(p) else _assert_table_sql
    func(p, table_name, table_data, schema_name, info)



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
