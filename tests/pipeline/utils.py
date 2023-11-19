import posixpath
from typing import Any, Dict, List, Tuple
import pytest
import random
from os import environ

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


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(p.completed_at is not None for p in info.load_packages) is True
    # no failed jobs in any of the packages
    assert all(len(p.jobs["failed_jobs"]) == 0 for p in info.load_packages) is True


def json_case_path(name: str) -> str:
    return f"{PIPELINE_TEST_CASES_PATH}{name}.json"


def load_json_case(name: str) -> DictStrAny:
    with open(json_case_path(name), "rb") as f:
        return json.load(f)


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict"""

    # try sql, could be other destination though
    try:
        with p.sql_client() as c:
            qualified_names = [c.make_qualified_table_name(name) for name in table_names]
            query = "\nUNION ALL\n".join([f"SELECT '{name}' as name, COUNT(1) as c FROM {q_name}" for name, q_name in zip(table_names, qualified_names)])
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

def load_data_table_counts(p: dlt.Pipeline) -> DictStrAny:
    tables = [table["name"] for table in p.default_schema.data_tables()]
    return load_table_counts(p, *tables)


def assert_data_table_counts(p: dlt.Pipeline, expected_counts: DictStrAny) -> None:
    table_counts = load_data_table_counts(p)
    assert table_counts == expected_counts, f"Table counts do not match, expected {expected_counts}, got {table_counts}"


def load_file(path: str, file: str) -> Tuple[str, List[Dict[str, Any]]]:
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
    client: FilesystemClient = p.destination_client()  # type: ignore[assignment]
    result: Dict[str, Any] = {}
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



def load_tables_to_dicts(p: dlt.Pipeline, *table_names: str) -> Dict[str, List[Dict[str, Any]]]:

    # try sql, could be other destination though
    try:
        result = {}
        for table_name in table_names:
            table_rows = []
            columns = p.default_schema.get_table_columns(table_name).keys()
            query_columns = ",".join(columns)

            with p.sql_client() as c:
                f_q_table_name = c.make_qualified_table_name(table_name)
                query = f"SELECT {query_columns} FROM {f_q_table_name}"
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
