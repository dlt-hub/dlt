from typing import Any, Dict, List, Set, Callable, Sequence
import pytest
import random
from os import environ
import io
import os

import dlt
from dlt.common import json, sleep
from dlt.common.configuration.utils import auto_cast
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.utils import get_table_format
from dlt.common.typing import DictStrAny
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.destinations.fs_client import FSClientBase
from dlt.destinations.exceptions import DatabaseUndefinedRelation, DestinationUndefinedEntity

from dlt.common.schema.typing import TTableSchema


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
        r_state = dlt.current.resource_state()
        r_state.setdefault("🦚🦚🦚", "")
        r_state["🦚🦚🦚"] += "🦚"
        yield [{"peacock": [1, 2, 3], "🔑id": 1}]

    @dlt.resource(name="🦚WidePeacock", selected=False)
    def wide_peacock():
        yield [{"Peacock": [1, 2, 3]}]

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


@dlt.resource(table_name="users")
def users_materialize_table_schema():
    yield dlt.mark.with_hints(
        # this is a special empty item which will materialize table schema
        dlt.mark.materialize_table_schema(),
        # emit table schema with the item
        dlt.mark.make_hints(
            columns=[
                {"name": "id", "data_type": "bigint", "precision": 4, "nullable": False},
                {"name": "name", "data_type": "text", "nullable": False},
            ]
        ),
    )


#
# Utils for accessing data in pipelines
#


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    # make sure we can serialize
    info.asstr(verbosity=2)
    info.asdict()
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(p.completed_at is not None for p in info.load_packages) is True
    # no failed jobs in any of the packages
    assert all(len(p.jobs["failed_jobs"]) == 0 for p in info.load_packages) is True


#
# Load utils
#


def _is_sftp(p: dlt.Pipeline) -> bool:
    if not p.destination:
        return False
    return (
        p.destination.destination_name == "filesystem"
        and p.destination_client().config.protocol == "sftp"  # type: ignore[attr-defined]
    )


def _load_jsonl_file(client: FSClientBase, filepath) -> List[Dict[str, Any]]:
    """
    load jsonl files into items list, only needed for sftp destination tests
    """

    # check if this is a file we want to read
    if os.path.splitext(filepath)[1] not in [".jsonl"]:
        return []

    # load jsonl into list
    result: List[Dict[str, Any]] = []
    for line in client.read_text(filepath).split("\n"):
        if line:
            result.append(json.loads(line))
    return result


#
# Load table dicts
#
def _load_tables_to_dicts_fs(
    p: dlt.Pipeline, *table_names: str, schema_name: str = None
) -> Dict[str, List[Dict[str, Any]]]:
    """Load all files from fs client, only needed for sftp destination tests"""
    client = p._fs_client(schema_name=schema_name)
    result: Dict[str, List[Dict[str, Any]]] = {}

    for table_name in table_names:
        files = client.list_table_files(table_name)
        # simulate missing table
        if len(files) == 0:
            raise DestinationUndefinedEntity(f"Table {table_name} not found")
        for file in files:
            result[table_name] = result.get(table_name, []) + _load_jsonl_file(client, file)

    return result


def _load_tables_to_dicts_sql(
    p: dlt.Pipeline, *table_names: str, schema_name: str = None
) -> Dict[str, List[Dict[str, Any]]]:
    """Uses dataset to load full table into python dicts"""
    result: Dict[str, List[Dict[str, Any]]] = {}
    for table_name in table_names:
        relation = p.dataset(schema=schema_name)[table_name]
        columns = list(relation.columns_schema.keys())
        for row in relation.fetchall():
            result[table_name] = result.get(table_name, []) + [dict(zip(columns, row))]
    return result


def load_tables_to_dicts(
    p: dlt.Pipeline,
    *table_names: str,
    schema_name: str = None,
    exclude_system_cols: bool = False,
    sortkey: str = None,
) -> Dict[str, List[Dict[str, Any]]]:
    def _exclude_system_cols(dict_: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in dict_.items() if not k.startswith("_dlt")}

    def _sort_list_of_dicts(list_: List[Dict[str, Any]], sortkey: str) -> List[Dict[str, Any]]:
        """Sort list of dictionaries by dictionary key."""
        return sorted(list_, key=lambda d: d[sortkey])

    # filesystem with sftp requires a fallback
    if _is_sftp(p):
        result = _load_tables_to_dicts_fs(p, *table_names, schema_name=schema_name)
    else:
        result = _load_tables_to_dicts_sql(p, *table_names, schema_name=schema_name)

    if exclude_system_cols:
        result = {k: [_exclude_system_cols(d) for d in v] for k, v in result.items()}
    if sortkey is not None:
        result = {k: _sort_list_of_dicts(v, sortkey) for k, v in result.items()}
    return result


def assert_records_as_set(actual: List[Dict[str, Any]], expected: List[Dict[str, Any]]) -> None:
    """Compares two lists of dicts regardless of order"""
    actual_set = set(frozenset(dict_.items()) for dict_ in actual)
    expected_set = set(frozenset(dict_.items()) for dict_ in expected)
    assert actual_set == expected_set


def assert_only_table_columns(
    p: dlt.Pipeline, table_name: str, expected_columns: Sequence[str], schema_name: str = None
) -> None:
    """Table has all and only the expected columns (excluding _dlt columns)"""
    rows = load_tables_to_dicts(p, table_name, schema_name=schema_name)[table_name]
    assert rows, f"Table {table_name} is empty"
    # Ignore _dlt columns
    columns = set(col for col in rows[0].keys() if not col.startswith("_dlt"))
    assert columns == set(expected_columns)


#
# Load table counts
#

def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict, if no table names are given, all data tables are counted"""
    if not table_names:
        table_names = [table["name"] for table in p.default_schema.data_tables()]  # type: ignore[assignment]
        
    # filesystem with sftp requires a fallback
    if _is_sftp(p):
        file_tables = _load_tables_to_dicts_fs(p, *table_names)
        return {table_name: len(items) for table_name, items in file_tables.items()}
    
    # otherwise we can use the dataset row counts
    counts = p.dataset().row_counts(table_names=list(table_names)).fetchall()
    return {row[0]: row[1] for row in counts}


def assert_table_counts(p: dlt.Pipeline, expected_counts: DictStrAny, *table_names: str) -> None:
    table_counts = load_table_counts(p, *table_names)
    assert (
        table_counts == expected_counts
    ), f"Table counts do not match, expected {expected_counts}, got {table_counts}"


def table_exists(p: dlt.Pipeline, table_name: str, schema_name: str = None) -> bool:
    """Returns True if table exists in the destination database/filesystem"""
    try:
        load_table_counts(p, table_name)
        return True
    except DestinationUndefinedEntity:
        return False


def assert_table(
    p: dlt.Pipeline,
    table_name: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    # for sftp we can just check if the table exists
    if _is_sftp(p):
        assert table_exists(p, table_name, schema_name)
        return

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


def assert_schema_on_data(
    table_schema: TTableSchema,
    rows: List[Dict[str, Any]],
    requires_nulls: bool,
    check_nested: bool,
) -> None:
    """Asserts that `rows` conform to `table_schema`. Fields and their order must conform to columns. Null values and
    python data types are checked.
    """
    table_columns = table_schema["columns"]
    columns_with_nulls: Set[str] = set()
    for row in rows:
        # check columns
        assert set(table_schema["columns"].keys()) == set(row.keys())
        # check column order
        assert list(table_schema["columns"].keys()) == list(row.keys())
        # check data types
        for key, value in row.items():
            print(key)
            print(value)
            if value is None:
                assert table_columns[key][
                    "nullable"
                ], f"column {key} must be nullable: value is None"
                # next value. we cannot validate data type
                columns_with_nulls.add(key)
                continue
            expected_dt = table_columns[key]["data_type"]
            # allow json strings
            if expected_dt == "json":
                if check_nested:
                    # NOTE: we expect a dict or a list here. simple types of null will fail the test
                    value = json.loads(value)
                else:
                    # skip checking nested types
                    continue
            actual_dt = py_type_to_sc_type(type(value))
            assert actual_dt == expected_dt

    if requires_nulls:
        # make sure that all nullable columns in table received nulls
        assert (
            set(col["name"] for col in table_columns.values() if col["nullable"])
            == columns_with_nulls
        ), "Some columns didn't receive NULLs which is required"
