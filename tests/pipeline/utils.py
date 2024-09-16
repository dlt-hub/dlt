from typing import Any, Dict, List, Set, Callable, Sequence
import pytest
import random
from os import environ
import io

import dlt
from dlt.common import json, sleep
from dlt.common.configuration.utils import auto_cast
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.utils import get_table_format
from dlt.common.typing import DictStrAny
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.destinations.fs_client import FSClientBase
from dlt.destinations.exceptions import DatabaseUndefinedRelation

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


def _is_filesystem(p: dlt.Pipeline) -> bool:
    if not p.destination:
        return False
    return p.destination.destination_name == "filesystem"


def _load_file(client: FSClientBase, filepath) -> List[Dict[str, Any]]:
    """
    util function to load a filesystem destination file and return parsed content
    values may not be cast to the right type, especially for insert_values, please
    make sure to do conversions and casting if needed in your tests
    """
    result: List[Dict[str, Any]] = []

    # check if this is a file we want to read
    file_name_items = filepath.split(".")
    ext = file_name_items[-1]
    if ext not in ["jsonl", "insert_values", "parquet"]:
        return []

    # load jsonl
    if ext == "jsonl":
        file_text = client.read_text(filepath)
        for line in file_text.split("\n"):
            if line:
                result.append(json.loads(line))

    # load insert_values (this is a bit volatile if the exact format of the source file changes)
    elif ext == "insert_values":
        file_text = client.read_text(filepath)
        lines = file_text.split("\n")
        cols = lines[0][15:-2].split(",")
        for line in lines[2:]:
            if line:
                values = map(auto_cast, line[1:-3].split(","))
                result.append(dict(zip(cols, values)))

    # load parquet
    elif ext == "parquet":
        import pyarrow.parquet as pq

        file_bytes = client.read_bytes(filepath)
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

    return result


#
# Load table dicts
#


def _load_tables_to_dicts_fs(
    p: dlt.Pipeline, *table_names: str, schema_name: str = None
) -> Dict[str, List[Dict[str, Any]]]:
    """For now this will expect the standard layout in the filesystem destination, if changed the results will not be correct"""
    client = p._fs_client(schema_name=schema_name)
    assert isinstance(client, FilesystemClient)

    result: Dict[str, Any] = {}

    delta_table_names = [
        table_name
        for table_name in table_names
        if get_table_format(client.schema.tables, table_name) == "delta"
    ]
    if len(delta_table_names) > 0:
        from dlt.common.libs.deltalake import get_delta_tables

        delta_tables = get_delta_tables(p, *table_names, schema_name=schema_name)

    for table_name in table_names:
        if table_name in client.schema.data_table_names() and table_name in delta_table_names:
            dt = delta_tables[table_name]
            result[table_name] = dt.to_pyarrow_table().to_pylist()
        else:
            table_files = client.list_table_files(table_name)
            for file in table_files:
                items = _load_file(client, file)
                if table_name in result:
                    result[table_name] = result[table_name] + items
                else:
                    result[table_name] = items
    return result


def _load_tables_to_dicts_sql(
    p: dlt.Pipeline, *table_names: str, schema_name: str = None
) -> Dict[str, List[Dict[str, Any]]]:
    result = {}
    schema = p.default_schema if not schema_name else p.schemas[schema_name]
    for table_name in table_names:
        table_rows = []
        columns = schema.get_table_columns(table_name).keys()
        query_columns = ",".join(map(p.sql_client().escape_column_name, columns))

        with p.sql_client() as c:
            query_columns = ",".join(map(c.escape_column_name, columns))
            f_q_table_name = c.make_qualified_table_name(table_name)
            query = f"SELECT {query_columns} FROM {f_q_table_name}"
            with c.execute_query(query) as cur:
                for row in list(cur.fetchall()):
                    table_rows.append(dict(zip(columns, row)))
        result[table_name] = table_rows
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

    if _is_filesystem(p):
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


def table_exists(p: dlt.Pipeline, table_name: str, schema_name: str = None) -> bool:
    """Returns True if table exists in the destination database/filesystem"""
    if _is_filesystem(p):
        client = p._fs_client(schema_name=schema_name)
        files = client.list_table_files(table_name)
        return not not files

    with p.sql_client(schema_name=schema_name) as c:
        try:
            qual_table_name = c.make_qualified_table_name(table_name)
            c.execute_sql(f"SELECT 1 FROM {qual_table_name} LIMIT 1")
            return True
        except DatabaseUndefinedRelation:
            return False


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
    client = p._fs_client()
    # assumes that each table has a folder
    files = client.list_table_files(table_name)
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


def load_table_distinct_counts(
    p: dlt.Pipeline, distinct_column: str, *table_names: str
) -> DictStrAny:
    """Returns counts of distinct values for column `distinct_column` for `table_names` as dict"""
    with p.sql_client() as c:
        query = "\nUNION ALL\n".join(
            [
                f"SELECT '{name}' as name, COUNT(DISTINCT {distinct_column}) as c FROM"
                f" {c.make_qualified_table_name(name)}"
                for name in table_names
            ]
        )

        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}
