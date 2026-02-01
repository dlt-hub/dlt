from typing import Any, Dict, List, Set, Sequence, Generator, cast
import pytest
import random
from os import environ
import io
import os
from collections import Counter
import dlt
from dlt.common import json, sleep
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import DictStrAny
from dlt.destinations.fs_client import FSClientBase
from dlt.destinations.exceptions import DestinationUndefinedEntity
from dlt.destinations.dataset.relation import ReadableDBAPIRelation
from dlt.common.schema.typing import TTableSchema


PIPELINE_TEST_CASES_PATH = "./tests/pipeline/cases/"


@pytest.fixture(autouse=True)
def drop_dataset_from_env() -> None:
    """Remove the ``DATASET_NAME`` environment variable before each test.

    This autouse fixture guarantees that tests start with a clean environment. Some
    pipelines derive the default destination dataset name from the environment
    variable ``DATASET_NAME`` – if it is left over from a previous test run the
    execution could pick up unexpected state. Clearing it here prevents such
    flakiness.
    """
    if "DATASET_NAME" in environ:
        del environ["DATASET_NAME"]


def json_case_path(name: str) -> str:
    """Return absolute path to a JSON test-case file residing in ``PIPELINE_TEST_CASES_PATH``.

    Args:
        name (str): Base file name without extension located in
            :pydata:`PIPELINE_TEST_CASES_PATH`.

    Returns:
        str: A string containing the full relative path to ``<name>.json``.
    """
    return f"{PIPELINE_TEST_CASES_PATH}{name}.json"


def load_json_case(name: str) -> DictStrAny:
    """Load and deserialize a JSON test-case file.

    Args:
        name (str): Base name of the JSON file located in
            :pydata:`PIPELINE_TEST_CASES_PATH` (without the ``.json`` suffix).

    Returns:
        DictStrAny: Parsed JSON structure represented as nested Python
        ``dict``/``list``/primitive types.
    """
    with open(json_case_path(name), "rb") as f:
        return json.load(f)


@dlt.source
def airtable_emojis():
    """Example source that produces four Airtable-like resources with emoji names.

    The source is intentionally whimsical to verify that pipelines behave
    correctly when table and column names contain non-ASCII characters.
    """

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


def run_deferred(iters: int) -> Generator[Any, None, None]:
    """Yield :pyfunc:`dlt.defer`-ed tasks that resolve to their input number.

    Args:
        iters (int): Number of deferred tasks to create.

    Yields:
        Any: A deferred object that sleeps for a random amount of time
        smaller than half a second and finally evaluates to the iteration index.
    """

    @dlt.defer
    def item(n):
        sleep(random.random() / 2)
        return n

    for n in range(iters):
        yield item(n)


@dlt.source
def many_delayed(many: int, iters: int) -> Generator[Any, None, None]:
    """Produce *many* resources that each emit *iters* deferred items.

    Args:
        many (int): Number of resources to create.
        iters (int): Number of deferred items produced by every resource.

    Yields:
        Any: Dynamically created resources named ``resource_<n>``.
    """
    for n in range(many):
        yield dlt.resource(run_deferred(iters), name="resource_" + str(n))


@dlt.resource(table_name="users")
def users_materialize_table_schema():
    """Emit a special item that forces materialisation of the *users* table.

    The function demonstrates how to pre-define a table schema without loading
    any real data. By yielding a ``dlt.mark.materialize_table_schema()`` item
    together with explicit column definitions, downstream tests can assert that
    the destination created the correct structure.

    Yields:
        Any: The marker object recognised by DLT to create/alter the table.
    """
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
    """Assert that all load packages completed successfully.

    Args:
        info (LoadInfo): The load information instance returned by DLT after
            executing ``pipeline.run()`` or ``pipeline.load()``.
        expected_load_packages (int, optional): How many load packages should be
            present in *info*. Defaults to 1.

    Raises:
        AssertionError: If the amount of load packages or any job status does
            not match the expectations.
    """
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
    """Check whether *p* is configured to use the SFTP variant of the filesystem destination.

    Args:
        p (dlt.Pipeline): Pipeline instance under test.

    Returns:
        bool: ``True`` if ``p.destination`` is "filesystem" *and* its protocol
        equals "sftp", otherwise ``False``.
    """
    if not p.destination:
        return False
    return (
        p.destination.destination_name == "filesystem"
        and p.destination_client().config.protocol == "sftp"  # type: ignore[attr-defined]
    )


def _is_abfss(p: dlt.Pipeline) -> bool:
    """Check whether *p* is configured to use the ABFS variant of the filesystem destination.

    Args:
        p (dlt.Pipeline): Pipeline instance under test.

    Returns:
        bool: ``True`` if ``p.destination`` is "filesystem" *and* its protocol
        equals "abfss", otherwise ``False``.
    """
    if not p.destination:
        return False
    return (
        p.destination.destination_name == "filesystem"
        and p.destination_client().config.protocol == "abfss"  # type: ignore[attr-defined]
    )


def _load_jsonl_file(client: FSClientBase, filepath: str) -> List[Dict[str, Any]]:
    """Read a ``.jsonl`` file from a filesystem destination into a list of dictionaries.

    The helper is used exclusively by tests exercising the SFTP-backed
    filesystem destination.

    Args:
        client (FSClientBase): Filesystem client capable of reading ``filepath``.
        filepath (str): The path of the file on the destination.

    Returns:
        List[Dict[str, Any]]: A list with one dictionary per JSON line. If
        *filepath* does not have a ``.jsonl`` extension an empty list is
        returned.
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
    """Load full table contents from the SFTP filesystem destination.

    Args:
        p (dlt.Pipeline): Pipeline whose destination should be inspected.
        *table_names (str): One or more table names to fetch. When the list is
            empty the caller should have determined the required tables earlier.
        schema_name (str, optional): Name of the schema to load from. Defaults
            to the pipeline's default schema.

    Returns:
        Dict[str, List[Dict[str, Any]]]: Mapping of table name to list of row
        dictionaries.

    Raises:
        DestinationUndefinedEntity: When any of the *table_names* cannot be
            found on the destination.
    """
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
    """Load full table contents via the destination's SQL interface.

    Args:
        p (dlt.Pipeline): Active pipeline whose dataset should be queried.
        *table_names (str): One or more table names to read.
        schema_name (str, optional): Override for the schema name.

    Returns:
        Dict[str, List[Dict[str, Any]]]: All rows from the requested tables
        converted into dictionaries keyed by column names.
    """
    result: Dict[str, List[Dict[str, Any]]] = {}
    for table_name in table_names:
        relation = p.dataset(schema=schema_name)[table_name]
        for row in relation.fetchall():
            result[table_name] = result.get(table_name, []) + [dict(zip(relation.columns, row))]
    return result


def load_tables_to_dicts(
    p: dlt.Pipeline,
    *table_names: str,
    schema_name: str = None,
    exclude_system_cols: bool = False,
    sortkey: str = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Return full table data as Python dictionaries.

    The helper abstracts over filesystem and SQL destinations and supports a
    variety of convenience tweaks such as column exclusion and stable sorting.

    Args:
        p (dlt.Pipeline): Pipeline whose destination should be accessed.
        *table_names (str): Optional list of table names to load. If omitted all
            data tables from the pipeline's default schema are selected.
        schema_name (str, optional): Explicit schema override. Defaults to the
            pipeline's default schema.
        exclude_system_cols (bool, optional): When ``True`` remove columns that
            start with the ``_dlt`` prefix from the returned data. Defaults to
            ``False``.
        sortkey (str, optional): Name of a column to lexicographically sort the
            resulting list of dictionaries inside each table.

    Returns:
        Dict[str, List[Dict[str, Any]]]: Table data keyed by table name.
    """

    if not table_names:
        table_names = [t["name"] for t in p.default_schema.data_tables()]  # type: ignore[assignment]

    # filesystem with sftp requires a fallback
    if _is_sftp(p):
        result = _load_tables_to_dicts_fs(p, *table_names, schema_name=schema_name)
    else:
        result = _load_tables_to_dicts_sql(p, *table_names, schema_name=schema_name)

    # exclude and sort
    def _exclude_system_cols(dict_: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in dict_.items() if not k.startswith("_dlt")}

    def _sort_list_of_dicts(list_: List[Dict[str, Any]], sortkey: str) -> List[Dict[str, Any]]:
        """Sort list of dictionaries by dictionary key."""
        return sorted(list_, key=lambda d: d[sortkey])

    if exclude_system_cols:
        result = {k: [_exclude_system_cols(d) for d in v] for k, v in result.items()}
    if sortkey is not None:
        result = {k: _sort_list_of_dicts(v, sortkey) for k, v in result.items()}

    # done
    return result


def assert_records_as_set(actual: List[Dict[str, Any]], expected: List[Dict[str, Any]]) -> None:
    """Assert that *actual* and *expected* contain the same records irrespective of order.

    Args:
        actual (List[Dict[str, Any]]): Records retrieved from the destination.
        expected (List[Dict[str, Any]]): Reference records defined in the test.

    Raises:
        AssertionError: When the two sets of records differ.
    """

    def dict_to_tuple(d):
        # Sort items to ensure consistent ordering
        return tuple(sorted(d.items()))

    counter1 = Counter(dict_to_tuple(d) for d in actual)
    counter2 = Counter(dict_to_tuple(d) for d in expected)

    assert counter1 == counter2


def assert_only_table_columns(
    p: dlt.Pipeline, table_name: str, expected_columns: Sequence[str], schema_name: str = None
) -> None:
    """Assert that a table contains exactly *expected_columns* (ignoring ``_dlt`` cols).

    Args:
        p (dlt.Pipeline): Pipeline whose destination should be inspected.
        table_name (str): Name of the table to validate.
        expected_columns (Sequence[str]): List of expected column names.
        schema_name (str, optional): Schema name override.

    Raises:
        AssertionError: If the table is empty or its columns differ from
            *expected_columns*.
    """
    rows = load_tables_to_dicts(p, table_name, schema_name=schema_name)[table_name]
    assert rows, f"Table {table_name} is empty"
    # Ignore _dlt columns
    columns = set(col for col in rows[0].keys() if not col.startswith("_dlt"))
    assert columns == set(expected_columns)


#
# Load table counts
#


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Return number of rows for each requested table name.

    Args:
        p (dlt.Pipeline): Pipeline instance.
        *table_names (str): Optional list of table names. When omitted, counts
            for all data tables in the default schema are returned.

    Returns:
        DictStrAny: Mapping of table name to integer row count.
    """
    if not table_names:
        table_names = [table["name"] for table in p.default_schema.data_tables()]  # type: ignore[assignment]

    # filesystem with sftp requires a fallback
    if _is_sftp(p):
        file_tables = _load_tables_to_dicts_fs(p, *table_names)
        return {table_name: len(items) for table_name, items in file_tables.items()}

    # NOTE: filesystem with abfss and no table format requires a fallback where we get each table count individually
    # this seems to be a bug in duckdb abfss and might be resolved in a future version.
    if _is_abfss(p):
        table_counts = {}
        for table in table_names:
            table_counts[table] = p.dataset().row_counts(table_names=[table]).fetchall()[0][1]
        return table_counts

    # otherwise we can use the dataset row counts
    counts = p.dataset().row_counts(table_names=list(table_names)).fetchall()
    return {row[0]: row[1] for row in counts}


def assert_empty_tables(p: dlt.Pipeline, *table_names: str) -> None:
    """Assert that the given tables are empty (or non-existent).

    The helper treats a ``DestinationUndefinedEntity`` exception raised by the
    destination the same as an empty table which aligns the behaviour between
    filesystem and SQL destinations.

    Args:
        p (dlt.Pipeline): Pipeline to inspect.
        *table_names (str): Table names that should not contain any rows.
    """
    for table in table_names:
        try:
            assert load_table_counts(p, table) == {table: 0}
        except DestinationUndefinedEntity:
            pass


def assert_table_counts(p: dlt.Pipeline, expected_counts: DictStrAny, *table_names: str) -> None:
    """Assert that *table_names* contain the expected number of rows.

    Args:
        p (dlt.Pipeline): Pipeline under test.
        expected_counts (DictStrAny): Mapping of table name to expected row
            count.
        *table_names (str): Optional subset of tables to compare. When omitted
            the keys of *expected_counts* are used.
    """
    table_counts = load_table_counts(p, *table_names)
    assert (
        table_counts == expected_counts
    ), f"Table counts do not match, expected {expected_counts}, got {table_counts}"


def table_exists(p: dlt.Pipeline, table_name: str, schema_name: str = None) -> bool:
    """Return whether *table_name* exists in the destination.

    Args:
        p (dlt.Pipeline): Pipeline instance.
        table_name (str): Name of the table to check.
        schema_name (str, optional): Schema override.

    Returns:
        bool: ``True`` if the table exists, ``False`` otherwise.
    """
    try:
        load_table_counts(p, table_name)
        return True
    except DestinationUndefinedEntity:
        return False


# NOTE: replace with direct dataset access in the code?
def select_data(
    p: dlt.Pipeline,
    sql: str,
    schema_name: str = None,
    dataset_name: str = None,
    _execute_raw_query: bool = False,
) -> List[Sequence[Any]]:
    """Execute *sql* against the pipeline's dataset and return all rows.

    Args:
        p (dlt.Pipeline): Pipeline instance.
        sql (str): Raw SQL query string to execute.
        schema_name (str, optional): Schema name override.
        dataset_name (str, optional): Temporary override for the dataset name –
            useful when the test needs to query a different dataset created by
            the same pipeline.
        _execute_raw_query (bool, optional): Whether to run the query as (raw) is or perform query normalization and lineage. Experimental.

    Returns:
        List[Sequence[Any]]: All rows returned by the query.
    """
    dataset = p.dataset(schema=schema_name)
    # TODO: fix multiple dataset layout and remove hack
    if dataset_name:
        dataset._dataset_name = dataset_name
    return list(dataset(sql, _execute_raw_query=_execute_raw_query).fetchall())


def assert_table_column(
    p: dlt.Pipeline,
    table_name: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    """Assert that the first column of *table_name* equals *table_data*.

    When *info* is provided, the helper additionally verifies that the second
    column contains a load-ID that belongs to one of the packages referenced in
    *info*.

    Args:
        p (dlt.Pipeline): Pipeline instance.
        table_name (str): Name of the table to validate.
        table_data (List[Any]): Expected values of the first column.
        schema_name (str, optional): Schema override.
        info (LoadInfo, optional): Load information to cross-check load IDs.
    """
    # for sftp we can just check if the table exists
    if _is_sftp(p):
        assert table_exists(p, table_name, schema_name)
        return

    dataset = p.dataset(schema=schema_name)

    # select full table
    assert_query_column(
        p,
        dataset[table_name].to_sql(),
        table_data,
        schema_name,
        info,
    )


def assert_query_column(
    p: dlt.Pipeline,
    sql: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    """Assert that the first column produced by *sql* equals *table_data*.

    The *sql* query must select exactly one value column (optionally followed by
    a load-ID column).

    Args:
        p (dlt.Pipeline): Pipeline instance.
        sql (str): Select statement that returns exactly one or two columns.
        table_data (List[Any]): Expected values of the first column.
        schema_name (str, optional): Schema override.
        info (LoadInfo, optional): When provided, the second query column must
            contain only IDs that belong to *info*.
    """
    rows = select_data(p, sql, schema_name, _execute_raw_query=True)
    assert len(rows) == len(table_data)

    # check load id
    if info:
        for row in rows:
            assert row[1] in info.loads_ids

    # compare values of first column, regardless of order
    list_1 = [{"v": row[0]} for row in rows]
    list_2 = [{"v": d} for d in table_data]
    assert_records_as_set(list_1, list_2)


def assert_schema_on_data(
    table_schema: TTableSchema,
    rows: List[Dict[str, Any]],
    requires_nulls: bool,
    check_nested: bool,
) -> None:
    """Validate that *rows* conform to *table_schema*.

    The function checks column names, order, nullability and that the Python
    value types match the destination data types described by the schema. For
    columns with ``json`` type the nested structure is optionally validated as
    well.

    Args:
        table_schema (TTableSchema): Table schema as returned by
            ``dlt.common.schema``.
        rows (List[Dict[str, Any]]): Table rows to validate.
        requires_nulls (bool): When ``True`` all nullable columns must appear at
            least once with a ``None`` value in *rows*.
        check_nested (bool): Whether to parse JSON strings and validate nested
            structure types.

    Raises:
        AssertionError: If any of the schema constraints are violated.
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
