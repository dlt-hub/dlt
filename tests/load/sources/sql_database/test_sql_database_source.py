import os
from copy import deepcopy
from typing import Any, Callable, cast, List, Optional, Set

import pytest

import dlt
from dlt.common import json
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.exceptions import MissingDependencyException

from dlt.common.schema.typing import TColumnSchema, TSortOrder, TTableSchemaColumns
from dlt.common.utils import uniq_id
from dlt.extract.exceptions import ResourceExtractionError

from dlt.sources import DltResource

from tests.pipeline.utils import (
    assert_load_info,
    assert_schema_on_data,
    load_tables_to_dicts,
)
from tests.load.sources.sql_database.test_helpers import mock_json_column
from tests.utils import data_item_length, load_table_counts


try:
    from dlt.sources.sql_database import (
        ReflectionLevel,
        TableBackend,
        sql_database,
        sql_table,
    )
    from dlt.sources.sql_database.helpers import unwrap_json_connector_x
    from tests.load.sources.sql_database.sql_source import SQLAlchemySourceDB
    import sqlalchemy as sa
except MissingDependencyException:
    pytest.skip("Tests require sql alchemy", allow_module_level=True)


@pytest.fixture(autouse=True)
def dispose_engines():
    yield
    import gc

    # will collect and dispose all hanging engines
    gc.collect()


@pytest.fixture(autouse=True)
def reset_os_environ():
    # Save the current state of os.environ
    original_environ = deepcopy(os.environ)
    yield
    # Restore the original state of os.environ
    os.environ.clear()
    os.environ.update(original_environ)


def make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="sql_database" + uniq_id(),
        destination=destination_name,
        dataset_name="test_sql_pipeline_" + uniq_id(),
        full_refresh=False,
    )


def convert_json_to_text(t):
    if isinstance(t, sa.JSON):
        return sa.Text
    return t


def default_test_callback(
    destination_name: str, backend: TableBackend
) -> Optional[Callable[[sa.types.TypeEngine], sa.types.TypeEngine]]:
    if backend == "pyarrow" and destination_name == "bigquery":
        return convert_json_to_text
    return None


def convert_time_to_us(table):
    """map transform converting time column to microseconds (ie. from nanoseconds)"""
    import pyarrow as pa
    from pyarrow import compute as pc

    time_ns_column = table["time_col"]
    time_us_column = pc.cast(time_ns_column, pa.time64("us"), safe=False)
    new_table = table.set_column(
        table.column_names.index("time_col"),
        "time_col",
        time_us_column,
    )
    return new_table


def test_pass_engine_credentials(sql_source_db: SQLAlchemySourceDB) -> None:
    # verify database
    database = sql_database(
        sql_source_db.engine, schema=sql_source_db.schema, table_names=["chat_message"]
    )
    assert len(list(database)) == sql_source_db.table_infos["chat_message"]["row_count"]

    # verify table
    table = sql_table(sql_source_db.engine, table="chat_message", schema=sql_source_db.schema)
    assert len(list(table)) == sql_source_db.table_infos["chat_message"]["row_count"]


def test_named_sql_table_config(sql_source_db: SQLAlchemySourceDB) -> None:
    # set the credentials per table name
    os.environ["SOURCES__SQL_DATABASE__CHAT_MESSAGE__CREDENTIALS"] = (
        sql_source_db.engine.url.render_as_string(False)
    )
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    assert table.name == "chat_message"
    assert len(list(table)) == sql_source_db.table_infos["chat_message"]["row_count"]

    with pytest.raises(ConfigFieldMissingException):
        sql_table(table="has_composite_key", schema=sql_source_db.schema)

    # set backend
    os.environ["SOURCES__SQL_DATABASE__CHAT_MESSAGE__BACKEND"] = "pandas"
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    # just one frame here
    assert len(list(table)) == 1

    os.environ["SOURCES__SQL_DATABASE__CHAT_MESSAGE__CHUNK_SIZE"] = "1000"
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    # now 10 frames with chunk size of 1000
    assert len(list(table)) == 10

    # make it fail on cursor
    os.environ["SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH"] = "updated_at_x"
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    with pytest.raises(ResourceExtractionError) as ext_ex:
        len(list(table))
    assert "'updated_at_x'" in str(ext_ex.value)


def test_general_sql_database_config(sql_source_db: SQLAlchemySourceDB) -> None:
    # set the credentials per table name
    os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"] = sql_source_db.engine.url.render_as_string(
        False
    )
    # applies to both sql table and sql database
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    assert len(list(table)) == sql_source_db.table_infos["chat_message"]["row_count"]
    database = sql_database(schema=sql_source_db.schema).with_resources("chat_message")
    assert len(list(database)) == sql_source_db.table_infos["chat_message"]["row_count"]

    # set backend
    os.environ["SOURCES__SQL_DATABASE__BACKEND"] = "pandas"
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    # just one frame here
    assert len(list(table)) == 1
    database = sql_database(schema=sql_source_db.schema).with_resources("chat_message")
    assert len(list(database)) == 1

    os.environ["SOURCES__SQL_DATABASE__CHUNK_SIZE"] = "1000"
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    # now 10 frames with chunk size of 1000
    assert len(list(table)) == 10
    database = sql_database(schema=sql_source_db.schema).with_resources("chat_message")
    assert len(list(database)) == 10

    # make it fail on cursor
    os.environ["SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH"] = "updated_at_x"
    table = sql_table(table="chat_message", schema=sql_source_db.schema)
    with pytest.raises(ResourceExtractionError) as ext_ex:
        len(list(table))
    assert "'updated_at_x'" in str(ext_ex.value)
    with pytest.raises(ResourceExtractionError) as ext_ex:
        list(sql_database(schema=sql_source_db.schema).with_resources("chat_message"))
    # other resources will be loaded, incremental is selective
    assert len(list(sql_database(schema=sql_source_db.schema).with_resources("app_user"))) > 0


@pytest.mark.parametrize("backend", ["sqlalchemy", "pandas", "pyarrow", "connectorx"])
@pytest.mark.parametrize("row_order", ["asc", "desc", None])
@pytest.mark.parametrize("last_value_func", [min, max, lambda x: max(x)])
def test_load_sql_table_resource_incremental_end_value(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    row_order: TSortOrder,
    last_value_func: Any,
) -> None:
    start_id = sql_source_db.table_infos["chat_message"]["ids"][0]
    end_id = sql_source_db.table_infos["chat_message"]["ids"][-1] // 2

    if last_value_func is min:
        start_id, end_id = end_id, start_id

    @dlt.source
    def sql_table_source() -> List[DltResource]:
        return [
            sql_table(
                credentials=sql_source_db.credentials,
                schema=sql_source_db.schema,
                table="chat_message",
                backend=backend,
                incremental=dlt.sources.incremental(
                    "id",
                    initial_value=start_id,
                    end_value=end_id,
                    row_order=row_order,
                    last_value_func=last_value_func,
                ),
            )
        ]

    try:
        rows = list(sql_table_source())
    except Exception as exc:
        if isinstance(exc.__context__, NotImplementedError):
            pytest.skip("Test skipped due to: " + str(exc.__context__))
        raise
    # half of the records loaded -1 record. end values is non inclusive
    assert data_item_length(rows) == abs(end_id - start_id)
    # check first and last id to see if order was applied
    if backend == "sqlalchemy":
        if row_order == "asc" and last_value_func is max:
            assert rows[0]["id"] == start_id
            assert rows[-1]["id"] == end_id - 1  # non inclusive
        if row_order == "desc" and last_value_func is max:
            assert rows[0]["id"] == end_id - 1  # non inclusive
            assert rows[-1]["id"] == start_id
        if row_order == "asc" and last_value_func is min:
            assert rows[0]["id"] == start_id
            assert (
                rows[-1]["id"] == end_id + 1
            )  # non inclusive, but + 1 because last value func is min
        if row_order == "desc" and last_value_func is min:
            assert (
                rows[0]["id"] == end_id + 1
            )  # non inclusive, but + 1 because last value func is min
            assert rows[-1]["id"] == start_id


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_load_sql_table_resource_select_columns(
    sql_source_db: SQLAlchemySourceDB, defer_table_reflect: bool, backend: TableBackend
) -> None:
    # get chat messages with content column removed
    chat_messages = sql_table(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table="chat_message",
        defer_table_reflect=defer_table_reflect,
        table_adapter_callback=lambda table: table._columns.remove(table.columns["content"]),  # type: ignore[attr-defined]
        backend=backend,
    )
    pipeline = make_pipeline("duckdb")
    load_info = pipeline.run(chat_messages)
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db, ["chat_message"])
    assert "content" not in pipeline.default_schema.tables["chat_message"]["columns"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_load_sql_table_source_select_columns(
    sql_source_db: SQLAlchemySourceDB, defer_table_reflect: bool, backend: TableBackend
) -> None:
    mod_tables: Set[str] = set()

    def adapt(table) -> None:
        mod_tables.add(table)
        if table.name == "chat_message":
            table._columns.remove(table.columns["content"])

    # get chat messages with content column removed
    all_tables = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        defer_table_reflect=defer_table_reflect,
        table_names=(list(sql_source_db.table_infos.keys()) if defer_table_reflect else None),
        table_adapter_callback=adapt,
        backend=backend,
    )
    pipeline = make_pipeline("duckdb")
    load_info = pipeline.run(all_tables)
    assert_load_info(load_info)
    assert_row_counts(pipeline, sql_source_db)
    assert "content" not in pipeline.default_schema.tables["chat_message"]["columns"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("reflection_level", ["full", "full_with_precision"])
@pytest.mark.parametrize("with_defer", [True, False])
def test_extract_without_pipeline(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
    with_defer: bool,
) -> None:
    # make sure that we can evaluate tables without pipeline
    source = sql_database(
        credentials=sql_source_db.credentials,
        table_names=["has_precision", "app_user", "chat_message", "chat_channel"],
        schema=sql_source_db.schema,
        reflection_level=reflection_level,
        defer_table_reflect=with_defer,
        backend=backend,
    )
    assert len(list(source)) > 0


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
@pytest.mark.parametrize("with_defer", [False, True])
@pytest.mark.parametrize("standalone_resource", [True, False])
def test_reflection_levels(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
    with_defer: bool,
    standalone_resource: bool,
) -> None:
    """Test all reflection, correct schema is inferred"""

    def prepare_source():
        if standalone_resource:

            @dlt.source
            def dummy_source():
                yield sql_table(
                    credentials=sql_source_db.credentials,
                    schema=sql_source_db.schema,
                    table="has_precision",
                    backend=backend,
                    defer_table_reflect=with_defer,
                    reflection_level=reflection_level,
                )
                yield sql_table(
                    credentials=sql_source_db.credentials,
                    schema=sql_source_db.schema,
                    table="app_user",
                    backend=backend,
                    defer_table_reflect=with_defer,
                    reflection_level=reflection_level,
                )

            return dummy_source()

        return sql_database(
            credentials=sql_source_db.credentials,
            table_names=["has_precision", "app_user"],
            schema=sql_source_db.schema,
            reflection_level=reflection_level,
            defer_table_reflect=with_defer,
            backend=backend,
        )

    source = prepare_source()

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)

    schema = pipeline.default_schema
    assert "has_precision" in schema.tables

    col_names = [col["name"] for col in schema.tables["has_precision"]["columns"].values()]
    expected_col_names = [col["name"] for col in PRECISION_COLUMNS]

    # on sqlalchemy json col is not written to schema if no types are discovered
    if backend == "sqlalchemy" and reflection_level == "minimal" and not with_defer:
        expected_col_names = [col for col in expected_col_names if col != "json_col"]

    assert col_names == expected_col_names

    # Pk col is always reflected
    pk_col = schema.tables["app_user"]["columns"]["id"]
    assert pk_col["primary_key"] is True

    if reflection_level == "minimal":
        resource_cols = source.resources["has_precision"].compute_table_schema()["columns"]
        schema_cols = pipeline.default_schema.tables["has_precision"]["columns"]
        # We should have all column names on resource hints after extract but no data type or precision
        for col, schema_col in zip(resource_cols.values(), schema_cols.values()):
            assert col.get("data_type") is None
            assert col.get("precision") is None
            assert col.get("scale") is None
            if backend == "sqlalchemy":  # Data types are inferred from pandas/arrow during extract
                assert schema_col.get("data_type") is None

    pipeline.normalize()
    # Check with/out precision after normalize
    schema_cols = pipeline.default_schema.tables["has_precision"]["columns"]
    if reflection_level == "full":
        # Columns have data type set
        assert_no_precision_columns(schema_cols, backend, False)

    elif reflection_level == "full_with_precision":
        # Columns have data type and precision scale set
        assert_precision_columns(schema_cols, backend, False)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
@pytest.mark.parametrize("with_defer", [False, True])
@pytest.mark.parametrize("standalone_resource", [True, False])
@pytest.mark.parametrize("resolve_foreign_keys", [True, False])
def test_reflect_foreign_keys_as_table_references(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
    with_defer: bool,
    standalone_resource: bool,
    resolve_foreign_keys: bool,
) -> None:
    """Test all reflection, correct schema is inferred"""

    def prepare_source():
        if standalone_resource:

            @dlt.source
            def dummy_source():
                yield sql_table(
                    credentials=sql_source_db.credentials,
                    schema=sql_source_db.schema,
                    table="has_composite_foreign_key",
                    backend=backend,
                    defer_table_reflect=with_defer,
                    reflection_level=reflection_level,
                    resolve_foreign_keys=resolve_foreign_keys,
                )
                yield sql_table(  # Has no foreign keys
                    credentials=sql_source_db.credentials,
                    schema=sql_source_db.schema,
                    table="app_user",
                    backend=backend,
                    defer_table_reflect=with_defer,
                    reflection_level=reflection_level,
                    resolve_foreign_keys=resolve_foreign_keys,
                )
                yield sql_table(
                    credentials=sql_source_db.credentials,
                    schema=sql_source_db.schema,
                    table="chat_message",
                    backend=backend,
                    defer_table_reflect=with_defer,
                    reflection_level=reflection_level,
                    resolve_foreign_keys=resolve_foreign_keys,
                )

            return dummy_source()

        return sql_database(
            credentials=sql_source_db.credentials,
            table_names=["has_composite_foreign_key", "app_user", "chat_message"],
            schema=sql_source_db.schema,
            reflection_level=reflection_level,
            defer_table_reflect=with_defer,
            backend=backend,
            resolve_foreign_keys=resolve_foreign_keys,
        )

    source = prepare_source()

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)

    schema = pipeline.default_schema
    # Verify tables have references hints set up
    app_user = schema.tables["app_user"]
    assert app_user.get("references") is None

    chat_message = schema.tables["chat_message"]
    if not resolve_foreign_keys:
        assert chat_message.get("references") is None
    else:
        assert sorted(chat_message["references"], key=lambda x: x["referenced_table"]) == [
            {"columns": ["user_id"], "referenced_columns": ["id"], "referenced_table": "app_user"},
            {
                "columns": ["channel_id"],
                "referenced_columns": ["id"],
                "referenced_table": "chat_channel",
            },
        ]

    has_composite_foreign_key = schema.tables["has_composite_foreign_key"]
    if not resolve_foreign_keys:
        assert has_composite_foreign_key.get("references") is None

    else:
        assert has_composite_foreign_key["references"] == [
            {
                "columns": ["other_a", "other_b", "other_c"],
                "referenced_columns": ["a", "b", "c"],
                "referenced_table": "has_composite_key",
            }
        ]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("standalone_resource", [True, False])
def test_type_adapter_callback(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend, standalone_resource: bool
) -> None:
    def conversion_callback(t):
        if isinstance(t, sa.JSON):
            return sa.Text
        elif hasattr(sa, "Double") and isinstance(t, sa.Double):
            return sa.BIGINT
        return t

    common_kwargs = dict(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        backend=backend,
        type_adapter_callback=conversion_callback,
        reflection_level="full",
    )

    if standalone_resource:
        source = sql_table(
            table="has_precision",
            **common_kwargs,  # type: ignore[arg-type]
        )
    else:
        source = sql_database(  # type: ignore[assignment]
            table_names=["has_precision"],
            **common_kwargs,  # type: ignore[arg-type]
        )

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)

    schema = pipeline.default_schema
    table = schema.tables["has_precision"]
    assert table["columns"]["json_col"]["data_type"] == "text"
    assert (
        table["columns"]["float_col"]["data_type"] == "bigint"
        if hasattr(sa, "Double")
        else "double"
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize(
    "table_name,nullable", (("has_precision", False), ("has_precision_nullable", True))
)
def test_all_types_with_precision_hints(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    table_name: str,
    nullable: bool,
) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        reflection_level="full_with_precision",
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")

    # add JSON unwrap for connectorx
    if backend == "connectorx":
        source.resources[table_name].add_map(unwrap_json_connector_x("json_col"))
    pipeline.extract(source)
    pipeline.normalize(loader_file_format="parquet")
    info = pipeline.load()
    assert_load_info(info)

    schema = pipeline.default_schema
    table = schema.tables[table_name]
    assert_precision_columns(table["columns"], backend, nullable)
    assert_schema_on_data(
        table,
        load_tables_to_dicts(pipeline, table_name)[table_name],
        nullable,
        backend in ["sqlalchemy", "pyarrow"],
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize(
    "table_name,nullable", (("has_precision", False), ("has_precision_nullable", True))
)
def test_all_types_no_precision_hints(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
    table_name: str,
    nullable: bool,
) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        reflection_level="full",
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")

    # add JSON unwrap for connectorx
    if backend == "connectorx":
        source.resources[table_name].add_map(unwrap_json_connector_x("json_col"))
    pipeline.extract(source)
    pipeline.normalize(loader_file_format="parquet")
    pipeline.load()

    schema = pipeline.default_schema
    # print(pipeline.default_schema.to_pretty_yaml())
    table = schema.tables[table_name]
    assert_no_precision_columns(table["columns"], backend, nullable)
    assert_schema_on_data(
        table,
        load_tables_to_dicts(pipeline, table_name)[table_name],
        nullable,
        backend in ["sqlalchemy", "pyarrow"],
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_incremental_composite_primary_key_from_table(
    sql_source_db: SQLAlchemySourceDB,
    backend: TableBackend,
) -> None:
    resource = sql_table(
        credentials=sql_source_db.credentials,
        table="has_composite_key",
        schema=sql_source_db.schema,
        backend=backend,
    )

    assert resource.incremental.primary_key == ["a", "b", "c"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("upfront_incremental", (True, False))
def test_set_primary_key_deferred_incremental(
    sql_source_db: SQLAlchemySourceDB,
    upfront_incremental: bool,
    backend: TableBackend,
) -> None:
    # this tests dynamically adds primary key to resource and as consequence to incremental
    updated_at = dlt.sources.incremental("updated_at")  # type: ignore[var-annotated]
    resource = sql_table(
        credentials=sql_source_db.credentials,
        table="chat_message",
        schema=sql_source_db.schema,
        defer_table_reflect=True,
        incremental=updated_at if upfront_incremental else None,
        backend=backend,
    )

    resource.apply_hints(incremental=None if upfront_incremental else updated_at)

    # nothing set for deferred reflect
    assert resource.incremental.primary_key is None

    def _assert_incremental(item):
        # for all the items, all keys must be present
        _r = dlt.current.source().resources[dlt.current.resource_name()]
        # assert _r.incremental._incremental is updated_at
        if len(item) == 0:
            # not yet propagated
            assert _r.incremental.primary_key is None
        else:
            assert _r.incremental.primary_key == ["id"]
        assert _r.incremental._incremental.primary_key == ["id"]
        assert _r.incremental._incremental._transformers["json"].primary_key == ["id"]
        assert _r.incremental._incremental._transformers["arrow"].primary_key == ["id"]
        return item

    pipeline = make_pipeline("duckdb")
    # must evaluate resource for primary key to be set
    pipeline.extract(resource.add_step(_assert_incremental))  # type: ignore[arg-type]

    assert resource.incremental.primary_key == ["id"]
    assert resource.incremental._incremental.primary_key == ["id"]
    assert resource.incremental._incremental._transformers["json"].primary_key == ["id"]
    assert resource.incremental._incremental._transformers["arrow"].primary_key == ["id"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_deferred_reflect_in_source(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    source = sql_database(
        credentials=sql_source_db.credentials,
        table_names=["has_precision", "chat_message"],
        schema=sql_source_db.schema,
        reflection_level="full_with_precision",
        defer_table_reflect=True,
        backend=backend,
    )
    # mock the right json values for backends not supporting it
    if backend in ("connectorx", "pandas"):
        source.resources["has_precision"].add_map(mock_json_column("json_col"))

    # no columns in both tables
    assert source.has_precision.columns == {}
    assert source.chat_message.columns == {}

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)
    # use insert values to convert parquet into INSERT
    pipeline.normalize(loader_file_format="insert_values")
    pipeline.load()
    precision_table = pipeline.default_schema.get_table("has_precision")
    assert_precision_columns(
        precision_table["columns"],
        backend,
        nullable=False,
    )
    assert_schema_on_data(
        precision_table,
        load_tables_to_dicts(pipeline, "has_precision")["has_precision"],
        True,
        backend in ["sqlalchemy", "pyarrow"],
    )
    assert len(source.chat_message.columns) > 0  # type: ignore[arg-type]
    assert source.chat_message.compute_table_schema()["columns"]["id"]["primary_key"] is True


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_deferred_reflect_no_source_connect(backend: TableBackend) -> None:
    source = sql_database(
        credentials="mysql+pymysql://test@test/test",
        table_names=["has_precision", "chat_message"],
        schema="schema",
        reflection_level="full_with_precision",
        defer_table_reflect=True,
        backend=backend,
    )

    # no columns in both tables
    assert source.has_precision.columns == {}
    assert source.chat_message.columns == {}


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_deferred_reflect_in_resource(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    table = sql_table(
        credentials=sql_source_db.credentials,
        table="has_precision",
        schema=sql_source_db.schema,
        reflection_level="full_with_precision",
        defer_table_reflect=True,
        backend=backend,
    )
    # mock the right json values for backends not supporting it
    if backend in ("connectorx", "pandas"):
        table.add_map(mock_json_column("json_col"))

    # no columns in both tables
    assert table.columns == {}

    pipeline = make_pipeline("duckdb")
    pipeline.extract(table)
    # use insert values to convert parquet into INSERT
    pipeline.normalize(loader_file_format="insert_values")
    pipeline.load()
    precision_table = pipeline.default_schema.get_table("has_precision")
    assert_precision_columns(
        precision_table["columns"],
        backend,
        nullable=False,
    )
    assert_schema_on_data(
        precision_table,
        load_tables_to_dicts(pipeline, "has_precision")["has_precision"],
        True,
        backend in ["sqlalchemy", "pyarrow"],
    )


@pytest.mark.parametrize("backend", ["pyarrow", "pandas", "connectorx"])
def test_destination_caps_context(sql_source_db: SQLAlchemySourceDB, backend: TableBackend) -> None:
    # use athena with timestamp precision == 3
    table = sql_table(
        credentials=sql_source_db.credentials,
        table="has_precision",
        schema=sql_source_db.schema,
        reflection_level="full_with_precision",
        defer_table_reflect=True,
        backend=backend,
    )

    # no columns in both tables
    assert table.columns == {}

    pipeline = make_pipeline("athena")
    pipeline.extract(table)
    pipeline.normalize()
    # timestamps are milliseconds
    columns = pipeline.default_schema.get_table("has_precision")["columns"]
    assert columns["datetime_tz_col"]["precision"] == columns["datetime_ntz_col"]["precision"] == 3
    # prevent drop
    pipeline.destination = None


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_sql_table_from_view(sql_source_db: SQLAlchemySourceDB, backend: TableBackend) -> None:
    """View can be extract by sql_table without any reflect flags"""
    table = sql_table(
        credentials=sql_source_db.credentials,
        table="chat_message_view",
        schema=sql_source_db.schema,
        backend=backend,
        # use minimal level so we infer types from DATA
        reflection_level="minimal",
        incremental=dlt.sources.incremental("_created_at"),
    )

    pipeline = make_pipeline("duckdb")
    info = pipeline.run(table)
    assert_load_info(info)

    assert_row_counts(pipeline, sql_source_db, ["chat_message_view"])
    assert "content" in pipeline.default_schema.tables["chat_message_view"]["columns"]
    assert "_created_at" in pipeline.default_schema.tables["chat_message_view"]["columns"]
    db_data = load_tables_to_dicts(pipeline, "chat_message_view")["chat_message_view"]
    assert "content" in db_data[0]
    assert "_created_at" in db_data[0]
    # make sure that all NULLs is not present
    assert "_null_ts" in pipeline.default_schema.tables["chat_message_view"]["columns"]


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_sql_database_include_views(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """include_view flag reflects and extracts views as tables"""
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        include_views=True,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")
    pipeline.run(source)

    assert_row_counts(pipeline, sql_source_db, include_views=True)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
def test_sql_database_include_view_in_table_names(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend
) -> None:
    """Passing a view explicitly in table_names should reflect it, regardless of include_views flag"""
    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table_names=["app_user", "chat_message_view"],
        include_views=False,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")
    pipeline.run(source)

    assert_row_counts(pipeline, sql_source_db, ["app_user", "chat_message_view"])


@pytest.mark.parametrize("backend", ["pyarrow", "pandas", "sqlalchemy"])
@pytest.mark.parametrize("standalone_resource", [True, False])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
@pytest.mark.parametrize("type_adapter", [True, False])
def test_infer_unsupported_types(
    sql_source_db_unsupported_types: SQLAlchemySourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
    standalone_resource: bool,
    type_adapter: bool,
) -> None:
    def type_adapter_callback(t):
        if isinstance(t, sa.ARRAY):
            return sa.JSON
        return t

    if backend == "pyarrow" and type_adapter:
        pytest.skip("Arrow does not support type adapter for arrays")

    common_kwargs = dict(
        credentials=sql_source_db_unsupported_types.credentials,
        schema=sql_source_db_unsupported_types.schema,
        reflection_level=reflection_level,
        backend=backend,
        type_adapter_callback=type_adapter_callback if type_adapter else None,
    )
    if standalone_resource:

        @dlt.source
        def dummy_source():
            yield sql_table(
                **common_kwargs,  # type: ignore[arg-type]
                table="has_unsupported_types",
            )

        source = dummy_source()
        source.max_table_nesting = 0
    else:
        source = sql_database(
            **common_kwargs,  # type: ignore[arg-type]
            table_names=["has_unsupported_types"],
        )
        source.max_table_nesting = 0

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)

    columns = pipeline.default_schema.tables["has_unsupported_types"]["columns"]

    pipeline.normalize()
    pipeline.load()

    assert_row_counts(pipeline, sql_source_db_unsupported_types, ["has_unsupported_types"])

    schema = pipeline.default_schema
    assert "has_unsupported_types" in schema.tables
    columns = schema.tables["has_unsupported_types"]["columns"]

    rows = load_tables_to_dicts(pipeline, "has_unsupported_types")["has_unsupported_types"]

    if backend == "pyarrow":
        # TODO: duckdb writes structs as strings (not json encoded) to json columns
        # Just check that it has a value

        assert isinstance(json.loads(rows[0]["unsupported_array_1"]), list)
        assert columns["unsupported_array_1"]["data_type"] == "json"
        # Other columns are loaded
        assert isinstance(rows[0]["supported_text"], str)
        assert isinstance(rows[0]["supported_int"], int)
    elif backend == "sqlalchemy":
        # sqla value is a dataclass and is inferred as json

        assert columns["unsupported_array_1"]["data_type"] == "json"

    elif backend == "pandas":
        # pandas parses it as string
        if type_adapter and reflection_level != "minimal":
            assert columns["unsupported_array_1"]["data_type"] == "json"

            assert isinstance(json.loads(rows[0]["unsupported_array_1"]), list)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_sql_database_included_columns(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend, defer_table_reflect: bool
) -> None:
    # include only some columns from the table
    os.environ["SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCLUDED_COLUMNS"] = json.dumps(
        ["id", "created_at"]
    )

    source = sql_database(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table_names=["chat_message"],
        reflection_level="full",
        defer_table_reflect=defer_table_reflect,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")
    pipeline.run(source)

    schema = pipeline.default_schema
    schema_cols = set(
        col
        for col in schema.get_table_columns("chat_message", include_incomplete=True)
        if not col.startswith("_dlt_")
    )
    assert schema_cols == {"id", "created_at"}

    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("defer_table_reflect", (False, True))
def test_sql_table_included_columns(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend, defer_table_reflect: bool
) -> None:
    source = sql_table(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        table="chat_message",
        reflection_level="full",
        defer_table_reflect=defer_table_reflect,
        backend=backend,
        included_columns=["id", "created_at"],
    )

    pipeline = make_pipeline("duckdb")
    pipeline.run(source)

    schema = pipeline.default_schema
    schema_cols = set(
        col
        for col in schema.get_table_columns("chat_message", include_incomplete=True)
        if not col.startswith("_dlt_")
    )
    assert schema_cols == {"id", "created_at"}

    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("standalone_resource", [True, False])
def test_query_adapter_callback(
    sql_source_db: SQLAlchemySourceDB, backend: TableBackend, standalone_resource: bool
) -> None:
    def query_adapter_callback(query, table):
        if table.name == "chat_channel":
            # Only select active channels
            return query.where(table.c.active.is_(True))
        # Use the original query for other tables
        return query

    common_kwargs = dict(
        credentials=sql_source_db.credentials,
        schema=sql_source_db.schema,
        reflection_level="full",
        backend=backend,
        query_adapter_callback=query_adapter_callback,
    )

    if standalone_resource:

        @dlt.source
        def dummy_source():
            yield sql_table(
                **common_kwargs,  # type: ignore[arg-type]
                table="chat_channel",
            )

            yield sql_table(
                **common_kwargs,  # type: ignore[arg-type]
                table="chat_message",
            )

        source = dummy_source()
    else:
        source = sql_database(
            **common_kwargs,  # type: ignore[arg-type]
            table_names=["chat_message", "chat_channel"],
        )

    pipeline = make_pipeline("duckdb")
    pipeline.extract(source)

    pipeline.normalize()
    pipeline.load()

    channel_rows = load_tables_to_dicts(pipeline, "chat_channel")["chat_channel"]
    assert channel_rows and all(row["active"] for row in channel_rows)

    # unfiltered table loads all rows
    assert_row_counts(pipeline, sql_source_db, ["chat_message"])


def assert_row_counts(
    pipeline: dlt.Pipeline,
    sql_source_db: SQLAlchemySourceDB,
    tables: Optional[List[str]] = None,
    include_views: bool = False,
) -> None:
    if not tables:
        tables = [
            tbl_name
            for tbl_name, info in sql_source_db.table_infos.items()
            if include_views or not info["is_view"]
        ]
    dest_counts = load_table_counts(pipeline, *tables)
    for table in tables:
        info = sql_source_db.table_infos[table]
        assert (
            dest_counts[table] == info["row_count"]
        ), f"Table {table} counts do not match with the source"


def assert_precision_columns(
    columns: TTableSchemaColumns, backend: TableBackend, nullable: bool
) -> None:
    actual = list(columns.values())
    expected = NULL_PRECISION_COLUMNS if nullable else NOT_NULL_PRECISION_COLUMNS
    # always has nullability set and always has hints
    expected = cast(List[TColumnSchema], deepcopy(expected))
    if backend == "sqlalchemy":
        expected = remove_timestamp_precision(expected)
        actual = remove_dlt_columns(actual)
    if backend == "pyarrow":
        expected = add_default_decimal_precision(expected)
    if backend == "pandas":
        expected = remove_timestamp_precision(expected, with_timestamps=False)
    if backend == "connectorx":
        # connector x emits 32 precision which gets merged with sql alchemy schema
        del columns["int_col"]["precision"]
    assert actual == expected


def assert_no_precision_columns(
    columns: TTableSchemaColumns, backend: TableBackend, nullable: bool
) -> None:
    actual = list(columns.values())
    # we always infer and emit nullability
    expected = cast(
        List[TColumnSchema],
        deepcopy(NULL_NO_PRECISION_COLUMNS if nullable else NOT_NULL_NO_PRECISION_COLUMNS),
    )
    if backend == "pyarrow":
        expected = cast(
            List[TColumnSchema],
            deepcopy(NULL_PRECISION_COLUMNS if nullable else NOT_NULL_PRECISION_COLUMNS),
        )
        # always has nullability set and always has hints
        # default precision is not set
        expected = remove_default_precision(expected)
        expected = add_default_decimal_precision(expected)
    elif backend == "sqlalchemy":
        # no precision, no nullability, all hints inferred
        # remove dlt columns
        actual = remove_dlt_columns(actual)
    elif backend == "pandas":
        # no precision, no nullability, all hints inferred
        # pandas destroys decimals
        expected = convert_non_pandas_types(expected)
        # on one of the timestamps somehow there is timezone info..., we only remove values set to false
        # to be sure no bad data is coming in
        actual = remove_timezone_info(actual, only_falsy=True)
    elif backend == "connectorx":
        expected = cast(
            List[TColumnSchema],
            deepcopy(NULL_PRECISION_COLUMNS if nullable else NOT_NULL_PRECISION_COLUMNS),
        )
        expected = convert_connectorx_types(expected)
        expected = remove_timezone_info(expected, only_falsy=False)
        # on one of the timestamps somehow there is timezone info..., we only remove values set to false
        # to be sure no bad data is coming in
        actual = remove_timezone_info(actual, only_falsy=True)

    assert actual == expected


def convert_non_pandas_types(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    for column in columns:
        if column["data_type"] == "timestamp":
            column["precision"] = 6
    return columns


def remove_dlt_columns(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    return [col for col in columns if not col["name"].startswith("_dlt")]


def remove_default_precision(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    for column in columns:
        if column["data_type"] == "bigint" and column.get("precision") == 32:
            del column["precision"]
        if column["data_type"] == "text" and column.get("precision"):
            del column["precision"]
    return remove_timezone_info(columns, only_falsy=False)


def remove_timezone_info(columns: List[TColumnSchema], only_falsy: bool) -> List[TColumnSchema]:
    for column in columns:
        if not only_falsy:
            column.pop("timezone", None)
        elif column.get("timezone") is False:
            column.pop("timezone", None)
    return columns


def remove_timestamp_precision(
    columns: List[TColumnSchema], with_timestamps: bool = True
) -> List[TColumnSchema]:
    for column in columns:
        if column["data_type"] == "timestamp" and column["precision"] == 6 and with_timestamps:
            del column["precision"]
        if column["data_type"] == "time" and column["precision"] == 6:
            del column["precision"]
    return columns


def convert_connectorx_types(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    """connector x converts decimals to double, otherwise tries to keep data types and precision
    nullability is not kept, string precision is not kept
    """
    for column in columns:
        if column["data_type"] == "bigint":
            if column["name"] == "int_col":
                column["precision"] = 32  # only int and bigint in connectorx
        if column["data_type"] == "text" and column.get("precision"):
            del column["precision"]
    return columns


def add_default_decimal_precision(columns: List[TColumnSchema]) -> List[TColumnSchema]:
    for column in columns:
        if column["data_type"] == "decimal" and not column.get("precision"):
            column["precision"] = 38
            column["scale"] = 9
    return columns


PRECISION_COLUMNS: List[TColumnSchema] = [
    {
        "data_type": "bigint",
        "name": "int_col",
    },
    {
        "data_type": "bigint",
        "name": "bigint_col",
    },
    {
        "data_type": "bigint",
        "precision": 32,
        "name": "smallint_col",
    },
    {
        "data_type": "decimal",
        "precision": 10,
        "scale": 2,
        "name": "numeric_col",
    },
    {
        "data_type": "decimal",
        "name": "numeric_default_col",
    },
    {
        "data_type": "text",
        "precision": 10,
        "name": "string_col",
    },
    {
        "data_type": "text",
        "name": "string_default_col",
    },
    {"data_type": "timestamp", "precision": 6, "name": "datetime_tz_col", "timezone": True},
    {"data_type": "timestamp", "precision": 6, "name": "datetime_ntz_col", "timezone": False},
    {
        "data_type": "date",
        "name": "date_col",
    },
    {
        "data_type": "time",
        "name": "time_col",
        "precision": 6,
    },
    {
        "data_type": "double",
        "name": "float_col",
    },
    {
        "data_type": "json",
        "name": "json_col",
    },
    {
        "data_type": "bool",
        "name": "bool_col",
    },
]

NOT_NULL_PRECISION_COLUMNS = [{"nullable": False, **column} for column in PRECISION_COLUMNS]
NULL_PRECISION_COLUMNS: List[TColumnSchema] = [
    {"nullable": True, **column} for column in PRECISION_COLUMNS
]

# but keep decimal precision
NO_PRECISION_COLUMNS: List[TColumnSchema] = [
    (
        {"name": column["name"], "data_type": column["data_type"]}  # type: ignore[misc]
        if column["data_type"] != "decimal"
        else dict(column)
    )
    for column in PRECISION_COLUMNS
]

NOT_NULL_NO_PRECISION_COLUMNS: List[TColumnSchema] = [
    {"nullable": False, **column} for column in NO_PRECISION_COLUMNS
]
NULL_NO_PRECISION_COLUMNS: List[TColumnSchema] = [
    {"nullable": True, **column} for column in NO_PRECISION_COLUMNS
]
