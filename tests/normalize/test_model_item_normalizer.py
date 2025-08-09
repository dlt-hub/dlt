from importlib import import_module
import pytest
import os
from concurrent.futures import ThreadPoolExecutor
import sqlglot
from typing import Iterator, List, Tuple, NamedTuple, Union, Optional
from packaging.version import Version

from dlt.common.destination import DestinationCapabilitiesContext, merge_caps_file_formats
from dlt.common.configuration.container import Container
from dlt.normalize import Normalize
from dlt.normalize.exceptions import NormalizeJobFailed
from dlt.common.storages import (
    NormalizeStorage,
    FileStorage,
)
from dlt.common.schema.exceptions import CannotCoerceNullException
from dlt.common.schema.schema import Schema
from dlt.common.schema import utils
from dlt.common.utils import read_dialect_and_sql
from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.extract.extract import ExtractStorage
from dlt.extract.hints import SqlModel

from tests.utils import clean_test_storage, TEST_DICT_CONFIG_PROVIDER, preserve_environ
from tests.load.test_model_item_format import (
    destination_configs,
    DESTINATIONS_SUPPORTING_MODEL,
)


def get_caps(dest_name: str):
    dest_cls = getattr(import_module("dlt.destinations"), dest_name)
    return dest_cls().capabilities


MODEL_CAPS = [get_caps(dest_name=dest) for dest in DESTINATIONS_SUPPORTING_MODEL]


@pytest.fixture
def caps(request) -> Iterator[DestinationCapabilitiesContext]:
    _caps = request.param()
    if (
        _caps.supported_staging_file_formats is not None
        and "model" in _caps.supported_staging_file_formats
    ):  # This is true for Athena and Dremio
        _stage_caps = get_caps("filesystem")()
        preferred_format, supported_formats = merge_caps_file_formats(
            "original_dest",
            "staging_dest",
            _caps,
            _stage_caps,
        )
        _caps.update({"preferred_loader_file_format": preferred_format})
        _caps.update({"supported_loader_file_formats": supported_formats})
    with Container().injectable_context(_caps):
        yield _caps


@pytest.fixture
def model_normalize(preserve_environ) -> Iterator[Normalize]:
    # adding dlt id is disabled by default, so we set it to true
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(True)
    yield from init_normalize()


def init_normalize(default_schemas_path: str = None) -> Iterator[Normalize]:
    clean_test_storage()
    # pass schema config fields to schema storage via dict config provider
    with TEST_DICT_CONFIG_PROVIDER().values(
        {"import_schema_path": default_schemas_path, "external_schema_format": "json"}
    ):
        # inject the destination capabilities
        n = Normalize()
        yield n


def extract_model(
    normalize_storage: NormalizeStorage, model: SqlModel, schema: Schema, table_name: str
) -> str:
    extractor = ExtractStorage(normalize_storage.config)
    load_id = extractor.create_load_package(schema)
    extractor.item_storages["model"].write_data_item(load_id, schema.name, table_name, model, None)
    extractor.close_writers(load_id)
    extractor.commit_new_load_package(load_id, schema)
    return load_id


def create_schema_with_complete_columns(
    table_name: str, data_type: str, columns: List[str], with_dlt_id: Optional[bool] = False
) -> Schema:
    cols = [utils.new_column(column_name=col, data_type=data_type) for col in columns]  # type: ignore[arg-type]
    if with_dlt_id:
        cols.append(utils.dlt_id_column())
    table = utils.new_table(table_name, columns=cols)
    schema = Schema("my_schema")
    schema.update_table(table)
    return schema


def extract_normalize_retrieve(
    model_normalize: Normalize,
    model: SqlModel,
    schema: Schema,
    table_name: str,
    fallback_dialect: TSqlGlotDialect,
) -> Tuple[str, str, str]:
    # Extract and normalize the model
    load_id = extract_model(model_normalize.normalize_storage, model, schema, table_name)
    with ThreadPoolExecutor(max_workers=1) as pool:
        model_normalize.run(pool)

    # Retrieve the normalized query from the storage
    model_files = model_normalize.load_storage.list_new_jobs(load_id)
    model_full_path = model_normalize.load_storage.normalized_packages.storage.make_full_path(
        model_files[0]
    )
    with FileStorage.open_zipsafe_ro(model_full_path, "r", encoding="utf-8") as f:
        select_dialect, normalized_select_query = read_dialect_and_sql(
            file_obj=f,
            fallback_dialect=fallback_dialect,
        )

    return select_dialect, normalized_select_query, load_id


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_star_select_rejection(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    """
    This test ensures that the model normalizer rejects star selects.
    """
    dialect = caps.sqlglot_dialect
    model = SqlModel.from_query_string(query="SELECT * FROM my_table")
    # Ensure the schema contains the table "my_table" with columns a, b
    schema = create_schema_with_complete_columns("my_table", "text", ["a_a", "b", "d"])
    with pytest.raises(
        NormalizeJobFailed,
        match=r"Model queries using a star \(`\*`\) expression cannot be normalized\.",
    ):
        _, _, _ = extract_normalize_retrieve(model_normalize, model, schema, "my_table", dialect)

    # Ensure star selects with explicit columns are also rejected
    another_model = SqlModel.from_query_string(query="SELECT t.*, NULL AS c FROM my_table AS t1")
    with pytest.raises(
        NormalizeJobFailed,
        match=r"Model queries using a star \(`\*`\) expression cannot be normalized\.",
    ):
        _, _, _ = extract_normalize_retrieve(
            model_normalize, another_model, schema, "my_table", dialect
        )


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_simple_model_normalizing(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    """
    This test demonstrates how a model with simple sql query is transformed by the normalizer for each destination capabilities.
    Explicit queries are used to clearly show the expected transformation process.
    """
    # TODO: tests for sqlalchemy dialects
    if caps.sqlglot_dialect in ["snowflake", "bigquery", "tsql", "postgres"] and (
        Version(sqlglot.__version__) < Version("26.0.0")
    ):
        pytest.skip(
            "Skipping some tests for older sqlglot versions as in the current version we have"
            " better support for these dialects and the sql output is slightly different"
        )

    # create escaped input
    dialect = caps.sqlglot_dialect
    sql_string = sqlglot.transpile(
        'SELECT "b", "A_a", "c" FROM "my_table"', read="duckdb", write=dialect
    )[0]
    model = SqlModel.from_query_string(query=sql_string, dialect=dialect)

    # Ensure the schema contains the table "my_table" with columns a, b
    schema = create_schema_with_complete_columns("my_table", "text", ["a_a", "b", "d"])

    select_dialect, normalized_select_query, load_id = extract_normalize_retrieve(
        model_normalize, model, schema, "my_table", dialect
    )

    assert select_dialect == dialect
    if dialect == "duckdb":
        assert (
            'SELECT _dlt_subquery."A_a" AS "a_a", _dlt_subquery."b" AS "b", NULL AS "d",'
            f' \'{load_id}\' AS "_dlt_load_id", UUID() AS "_dlt_id" FROM (SELECT "b", "A_a", "c"'
            ' FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )
    elif dialect == "bigquery":
        assert (
            "SELECT _dlt_subquery.`A_a` AS `a_a`, _dlt_subquery.`b` AS `b`, NULL AS `d`,"
            f" '{load_id}' AS `_dlt_load_id`, GENERATE_UUID() AS `_dlt_id` FROM (SELECT `b`, `A_a`,"
            " `c` FROM `my_table`) AS _dlt_subquery\n"
            == normalized_select_query
        )
    elif dialect == "clickhouse":
        assert (
            'SELECT _dlt_subquery."A_a" AS "a_a", _dlt_subquery."b" AS "b", NULL AS "d",'
            f' \'{load_id}\' AS "_dlt_load_id", generateUUIDv4() AS "_dlt_id" FROM (SELECT "b",'
            ' "A_a", "c" FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )
    elif dialect == "databricks":
        assert (
            "SELECT _dlt_subquery.`A_a` AS `a_a`, _dlt_subquery.`b` AS `b`, NULL AS `d`,"
            f" '{load_id}' AS `_dlt_load_id`, UUID() AS `_dlt_id` FROM (SELECT `b`, `A_a`, `c` FROM"
            " `my_table`) AS _dlt_subquery\n"
            == normalized_select_query
        )
    elif dialect == "tsql":  # mssql and synapse
        assert (
            "SELECT _dlt_subquery.[A_a] AS [a_a], _dlt_subquery.[b] AS [b], NULL AS [d],"
            f" '{load_id}' AS [_dlt_load_id], NEWID() AS [_dlt_id] FROM (SELECT [b] AS [b], [A_a]"
            " AS [A_a], [c] AS [c] FROM [my_table]) AS _dlt_subquery\n"
            == normalized_select_query
        )
    elif dialect == "postgres":
        assert (
            'SELECT _dlt_subquery."A_a" AS "a_a", _dlt_subquery."b" AS "b", NULL AS "d",'
            f' \'{load_id}\' AS "_dlt_load_id", GEN_RANDOM_UUID() AS "_dlt_id" FROM (SELECT "b",'
            ' "A_a", "c" FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )
    elif dialect == "redshift":
        assert (
            'SELECT _dlt_subquery."A_a" AS "a_a", _dlt_subquery."b" AS "b", NULL AS "d",'
            f" '{load_id}' AS \"_dlt_load_id\", MD5('{load_id}' || '-' || ROW_NUMBER() OVER ())"
            ' AS "_dlt_id" FROM (SELECT "b", "A_a", "c" FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )
    elif dialect == "snowflake":
        assert (
            'SELECT _dlt_subquery."A_a" AS "A_A", _dlt_subquery."b" AS "B", NULL AS "D",'
            f' \'{load_id}\' AS "_DLT_LOAD_ID", UUID_STRING() AS "_DLT_ID" FROM (SELECT "b", "A_a",'
            ' "c" FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )
    elif dialect == "athena":
        assert (
            'SELECT _dlt_subquery."A_a" AS "a_a", _dlt_subquery."b" AS "b", NULL AS "d",'
            f' \'{load_id}\' AS "_dlt_load_id", CAST(UUID() AS VARCHAR) AS "_dlt_id" FROM (SELECT'
            ' "b", "A_a", "c" FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )
    elif dialect == "presto":  # dremio
        assert (
            'SELECT _dlt_subquery."A_a" AS "a_a", _dlt_subquery."b" AS "b", NULL AS "d",'
            f' \'{load_id}\' AS "_dlt_load_id", UUID() AS "_dlt_id" FROM (SELECT "b", "A_a", "c"'
            ' FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )
    else:
        assert (
            'SELECT _dlt_subquery."A_a" AS "a_a", _dlt_subquery."b" AS "b", NULL AS "d",'
            f' \'{load_id}\' AS "_dlt_load_id", UUID() AS "_dlt_id" FROM (SELECT "b", "A_a", "c"'
            ' FROM "my_table") AS _dlt_subquery\n'
            == normalized_select_query
        )


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_selected_column_names_normalized(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    # Define a query with illegal characters in column names to test normalization
    illegal_select_query = """
SELECT
  "t0"."a/a",
  "t0"."_dlt.load_id",
  "t0"."_dlt,id"
FROM  "my_table" AS "t0"
LIMIT 5
"""
    dialect = caps.sqlglot_dialect

    # Create a SQL model from the query
    model = SqlModel.from_query_string(query=illegal_select_query, dialect=dialect)

    # Ensure the schema contains the table "my_table"
    schema = create_schema_with_complete_columns(
        "my_table", "text", ["a_a", "_dlt_load_id", "_dlt_id"]
    )

    _, normalized_select_query, _ = extract_normalize_retrieve(
        model_normalize, model, schema, "my_table", dialect
    )

    # Parse the normalized model query to extract the subquery
    parsed_norm_select_query = sqlglot.parse_one(normalized_select_query, read=dialect)

    # Ensure the normalized model query contains a subquery in the FROM clause
    from_clause = parsed_norm_select_query.args.get("from")
    assert isinstance(from_clause, sqlglot.exp.From)
    assert isinstance(from_clause.this, sqlglot.exp.Subquery)
    assert isinstance(from_clause.this.this, sqlglot.exp.Select)
    parsed_subquery = from_clause.this.this

    # Parse the original illegal query for comparison
    parsed_illegal_select_query = sqlglot.parse_one(illegal_select_query, read=dialect)

    # For tsql, ensure all columns in the subquery are explicitly aliased
    # because even though we don't have aliases in the original query,
    # sqlglot adds aliases in subqueries for tsql by default
    if dialect == "tsql":
        aliased_columns = [
            col.as_(col.output_name) for col in parsed_illegal_select_query.expressions
        ]
        parsed_illegal_select_query.set("expressions", aliased_columns)

    # Ensure the subquery in the normalized query matches the original query
    assert parsed_subquery.sql(dialect) == parsed_illegal_select_query.sql(dialect)

    # Verify that illegal characters in column names were normalized in the outer query
    # Check that the outer query contains exactly 3 columns with normalized aliases
    assert len(parsed_norm_select_query.expressions) == 3
    assert parsed_norm_select_query.expressions[0].alias == caps.casefold_identifier("a_a")
    assert parsed_norm_select_query.expressions[1].alias == caps.casefold_identifier("_dlt_load_id")
    assert parsed_norm_select_query.expressions[2].alias == caps.casefold_identifier("_dlt_id")


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(["d", "c", "b", "a"], id="d-c-b-a"),
        pytest.param(["b", "d", "a", "c"], id="b-d-a-c"),
    ],
)
@pytest.mark.parametrize(
    "add_dlt_columns",
    [True, False],
)
@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_selected_column_names_reordering(
    caps: DestinationCapabilitiesContext,
    columns: List[str],
    add_dlt_columns: bool,
    preserve_environ,
) -> None:
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_LOAD_ID"] = str(add_dlt_columns)
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(add_dlt_columns)
    model_normalize = next(init_normalize())
    # Define a query using the randomly ordered columns
    cols = ", ".join(f"{col}" for col in columns)
    model_query = f"SELECT {cols} FROM my_table"
    dialect = caps.sqlglot_dialect

    # Create a SQL model from the query
    model = SqlModel.from_query_string(query=model_query, dialect=dialect)

    # Ensure the schema contains the table "my_table" with columns from "a" to "d" appearing
    # in alphabetical order
    schema = create_schema_with_complete_columns("my_table", "text", ["a", "b", "c", "d"])

    select_dialect, normalized_select_query, load_id = extract_normalize_retrieve(
        model_normalize, model, schema, "my_table", dialect
    )

    assert select_dialect == dialect

    # Parse the normalized model query to extract the aliases of the outer select
    parsed_norm_select_query = sqlglot.parse_one(normalized_select_query, read=dialect)
    assert isinstance(parsed_norm_select_query, sqlglot.exp.Select)
    aliases = [select.alias for select in parsed_norm_select_query.selects]
    if add_dlt_columns:
        assert aliases == [
            caps.casefold_identifier(col) for col in ["a", "b", "c", "d", "_dlt_load_id", "_dlt_id"]
        ]
    else:
        assert aliases == [caps.casefold_identifier(col) for col in ["a", "b", "c", "d"]]


@pytest.mark.parametrize("nullable", [True, False])
@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_select_column_added_from_schema(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize, nullable: bool
) -> None:
    model_query = "SELECT a, b, c FROM my_table"
    dialect = caps.sqlglot_dialect

    # Create a SQL model from the query
    model = SqlModel.from_query_string(query=model_query, dialect=dialect)

    # Ensure the schema contains the table "my_table" with columns a, b, c and additionally d
    schema = create_schema_with_complete_columns("my_table", "text", ["a", "b", "c", "d"])
    schema.tables["my_table"]["columns"]["d"].update({"nullable": nullable})

    if nullable is False:
        with pytest.raises(NormalizeJobFailed) as py_exc:
            select_dialect, normalized_select_query, _ = extract_normalize_retrieve(
                model_normalize, model, schema, "my_table", dialect
            )
        assert isinstance(py_exc.value.__cause__, CannotCoerceNullException)
    else:
        select_dialect, normalized_select_query, _ = extract_normalize_retrieve(
            model_normalize, model, schema, "my_table", dialect
        )

        assert select_dialect == dialect

        # Parse the normalized model query to extract the aliases of the outer select
        parsed_norm_select_query = sqlglot.parse_one(normalized_select_query, read=dialect)
        assert isinstance(parsed_norm_select_query, sqlglot.exp.Select)
        aliases_selects = {select.alias: select for select in parsed_norm_select_query.selects}
        assert list(aliases_selects.keys()) == [
            caps.casefold_identifier(col) for col in ["a", "b", "c", "d", "_dlt_load_id", "_dlt_id"]
        ]
        d_select = aliases_selects[caps.casefold_identifier("d")]
        assert isinstance(d_select.this, sqlglot.exp.Null), "'d' must be selected as NULL"


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_select_column_missing_in_schema(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    # NOTE: This should generally not happen, but we kept the test anyway.
    model_query = "SELECT a, b, c FROM my_table"
    dialect = caps.sqlglot_dialect

    # Create a SQL model from the query
    model = SqlModel.from_query_string(query=model_query, dialect=dialect)

    # Ensure the schema contains the table "my_table" with columns a, c, but not b
    schema = create_schema_with_complete_columns("my_table", "text", ["a", "c"])
    schema.tables["my_table"]["columns"]

    select_dialect, normalized_select_query, _ = extract_normalize_retrieve(
        model_normalize, model, schema, "my_table", dialect
    )

    assert select_dialect == dialect

    # Parse the normalized model query to extract the aliases of the outer select
    parsed_norm_select_query = sqlglot.parse_one(normalized_select_query, read=dialect)
    assert isinstance(parsed_norm_select_query, sqlglot.exp.Select)
    aliases = [select.alias for select in parsed_norm_select_query.selects]
    assert aliases == [
        caps.casefold_identifier(col) for col in ["a", "c", "_dlt_load_id", "_dlt_id"]
    ]


@pytest.mark.parametrize(
    "add_dlt_load_id, add_dlt_id", [(True, True), (True, False), (False, True), (False, False)]
)
@pytest.mark.parametrize("caps", [get_caps("duckdb")], indirect=True, ids=["duckdb"])
def test_dlt_column_addition_configs(
    caps: DestinationCapabilitiesContext, add_dlt_load_id: bool, add_dlt_id: bool, preserve_environ
) -> None:
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_LOAD_ID"] = str(add_dlt_load_id)
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(add_dlt_id)
    model_normalize = next(init_normalize())
    dialect = caps.sqlglot_dialect

    schema = create_schema_with_complete_columns("my_table", "text", ["a", "b"])

    model = SqlModel.from_query_string(query="SELECT a, b FROM my_table")
    _, normalized_query, load_id = extract_normalize_retrieve(
        model_normalize, model, schema, "my_table", dialect
    )

    another_model = SqlModel.from_query_string(
        query="SELECT a, b, MY_CUSTOM_FUNC() AS _dlt_id FROM my_table"
    )
    _, another_norm_query, another_load_id = extract_normalize_retrieve(
        model_normalize, another_model, schema, "my_table", dialect
    )

    if add_dlt_load_id and add_dlt_id:
        assert (
            normalized_query
            == f'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", \'{load_id}\' AS'
            ' "_dlt_load_id", UUID() AS "_dlt_id" FROM (SELECT a, b FROM my_table) AS'
            " _dlt_subquery\n"
        )
        assert (
            another_norm_query
            == 'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b",'
            f" '{another_load_id}' AS"
            ' "_dlt_load_id", _dlt_subquery."_dlt_id" AS "_dlt_id" FROM (SELECT a, b,'
            " MY_CUSTOM_FUNC() AS _dlt_id FROM my_table) AS _dlt_subquery\n"
        )
    elif add_dlt_load_id and not add_dlt_id:
        assert (
            normalized_query
            == f'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", \'{load_id}\' AS '
            '"_dlt_load_id"'
            " FROM (SELECT a, b FROM my_table) AS _dlt_subquery\n"
        )
        assert (
            another_norm_query
            == 'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b",'
            f" '{another_load_id}' AS"
            ' "_dlt_load_id" FROM (SELECT a, b, MY_CUSTOM_FUNC() AS _dlt_id FROM my_table) AS'
            " _dlt_subquery\n"
        )
    elif not add_dlt_load_id and add_dlt_id:
        assert (
            normalized_query
            == 'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", UUID() AS "_dlt_id" FROM'
            " (SELECT"
            " a, b FROM my_table) AS _dlt_subquery\n"
        )
        assert (
            another_norm_query
            == 'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", _dlt_subquery."_dlt_id"'
            ' AS "_dlt_id" FROM (SELECT a, b, MY_CUSTOM_FUNC() AS _dlt_id FROM my_table) AS'
            " _dlt_subquery\n"
        )
    elif not add_dlt_load_id and not add_dlt_id:
        assert (
            normalized_query
            == 'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b" FROM (SELECT a, b FROM'
            " my_table)"
            " AS _dlt_subquery\n"
        )
        assert (
            another_norm_query
            == 'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b"'
            " FROM (SELECT a, b, MY_CUSTOM_FUNC() AS _dlt_id FROM my_table) AS _dlt_subquery\n"
        )


class EdgeCaseQuery(NamedTuple):
    query: str
    add_dlt_columns: bool
    should_fail: bool
    expected_sql_template: Optional[str]


EDGE_CASE_QUERIES = [
    EdgeCaseQuery(
        query="WITH temp AS (SELECT a, b FROM my_table) SELECT a, b FROM temp",
        add_dlt_columns=True,
        should_fail=False,
        expected_sql_template=(
            'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", NULL AS "d", \'{load_id}\''
            ' AS "_dlt_load_id", UUID() AS "_dlt_id" FROM (WITH temp AS (SELECT a, b FROM my_table)'
            " SELECT a, b FROM temp) AS _dlt_subquery\n"
        ),
    ),
    EdgeCaseQuery(
        query="SELECT t1.*, t2.a FROM (t1 JOIN t2)",
        add_dlt_columns=True,
        should_fail=True,
        expected_sql_template=None,
    ),
    EdgeCaseQuery(
        query="WITH temp AS (SELECT a, b, c FROM my_table) SELECT a, b FROM temp",
        add_dlt_columns=True,
        should_fail=False,
        expected_sql_template=(
            'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", NULL AS "d", \'{load_id}\''
            ' AS "_dlt_load_id", UUID() AS "_dlt_id" FROM (WITH temp AS (SELECT a, b, c FROM'
            " my_table) SELECT a, b FROM temp) AS _dlt_subquery\n"
        ),
    ),
    EdgeCaseQuery(
        query="SELECT a, b FROM (SELECT * FROM my_table)",
        add_dlt_columns=True,
        should_fail=False,
        expected_sql_template=(
            'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", NULL AS "d", \'{load_id}\''
            ' AS "_dlt_load_id", UUID() AS "_dlt_id" FROM (SELECT a, b FROM (SELECT * FROM'
            " my_table)) AS _dlt_subquery\n"
        ),
    ),
    EdgeCaseQuery(
        query="SELECT t1.*, t2.a FROM (t1 JOIN t2)",
        add_dlt_columns=True,
        should_fail=True,
        expected_sql_template=None,
    ),
    EdgeCaseQuery(
        query=(
            "SELECT my_table.b, other_table.a FROM my_table JOIN other_table ON my_table.id ="
            " other_table.id"
        ),
        add_dlt_columns=True,
        should_fail=False,
        expected_sql_template=(
            'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", NULL AS "d", \'{load_id}\''
            ' AS "_dlt_load_id", UUID() AS "_dlt_id" FROM (SELECT my_table.b, other_table.a FROM'
            " my_table JOIN other_table ON my_table.id = other_table.id) AS _dlt_subquery\n"
        ),
    ),
    EdgeCaseQuery(
        query=(
            "SELECT my_table.b AS a, other_table.a AS b FROM my_table JOIN other_table ON"
            " my_table.id = other_table.id"
        ),
        add_dlt_columns=False,
        should_fail=False,
        expected_sql_template=(
            'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", NULL AS "d" FROM (SELECT'
            " my_table.b AS a, other_table.a AS b FROM my_table JOIN other_table ON my_table.id ="
            " other_table.id) AS _dlt_subquery\n"
        ),
    ),
    EdgeCaseQuery(
        query=(
            "SELECT my_table.b AS a, my_table.a AS b FROM (SELECT a AS a, b AS b FROM my_table) AS"
            " my_table"
        ),
        add_dlt_columns=False,
        should_fail=False,
        expected_sql_template=(
            'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", NULL AS "d" FROM (SELECT'
            " my_table.b AS a, my_table.a AS b FROM (SELECT a AS a, b AS b FROM my_table) AS"
            " my_table) AS _dlt_subquery\n"
        ),
    ),
    EdgeCaseQuery(
        query="WITH my_table AS (SELECT 1 AS a, 2 AS b) SELECT a, b FROM my_table",
        add_dlt_columns=False,
        should_fail=False,
        expected_sql_template=(
            'SELECT _dlt_subquery."a" AS "a", _dlt_subquery."b" AS "b", NULL AS "d" FROM (WITH'
            " my_table AS (SELECT 1 AS a, 2 AS b) SELECT a, b FROM my_table) AS"
            " _dlt_subquery\n"
        ),
    ),
]


@pytest.mark.parametrize(
    "case",
    EDGE_CASE_QUERIES,
    ids=[f"query-{i}" for i in range(len(EDGE_CASE_QUERIES))],
)
@pytest.mark.parametrize("caps", [get_caps("duckdb")], indirect=True, ids=["duckdb"])
def test_model_normalizer_edge_cases_on_duckdb(
    caps: DestinationCapabilitiesContext,
    case: EdgeCaseQuery,
    preserve_environ,
):
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_LOAD_ID"] = str(case.add_dlt_columns)
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(case.add_dlt_columns)
    model_normalize = next(init_normalize())
    dialect = caps.sqlglot_dialect

    model = SqlModel.from_query_string(query=case.query, dialect=dialect)
    schema = create_schema_with_complete_columns("my_table", "text", ["a", "b", "d"])

    if not case.should_fail:
        _, normalized_query, load_id = extract_normalize_retrieve(
            model_normalize, model, schema, "my_table", dialect
        )
        expected_sql = case.expected_sql_template.format(load_id=load_id)
        assert normalized_query == expected_sql
    else:
        with pytest.raises(
            NormalizeJobFailed,
            match=r"Model queries using a star \(`\*`\) expression cannot be normalized\.",
        ):
            _, _, _ = extract_normalize_retrieve(
                model_normalize, model, schema, "my_table", dialect
            )
