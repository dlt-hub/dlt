from tests.load.pipeline.test_model_item_format import DESTINATIONS_SUPPORTING_MODEL
from importlib import import_module
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.configuration.container import Container
from dlt.normalize import Normalize
from dlt.common.storages import (
    NormalizeStorage,
    FileStorage,
)
from dlt.extract.extract import ExtractStorage
from dlt.extract.hints import SqlModel
from dlt.common.utils import read_dialect_and_sql
from dlt.common.data_types import TDataType

from tests.utils import clean_test_storage, TEST_DICT_CONFIG_PROVIDER

import pytest
import os

from typing import Iterator, List, Tuple

from dlt.common.schema.schema import Schema
from concurrent.futures import ThreadPoolExecutor

from dlt.common.schema import utils

import sqlglot


def get_caps(dest_name: str):
    dest_cls = getattr(import_module("dlt.destinations"), dest_name)
    return dest_cls().capabilities


MODEL_CAPS = [get_caps(dest) for dest in DESTINATIONS_SUPPORTING_MODEL]


@pytest.fixture
def caps(request) -> Iterator[DestinationCapabilitiesContext]:
    _caps = request.param()
    with Container().injectable_context(_caps):
        yield _caps


@pytest.fixture
def model_normalize() -> Iterator[Normalize]:
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
    table_name: str, data_type: str, columns: List[str]
) -> Schema:
    cols = [utils.new_column(column_name=col, data_type=data_type) for col in columns]  # type: ignore[arg-type]
    table = utils.new_table(table_name, columns=cols)
    schema = Schema("my_schema")
    schema.update_table(table)
    return schema


def extract_normalize_retrieve(
    model_normalize: Normalize,
    model: SqlModel,
    schema: Schema,
    table_name: str,
    fallback_dialect: str,
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
def test_simple_model_normalizing(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    """
    This test demonstrates how a model with simple sql query is transformed by the normalizer for each destination capabilities.
    Explicit queries are used to clearly show the expected transformation process.
    """
    # TODO: tests for dialects teradata, mysql, presto and athena
    dialect = caps.sqlglot_dialect
    model = SqlModel.from_query_string(query="SELECT a, b FROM my_table", dialect=dialect)

    # Ensure the schema contains the table "my_table" with columns a, b
    schema = create_schema_with_complete_columns("my_table", "text", ["a", "b"])

    select_dialect, normalized_select_query, load_id = extract_normalize_retrieve(
        model_normalize, model, schema, "my_table", dialect
    )

    assert select_dialect == dialect
    if dialect == "duckdb":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, UUID() AS"
            " _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == normalized_select_query
        )
    elif dialect == "bigquery":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, GENERATE_UUID()"
            " AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == normalized_select_query
        )
    elif dialect == "clickhouse":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id,"
            " generateUUIDv4() AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == normalized_select_query
        )
    elif dialect == "databricks":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, UUID()"
            " AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == normalized_select_query
        )
    elif dialect == "tsql":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, NEWID()"
            " AS _dlt_id FROM (SELECT a AS a, b AS b FROM my_table) AS subquery\n"
            == normalized_select_query
        )
    elif dialect == "postgres":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id,"
            " GEN_RANDOM_UUID() AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == normalized_select_query
        )
    elif dialect == "redshift":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id,"
            " MD5(CAST(ROW_NUMBER() OVER () AS VARCHAR(MAX))) AS _dlt_id FROM (SELECT a, b FROM"
            " my_table) AS subquery\n"
            == normalized_select_query
        )
    elif dialect == "snowflake":
        assert (
            f"SELECT subquery.a AS A, subquery.b AS B, '{load_id}' AS _DLT_LOAD_ID, UUID_STRING()"
            " AS _DLT_ID FROM (SELECT a, b FROM my_table) AS subquery\n"
            == normalized_select_query
        )
    else:
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, UUID()"
            " AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
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
    monkeypatch,
) -> None:
    monkeypatch.setenv("NORMALIZE__MODEL_NORMALIZER__ADD_DLT_LOAD_ID", str(add_dlt_columns))
    monkeypatch.setenv("NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID", str(add_dlt_columns))
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


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_select_column_added_from_schema(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    model_query = "SELECT a, b, c FROM my_table"
    dialect = caps.sqlglot_dialect

    # Create a SQL model from the query
    model = SqlModel.from_query_string(query=model_query, dialect=dialect)

    # Ensure the schema contains the table "my_table" with columns a, b, c and additionally d
    schema = create_schema_with_complete_columns("my_table", "text", ["a", "b", "c", "d"])

    select_dialect, normalized_select_query, _ = extract_normalize_retrieve(
        model_normalize, model, schema, "my_table", dialect
    )

    assert select_dialect == dialect

    # Parse the normalized model query to extract the aliases of the outer select
    parsed_norm_select_query = sqlglot.parse_one(normalized_select_query, read=dialect)
    assert isinstance(parsed_norm_select_query, sqlglot.exp.Select)
    aliases = [select.alias for select in parsed_norm_select_query.selects]
    assert aliases == [
        caps.casefold_identifier(col) for col in ["a", "b", "c", "d", "_dlt_load_id", "_dlt_id"]
    ]


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_select_column_missing_in_schema(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    model_query = "SELECT a, b, c FROM my_table"
    dialect = caps.sqlglot_dialect

    # Create a SQL model from the query
    model = SqlModel.from_query_string(query=model_query, dialect=dialect)

    # Ensure the schema contains the table "my_table" with columns a, c, but not b
    schema = create_schema_with_complete_columns("my_table", "text", ["a", "c"])

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


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_normalize_typed_json(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    return
