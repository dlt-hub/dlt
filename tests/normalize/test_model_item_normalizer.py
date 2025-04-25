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

from tests.utils import clean_test_storage, TEST_DICT_CONFIG_PROVIDER

import pytest

from typing import Iterator

from dlt.common.schema.schema import Schema
from concurrent.futures import ThreadPoolExecutor


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


@pytest.mark.parametrize("caps", MODEL_CAPS, indirect=True, ids=DESTINATIONS_SUPPORTING_MODEL)
def test_model_item_normalizer(
    caps: DestinationCapabilitiesContext, model_normalize: Normalize
) -> None:
    # TODO: test on unallowed characters, casefolding etc
    dialect = caps.sqlglot_dialect
    model = SqlModel.from_query_string(query="SELECT a, b FROM my_table", dialect=dialect)
    load_id = extract_model(
        model_normalize.normalize_storage, model, Schema("my_schema"), "my_table"
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        model_normalize.run(pool)
    model_files = model_normalize.load_storage.list_new_jobs(load_id)
    model_full_path = model_normalize.load_storage.normalized_packages.storage.make_full_path(
        model_files[0]
    )

    with FileStorage.open_zipsafe_ro(model_full_path, "r", encoding="utf-8") as f:
        select_dialect, select_statement = read_dialect_and_sql(
            file_obj=f,
            fallback_dialect=dialect,
        )

    assert select_dialect == dialect
    if dialect == "duckdb":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, UUID() AS"
            " _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == select_statement
        )
    elif dialect == "bigquery":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, GENERATE_UUID()"
            " AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == select_statement
        )
    elif dialect == "clickhouse":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id,"
            " generateUUIDv4() AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == select_statement
        )
    elif dialect == "databricks":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, UUID()"
            " AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == select_statement
        )
    elif dialect == "tsql":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, NEWID()"
            " AS _dlt_id FROM (SELECT a AS a, b AS b FROM my_table) AS subquery\n"
            == select_statement
        )
    elif dialect == "postgres":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id,"
            " GEN_RANDOM_UUID() AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == select_statement
        )
    elif dialect == "redshift":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id,"
            " MD5(CAST(ROW_NUMBER() OVER () AS VARCHAR(MAX))) AS _dlt_id FROM (SELECT a, b FROM"
            " my_table) AS subquery\n"
            == select_statement
        )
    elif dialect == "snowflake":
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, UUID_STRING()"
            " AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == select_statement
        )
    else:
        assert (
            f"SELECT subquery.a AS a, subquery.b AS b, '{load_id}' AS _dlt_load_id, UUID()"
            " AS _dlt_id FROM (SELECT a, b FROM my_table) AS subquery\n"
            == select_statement
        )
