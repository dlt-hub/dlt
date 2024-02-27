import dlt, os

from dlt.common.utils import uniq_id

from tests.utils import TEST_STORAGE_ROOT
from dlt.common.schema import Schema
from dlt.common.storages.schema_storage import SchemaStorage


IMPORT_SCHEMA_PATH = os.path.join(TEST_STORAGE_ROOT, "schemas", "import")
EXPORT_SCHEMA_PATH = os.path.join(TEST_STORAGE_ROOT, "schemas", "export")


EXAMPLE_DATA = [{"id": 1, "name": "dave"}]


def _get_import_schema(schema_name: str) -> Schema:
    return SchemaStorage.load_schema_file(IMPORT_SCHEMA_PATH, schema_name)


def _get_export_schema(schema_name: str) -> Schema:
    return SchemaStorage.load_schema_file(EXPORT_SCHEMA_PATH, schema_name)


def test_schemas_files_get_created() -> None:
    name = "schema_test" + uniq_id()

    p = dlt.pipeline(
        pipeline_name=name,
        destination="duckdb",
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
    )

    p.run(EXAMPLE_DATA, table_name="person")

    # basic check we have the table def in the export schema
    export_schema = _get_export_schema(name)
    assert export_schema.tables["person"]["columns"]["id"]["data_type"] == "bigint"
    assert export_schema.tables["person"]["columns"]["name"]["data_type"] == "text"

    # discovered columns are not present in the import schema
    import_schema = _get_import_schema(name)
    assert "id" not in import_schema.tables["person"]["columns"]
    assert "name" not in import_schema.tables["person"]["columns"]


def test_provided_columns_exported_to_import() -> None:
    name = "schema_test" + uniq_id()

    p = dlt.pipeline(
        pipeline_name=name,
        destination="duckdb",
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
    )

    p.run(EXAMPLE_DATA, table_name="person", columns={"id": {"data_type": "text"}})

    # updated columns are in export
    export_schema = _get_export_schema(name)
    assert export_schema.tables["person"]["columns"]["id"]["data_type"] == "text"
    assert export_schema.tables["person"]["columns"]["name"]["data_type"] == "text"

    # discovered columns are not present in the import schema
    # but provided column is
    import_schema = _get_import_schema(name)
    assert "name" not in import_schema.tables["person"]["columns"]
    assert import_schema.tables["person"]["columns"]["id"]["data_type"] == "text"


def test_import_schema_is_respected() -> None:
    name = "schema_test" + uniq_id()

    p = dlt.pipeline(
        pipeline_name=name,
        destination="duckdb",
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
    )
    p.run(EXAMPLE_DATA, table_name="person")
    assert p.default_schema.tables["person"]["columns"]["id"]["data_type"] == "bigint"

    # take default schema, modify column type and save it to import folder
    modified_schema = p.default_schema.clone()
    modified_schema.tables["person"]["columns"]["id"]["data_type"] = "text"
    with open(os.path.join(IMPORT_SCHEMA_PATH, name + ".schema.yaml"), "w", encoding="utf-8") as f:
        f.write(modified_schema.to_pretty_yaml())

    # run load again import schema is not used as a schema exists
    p.run(EXAMPLE_DATA, table_name="person")
    assert p.default_schema.tables["person"]["columns"]["id"]["data_type"] == "bigint"

    # import schema is not overwritten
    assert _get_import_schema(name).tables["person"]["columns"]["id"]["data_type"] == "text"

    # when creating a new schema (e.g. with full refresh), this will work
    p = dlt.pipeline(
        pipeline_name=name,
        destination="duckdb",
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
        full_refresh=True,
    )
    p.run(EXAMPLE_DATA, table_name="person")
    assert p.default_schema.tables["person"]["columns"]["id"]["data_type"] == "text"

    # import schema is not overwritten
    assert _get_import_schema(name).tables["person"]["columns"]["id"]["data_type"] == "text"

    # export now includes the modified column type
    export_schema = _get_export_schema(name)
    assert export_schema.tables["person"]["columns"]["id"]["data_type"] == "text"
    assert export_schema.tables["person"]["columns"]["name"]["data_type"] == "text"
