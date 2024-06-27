import dlt, os, pytest

from dlt.common.utils import uniq_id

from tests.pipeline.utils import assert_load_info
from tests.utils import TEST_STORAGE_ROOT
from dlt.common.schema import Schema
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt.destinations import dummy


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
        destination=dummy(completed_prob=1),
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
        destination=dummy(completed_prob=1),
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
        destination=dummy(completed_prob=1),
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
    )
    p.run(EXAMPLE_DATA, table_name="person")
    # initial schema + evolved in normalize == version 2
    assert p.default_schema.stored_version == 2
    assert p.default_schema.tables["person"]["columns"]["id"]["data_type"] == "bigint"
    # import schema got saved
    import_schema = _get_import_schema(name)
    assert "person" in import_schema.tables
    # initial schema (after extract) got saved
    assert import_schema.stored_version == 1
    # import schema hash is set
    assert p.default_schema._imported_version_hash == import_schema.version_hash
    assert not p.default_schema.is_modified

    # take default schema, modify column type and save it to import folder
    modified_schema = p.default_schema.clone()
    modified_schema.tables["person"]["columns"]["id"]["data_type"] = "text"
    with open(os.path.join(IMPORT_SCHEMA_PATH, name + ".schema.yaml"), "w", encoding="utf-8") as f:
        f.write(modified_schema.to_pretty_yaml())

    # import schema will be imported into pipeline
    p.run(EXAMPLE_DATA, table_name="person")
    # again: extract + normalize
    assert p.default_schema.stored_version == 3
    # change in pipeline schema
    assert p.default_schema.tables["person"]["columns"]["id"]["data_type"] == "text"
    # import schema is not overwritten
    assert _get_import_schema(name).tables["person"]["columns"]["id"]["data_type"] == "text"

    # when creating a new schema (e.g. with full refresh), this will work
    p = dlt.pipeline(
        pipeline_name=name,
        destination=dummy(completed_prob=1),
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
        dev_mode=True,
    )
    p.extract(EXAMPLE_DATA, table_name="person")
    # starts with import schema v 1 that is dirty -> 2
    assert p.default_schema.stored_version == 3
    p.normalize()
    assert p.default_schema.stored_version == 3
    info = p.load()
    assert_load_info(info)
    assert p.default_schema.stored_version == 3

    assert p.default_schema.tables["person"]["columns"]["id"]["data_type"] == "text"

    # import schema is not overwritten
    assert _get_import_schema(name).tables["person"]["columns"]["id"]["data_type"] == "text"

    # export now includes the modified column type
    export_schema = _get_export_schema(name)
    assert export_schema.tables["person"]["columns"]["id"]["data_type"] == "text"
    assert export_schema.tables["person"]["columns"]["name"]["data_type"] == "text"


def test_only_explicit_hints_in_import_schema() -> None:
    @dlt.source(schema_contract={"columns": "evolve"})
    def source():
        @dlt.resource(primary_key="id", name="person")
        def resource():
            yield EXAMPLE_DATA

        return resource()

    p = dlt.pipeline(
        pipeline_name=uniq_id(),
        destination=dummy(completed_prob=1),
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
        dev_mode=True,
    )
    p.run(source())

    # import schema has only the primary key hint, but no name or data types
    import_schema = _get_import_schema("source")
    assert import_schema.tables["person"]["columns"].keys() == {"id"}
    assert import_schema.tables["person"]["columns"]["id"] == {
        "nullable": False,
        "primary_key": True,
        "name": "id",
    }

    # pipeline schema has all the stuff
    assert p.default_schema.tables["person"]["columns"].keys() == {
        "id",
        "name",
        "_dlt_load_id",
        "_dlt_id",
    }
    assert p.default_schema.tables["person"]["columns"]["id"] == {
        "nullable": False,
        "primary_key": True,
        "name": "id",
        "data_type": "bigint",
    }

    # adding column to the resource will not change the import schema, but the pipeline schema will evolve
    @dlt.resource(primary_key="id", name="person", columns={"email": {"data_type": "text"}})
    def resource():
        yield EXAMPLE_DATA

    p.run(resource())

    # check schemas
    import_schema = _get_import_schema("source")
    assert import_schema.tables["person"]["columns"].keys() == {"id"}
    assert p.default_schema.tables["person"]["columns"].keys() == {
        "id",
        "name",
        "_dlt_load_id",
        "_dlt_id",
        "email",
    }

    # changing the import schema will force full update
    import_schema.tables["person"]["columns"]["age"] = {
        "data_type": "bigint",
        "nullable": True,
        "name": "age",
    }
    with open(
        os.path.join(IMPORT_SCHEMA_PATH, "source" + ".schema.yaml"), "w", encoding="utf-8"
    ) as f:
        f.write(import_schema.to_pretty_yaml())

    # run with the original source, email hint should be gone after this, but we now have age
    p.run(source())

    assert p.default_schema.tables["person"]["columns"].keys() == {
        "id",
        "name",
        "_dlt_load_id",
        "_dlt_id",
        "age",
    }
    import_schema = _get_import_schema("source")
    assert import_schema.tables["person"]["columns"].keys() == {"id", "age"}
