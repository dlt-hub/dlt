from typing import List, Any, Dict
import dlt, os
import pytest

from dlt.common.utils import uniq_id

from tests.pipeline.utils import assert_load_info
from tests.utils import get_test_storage_root
from dlt.common.schema import Schema
from dlt.common.storages.schema_storage import SchemaStorage

from dlt.destinations import dummy


IMPORT_SCHEMA_PATH = os.path.join(get_test_storage_root(), "schemas", "import")
EXPORT_SCHEMA_PATH = os.path.join(get_test_storage_root(), "schemas", "export")


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
        pipeline_name="p" + uniq_id(),
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


@pytest.mark.parametrize(
    "table_nesting",
    [0, 1],
)
def test_import_schema_preserves_max_nesting(table_nesting: int) -> None:
    # Add nested field
    @dlt.resource(max_table_nesting=table_nesting, table_name="my_table")
    def nested_data():
        nested_example_data = EXAMPLE_DATA[0]
        nested_example_data["children"] = [{"id": 2, "name": "Max"}, {"id": 3, "name": "Julia"}]
        yield nested_example_data

    p = dlt.pipeline(
        pipeline_name="schema_test" + uniq_id(),
        destination="duckdb",
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
    )
    p.run(nested_data())

    if table_nesting == 0:
        assert "my_table__children" not in p.default_schema.tables
    else:
        assert "my_table__children" in p.default_schema.tables


def test_empty_column_later_becoming_child_table_removed() -> None:
    """
    Test that columns with `seen-null-first` hints are properly removed
    from the export schema when they become nested tables.
    """
    name = "schema_test" + uniq_id()
    p = dlt.pipeline(
        pipeline_name=name,
        destination=dummy(completed_prob=1),
        export_schema_path=EXPORT_SCHEMA_PATH,
    )

    # Create column names that exceed max identifier length
    # to ensure that shortened names of nested tables are internally still correctly
    # tracked back to column names in the respective parent tables
    col_a, col_b = (ch * (p.destination.capabilities().max_identifier_length + 1) for ch in "ab")

    @dlt.resource(table_name="my_table")
    def nested_data(with_grandchild: bool):
        nested_example_data = EXAMPLE_DATA[0]
        children_list: List[Dict[str, Any]] = [{"id": 2, "name": "Max"}]
        nested_example_data[col_a] = children_list
        if with_grandchild:
            children_list[0][col_b] = [{"id": 3, "name": "Maximilian"}]
        else:
            children_list[0][col_b] = None
        yield nested_example_data

    # 1. Column 'b' is null, should get 'seen-null-first' hint
    p.run(nested_data(with_grandchild=False))

    # Calculate expected table and column names
    norm_col_a, norm_col_b = (
        p.default_schema.naming.shorten_fragments(col) for col in [col_a, col_b]
    )

    nested_tbl = p.default_schema.naming.shorten_fragments("my_table", f"{norm_col_a}")
    nested_nested_tbl = p.default_schema.naming.shorten_fragments(
        "my_table", f"{norm_col_a}", f"{norm_col_b}"
    )

    # Column 'b' should exist with 'seen-null-first' hint in the table nested_tbl_name
    export_schema = _get_export_schema(name)
    assert set(export_schema.tables[nested_tbl]["columns"].keys()) == {
        "_dlt_list_idx",
        "_dlt_parent_id",
        "id",
        "_dlt_id",
        "name",
        norm_col_b,
    }
    assert (
        export_schema.tables[nested_tbl]["columns"]
        .get(norm_col_b)["x-normalizer"]
        .get("seen-null-first", False)
    )

    # 2. Column 'b' gets complex data, should create new nested table
    p.run(nested_data(with_grandchild=True))

    # Column 'b' should be removed from the parent table nested_tbl_name
    # because it now has its own nested table nested_nested_tbl_name
    export_schema = _get_export_schema(name)
    assert set(export_schema.tables[nested_tbl]["columns"].keys()) == {
        "_dlt_list_idx",
        "_dlt_parent_id",
        "id",
        "_dlt_id",
        "name",
    }
    assert nested_tbl in export_schema.tables
    assert nested_nested_tbl in export_schema.tables
    assert nested_tbl in p.default_schema.tables
    assert nested_nested_tbl in p.default_schema.tables


@pytest.mark.parametrize(
    "use_long_col_name",
    [True, False],
    ids=["long_col_names", "short_col_names"],
)
def test_empty_column_later_becoming_compound_columns_removed(use_long_col_name: bool) -> None:
    """
    Test that columns with `seen-null-first` hints are properly removed
    from the export schema when they become compound column(s) in the same table.
    """
    name = "schema_test" + uniq_id()
    p = dlt.pipeline(
        pipeline_name=name,
        destination=dummy(completed_prob=1),
        export_schema_path=EXPORT_SCHEMA_PATH,
    )

    col_a = (
        "a" * (p.destination.capabilities().max_identifier_length + 1) if use_long_col_name else "a"
    )

    # 1. Column 'a' is null, should get 'seen-null-first' hint
    p.run([{"id": 1, col_a: None}], table_name="my_table")

    # Calculate column name
    norm_col_a = p.default_schema.naming.shorten_fragments(col_a)

    # Column 'a' should exist with 'seen-null-first' hint in the table
    export_schema = _get_export_schema(name)
    assert set(export_schema.tables["my_table"]["columns"].keys()) == {
        "id",
        norm_col_a,
        "_dlt_id",
        "_dlt_load_id",
    }
    assert (
        export_schema.tables["my_table"]["columns"]
        .get(norm_col_a)["x-normalizer"]
        .get("seen-null-first", False)
    )

    # 2. Column 'a' gets complex data, should create compound columns
    p.run([{"id": 1, col_a: {"col1": 1, "col2": "hey"}}], table_name="my_table")

    # Column 'a' should be removed from the table
    # because the values were normalized to compound columns
    export_schema = _get_export_schema(name)
    if not use_long_col_name:
        assert set(export_schema.tables["my_table"]["columns"].keys()) == {
            "id",
            "_dlt_id",
            "_dlt_load_id",
            "a__col1",
            "a__col2",
        }
    else:
        # TODO: Currently we don't properly remove columns with `seen-null-first` hint
        # when they become compound column(s) in the same table if they have very long names
        # because we're merely using the naming convention's path separator
        # to detect whether a column is a compund column.
        # See Normalize.clean_x_normalizer and JsonLItemsNormalizer._coerce_null_value
        assert set(export_schema.tables["my_table"]["columns"].keys()) == {
            "id",
            "_dlt_id",
            "_dlt_load_id",
            norm_col_a,
            p.default_schema.naming.shorten_fragments(norm_col_a, "col1"),
            p.default_schema.naming.shorten_fragments(norm_col_a, "col2"),
        }
