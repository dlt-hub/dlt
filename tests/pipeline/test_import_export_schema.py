from decimal import Decimal
from typing import List, Any, Dict
import dlt, os
import pytest

from dlt.common.schema.utils import has_table_seen_data
from dlt.common.typing import TRefreshMode
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


@pytest.mark.parametrize(
    "edit_in_refresh_run",
    [False, True],
    ids=["import_applied_before", "import_edited_in_refresh_run"],
)
@pytest.mark.parametrize("refresh", ["drop_sources", "drop_resources"])
def test_refresh_recreates_tables_from_import_schema(
    refresh: TRefreshMode, edit_in_refresh_run: bool
) -> None:
    """Tables dropped by refresh are recreated from the import schema and the resource hints,
    for tables inside and outside of the import schema."""

    @dlt.resource
    def runs():
        yield {"id": 1}

    @dlt.resource(
        columns={"rate": {"data_type": "decimal", "precision": 18, "scale": 6, "nullable": False}}
    )
    def prices():
        yield {"label": "a", "rate": Decimal("0.004")}

    @dlt.source
    def actions(with_prices: bool):
        return [runs, prices] if with_prices else [runs]

    p = dlt.pipeline(
        pipeline_name="schema_test" + uniq_id(),
        destination="duckdb",
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
    )
    # the first run writes the import schema, so `prices` stays outside of it
    p.run(actions(with_prices=False))
    p.run(actions(with_prices=True))
    import_schema = _get_import_schema("actions")
    assert "prices" not in import_schema.tables
    runs_table = import_schema.tables["runs"]
    runs_table["description"] = "workflow runs"
    runs_table["columns"]["id"] = {"name": "id", "data_type": "text", "description": "run id"}
    with open(os.path.join(IMPORT_SCHEMA_PATH, "actions.schema.yaml"), "w", encoding="utf-8") as f:
        f.write(import_schema.to_pretty_yaml())

    def assert_schema_hints() -> None:
        schema = p.default_schema
        assert schema.tables["runs"]["description"] == "workflow runs"
        assert schema.tables["runs"]["columns"]["id"]["data_type"] == "text"
        assert schema.tables["runs"]["columns"]["id"]["description"] == "run id"
        rate = schema.tables["prices"]["columns"]["rate"]
        assert (rate["precision"], rate["scale"], rate["nullable"]) == (18, 6, False)
        assert schema._imported_version_hash == _get_import_schema("actions").version_hash

    if not edit_in_refresh_run:
        # applies the edited import schema
        p.run(actions(with_prices=True))
        assert_schema_hints()

    p.run(actions(with_prices=True), refresh=refresh)
    assert_schema_hints()
    # both tables were recreated in the destination
    with p.sql_client() as client:
        rows = client.execute_sql(
            "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns"
            " WHERE table_schema = %s AND column_name IN ('id', 'rate') ORDER BY table_name",
            p.dataset_name,
        )
    assert rows == [("prices", "rate", "DECIMAL(18,6)", "NO"), ("runs", "id", "VARCHAR", "YES")]


def test_import_schema_seen_data_after_dataset_dropped() -> None:
    """A changed import schema keeps that a table seen data. When the dataset is dropped, the
    pipeline sync wipes the pipeline and the schema is imported again without that hint."""
    name = "schema_test" + uniq_id()
    p = dlt.pipeline(
        pipeline_name=name,
        destination="duckdb",
        import_schema_path=IMPORT_SCHEMA_PATH,
        export_schema_path=EXPORT_SCHEMA_PATH,
    )
    p.run(EXAMPLE_DATA, table_name="person")
    import_schema = _get_import_schema(name)
    import_schema.tables["person"]["description"] = "people"
    with open(os.path.join(IMPORT_SCHEMA_PATH, name + ".schema.yaml"), "w", encoding="utf-8") as f:
        f.write(import_schema.to_pretty_yaml())
    person = p.default_schema.tables["person"]
    assert person["description"] == "people"
    assert has_table_seen_data(person)

    with p.destination_client() as client:
        client.drop_storage()
    p.sync_destination()
    assert p.default_schema_name is None
    person = p.schemas[name].tables["person"]
    assert person["description"] == "people"
    assert not has_table_seen_data(person)

    assert_load_info(p.run(EXAMPLE_DATA, table_name="person"))
    assert has_table_seen_data(p.default_schema.tables["person"])


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
        nested_example_data = dict(EXAMPLE_DATA[0])
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
    Test that null columns are not present in schema and when they later
    receive complex data, the nested table is created correctly.
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
        nested_example_data = dict(EXAMPLE_DATA[0])
        children_list: List[Dict[str, Any]] = [{"id": 2, "name": "Max"}]
        nested_example_data[col_a] = children_list
        if with_grandchild:
            children_list[0][col_b] = [{"id": 3, "name": "Maximilian"}]
        else:
            children_list[0][col_b] = None
        yield nested_example_data

    # 1. Column 'b' is null, should NOT appear in schema at all
    p.run(nested_data(with_grandchild=False))

    # Calculate expected table and column names
    norm_col_a, norm_col_b = (
        p.default_schema.naming.shorten_fragments(col) for col in [col_a, col_b]
    )

    nested_tbl = p.default_schema.naming.shorten_fragments("my_table", f"{norm_col_a}")
    nested_nested_tbl = p.default_schema.naming.shorten_fragments(
        "my_table", f"{norm_col_a}", f"{norm_col_b}"
    )

    # null column 'b' should not appear in schema
    export_schema = _get_export_schema(name)
    assert set(export_schema.tables[nested_tbl]["columns"].keys()) == {
        "_dlt_list_idx",
        "_dlt_parent_id",
        "id",
        "_dlt_id",
        "name",
    }

    # 2. Column 'b' gets complex data, should create new nested table
    p.run(nested_data(with_grandchild=True))

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
    Test that null columns are not present in schema and when they later
    receive complex data, compound columns are created correctly.
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

    # 1. Column 'a' is null, should NOT appear in schema at all
    p.run([{"id": 1, col_a: None}], table_name="my_table")

    # Calculate column name
    norm_col_a = p.default_schema.naming.shorten_fragments(col_a)

    # null column 'a' should not appear in schema
    export_schema = _get_export_schema(name)
    assert set(export_schema.tables["my_table"]["columns"].keys()) == {
        "id",
        "_dlt_id",
        "_dlt_load_id",
    }

    # 2. Column 'a' gets complex data, should create compound columns
    p.run([{"id": 1, col_a: {"col1": 1, "col2": "hey"}}], table_name="my_table")

    # compound columns should appear in the table
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
        assert set(export_schema.tables["my_table"]["columns"].keys()) == {
            "id",
            "_dlt_id",
            "_dlt_load_id",
            p.default_schema.naming.shorten_fragments(norm_col_a, "col1"),
            p.default_schema.naming.shorten_fragments(norm_col_a, "col2"),
        }
