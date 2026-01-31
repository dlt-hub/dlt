from copy import deepcopy
import pathlib
import textwrap

import pytest
from pydbml import PyDBML  # type: ignore[import-untyped]
from pydbml.classes import Reference, Table, Column  # type: ignore[import-untyped]

import dlt
from dlt.common.schema.typing import TColumnSchema, TTableReference, TTableSchema
from dlt.common.schema.utils import remove_column_defaults
from dlt.helpers.dbml import (
    export_to_dbml,
    schema_to_dbml,
    _to_dbml_column,
    _from_dbml_column,
    _to_dbml_table,
    _from_dbml_table,
    _to_dbml_reference,
    _from_dbml_reference,
    _group_tables_by_resource,
)


@pytest.fixture
def example_schema() -> dlt.Schema:
    return dlt.Schema.from_dict(
        {
            "version": 2,
            "version_hash": "iW0MtTw8NXm1r/amMiYpOF63Of44Mx5VfYOh5DM6/7s=",
            "engine_version": 11,
            "name": "fruit_with_ref",
            "tables": {
                "_dlt_version": {
                    "name": "_dlt_version",
                    "columns": {
                        "version": {"name": "version", "data_type": "bigint", "nullable": False},
                        "engine_version": {
                            "name": "engine_version",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "inserted_at": {
                            "name": "inserted_at",
                            "data_type": "timestamp",
                            "nullable": False,
                        },
                        "schema_name": {
                            "name": "schema_name",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "version_hash": {
                            "name": "version_hash",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "schema": {"name": "schema", "data_type": "text", "nullable": False},
                    },
                    "write_disposition": "skip",
                    "resource": "_dlt_version",
                    "description": "Created by DLT. Tracks schema updates",
                },
                "_dlt_loads": {
                    "name": "_dlt_loads",
                    "columns": {
                        "load_id": {"name": "load_id", "data_type": "text", "nullable": False},
                        "schema_name": {
                            "name": "schema_name",
                            "data_type": "text",
                            "nullable": True,
                        },
                        "status": {"name": "status", "data_type": "bigint", "nullable": False},
                        "inserted_at": {
                            "name": "inserted_at",
                            "data_type": "timestamp",
                            "nullable": False,
                        },
                        "schema_version_hash": {
                            "name": "schema_version_hash",
                            "data_type": "text",
                            "nullable": True,
                        },
                    },
                    "write_disposition": "skip",
                    "resource": "_dlt_loads",
                    "description": "Created by DLT. Tracks completed loads",
                },
                "customers": {
                    "columns": {
                        "id": {
                            "name": "id",
                            "nullable": False,
                            "primary_key": True,
                            "data_type": "bigint",
                        },
                        "name": {
                            "x-annotation-pii": True,
                            "name": "name",
                            "data_type": "text",
                            "nullable": True,
                        },
                        "city": {"name": "city", "data_type": "text", "nullable": True},
                        "_dlt_load_id": {
                            "name": "_dlt_load_id",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "write_disposition": "append",
                    "name": "customers",
                    "resource": "customers",
                    "x-normalizer": {"seen-data": True},
                },
                "purchases": {
                    "columns": {
                        "id": {
                            "name": "id",
                            "nullable": False,
                            "primary_key": True,
                            "data_type": "bigint",
                        },
                        "customer_id": {
                            "name": "customer_id",
                            "data_type": "bigint",
                            "nullable": True,
                        },
                        "inventory_id": {
                            "name": "inventory_id",
                            "data_type": "bigint",
                            "nullable": True,
                        },
                        "quantity": {"name": "quantity", "data_type": "bigint", "nullable": True},
                        "date": {"name": "date", "data_type": "text", "nullable": True},
                        "_dlt_load_id": {
                            "name": "_dlt_load_id",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "write_disposition": "append",
                    "references": [
                        {
                            "columns": ["customer_id"],
                            "referenced_table": "customers",
                            "referenced_columns": ["id"],
                        }
                    ],
                    "name": "purchases",
                    "resource": "purchases",
                    "x-normalizer": {"seen-data": True},
                },
                "_dlt_pipeline_state": {
                    "columns": {
                        "version": {"name": "version", "data_type": "bigint", "nullable": False},
                        "engine_version": {
                            "name": "engine_version",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "pipeline_name": {
                            "name": "pipeline_name",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "state": {"name": "state", "data_type": "text", "nullable": False},
                        "created_at": {
                            "name": "created_at",
                            "data_type": "timestamp",
                            "nullable": False,
                        },
                        "version_hash": {
                            "name": "version_hash",
                            "data_type": "text",
                            "nullable": True,
                        },
                        "_dlt_load_id": {
                            "name": "_dlt_load_id",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "write_disposition": "append",
                    "file_format": "preferred",
                    "name": "_dlt_pipeline_state",
                    "resource": "_dlt_pipeline_state",
                    "x-normalizer": {"seen-data": True},
                },
                "purchases__items": {
                    "name": "purchases__items",
                    "columns": {
                        "name": {"name": "name", "data_type": "text", "nullable": True},
                        "price": {"name": "price", "data_type": "bigint", "nullable": True},
                        "_dlt_parent_id": {
                            "name": "_dlt_parent_id",
                            "data_type": "text",
                            "nullable": False,
                            "parent_key": True,
                        },
                        "_dlt_list_idx": {
                            "name": "_dlt_list_idx",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "parent": "purchases",
                    "x-normalizer": {"seen-data": True},
                },
            },
            "settings": {
                "detections": ["iso_timestamp"],
                "default_hints": {
                    "not_null": [
                        "_dlt_id",
                        "_dlt_root_id",
                        "_dlt_parent_id",
                        "_dlt_list_idx",
                        "_dlt_load_id",
                    ],
                    "parent_key": ["_dlt_parent_id"],
                    "root_key": ["_dlt_root_id"],
                    "unique": ["_dlt_id"],
                    "row_key": ["_dlt_id"],
                },
            },
            "normalizers": {
                "names": "snake_case",
                "json": {"module": "dlt.common.normalizers.json.relational"},
            },
            "previous_hashes": [
                "+stnjP5XdPbykNQJVpK/zpfo0iVbyRFfSIIRzuPzcI4=",
                "nTU+qnLwEmiMSWTwu+QH321j4zl8NrOVL4Hx/GxQAHE=",
            ],
        }
    )


def assert_equal_dbml_columns(col1: Column, col2: Column) -> None:
    """Assert two dbml columns are equal, ignoring the `properties` field

    We use the `properties` field for internal purposes and it shouldn't
    be considered for equality comparison.
    """
    assert col1.name == col2.name
    assert col1.type == col2.type
    assert col1.unique == col2.unique
    assert col1.not_null == col2.not_null
    assert col1.pk == col2.pk
    assert col1.note == col2.note
    assert col1.autoinc == col2.autoinc
    assert col1.comment == col2.comment
    assert col1.default == col2.default
    assert col1.properties == col2.properties
    # we don't compare `.table` because of circular references
    # between PyDBML `Column` and `Table` objects.
    # assert col1.table == col2.table


def assert_equal_dbml_tables(table1: Table, table2: Table) -> None:
    """Assert two dbml tables are equal, ignoring the `properties` field

    We use the `properties` field for internal purposes and it shouldn't
    be considered for equality comparison.
    """
    assert table1.name == table2.name
    assert table1.note == table2.note
    assert table1.schema == table2.schema
    assert table1.comment == table2.comment
    assert table1.indexes == table2.indexes
    assert table1.alias == table2.alias
    assert table1.header_color == table2.header_color
    assert table1.abstract == table2.abstract
    assert table1.properties == table2.properties

    for col1, col2 in zip(table1.columns, table2.columns):
        assert_equal_dbml_columns(col1, col2)


def assert_equal_dbml_references(ref1: Reference, ref2: Reference) -> None:
    assert ref1.database == ref2.database
    assert ref1.type == ref2.type
    assert ref1.name == ref2.name
    assert ref1.comment == ref2.comment
    assert ref1.on_update == ref2.on_update
    assert ref1.on_delete == ref2.on_delete
    assert ref1.inline == ref2.inline

    for from_col1, from_col2 in zip(ref1.col1, ref2.col1):
        assert_equal_dbml_columns(from_col1, from_col2)

    for to_col1, to_col2 in zip(ref1.col2, ref2.col2):
        assert_equal_dbml_columns(to_col1, to_col2)


@pytest.mark.parametrize(
    "hints,dbml_col",
    [
        (
            {"name": "simple_col", "data_type": "text"},
            Column(name="simple_col", type="text"),
        ),
        (
            {"name": "nullable_col", "data_type": "text", "nullable": False},
            Column(name="nullable_col", type="text", not_null=True),
        ),
        (
            {"name": "nullable_col", "data_type": "text", "nullable": True},  # default value
            Column(name="nullable_col", type="text", not_null=False),
        ),
        (
            {"name": "unique_col", "data_type": "text", "unique": True},  # default value
            Column(name="unique_col", type="text", unique=True),
        ),
        (
            {"name": "unique_col", "data_type": "text", "unique": False},
            Column(name="unique_col", type="text", unique=False),
        ),
        (
            {"name": "primary_key_col", "data_type": "text", "primary_key": True},
            Column(name="primary_key_col", type="text", pk=True),
        ),
        (
            {"name": "description_col", "data_type": "text", "description": "foo"},
            Column(name="description_col", type="text", note="foo"),
        ),
        (
            {"name": "custom_bool_col", "data_type": "text", "x-pii": True},
            Column(name="custom_bool_col", type="text", properties={"x-pii": "True"}),
        ),
        (
            {"name": "custom_str_col", "data_type": "text", "x-label": "custom"},
            Column(name="custom_str_col", type="text", properties={"x-label": "custom"}),
        ),
        (
            # we ignore `x-normalizer` because it's a processing hint to indicate `data_type` is not set yet
            {
                "name": "unknown_data_type_col",
                "nullable": True,
                "x-normalizer": {"seen-null-first": True},
            },
            Column(name="unknown_data_type_col", type="UNKNOWN", not_null=False),
        ),
    ],
)
def test_to_and_from_dbml_column(hints: TColumnSchema, dbml_col: Column) -> None:
    """Test `dlt -> dbml -> dlt`.

    This is different from `dbml -> dlt` because we assume that the dbml column
    includes some metadata stored on `properties` field.
    """
    hints_without_defaults_and_procesing_hints = remove_column_defaults(deepcopy(hints))
    for hint in ("x-normalizer", "x-loader", "x-extractor"):
        hints_without_defaults_and_procesing_hints.pop(hint, None)  # type: ignore[misc]

    # dlt -> dbml
    inferred_dbml_col = _to_dbml_column(hints)
    assert_equal_dbml_columns(dbml_col, inferred_dbml_col)

    # dbml -> dlt
    inferred_hints = _from_dbml_column(dbml_col)
    assert inferred_hints == hints_without_defaults_and_procesing_hints

    # dlt -> dbml -> dlt
    inferred_hints_from_inferred_col = _from_dbml_column(inferred_dbml_col)
    assert inferred_hints_from_inferred_col == hints_without_defaults_and_procesing_hints


# NOTE this test doesn't include `references` field because creating `references`
# requires building all `Table` objects first.
@pytest.mark.parametrize(
    "table_schema,dbml_table",
    [
        (
            {
                "name": "simple_table",
                "columns": {
                    "foo": {"name": "foo", "data_type": "text"},
                    "bar": {"name": "bar", "data_type": "bigint"},
                },
            },
            Table(
                name="simple_table",
                columns=[Column(name="foo", type="text"), Column(name="bar", type="bigint")],
            ),
        ),
        (
            {
                "name": "table_with_description",
                "columns": {"foo": {"name": "foo", "data_type": "text"}},
                "description": "my description",
            },
            Table(
                name="table_with_description",
                columns=[Column(name="foo", type="text")],
                note="my description",
            ),
        ),
        # this is a valid DBML table object, but the rendered output is invalid
        # the function `schema_to_dbml()` should skip rendering it
        (
            {"name": "table_without_columns", "columns": {}},
            Table(name="table_without_columns", columns=[]),
        ),
    ],
)
def test_to_and_from_dbml_table(table_schema: TTableSchema, dbml_table: Table) -> None:
    """Test `dlt -> dbml -> dlt`.

    This is different from `dbml -> dlt` because we assume that the dbml table
    includes some metadata stored on `properties` field.
    """
    # dlt -> dbml
    inferred_dbml_table = _to_dbml_table(table_schema)
    assert_equal_dbml_tables(dbml_table, inferred_dbml_table)

    # dbml -> dlt
    inferred_hints = _from_dbml_table(dbml_table)
    assert table_schema == inferred_hints

    # dlt -> dbml -> dlt
    inferred_hints_from_inferred_table = _from_dbml_table(inferred_dbml_table)
    assert table_schema == inferred_hints_from_inferred_table


def test_to_and_from_dbml_reference() -> None:
    tables = [
        Table(
            name="customers",
            columns=[Column(name="id", type="text"), Column(name="name", type="text")],
        ),
        Table(
            name="orders",
            columns=[
                Column(name="customer_id", type="text"),
                Column(name="order_id", type="bigint"),
            ],
        ),
    ]
    expected_dbml_reference = Reference(
        type="<>",  # default cardinality; loosest setting
        col1=[tables[0].columns[0]],  # refers to `customers.id`
        col2=[tables[1].columns[0]],  # refers to `orders.customer_id`
    )
    expected_dlt_reference = TTableReference(
        table="customers",
        columns=["id"],
        referenced_columns=["customer_id"],
        referenced_table="orders",
    )

    # dlt -> dbml
    inferred_dbml_reference = _to_dbml_reference(
        from_table_name="customers",
        reference=expected_dlt_reference,
        tables=tables,
        cardinality="<>",
    )
    assert_equal_dbml_references(expected_dbml_reference, inferred_dbml_reference)

    # dbml -> dlt
    inferred_dlt_reference = _from_dbml_reference(expected_dbml_reference)
    assert expected_dlt_reference == inferred_dlt_reference

    # dlt -> dbml -> dlt
    inferred_dlt_reference_from_inferred_reference = _from_dbml_reference(inferred_dbml_reference)
    assert expected_dlt_reference == inferred_dlt_reference_from_inferred_reference


def test_schema_to_dbml(example_schema: dlt.Schema) -> None:
    expected_dbml = textwrap.dedent("""\
        Table "customers" {
            "id" bigint [pk, not null]
            "name" text
            "city" text
            "_dlt_load_id" text [not null]
            "_dlt_id" text [unique, not null]
        }

        Table "purchases" {
            "id" bigint [pk, not null]
            "customer_id" bigint
            "inventory_id" bigint
            "quantity" bigint
            "date" text
            "_dlt_load_id" text [not null]
            "_dlt_id" text [unique, not null]
        }

        Table "purchases__items" {
            "name" text
            "price" bigint
            "_dlt_parent_id" text [not null]
            "_dlt_list_idx" bigint [not null]
            "_dlt_id" text [unique, not null]
        }

        Table "_dlt_version" {
            "version" bigint [not null]
            "engine_version" bigint [not null]
            "inserted_at" timestamp [not null]
            "schema_name" text [not null]
            "version_hash" text [not null]
            "schema" text [not null]
            Note {
                'Created by DLT. Tracks schema updates'
            }
        }

        Table "_dlt_loads" {
            "load_id" text [not null]
            "schema_name" text
            "status" bigint [not null]
            "inserted_at" timestamp [not null]
            "schema_version_hash" text
            Note {
                'Created by DLT. Tracks completed loads'
            }
        }

        Table "_dlt_pipeline_state" {
            "version" bigint [not null]
            "engine_version" bigint [not null]
            "pipeline_name" text [not null]
            "state" text [not null]
            "created_at" timestamp [not null]
            "version_hash" text
            "_dlt_load_id" text [not null]
            "_dlt_id" text [unique, not null]
        }

        Ref {
            "customers"."_dlt_load_id" > "_dlt_loads"."load_id"
        }

        Ref {
            "purchases"."customer_id" <> "customers"."id"
        }

        Ref {
            "purchases"."_dlt_load_id" > "_dlt_loads"."load_id"
        }

        Ref {
            "_dlt_pipeline_state"."_dlt_load_id" > "_dlt_loads"."load_id"
        }

        Ref {
            "purchases__items"."_dlt_parent_id" > "purchases"."_dlt_id"
        }

        Ref {
            "_dlt_version"."version_hash" < "_dlt_loads"."schema_version_hash"
        }

        Ref {
            "_dlt_version"."schema_name" <> "_dlt_loads"."schema_name"
        }

        TableGroup "customers" {
            "customers"
        }

        TableGroup "purchases" {
            "purchases"
            "purchases__items"
        }

        TableGroup "_dlt" {
            "_dlt_version"
            "_dlt_loads"
            "_dlt_pipeline_state"
        }""")

    stored_schema = example_schema.to_dict()
    dbml_schema = schema_to_dbml(stored_schema, group_by_resource=True)
    assert dbml_schema.dbml == expected_dbml


def test_group_tables_by_resource(example_schema: dlt.Schema) -> None:
    stored_schema = example_schema.to_dict()
    dbml_schema = schema_to_dbml(stored_schema)
    dbml_table_groups = _group_tables_by_resource(schema=stored_schema, db=dbml_schema)

    assert len(dbml_table_groups) == 3

    customers_group = dbml_table_groups[0]
    purchases_group = dbml_table_groups[1]
    dlt_group = dbml_table_groups[2]

    assert customers_group.name == "customers"
    assert purchases_group.name == "purchases"
    assert dlt_group.name == "_dlt"

    assert len(customers_group.items) == 1
    customers_group_members = set([item.name for item in customers_group.items])
    assert customers_group_members == {"customers"}

    assert len(purchases_group.items) == 2
    purchases_group_members = set([item.name for item in purchases_group.items])
    assert purchases_group_members == {"purchases", "purchases__items"}

    assert len(dlt_group.items) == 3
    dlt_group_members = set([item.name for item in dlt_group.items])
    assert dlt_group_members == {"_dlt_loads", "_dlt_version", "_dlt_pipeline_state"}


def test_export_to_dbml_as_string(example_schema: dlt.Schema) -> None:
    stored_schema = example_schema.to_dict()
    dbml_schema = schema_to_dbml(stored_schema)
    expected_output = PyDBML(dbml_schema.dbml)

    output = export_to_dbml(example_schema, path=None)
    loaded_dbml = PyDBML(output)

    assert isinstance(output, str)
    # for some reason, the PyDBML objects are not directly equal
    assert expected_output.dbml == loaded_dbml.dbml


def test_export_to_dbml_to_file(example_schema: dlt.Schema, tmp_path: pathlib.Path) -> None:
    stored_schema = example_schema.to_dict()
    dbml_schema = schema_to_dbml(stored_schema)
    expected_output = PyDBML(dbml_schema.dbml)

    file_path = tmp_path / "my_schema.dbml"
    output = export_to_dbml(example_schema, path=file_path)
    # PyDBML can directly load from file
    loaded_dbml = PyDBML(file_path)

    assert isinstance(output, str)
    # for some reason, the PyDBML objects are not directly equal
    assert expected_output.dbml == loaded_dbml.dbml


@pytest.mark.parametrize("remove_processing_hints", (True, False))
def test_schema_to_dbml_method(example_schema: dlt.Schema, remove_processing_hints: bool) -> None:
    """Test that export method and functions are equivalent.
    Assert if `Schema.to_dbml()` should impact DBML output.
    """
    # `export_to_dbml()` implicitly calls `Schema.to_dict()` with default kwargs.
    dbml_from_function = export_to_dbml(example_schema)
    dbml_from_method = example_schema.to_dbml(remove_processing_hints=remove_processing_hints)

    assert dbml_from_function == dbml_from_method
