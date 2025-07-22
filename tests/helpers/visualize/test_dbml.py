import pathlib
import textwrap

import pytest
from pydbml import PyDBML  # type: ignore[import-untyped]
from pydbml.classes import Reference, Table, Column  # type: ignore[import-untyped]

import dlt
from dlt.common.schema.typing import TColumnSchema, TTableReference, TTableSchema
from dlt.helpers.visualize._dbml import (
    DEFAULT_RELATION_TYPE,
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
            "version": 16,
            "version_hash": "C5An8WClbavalXDdNSqXbdI7Swqh/mTWMcwWKCF//EE=",
            "engine_version": 8,
            "name": "ethereum",
            "tables": {
                "_dlt_loads": {
                    "columns": {
                        "load_id": {"nullable": False, "data_type": "text", "name": "load_id"},
                        "schema_name": {
                            "nullable": True,
                            "data_type": "text",
                            "name": "schema_name",
                        },
                        "status": {"nullable": False, "data_type": "bigint", "name": "status"},
                        "inserted_at": {
                            "nullable": False,
                            "data_type": "timestamp",
                            "name": "inserted_at",
                        },
                        "schema_version_hash": {
                            "nullable": True,
                            "data_type": "text",
                            "name": "schema_version_hash",
                        },
                    },
                    "write_disposition": "skip",
                    "description": "Created by DLT. Tracks completed loads",
                    "schema_contract": {},
                    "name": "_dlt_loads",
                    "resource": "_dlt_loads",
                },
                "_dlt_version": {
                    "columns": {
                        "version": {"nullable": False, "data_type": "bigint", "name": "version"},
                        "engine_version": {
                            "nullable": False,
                            "data_type": "bigint",
                            "name": "engine_version",
                        },
                        "inserted_at": {
                            "nullable": False,
                            "data_type": "timestamp",
                            "name": "inserted_at",
                        },
                        "schema_name": {
                            "nullable": False,
                            "data_type": "text",
                            "name": "schema_name",
                        },
                        "version_hash": {
                            "nullable": False,
                            "data_type": "text",
                            "name": "version_hash",
                        },
                        "schema": {"nullable": False, "data_type": "text", "name": "schema"},
                    },
                    "write_disposition": "skip",
                    "description": "Created by DLT. Tracks schema updates",
                    "schema_contract": {},
                    "name": "_dlt_version",
                    "resource": "_dlt_version",
                },
                "blocks": {
                    "description": "Ethereum blocks",
                    "x-annotation": "this will be preserved on save",
                    "write_disposition": "append",
                    "filters": {"includes": [], "excludes": []},
                    "columns": {
                        "_dlt_load_id": {
                            "nullable": False,
                            "description": "load id coming from the extractor",
                            "data_type": "text",
                            "name": "_dlt_load_id",
                        },
                        "_dlt_id": {
                            "nullable": False,
                            "unique": True,
                            "data_type": "text",
                            "name": "_dlt_id",
                        },
                        "number": {
                            "nullable": False,
                            "primary_key": True,
                            "data_type": "bigint",
                            "name": "number",
                        },
                        "parent_hash": {
                            "nullable": True,
                            "data_type": "text",
                            "name": "parent_hash",
                        },
                        "hash": {
                            "nullable": False,
                            "cluster": True,
                            "unique": True,
                            "data_type": "text",
                            "name": "hash",
                        },
                        "base_fee_per_gas": {
                            "nullable": False,
                            "data_type": "wei",
                            "name": "base_fee_per_gas",
                        },
                        "difficulty": {"nullable": False, "data_type": "wei", "name": "difficulty"},
                        "extra_data": {"nullable": True, "data_type": "text", "name": "extra_data"},
                        "gas_limit": {
                            "nullable": False,
                            "data_type": "bigint",
                            "name": "gas_limit",
                        },
                        "gas_used": {"nullable": False, "data_type": "bigint", "name": "gas_used"},
                        "logs_bloom": {
                            "nullable": True,
                            "data_type": "binary",
                            "name": "logs_bloom",
                        },
                        "miner": {"nullable": True, "data_type": "text", "name": "miner"},
                        "mix_hash": {"nullable": True, "data_type": "text", "name": "mix_hash"},
                        "nonce": {"nullable": True, "data_type": "text", "name": "nonce"},
                        "receipts_root": {
                            "nullable": True,
                            "data_type": "text",
                            "name": "receipts_root",
                        },
                        "sha3_uncles": {
                            "nullable": True,
                            "data_type": "text",
                            "name": "sha3_uncles",
                        },
                        "size": {"nullable": True, "data_type": "bigint", "name": "size"},
                        "state_root": {
                            "nullable": False,
                            "data_type": "text",
                            "name": "state_root",
                        },
                        "timestamp": {
                            "nullable": False,
                            "unique": True,
                            "sort": True,
                            "data_type": "timestamp",
                            "name": "timestamp",
                        },
                        "total_difficulty": {
                            "nullable": True,
                            "data_type": "wei",
                            "name": "total_difficulty",
                        },
                        "transactions_root": {
                            "nullable": False,
                            "data_type": "text",
                            "name": "transactions_root",
                        },
                    },
                    "schema_contract": {},
                    "name": "blocks",
                    "resource": "blocks",
                },
                "blocks__transactions": {
                    "parent": "blocks",
                    "columns": {
                        "_dlt_id": {
                            "nullable": False,
                            "unique": True,
                            "data_type": "text",
                            "name": "_dlt_id",
                        },
                        "block_number": {
                            "nullable": False,
                            "primary_key": True,
                            "foreign_key": True,
                            "data_type": "bigint",
                            "name": "block_number",
                        },
                        "transaction_index": {
                            "nullable": False,
                            "primary_key": True,
                            "data_type": "bigint",
                            "name": "transaction_index",
                        },
                        "hash": {
                            "nullable": False,
                            "unique": True,
                            "data_type": "text",
                            "name": "hash",
                        },
                        "block_hash": {
                            "nullable": False,
                            "cluster": True,
                            "data_type": "text",
                            "name": "block_hash",
                        },
                        "block_timestamp": {
                            "nullable": False,
                            "sort": True,
                            "data_type": "timestamp",
                            "name": "block_timestamp",
                        },
                        "chain_id": {"nullable": True, "data_type": "text", "name": "chain_id"},
                        "from": {"nullable": True, "data_type": "text", "name": "from"},
                        "gas": {"nullable": True, "data_type": "bigint", "name": "gas"},
                        "gas_price": {"nullable": True, "data_type": "bigint", "name": "gas_price"},
                        "input": {"nullable": True, "data_type": "text", "name": "input"},
                        "max_fee_per_gas": {
                            "nullable": True,
                            "data_type": "wei",
                            "name": "max_fee_per_gas",
                        },
                        "max_priority_fee_per_gas": {
                            "nullable": True,
                            "data_type": "wei",
                            "name": "max_priority_fee_per_gas",
                        },
                        "nonce": {"nullable": True, "data_type": "bigint", "name": "nonce"},
                        "r": {"nullable": True, "data_type": "text", "name": "r"},
                        "s": {"nullable": True, "data_type": "text", "name": "s"},
                        "status": {"nullable": True, "data_type": "bigint", "name": "status"},
                        "to": {"nullable": True, "data_type": "text", "name": "to"},
                        "type": {"nullable": True, "data_type": "text", "name": "type"},
                        "v": {"nullable": True, "data_type": "bigint", "name": "v"},
                        "value": {"nullable": False, "data_type": "wei", "name": "value"},
                        "eth_value": {
                            "nullable": True,
                            "data_type": "decimal",
                            "name": "eth_value",
                        },
                    },
                    "name": "blocks__transactions",
                },
                "blocks__transactions__logs": {
                    "parent": "blocks__transactions",
                    "columns": {
                        "_dlt_id": {
                            "nullable": False,
                            "unique": True,
                            "data_type": "text",
                            "name": "_dlt_id",
                        },
                        "address": {"nullable": False, "data_type": "text", "name": "address"},
                        "block_timestamp": {
                            "nullable": False,
                            "sort": True,
                            "data_type": "timestamp",
                            "name": "block_timestamp",
                        },
                        "block_hash": {
                            "nullable": False,
                            "cluster": True,
                            "data_type": "text",
                            "name": "block_hash",
                        },
                        "block_number": {
                            "nullable": False,
                            "primary_key": True,
                            "foreign_key": True,
                            "data_type": "bigint",
                            "name": "block_number",
                        },
                        "transaction_index": {
                            "nullable": False,
                            "primary_key": True,
                            "foreign_key": True,
                            "data_type": "bigint",
                            "name": "transaction_index",
                        },
                        "log_index": {
                            "nullable": False,
                            "primary_key": True,
                            "data_type": "bigint",
                            "name": "log_index",
                        },
                        "data": {"nullable": True, "data_type": "text", "name": "data"},
                        "removed": {"nullable": True, "data_type": "bool", "name": "removed"},
                        "transaction_hash": {
                            "nullable": False,
                            "data_type": "text",
                            "name": "transaction_hash",
                        },
                    },
                    "name": "blocks__transactions__logs",
                },
            },
            "settings": {
                "default_hints": {
                    "foreign_key": ["_dlt_parent_id"],
                    "not_null": ["re:^_dlt_id$", "_dlt_root_id", "_dlt_parent_id", "_dlt_list_idx"],
                    "unique": ["_dlt_id"],
                    "cluster": ["block_hash"],
                    "partition": ["block_timestamp"],
                    "root_key": ["_dlt_root_id"],
                },
                "preferred_types": {"timestamp": "timestamp", "block_timestamp": "timestamp"},
                "schema_contract": {},
            },
            "normalizers": {
                "names": "dlt.common.normalizers.names.snake_case",
                "json": {
                    "module": "dlt.common.normalizers.json.relational",
                    "config": {
                        "generate_dlt_id": True,
                        "propagation": {
                            "root": {"_dlt_id": "_dlt_root_id"},
                            "tables": {
                                "blocks": {"timestamp": "block_timestamp", "hash": "block_hash"}
                            },
                        },
                    },
                },
            },
            "previous_hashes": ["yjMtV4Zv0IJlfR5DPMwuXxGg8BRhy7E79L26XAHWEGE="],
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
    # we dont't compare `.table` because of circular references
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
            {"name": "simple_column", "data_type": "text"},
            Column(name="simple_column", type="text"),
        ),
        (
            {"name": "nullable_column", "data_type": "text", "nullable": False},
            Column(name="nullable_column", type="text", not_null=True),
        ),
        (
            {"name": "unique_column", "data_type": "text", "unique": True},
            Column(name="unique_column", type="text", unique=True),
        ),
        (
            {"name": "primary_key_column", "data_type": "text", "primary_key": True},
            Column(name="primary_key_column", type="text", pk=True),
        ),
        (
            {"name": "description_column", "data_type": "text", "description": "foo"},
            Column(name="description_column", type="text", note="foo"),
        ),
    ],
)
def test_to_and_from_dbml_column(hints: TColumnSchema, dbml_col: Column) -> None:
    """Test `dlt -> dbml -> dlt`.

    This is different from `dbml -> dlt` because we assume that the dbml column
    includes some metadata stored on `properties` field.
    """
    # dlt -> dbml
    inferred_dbml_col = _to_dbml_column(hints)
    assert_equal_dbml_columns(dbml_col, inferred_dbml_col)

    # we expect the conversion from original `pydbml.Column` to dlt
    # to fail when it includes other keys than `name` and `data_type`.
    # the conversion requires metadata stored on the `properties` field.
    # TODO support this case more robustly
    if hints.keys() != {"name", "data_type"}:
        # dbml -> dlt
        with pytest.raises(AssertionError):
            assert hints == _from_dbml_column(dbml_col)

    # dlt -> dbml -> dlt
    inferred_hints = _from_dbml_column(inferred_dbml_col)
    assert hints == inferred_hints


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
            Table(name="simple_table", columns=[Column(name="foo", type="text")]),
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

    # dlt -> dbml -> dlt
    inferred_hints = _from_dbml_table(inferred_dbml_table)
    assert table_schema == inferred_hints


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
        type=DEFAULT_RELATION_TYPE,
        col1=[tables[0].columns[0]],  # refers to `customers.id`
        col2=[tables[1].columns[0]],  # refers to `orders.customer_id`
    )
    expected_dlt_reference = TTableReference(
        columns=["id"],
        referenced_columns=["customer_id"],
        referenced_table="orders",
    )

    # dlt -> dbml
    inferred_dbml_reference = _to_dbml_reference(
        from_table="customers",
        from_columns="id",
        to_table="orders",
        to_columns="customer_id",
        tables=tables,
    )
    assert_equal_dbml_references(expected_dbml_reference, inferred_dbml_reference)

    # dlt -> dbml -> dlt
    inferred_dlt_reference = _from_dbml_reference(inferred_dbml_reference)
    assert expected_dlt_reference == inferred_dlt_reference

    # dbml -> dlt
    inferred_dlt_reference = _from_dbml_reference(expected_dbml_reference)
    assert expected_dlt_reference == inferred_dlt_reference


def test_schema_to_dbml(example_schema: dlt.Schema) -> None:
    expected_dbml = textwrap.dedent("""\
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

        Table "blocks" {
            "_dlt_load_id" text [not null, note: 'load id coming from the extractor']
            "_dlt_id" text [unique, not null]
            "number" bigint [pk, not null]
            "parent_hash" text
            "hash" text [unique, not null]
            "base_fee_per_gas" wei [not null]
            "difficulty" wei [not null]
            "extra_data" text
            "gas_limit" bigint [not null]
            "gas_used" bigint [not null]
            "logs_bloom" binary
            "miner" text
            "mix_hash" text
            "nonce" text
            "receipts_root" text
            "sha3_uncles" text
            "size" bigint
            "state_root" text [not null]
            "timestamp" timestamp [unique, not null]
            "total_difficulty" wei
            "transactions_root" text [not null]
            Note {
                'Ethereum blocks'
            }
        }

        Table "blocks__transactions" {
            "_dlt_id" text [unique, not null]
            "block_number" bigint [pk, not null]
            "transaction_index" bigint [pk, not null]
            "hash" text [unique, not null]
            "block_hash" text [not null]
            "block_timestamp" timestamp [not null]
            "chain_id" text
            "from" text
            "gas" bigint
            "gas_price" bigint
            "input" text
            "max_fee_per_gas" wei
            "max_priority_fee_per_gas" wei
            "nonce" bigint
            "r" text
            "s" text
            "status" bigint
            "to" text
            "type" text
            "v" bigint
            "value" wei [not null]
            "eth_value" decimal
        }

        Table "blocks__transactions__logs" {
            "_dlt_id" text [unique, not null]
            "address" text [not null]
            "block_timestamp" timestamp [not null]
            "block_hash" text [not null]
            "block_number" bigint [pk, not null]
            "transaction_index" bigint [pk, not null]
            "log_index" bigint [pk, not null]
            "data" text
            "removed" bool
            "transaction_hash" text [not null]
        }""")
    dbml_schema = schema_to_dbml(example_schema, group_by_resource=True)
    print(dbml_schema.dbml)
    assert dbml_schema.dbml == expected_dbml


def test_group_tables_by_resource(example_schema: dlt.Schema) -> None:
    dbml_schema = schema_to_dbml(example_schema)
    dbml_table_groups = _group_tables_by_resource(schema=example_schema, db=dbml_schema)

    assert len(dbml_table_groups) == 2

    blocks_group = dbml_table_groups[0]
    dlt_group = dbml_table_groups[1]

    assert blocks_group.name == "blocks"
    assert dlt_group.name == "_dlt"
    assert len(blocks_group.items) == 3
    blocks_group_members = set([item.name for item in blocks_group.items])
    assert blocks_group_members == {"blocks", "blocks__transactions", "blocks__transactions__logs"}
    assert len(dlt_group.items) == 2
    dlt_group_members = set([item.name for item in dlt_group.items])
    assert dlt_group_members == {"_dlt_loads", "_dlt_version"}


def test_export_to_dbml_default_args(example_schema: dlt.Schema) -> None:
    """Assert a string is returned when no path is provided"""
    output = export_to_dbml(example_schema)
    assert isinstance(output, str)


def test_export_to_dbml_as_string(example_schema: dlt.Schema) -> None:
    dbml_schema = schema_to_dbml(example_schema)
    expected_output = PyDBML(dbml_schema.dbml)

    output = export_to_dbml(example_schema, path=None)
    loaded_dbml = PyDBML(output)

    assert isinstance(output, str)
    # for some reason, the PyDBML objects are not directly equal
    assert expected_output.dbml == loaded_dbml.dbml


def test_export_to_dbml_to_file(
    example_schema: dlt.Schema,
    tmp_path: pathlib.Path,
) -> None:
    dbml_schema = schema_to_dbml(example_schema)
    expected_output = PyDBML(dbml_schema.dbml)

    file_path = tmp_path / "my_schema.dbml"
    output = export_to_dbml(example_schema, path=file_path)
    # PyDBML can directly load from file
    loaded_dbml = PyDBML(file_path)

    assert output is None
    # for some reason, the PyDBML objects are not directly equal
    assert expected_output.dbml == loaded_dbml.dbml
