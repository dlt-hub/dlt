from typing import cast

import pytest

import dlt
from dlt.common.schema.typing import (
    TColumnSchema,
    TStoredSchema,
    TTableReferenceStandalone,
    TTableSchema,
)
from dlt.helpers.mermaid import (
    schema_to_mermaid,
    _to_mermaid_column,
    _to_mermaid_reference,
    _to_mermaid_table,
)


@pytest.mark.parametrize(
    "hints,expected_mermaid_col",
    [
        (
            {"name": "simple_col", "data_type": "text"},
            "text simple_col\n",
        ),
        (
            {"name": "unique_col", "data_type": "text", "unique": True},  # default value
            "text unique_col UK\n",
        ),
        (
            {"name": "unique_col", "data_type": "text", "unique": False},
            "text unique_col\n",
        ),
        (
            {"name": "primary_key_col", "data_type": "text", "primary_key": False},
            "text primary_key_col\n",
        ),
        (
            {"name": "primary_key_col", "data_type": "text", "primary_key": True},
            "text primary_key_col PK\n",
        ),
        (
            {
                "name": "unique_and_primary_col",
                "data_type": "text",
                "primary_key": True,
                "unique": True,
            },
            "text unique_and_primary_col PK,UK\n",
        ),
        (  # change the order of `primary_key` and `unique` in dict
            {
                "name": "unique_and_primary_col",
                "data_type": "text",
                "unique": True,
                "primary_key": True,
            },
            "text unique_and_primary_col PK,UK\n",
        ),
        (
            {"name": "description_col", "data_type": "text", "description": "foo"},
            'text description_col "foo"\n',
        ),
        (
            {"name": "incomplete_col"},  # no data_type set yet
            "no_data_seen incomplete_col\n",
        ),
    ],
)
def test_to_mermaid_column(hints: TColumnSchema, expected_mermaid_col: str) -> None:
    """Test `dlt -> mermaid`."""
    inferred_mermaid_col = _to_mermaid_column(hints)
    assert inferred_mermaid_col == expected_mermaid_col


@pytest.mark.parametrize(
    "table,expected_mermaid_table",
    [
        (
            {
                "name": "simple_table",
                "columns": {
                    "foo": {"name": "foo", "data_type": "text"},
                    "bar": {"name": "bar", "data_type": "bigint"},
                },
            },
            "simple_table{\n    text foo\n    bigint bar\n}\n",
        ),
        (  # incomplete columns are skipped
            {
                "name": "mixed_table",
                "columns": {
                    "complete_col": {"name": "complete_col", "data_type": "text"},
                    "incomplete_col": {"name": "incomplete_col"},
                },
            },
            "mixed_table{\n    text complete_col\n}\n",
        ),
    ],
)
def test_to_and_from_dbml_table(table: TTableSchema, expected_mermaid_table: str) -> None:
    """Test `dlt -> mermaid`."""
    inferred_mermaid_table = _to_mermaid_table(table)
    assert inferred_mermaid_table == expected_mermaid_table


@pytest.mark.parametrize(
    "reference, expected_mermaid_reference",
    [
        (
            TTableReferenceStandalone(
                table="customers",
                columns=["id"],
                referenced_columns=["customer_id"],
                referenced_table="orders",
                label="ordered",
                cardinality="zero_to_many",
            ),
            'customers |o--|{ orders : "ordered"\n',
        ),
        (  # default label
            TTableReferenceStandalone(
                table="customers",
                columns=["id"],
                referenced_columns=["customer_id"],
                referenced_table="orders",
                cardinality="zero_to_many",
            ),
            'customers |o--|{ orders : ""\n',
        ),
        (  # default cardinality
            TTableReferenceStandalone(
                table="customers",
                columns=["id"],
                referenced_columns=["customer_id"],
                referenced_table="orders",
                label="ordered",
            ),
            'customers ||--|{ orders : "ordered"\n',
        ),
        (  # default cardinality
            TTableReferenceStandalone(
                table="customers",
                columns=["id"],
                referenced_columns=["customer_id"],
                referenced_table="orders",
                label="multi word label",
            ),
            'customers ||--|{ orders : "multi word label"\n',
        ),
    ],
)
def test_to_mermaid_reference(
    reference: TTableReferenceStandalone, expected_mermaid_reference: str
) -> None:
    inferred_mermaid_reference = _to_mermaid_reference(reference)
    assert inferred_mermaid_reference == expected_mermaid_reference


def test_schema_to_mermaid_generates_an_er_diagram(example_schema: dlt.Schema):
    mermaid_str = schema_to_mermaid(example_schema.to_dict(), references=example_schema.references)
    assert mermaid_str.startswith("erDiagram")


@pytest.mark.parametrize("remove_process_hints", [False, True])
def test_schema_to_mermaid_invariant_to_processing_hint(
    example_schema: dlt.Schema, remove_process_hints: bool
):
    expected_mermaid_str = """\
erDiagram
    _dlt_version{
    bigint version
    bigint engine_version
    timestamp inserted_at
    text schema_name
    text version_hash
    text schema
}
    _dlt_loads{
    text load_id
    text schema_name
    bigint status
    timestamp inserted_at
    text schema_version_hash
}
    customers{
    bigint id PK
    text name
    text city
    text _dlt_load_id
    text _dlt_id UK
}
    purchases{
    bigint id PK
    bigint customer_id
    bigint inventory_id
    bigint quantity
    text date
    text _dlt_load_id
    text _dlt_id UK
}
    _dlt_pipeline_state{
    bigint version
    bigint engine_version
    text pipeline_name
    text state
    timestamp created_at
    text version_hash
    text _dlt_load_id
    text _dlt_id UK
}
    purchases__items{
    bigint purchase_id
    text name
    bigint price
    text _dlt_root_id
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    customers }|--|| _dlt_loads : "_dlt_load"
    purchases }|--|| _dlt_loads : "_dlt_load"
    purchases ||--|{ customers : ""
    _dlt_pipeline_state }|--|| _dlt_loads : "_dlt_load"
    purchases__items }|--|| purchases : "_dlt_parent"
    purchases__items }|--|| purchases : "_dlt_root"
    _dlt_version ||--|{ _dlt_loads : "_dlt_schema_version"
    _dlt_version }|--|{ _dlt_loads : "_dlt_schema_name"
"""
    schema_dict = example_schema.to_dict(remove_processing_hints=remove_process_hints)
    mermaid_str = schema_to_mermaid(
        schema_dict,
        references=example_schema.references,
    )
    assert mermaid_str == expected_mermaid_str


def test_schema_to_mermaid_exclude_dlt_tables(example_schema: dlt.Schema) -> None:
    expected_mermaid_str = """\
erDiagram
    customers{
    bigint id PK
    text name
    text city
    text _dlt_load_id
    text _dlt_id UK
}
    purchases{
    bigint id PK
    bigint customer_id
    bigint inventory_id
    bigint quantity
    text date
    text _dlt_load_id
    text _dlt_id UK
}
    purchases__items{
    bigint purchase_id
    text name
    bigint price
    text _dlt_root_id
    text _dlt_parent_id
    bigint _dlt_list_idx
    text _dlt_id UK
}
    purchases ||--|{ customers : ""
    purchases__items }|--|| purchases : "_dlt_parent"
    purchases__items }|--|| purchases : "_dlt_root"
"""

    schema_dict = example_schema.to_dict()
    mermaid_str = schema_to_mermaid(
        schema_dict,
        references=example_schema.references,
        include_dlt_tables=False,
    )
    assert mermaid_str == expected_mermaid_str


def test_schema_to_mermaid_hide_columns(example_schema: dlt.Schema) -> None:
    expected_mermaid_str = """\
erDiagram
    _dlt_version{
}
    _dlt_loads{
}
    customers{
}
    purchases{
}
    _dlt_pipeline_state{
}
    purchases__items{
}
    customers }|--|| _dlt_loads : "_dlt_load"
    purchases }|--|| _dlt_loads : "_dlt_load"
    purchases ||--|{ customers : ""
    _dlt_pipeline_state }|--|| _dlt_loads : "_dlt_load"
    purchases__items }|--|| purchases : "_dlt_parent"
    purchases__items }|--|| purchases : "_dlt_root"
    _dlt_version ||--|{ _dlt_loads : "_dlt_schema_version"
    _dlt_version }|--|{ _dlt_loads : "_dlt_schema_name"
"""

    schema_dict = example_schema.to_dict()
    mermaid_str = schema_to_mermaid(
        schema_dict,
        references=example_schema.references,
        hide_columns=True,
    )
    assert mermaid_str == expected_mermaid_str


def test_schema_to_mermaid_skips_incomplete_tables_and_columns() -> None:
    """Tables with only incomplete columns are excluded, and incomplete columns within
    complete tables are skipped."""
    stored_schema = cast(
        TStoredSchema,
        {
            "tables": {
                "complete_table": {
                    "name": "complete_table",
                    "columns": {
                        "id": {"name": "id", "data_type": "bigint", "primary_key": True},
                        "name": {"name": "name", "data_type": "text"},
                        "pending": {"name": "pending"},
                    },
                },
                "incomplete_table": {
                    "name": "incomplete_table",
                    "columns": {
                        "no_type_a": {"name": "no_type_a"},
                        "no_type_b": {"name": "no_type_b"},
                    },
                },
            },
        },
    )

    expected = """\
erDiagram
    complete_table{
    bigint id PK
    text name
}
"""
    mermaid_str = schema_to_mermaid(stored_schema, references=[])
    assert mermaid_str == expected
