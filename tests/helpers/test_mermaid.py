import re
import pytest
import dlt
from dlt.helpers.mermaid import schema_to_mermaid


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
                        "purchase_id": {
                            "name": "purchase_id",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "name": {"name": "name", "data_type": "text", "nullable": False},
                        "price": {"name": "price", "data_type": "bigint", "nullable": False},
                        "_dlt_root_id": {
                            "name": "_dlt_root_id",
                            "data_type": "text",
                            "nullable": False,
                            "root_key": True,
                        },
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


def test_schema_to_mermaid_generates_an_er_diagram(example_schema: dlt.Schema):
    mermaid_str = schema_to_mermaid(example_schema.to_dict(), example_schema.references)
    assert mermaid_str.startswith("erDiagram")


def test_schema_to_mermaid_generates_valid_mermaid_str_without_dlt_tables(example_schema: dlt.Schema):
    expected_mermaid_str = _expected_mermaid_str()
    mermaid_str = schema_to_mermaid(
        example_schema.to_dict(), example_schema.references, include_dlt_tables=False
    )

    assert _normalize_whitespace(mermaid_str) == _normalize_whitespace(expected_mermaid_str)


@pytest.mark.parametrize("remove_process_hints", [False, True])
def test_schema_to_mermaid_with_processing_hints(
    example_schema: dlt.Schema, remove_process_hints: bool
):
    """Test that schema_to_mermaid produces the expected Mermaid string
    both when hints are present and when they are removed.
    """
    expected_mermaid_str = _expected_mermaid_str()

    schema_dict = example_schema.to_dict(remove_processing_hints=remove_process_hints)

    mermaid_str = schema_to_mermaid(
        schema_dict,
        example_schema.references,
        include_dlt_tables=False,
    )

    assert _normalize_whitespace(mermaid_str) == _normalize_whitespace(expected_mermaid_str)


@pytest.mark.parametrize("include_dlt_tables", [False, True])
def test_schema_to_mermaid_with_dlt_tables_included(
    example_schema: dlt.Schema, include_dlt_tables: bool
):
    """Test that schema_to_mermaid produces the expected Mermaid string
    both when hints are present and when they are removed.
    """
    expected_mermaid_str = _expected_mermaid_str(with_dlt_tables=include_dlt_tables)

    schema_dict = example_schema.to_dict()

    mermaid_str = schema_to_mermaid(
        schema_dict,
        example_schema.references,
        include_dlt_tables=include_dlt_tables,
    )

    assert _normalize_whitespace(mermaid_str) == _normalize_whitespace(expected_mermaid_str)


def _expected_mermaid_str(with_dlt_tables: bool = False):
    if with_dlt_tables:
        return """
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
            customers }|--|| _dlt_loads : _dlt_load 
            purchases }|--|| _dlt_loads : _dlt_load 
            purchases |o--o| customers : contains 
            _dlt_pipeline_state }|--|| _dlt_loads : _dlt_load 
            purchases__items }|--|| purchases : _dlt_parent 
            purchases__items }|--|| purchases : _dlt_root 
        """
    return """
    erDiagram
        customers {
            bigint id PK
            text name
            text city
            text _dlt_load_id
            text _dlt_id UK  
        }
        purchases {
            bigint id PK
            bigint customer_id
            bigint inventory_id
            bigint quantity
            text date
            text _dlt_load_id
            text _dlt_id UK
        }
        purchases__items {
            bigint purchase_id
            text name
            bigint price
            text _dlt_root_id
            text _dlt_parent_id
            bigint _dlt_list_idx
            text _dlt_id UK
        }
        purchases |o--o| customers : contains
        purchases__items }|--|| purchases : _dlt_parent
        purchases__items }|--|| purchases : _dlt_root
    """
    

def _normalize_whitespace(text):
    """Normalize whitespace in a string by replacing all whitespace sequences with single spaces."""
    # remove spaces
    normalized = re.sub(r"\s+", "", text)
    # Strip leading/trailing whitespace
    return normalized.strip()
