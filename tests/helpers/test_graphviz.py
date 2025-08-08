import pathlib
import tempfile
from typing import Any, Optional

import pytest
import graphviz  # type: ignore[import-untyped]

import dlt
from dlt.common.schema.typing import PIPELINE_STATE_TABLE_NAME, VERSION_TABLE_NAME, LOADS_TABLE_NAME
from dlt.helpers.graphviz import _render_with_graphviz, schema_to_graphviz, TABLE_HEADER_PORT


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


def is_valid_dot(dot: str) -> bool:
    """Pass the DOT to the Graphviz render engine; throws exception if invalid DOT"""
    try:
        graphviz.Source(dot).pipe()
    except Exception:
        return False

    return True


def test_generate_valid_graphviz(example_schema: dlt.Schema, tmp_path: pathlib.Path) -> None:
    """Validate the generated DOT graph can be rendered. If it can be rendered to `.png`,
    it can be rendered to any other format supported by Graphviz (jpeg, pdf, svg, html, etc.)
    """
    file_name = "dlt-schema-graphviz"
    format_ = "png"
    expected_file_path = (tmp_path / file_name).with_suffix(f".{format_}")

    stored_schema = example_schema.to_dict()
    dot = schema_to_graphviz(stored_schema)
    graph = graphviz.Source(source=dot)

    graph.render(filename=file_name, directory=tmp_path, format=format_, cleanup=True)
    assert expected_file_path.exists()


@pytest.mark.parametrize("include_dlt_tables", (True, False))
def test_include_dlt_tables(example_schema: dlt.Schema, include_dlt_tables: bool) -> None:
    stored_schema = example_schema.to_dict()
    dot = schema_to_graphviz(stored_schema, include_dlt_tables=include_dlt_tables)

    # ensures the table name doesn't appear in tables (nodes) or references (edges)
    assert (LOADS_TABLE_NAME in dot) is include_dlt_tables
    assert (VERSION_TABLE_NAME in dot) is include_dlt_tables
    assert (PIPELINE_STATE_TABLE_NAME in dot) is include_dlt_tables
    assert is_valid_dot(dot)


@pytest.mark.parametrize("include_internal_dlt_ref", (True, False))
def test_include_internal_dlt_ref(
    example_schema: dlt.Schema, include_internal_dlt_ref: bool
) -> None:
    expected_refs = [
        f"{VERSION_TABLE_NAME}:{TABLE_HEADER_PORT} -> {LOADS_TABLE_NAME}:{TABLE_HEADER_PORT}",
        (
            f"{PIPELINE_STATE_TABLE_NAME}:{TABLE_HEADER_PORT} ->"
            f" {LOADS_TABLE_NAME}:{TABLE_HEADER_PORT}"
        ),
    ]

    stored_schema = example_schema.to_dict()
    dot = schema_to_graphviz(
        stored_schema,
        include_dlt_tables=True,  # must be True to produce references
        include_internal_dlt_ref=include_internal_dlt_ref,
        # disable other refs to isolate tested behavior
        include_parent_child_ref=False,
        include_root_child_ref=False,
    )

    for ref in expected_refs:
        assert (ref in dot) is include_internal_dlt_ref

    assert is_valid_dot(dot)


@pytest.mark.parametrize("include_parent_child_ref", (True, False))
def test_include_parent_child_ref(
    example_schema: dlt.Schema, include_parent_child_ref: bool
) -> None:
    expected_refs = [
        # table edge
        f"purchases__items:{TABLE_HEADER_PORT} -> purchases:{TABLE_HEADER_PORT}",
        # column edge; `f4` points to `purchases__items._dlt_parent_id`, the 4th column (1-indexed)
        "purchases__items:f4:_ -> purchases:f7:_",
    ]

    stored_schema = example_schema.to_dict()
    dot = schema_to_graphviz(
        stored_schema,
        include_parent_child_ref=include_parent_child_ref,
        # disable other refs to isolate tested behavior
        include_root_child_ref=False,
        include_internal_dlt_ref=False,
    )

    for ref in expected_refs:
        assert (ref in dot) is include_parent_child_ref

    assert is_valid_dot(dot)


@pytest.mark.parametrize("include_root_child_ref", (True, False))
def test_include_root_child_ref(example_schema: dlt.Schema, include_root_child_ref: bool) -> None:
    expected_refs = [
        # table edge
        f"purchases__items:{TABLE_HEADER_PORT} -> purchases:{TABLE_HEADER_PORT}",
        # column edge; `f3` points to `purchases__items._dlt_root_id`, the 3rd column (1-indexed)
        "purchases__items:f3:_ -> purchases:f7:_",
    ]

    stored_schema = example_schema.to_dict()
    dot = schema_to_graphviz(
        stored_schema,
        include_root_child_ref=include_root_child_ref,
        # disable other refs to isolate tested behavior
        include_parent_child_ref=False,
        include_internal_dlt_ref=False,
    )

    for ref in expected_refs:
        assert (ref in dot) is include_root_child_ref

    assert is_valid_dot(dot)


@pytest.mark.parametrize(
    ("path", "format_", "render_kwargs", "expected_relative_path"),
    [
        ("./sub/from_suffix.svg", None, None, "./sub/from_suffix.svg"),
        ("./sub/from_arg", "svg", None, "./sub/from_arg.svg"),
        ("./sub/matching_suffix_arg.svg", "svg", None, "./sub/matching_suffix_arg.svg"),
        ("./sub/arg_appended.png", "svg", None, "./sub/arg_appended.png.svg"),
        ("./sub/kwargs_ignored.png", None, {"format": "svg"}, "./sub/kwargs_ignored.png"),
    ],
)
def test_resolved_path(
    path: str,
    format_: str,
    render_kwargs: Optional[dict[str, Any]],
    expected_relative_path: str,
    tmp_path: pathlib.Path,
) -> None:
    path_arg = tmp_path / path
    expected_output_path = tmp_path / expected_relative_path

    dot_string = """\
strict digraph {
    base;
    driver;

    base -> "node";
    driver -> base;
    driver -> "graph";
    driver -> "node";
}"""
    output_path = _render_with_graphviz(
        dot_source=graphviz.Source(dot_string),
        path=path_arg,
        format_=format_,
        save_dot_file=False,
        render_kwargs=render_kwargs,
    )

    assert pathlib.Path(output_path) == expected_output_path
