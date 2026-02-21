import pathlib
import tempfile
from typing import Any, Optional

import pytest
import graphviz  # type: ignore[import-untyped]

import dlt
from dlt.common.schema.typing import PIPELINE_STATE_TABLE_NAME, VERSION_TABLE_NAME, LOADS_TABLE_NAME
from dlt.helpers.graphviz import _render_with_graphviz, schema_to_graphviz, TABLE_HEADER_PORT


def is_valid_dot(dot: str) -> bool:
    """Pass the DOT to the Graphviz render engine; throws exception if invalid DOT"""
    try:
        graphviz.Source(dot).pipe()
    except Exception:
        return False

    return True


def test_schema_to_graphviz_skips_incomplete_tables_and_columns() -> None:
    """Tables with only incomplete columns are excluded, and incomplete columns within
    complete tables are skipped."""
    stored_schema: dict = {
        "name": "test_schema",
        "tables": {
            "complete_table": {
                "name": "complete_table",
                "columns": {
                    "id": {
                        "name": "id",
                        "data_type": "bigint",
                        "primary_key": True,
                        "nullable": False,
                    },
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
    }

    dot = schema_to_graphviz(
        stored_schema,
        include_dlt_tables=False,
        include_internal_dlt_ref=False,
        include_parent_child_ref=False,
        include_root_child_ref=False,
    )

    assert "complete_table" in dot
    assert "incomplete_table" not in dot
    # complete columns rendered, incomplete skipped
    assert "id" in dot
    assert "bigint" in dot
    assert "pending" not in dot
    assert is_valid_dot(dot)


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
        # column edge; `f5` points to `purchases__items._dlt_parent_id`, the 5th column (1-indexed)
        "purchases__items:f5:_ -> purchases:f7:_",
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
        # column edge; `f4` points to `purchases__items._dlt_root_id`, the 4th column (1-indexed)
        "purchases__items:f4:_ -> purchases:f7:_",
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
