"""Module with mark functions that make data to be specially processed"""
from typing import Any, Dict, List, Union

from dlt.extract import (
    make_hints,
    make_nested_hints,
    with_file_import,
    with_hints,
    with_table_name,
    materialize_schema_item as materialize_table_schema,
)


def with_json_flatten(
    columns: Dict[str, Union[bool, List[str]]],
    keep_original: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    Helper function to create column hints for JSON expansion.
    
    Args:
        columns: Dict mapping column names to flatten specification
            - True: Flatten entire JSON object
            - List[str]: Flatten only specified paths (e.g., ["user.name", "user.email"])
        keep_original: Whether to keep original JSON string column alongside flattened columns
    
    Returns:
        Column hints dict compatible with apply_hints() or make_hints(columns=...)
    
    Example:
        @dlt.resource(
            apply_hints=dlt.mark.with_json_flatten(
                {"metadata": True, "data": ["user.name", "user.email"]},
                keep_original=True
            )
        )
        def my_resource():
            yield {"metadata": '{"name": "John"}'}
    """
    hints = {}
    for col, spec in columns.items():
        hints[col] = {
            "x-json-flatten": spec,
            "x-json-keep-original": keep_original,
        }
    return hints


__all__ = [
    "with_table_name",
    "with_hints",
    "with_file_import",
    "make_hints",
    "make_nested_hints",
    "materialize_table_schema",
    "with_json_flatten",
]
