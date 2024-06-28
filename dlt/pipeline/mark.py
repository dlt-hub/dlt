"""Module with mark functions that make data to be specially processed"""
from dlt.extract import (
    with_table_name,
    with_hints,
    with_file_import,
    make_hints,
    materialize_schema_item as materialize_table_schema,
)
