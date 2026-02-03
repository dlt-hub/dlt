"""Tests for arrow data extraction with column name normalization.

Verifies that ArrowExtractor properly normalizes column names during extraction
and does not produce mixed normalized/non-normalized identifiers in the schema.
"""

import os

import pyarrow as pa
import pytest

import dlt
from dlt.common.storages import (
    SchemaStorage,
    SchemaStorageConfiguration,
    NormalizeStorageConfiguration,
)
from dlt.extract import DltSource
from dlt.extract.extract import Extract

from tests.utils import clean_test_storage, get_test_storage_root


@pytest.fixture
def extract_step() -> Extract:
    clean_test_storage(init_normalize=True)
    schema_storage = SchemaStorage(
        SchemaStorageConfiguration(
            schema_volume_path=os.path.join(get_test_storage_root(), "schemas")
        ),
        makedirs=True,
    )
    return Extract(schema_storage, NormalizeStorageConfiguration())


def test_arrow_mixed_case_columns_normalized(extract_step: Extract) -> None:
    """Arrow table with PascalCase columns must produce only snake_case columns in schema.

    Reproduces the bug where aliasing between `computed` and `arrow_table` in
    ArrowExtractor._compute_tables caused non-normalized column names to be merged
    into the schema alongside normalized ones, leading to NameNormalizationCollision
    in the normalizer.
    """
    table = pa.table({"Numbers": [1, 2, 3], "Strings": ["a", "b", "c"]})

    @dlt.resource(name="mixed_case")
    def mixed_case_resource():
        yield table

    source = DltSource(dlt.Schema("arrow_test"), "module", [mixed_case_resource])
    extract_step.extract(source, 20, 1)

    schema_table = source.schema.tables["mixed_case"]
    col_names = list(schema_table["columns"].keys())
    # only normalized (lowercase) names should be present
    assert "numbers" in col_names
    assert "strings" in col_names
    # non-normalized names must not appear
    assert "Numbers" not in col_names
    assert "Strings" not in col_names
    assert len(col_names) == 2


def test_arrow_columns_merge_with_resource_hints(extract_step: Extract) -> None:
    """Resource column hints must merge correctly with arrow-inferred columns after normalization.

    When a resource defines column hints with non-normalized names and yields arrow data
    with matching columns, the resulting schema should have only normalized column names
    and preserve the hint properties (like data_type).
    """
    table = pa.table({"Numbers": [1, 2, 3], "Strings": ["a", "b", "c"]})

    @dlt.resource(
        name="with_hints",
        columns={
            "Numbers": {"data_type": "bigint"},
            "Strings": {"data_type": "text"},
            # nullable hint-only column not present in arrow data
            "ExtraCol": {"data_type": "double", "nullable": True},
        },
    )
    def hinted_resource():
        yield table

    source = DltSource(dlt.Schema("arrow_test"), "module", [hinted_resource])
    extract_step.extract(source, 20, 1)

    schema_table = source.schema.tables["with_hints"]
    col_names = list(schema_table["columns"].keys())
    # only normalized names
    assert "numbers" in col_names
    assert "strings" in col_names
    assert "Numbers" not in col_names
    assert "Strings" not in col_names
    assert "ExtraCol" not in col_names
    # hint properties preserved through normalization
    assert schema_table["columns"]["numbers"]["data_type"] == "bigint"
    assert schema_table["columns"]["strings"]["data_type"] == "text"
    # hint-only column not in arrow data is still in the schema with normalized name
    assert "extra_col" in col_names
    assert schema_table["columns"]["extra_col"]["data_type"] == "double"
    assert schema_table["columns"]["extra_col"]["nullable"] is True


def test_arrow_multiple_items_mixed_case(extract_step: Extract) -> None:
    """Multiple arrow items in a batch must produce only normalized columns.

    Tests the accumulation path in ArrowExtractor._compute_tables where
    arrow_tables.get(table_name) returns a previously processed result
    and merges with the next item's schema. The third table has an already-normalized
    column name ("strings") mixed with a non-normalized one ("Numbers").
    """
    table1 = pa.table({"Numbers": [1, 2], "Strings": ["a", "b"]})
    table2 = pa.table({"Numbers": [3, 4], "Strings": ["c", "d"]})
    # third table: "strings" is already normalized, "Numbers" is not
    table3 = pa.table({"Numbers": [5, 6], "strings": ["e", "f"]})

    @dlt.resource(
        name="multi_items",
        columns={
            "Numbers": {"data_type": "bigint"},
            # hint-only column not present in any arrow table
            "ExtraHint": {"data_type": "text", "nullable": True},
        },
    )
    def multi_item_resource():
        yield [table1, table2, table3]

    source = DltSource(dlt.Schema("arrow_test"), "module", [multi_item_resource])
    extract_step.extract(source, 20, 1)

    schema_table = source.schema.tables["multi_items"]
    col_names = list(schema_table["columns"].keys())
    assert "numbers" in col_names
    assert "strings" in col_names
    assert "Numbers" not in col_names
    assert "Strings" not in col_names
    # hint-only column persisted with normalized name
    assert "extra_hint" in col_names
    assert "ExtraHint" not in col_names
    assert schema_table["columns"]["extra_hint"]["data_type"] == "text"
    assert schema_table["columns"]["extra_hint"]["nullable"] is True
    # hint data_type preserved on arrow-inferred column
    assert schema_table["columns"]["numbers"]["data_type"] == "bigint"
    assert len(col_names) == 3


def test_arrow_columns_with_special_chars_normalized(extract_step: Extract) -> None:
    """Arrow columns with special characters are properly normalized in the schema.

    Characters like ^ are replaced with _ during normalization. Verifies the full
    normalization pipeline works without mixing normalized and non-normalized identifiers.
    """
    table = pa.table({"col^New": [1, 2, 3], "col2": [4, 5, 6]})

    @dlt.resource(name="special_chars")
    def special_chars_resource():
        yield table

    source = DltSource(dlt.Schema("arrow_test"), "module", [special_chars_resource])
    extract_step.extract(source, 20, 1)

    schema_table = source.schema.tables["special_chars"]
    col_names = list(schema_table["columns"].keys())
    # col^New should be normalized to col_new
    assert "col_new" in col_names
    assert "col2" in col_names
    assert "col^New" not in col_names
    assert len(col_names) == 2
