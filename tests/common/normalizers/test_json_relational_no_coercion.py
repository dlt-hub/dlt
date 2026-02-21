import os
from typing import Iterator

import pytest

from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.typing import TNormalizersConfig
from dlt.common.schema import Schema
from dlt.common.storages import NormalizeStorage
from dlt.common.storages.configuration import NormalizeStorageConfiguration
from dlt.common.normalizers.json.relational_no_coercion import (
    DataItemNormalizer as NoCoercionNormalizer,
)
from dlt.normalize.items_normalizers.jsonl import JsonLItemsNormalizer
from dlt.normalize.normalize import Normalize

from tests.normalize.utils import DEFAULT_CAPS
from tests.utils import get_test_storage_root

NO_COERCION_NORMALIZERS: TNormalizersConfig = {
    "names": "snake_case",
    "json": {"module": "relational_no_coercion"},
}


@pytest.fixture(autouse=True)
def default_caps() -> Iterator[DestinationCapabilitiesContext]:
    with Container().injectable_context(DEFAULT_CAPS()) as caps:
        yield caps


@pytest.fixture
def norm() -> NoCoercionNormalizer:
    schema = Schema("test_no_coercion", NO_COERCION_NORMALIZERS)
    return schema.data_item_normalizer  # type: ignore[return-value]


@pytest.fixture
def item_normalizer() -> JsonLItemsNormalizer:
    n = Normalize()
    schema = Schema("event", NO_COERCION_NORMALIZERS)
    normalize_storage = NormalizeStorage(
        True,
        NormalizeStorageConfiguration(
            os.path.join(get_test_storage_root(), "pipeline", "normalized")
        ),
    )
    return JsonLItemsNormalizer(None, None, normalize_storage, schema, "load_id", n.config)


def test_can_coerce_type_same_only(norm: NoCoercionNormalizer) -> None:
    assert norm.can_coerce_type("text", "text") is True
    assert norm.can_coerce_type("bigint", "bigint") is True
    # cross-type always False
    assert norm.can_coerce_type("text", "bigint") is False
    assert norm.can_coerce_type("bigint", "text") is False
    assert norm.can_coerce_type("timestamp", "text") is False


def test_coerce_type_same_type(norm: NoCoercionNormalizer) -> None:
    assert norm.coerce_type("text", "text", "hello") == "hello"
    assert norm.coerce_type("bigint", "bigint", 42) == 42
    assert norm.coerce_type("double", "double", 3.14) == 3.14


def test_coerce_type_cross_type_raises(norm: NoCoercionNormalizer) -> None:
    with pytest.raises(ValueError):
        norm.coerce_type("text", "bigint", 123)
    with pytest.raises(ValueError):
        norm.coerce_type("bigint", "text", "456")
    with pytest.raises(ValueError):
        norm.coerce_type("timestamp", "text", "2024-01-01")


def test_type_map_populated(norm: NoCoercionNormalizer) -> None:
    m = norm.py_type_to_sc_type_map
    assert m[str] == "text"
    assert m[int] == "bigint"
    assert m[float] == "double"
    assert m[bool] == "bool"


def test_structural_normalization_works(norm: NoCoercionNormalizer) -> None:
    row = {"a": 1, "b": {"c": 2, "d": 3}, "e": [{"f": 4}]}
    rows = list(norm.normalize_data_item(row, "load_id", "table"))
    # root + 1 nested list element
    assert len(rows) == 2
    root = rows[0][1]
    assert root["a"] == 1
    # b is flattened
    assert root["b__c"] == 2
    assert root["b__d"] == 3
    # e is a child table
    child = rows[1][1]
    assert child["f"] == 4


def _make_relational_item_normalizer() -> JsonLItemsNormalizer:
    """Item normalizer with standard relational (coercing) normalizer for contrast."""
    n = Normalize()
    schema = Schema("event")
    normalize_storage = NormalizeStorage(
        True,
        NormalizeStorageConfiguration(
            os.path.join(get_test_storage_root(), "pipeline", "normalized")
        ),
    )
    return JsonLItemsNormalizer(None, None, normalize_storage, schema, "load_id", n.config)


# (column_type_value, mismatched_value, expected_variant_suffix, expected_variant_data_type)
# column_type_value establishes the column; mismatched_value triggers coercion in relational
# but creates a variant in no-coercion
VARIANT_CASES = [
    # text column ← non-text values
    ("hello", 42, "v_bigint", "bigint"),
    ("hello", 3.14, "v_double", "double"),
    ("hello", True, "v_bool", "bool"),
    # bigint column ← non-bigint values
    (100, "200", "v_text", "text"),
    (100, 1.5, "v_double", "double"),
    # double column ← non-double values
    (1.5, "3.14", "v_text", "text"),
    (1.5, 42, "v_bigint", "bigint"),
    # bool column ← non-bool values
    (True, "true", "v_text", "text"),
    (True, 1, "v_bigint", "bigint"),
]


@pytest.mark.parametrize(
    "establish_value,mismatch_value,variant_suffix,variant_type",
    VARIANT_CASES,
    ids=[
        "text_col_gets_bigint",
        "text_col_gets_double",
        "text_col_gets_bool",
        "bigint_col_gets_text",
        "bigint_col_gets_double",
        "double_col_gets_text",
        "double_col_gets_bigint",
        "bool_col_gets_text",
        "bool_col_gets_bigint",
    ],
)
def test_type_mismatch_creates_variant(
    item_normalizer: JsonLItemsNormalizer,
    establish_value: object,
    mismatch_value: object,
    variant_suffix: str,
    variant_type: str,
) -> None:
    """Every cross-type mismatch that relational would coerce must become a variant."""
    # establish column type with first row
    _, new_table = item_normalizer._coerce_row("tbl", None, {"col": establish_value})
    item_normalizer.schema.update_table(new_table)

    # send mismatched type
    new_row, new_table = item_normalizer._coerce_row("tbl", None, {"col": mismatch_value})
    assert new_table is not None, "expected variant column schema update"
    variant_col = f"col__{variant_suffix}"
    assert variant_col in new_table["columns"]
    assert new_table["columns"][variant_col]["data_type"] == variant_type
    assert new_table["columns"][variant_col]["variant"] is True
    # value lands in variant column, not the original
    assert new_row[variant_col] == mismatch_value
    assert "col" not in new_row


# cases where relational normalizer successfully coerces (no variant)
# bigint←double(1.5) fails because 1.5 is not a whole number
# bool←text("true") and bool←bigint(1) also create variants in relational
RELATIONAL_COERCE_CASES = [
    # text column ← other types: always coerces via str()
    ("hello", 42, "text_col_gets_bigint"),
    ("hello", 3.14, "text_col_gets_double"),
    ("hello", True, "text_col_gets_bool"),
    # bigint column ← text with valid int
    (100, "200", "bigint_col_gets_text"),
    # double column ← text and bigint
    (1.5, "3.14", "double_col_gets_text"),
    (1.5, 42, "double_col_gets_bigint"),
]


@pytest.mark.parametrize(
    "establish_value,mismatch_value",
    [(c[0], c[1]) for c in RELATIONAL_COERCE_CASES],
    ids=[c[2] for c in RELATIONAL_COERCE_CASES],
)
def test_relational_coerces_same_cases(
    establish_value: object,
    mismatch_value: object,
) -> None:
    """Contrast: relational normalizer coerces these instead of creating variants."""
    rel = _make_relational_item_normalizer()
    _, new_table = rel._coerce_row("tbl", None, {"col": establish_value})
    rel.schema.update_table(new_table)

    new_row, new_table = rel._coerce_row("tbl", None, {"col": mismatch_value})
    # no variant — value coerced into the original column
    assert new_table is None, "relational normalizer should coerce, not create variant"
    assert "col" in new_row
    assert not any(k.startswith("col__v_") for k in new_row)


def test_same_type_no_variant(item_normalizer: JsonLItemsNormalizer) -> None:
    """Same-type values pass through without variant columns."""
    row_1 = {"name": "alice"}
    _, new_table = item_normalizer._coerce_row("users", None, row_1)
    item_normalizer.schema.update_table(new_table)

    row_2 = {"name": "bob"}
    new_row, new_table = item_normalizer._coerce_row("users", None, row_2)
    assert new_table is None
    assert new_row["name"] == "bob"
