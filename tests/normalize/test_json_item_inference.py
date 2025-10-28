import os
from pendulum import UTC
import pytest
from copy import deepcopy
from typing import Any, Iterator, List, Sequence
from dlt.common.libs.hexbytes import HexBytes
from dlt.common import Wei, Decimal, pendulum, json
from dlt.common.configuration.container import Container
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.json import custom_pua_decode
from dlt.common.schema import Schema, utils
from dlt.common.schema.typing import TColumnSchema, TSchemaUpdate
from dlt.common.schema.exceptions import (
    CannotCoerceColumnException,
    CannotCoerceNullException,
    ParentTableNotFoundException,
    SchemaCorruptedException,
    TablePropertiesConflictException,
)
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.time import ensure_pendulum_datetime_non_utc
from dlt.common.typing import StrAny, TDataItems
from dlt.common.utils import uniq_id
from dlt.normalize.items_normalizers import JsonLItemsNormalizer
from dlt.normalize.normalize import Normalize

from tests.common.utils import load_json_case
from tests.normalize.utils import DEFAULT_CAPS, add_preferred_types


@pytest.fixture(scope="module", autouse=True)
def default_caps() -> Iterator[DestinationCapabilitiesContext]:
    # set the postgres caps as default for the whole module
    with Container().injectable_context(DEFAULT_CAPS()) as caps:
        yield caps


@pytest.fixture
def item_normalizer() -> JsonLItemsNormalizer:
    n = Normalize()
    schema = Schema("event")
    add_preferred_types(schema)
    return JsonLItemsNormalizer(None, None, schema, "load_id", n.config)


@pytest.fixture
def schema(item_normalizer: JsonLItemsNormalizer) -> Schema:
    return item_normalizer.schema


def test_get_preferred_type(schema: Schema) -> None:
    assert "timestamp" in map(lambda m: m[1], schema._compiled_preferred_types)
    assert "double" in map(lambda m: m[1], schema._compiled_preferred_types)

    assert schema.get_preferred_type("timestamp") == "timestamp"
    assert schema.get_preferred_type("value") == "wei"
    assert schema.get_preferred_type("timestamp_confidence_entity") == "double"
    assert schema.get_preferred_type("_timestamp") is None


def test_map_column_preferred_type(item_normalizer: JsonLItemsNormalizer) -> None:
    # preferred type match
    assert item_normalizer._infer_column_type(1278712.0, "confidence") == "double"
    # preferred type can be coerced
    assert item_normalizer._infer_column_type(1278712, "confidence") == "double"
    assert item_normalizer._infer_column_type("18271", "confidence") == "double"

    # timestamp from coercable type
    assert item_normalizer._infer_column_type(18271, "timestamp") == "timestamp"
    assert item_normalizer._infer_column_type("18271.11", "timestamp") == "timestamp"
    assert (
        item_normalizer._infer_column_type("2022-05-10T00:54:38.237000+00:00", "timestamp")
        == "timestamp"
    )

    # value should be wei
    assert item_normalizer._infer_column_type(" 0xfe ", "value") == "wei"
    # number should be decimal
    assert item_normalizer._infer_column_type(" -0.821 ", "number") == "decimal"

    # if value cannot be coerced, column type still preferred types
    assert item_normalizer._infer_column_type(False, "value") == "wei"
    assert item_normalizer._infer_column_type("AA", "confidence") == "double"

    # skip preferred
    assert item_normalizer._infer_column_type(False, "value", skip_preferred=True) == "bool"
    assert item_normalizer._infer_column_type("AA", "confidence", skip_preferred=True) == "text"


def test_map_column_type(item_normalizer: JsonLItemsNormalizer) -> None:
    # default mappings
    assert item_normalizer._infer_column_type("18271.11", "_column_name") == "text"
    assert item_normalizer._infer_column_type(["city"], "_column_name") == "json"
    assert item_normalizer._infer_column_type(0x72, "_column_name") == "bigint"
    assert item_normalizer._infer_column_type(0x72, "_column_name") == "bigint"
    assert item_normalizer._infer_column_type(b"bytes str", "_column_name") == "binary"
    assert item_normalizer._infer_column_type(b"bytes str", "_column_name") == "binary"
    assert item_normalizer._infer_column_type(HexBytes(b"bytes str"), "_column_name") == "binary"


def test_map_column_type_json(item_normalizer: JsonLItemsNormalizer) -> None:
    # json type mappings
    v_list = [1, 2, "3", {"json": True}]
    v_dict = {"list": [1, 2], "str": "json"}
    # json types must be cast to text
    assert item_normalizer._infer_column_type(v_list, "cx_value") == "json"
    assert item_normalizer._infer_column_type(v_dict, "cx_value") == "json"


def test_coerce_row(item_normalizer: JsonLItemsNormalizer) -> None:
    timestamp_float = 78172.128
    timestamp_str = "1970-01-01T21:42:52.128000+00:00"
    # add new column with preferred
    row_1 = {
        "timestamp": timestamp_float,
        "confidence": "0.1",
        "value": "0xFF",
        "number": Decimal("128.67"),
    }
    new_row_1, new_table = item_normalizer._coerce_row("event_user", None, row_1)
    # convert columns to list, they must correspond to the order of fields in row_1
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "timestamp"
    assert new_columns[0]["name"] == "timestamp"
    assert "variant" not in new_columns[0]
    assert new_columns[1]["data_type"] == "double"
    assert "variant" not in new_columns[1]
    assert new_columns[2]["data_type"] == "wei"
    assert "variant" not in new_columns[2]
    assert new_columns[3]["data_type"] == "decimal"
    assert "variant" not in new_columns[3]
    # also rows values should be coerced (confidence)
    assert new_row_1 == {
        "timestamp": pendulum.parse(timestamp_str),
        "confidence": 0.1,
        "value": 255,
        "number": Decimal("128.67"),
    }

    # update schema
    item_normalizer.schema.update_table(new_table)

    # no coercion on confidence
    row_2 = {"timestamp": timestamp_float, "confidence": 0.18721}
    new_row_2, new_table = item_normalizer._coerce_row("event_user", None, row_2)
    assert new_table is None
    assert new_row_2 == {"timestamp": pendulum.parse(timestamp_str), "confidence": 0.18721}

    # all coerced
    row_3 = {"timestamp": "78172.128", "confidence": 1}
    new_row_3, new_table = item_normalizer._coerce_row("event_user", None, row_3)
    assert new_table is None
    assert new_row_3 == {"timestamp": pendulum.parse(timestamp_str), "confidence": 1.0}

    # create variant column where variant column will have preferred
    # variant column should not be checked against preferred
    row_4 = {"timestamp": "78172.128", "confidence": "STR"}
    new_row_4, new_table = item_normalizer._coerce_row("event_user", None, row_4)
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "text"
    assert new_columns[0]["name"] == "confidence__v_text"
    assert new_columns[0]["variant"] is True
    assert new_row_4 == {"timestamp": pendulum.parse(timestamp_str), "confidence__v_text": "STR"}
    item_normalizer.schema.update_table(new_table)

    # add against variant
    new_row_4, new_table = item_normalizer._coerce_row("event_user", None, row_4)
    assert new_table is None
    assert new_row_4 == {"timestamp": pendulum.parse(timestamp_str), "confidence__v_text": "STR"}

    # another variant
    new_row_5, new_table = item_normalizer._coerce_row("event_user", None, {"confidence": False})
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "bool"
    assert new_columns[0]["name"] == "confidence__v_bool"
    assert new_columns[0]["variant"] is True
    assert new_row_5 == {"confidence__v_bool": False}
    item_normalizer.schema.update_table(new_table)

    # variant column clashes with existing column - create new_colbool_v_binary column that would be created for binary variant, but give it a type datetime
    _, new_table = item_normalizer._coerce_row(
        "event_user", None, {"new_colbool": False, "new_colbool__v_timestamp": b"not fit"}
    )
    item_normalizer.schema.update_table(new_table)
    with pytest.raises(CannotCoerceColumnException) as exc_val:
        # now pass the binary that would create binary variant - but the column is occupied by text type
        item_normalizer._coerce_row("event_user", None, {"new_colbool": pendulum.now()})
    assert exc_val.value.table_name == "event_user"
    assert exc_val.value.column_name == "new_colbool__v_timestamp"
    assert exc_val.value.from_type == "timestamp"
    assert exc_val.value.to_type == "binary"
    # this must be datatime instance
    assert not isinstance(exc_val.value.coerced_value, bytes)


def test_coerce_row_iso_timestamp(item_normalizer: JsonLItemsNormalizer) -> None:
    timestamp_str = "2022-05-10T00:17:15.300000+00:00"
    # will generate timestamp type
    row_1 = {"timestamp": timestamp_str}
    coerced_row, new_table = item_normalizer._coerce_row("event_user", None, row_1)
    assert coerced_row["timestamp"].tzinfo == UTC
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "timestamp"
    assert new_columns[0]["name"] == "timestamp"
    item_normalizer.schema.update_table(new_table)

    # will coerce float
    row_2 = {"timestamp": 78172.128}
    coerced_row, new_table = item_normalizer._coerce_row("event_user", None, row_2)
    assert coerced_row["timestamp"].tzinfo == UTC
    # no new columns
    assert new_table is None

    # will generate variant
    row_3 = {"timestamp": "übermorgen"}
    _, new_table = item_normalizer._coerce_row("event_user", None, row_3)
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["name"] == "timestamp__v_text"


def test_coerce_tz_awareness_supports_naive(item_normalizer: JsonLItemsNormalizer):
    # non UTC timestamp
    timestamp_str = "2022-05-10T00:17:15.300000+02:00"
    row_1 = {"timestamp": timestamp_str}
    coerced_row, new_table = item_normalizer._coerce_row("event_user", None, row_1)
    assert coerced_row["timestamp"] == ensure_pendulum_datetime_non_utc(
        "2022-05-09T22:17:15.300000+00:00"
    )
    assert coerced_row["timestamp"].tzinfo == UTC
    assert "timezone" not in new_table["columns"]["timestamp"]


def test_coerce_timestamp_timezone_new_column_default(item_normalizer: JsonLItemsNormalizer):
    """Test timezone normalization when creating a new timestamp column with default behavior."""
    # Test with timezone-aware timestamp
    timestamp_str = "2022-05-10T00:17:15.300000+05:00"
    row = {"created_at": timestamp_str}

    coerced_row, new_table = item_normalizer._coerce_row("event_user", None, row)

    # Should be normalized to UTC (default timezone=True behavior)
    expected = ensure_pendulum_datetime_non_utc("2022-05-09T19:17:15.300000+00:00")
    assert coerced_row["created_at"] == expected
    assert coerced_row["created_at"].tzinfo == UTC

    # Column should be created with timestamp type
    assert new_table["columns"]["created_at"]["data_type"] == "timestamp"
    # Default timezone behavior should not be explicitly set (defaults to True)
    assert "timezone" not in new_table["columns"]["created_at"]


def test_coerce_timestamp_timezone_existing_column_preserve(item_normalizer: JsonLItemsNormalizer):
    """Test timezone normalization with existing timestamp column that preserves as naive."""
    # First create a column with timezone=False (convert to naive)
    timestamp_str_1 = "2022-05-10T00:17:15.300000+02:00"
    row_1 = {"event_time": timestamp_str_1}

    coerced_row_1, new_table = item_normalizer._coerce_row("events", None, row_1)
    # Modify the column to convert to naive datetime
    new_table["columns"]["event_time"]["timezone"] = False
    item_normalizer.schema.update_table(new_table)

    # Now add another row with different timezone
    timestamp_str_2 = "2022-05-10T05:30:45.500000-03:00"
    row_2 = {"event_time": timestamp_str_2}

    coerced_row_2, new_table_2 = item_normalizer._coerce_row("events", None, row_2)

    # Should be converted to naive datetime since existing column has timezone=False
    # The time should be converted to UTC first, then made naive
    expected_utc = ensure_pendulum_datetime_non_utc("2022-05-10T08:30:45.500000+00:00")
    expected_naive = expected_utc.naive()
    assert coerced_row_2["event_time"] == expected_naive
    assert coerced_row_2["event_time"].tzinfo is None

    # No new table should be created
    assert new_table_2 is None


def test_coerce_timestamp_timezone_existing_column_normalize_utc(
    item_normalizer: JsonLItemsNormalizer,
):
    """Test timezone normalization with existing timestamp column that normalizes to UTC."""
    # First create a column with timezone=True (normalize to UTC) - this is the default
    timestamp_str_1 = "2022-05-10T00:17:15.300000+02:00"
    row_1 = {"updated_at": timestamp_str_1}

    coerced_row_1, new_table = item_normalizer._coerce_row("events", None, row_1)
    # Explicitly set timezone=True to test the behavior
    new_table["columns"]["updated_at"]["timezone"] = True
    item_normalizer.schema.update_table(new_table)

    # Now add another row with different timezone
    timestamp_str_2 = "2022-05-10T05:30:45.500000-05:00"
    row_2 = {"updated_at": timestamp_str_2}

    coerced_row_2, new_table_2 = item_normalizer._coerce_row("events", None, row_2)

    # Should be normalized to UTC since existing column has timezone=True
    expected = ensure_pendulum_datetime_non_utc("2022-05-10T10:30:45.500000+00:00")
    assert coerced_row_2["updated_at"] == expected
    assert coerced_row_2["updated_at"].tzinfo == UTC

    # No new table should be created
    assert new_table_2 is None


def test_coerce_timestamp_timezone_incomplete_column(item_normalizer: JsonLItemsNormalizer):
    """Test timezone normalization when column exists but is incomplete (no data_type)."""
    # Create an incomplete column first
    incomplete_col = utils.new_column("process_time", nullable=False)
    incomplete_col["timezone"] = False  # Set timezone preference to convert to naive
    table = utils.new_table("processing", columns=[incomplete_col])
    item_normalizer.schema.update_table(table, normalize_identifiers=False)

    # Now process a timestamp value
    timestamp_str = "2022-05-10T12:00:00.000000+08:00"
    row = {"process_time": timestamp_str}

    coerced_row, new_table = item_normalizer._coerce_row("processing", None, row)

    # Should be converted to naive datetime since incomplete column had timezone=False
    # First converted to UTC, then made naive
    expected_utc = ensure_pendulum_datetime_non_utc("2022-05-10T04:00:00.000000+00:00")
    expected_naive = expected_utc.naive()
    assert coerced_row["process_time"] == expected_naive
    assert coerced_row["process_time"].tzinfo is None

    # New table should contain the completed column
    assert new_table["columns"]["process_time"]["data_type"] == "timestamp"
    assert new_table["columns"]["process_time"]["timezone"] is False
    assert new_table["columns"]["process_time"]["nullable"] is False


def test_coerce_timestamp_naive_datetime_input(item_normalizer: JsonLItemsNormalizer):
    """Test that naive datetime input values are handled correctly."""
    # Use a naive datetime (no timezone info)
    naive_dt = pendulum.parse("2022-05-10T15:30:00").naive()  # type: ignore[union-attr]
    row = {"local_time": naive_dt}

    coerced_row, new_table = item_normalizer._coerce_row("local_events", None, row)

    # Should still be processed correctly
    assert "local_time" in coerced_row
    assert new_table["columns"]["local_time"]["data_type"] == "timestamp"

    # With default timezone=True behavior, naive input should be treated as UTC
    # The exact behavior depends on normalize_timezone implementation
    # but it should not raise an exception
    assert coerced_row["local_time"] is not None
    # Since default is timezone=True, result should be timezone-aware UTC
    assert coerced_row["local_time"].tzinfo == UTC


def test_shorten_variant_column(item_normalizer: JsonLItemsNormalizer) -> None:
    item_normalizer.naming.max_length = 9
    timestamp_float = 78172.128
    # add new column with preferred
    row_1 = {
        "timestamp": timestamp_float,
        "confidence": "0.1",
        "value": "0xFF",
        "number": Decimal("128.67"),
    }
    _, new_table = item_normalizer._coerce_row("event_user", None, row_1)
    # schema assumes that identifiers are already normalized so confidence even if it is longer than 9 chars
    item_normalizer.schema.update_table(new_table, normalize_identifiers=False)
    assert "confidence" in item_normalizer.schema.tables["event_user"]["columns"]
    # confidence_123456
    # now variant is created and this will be normalized
    new_row_2, new_table = item_normalizer._coerce_row("event_user", None, {"confidence": False})
    tag = item_normalizer.naming._compute_tag(
        "confidence__v_bool", collision_prob=item_normalizer.naming._DEFAULT_COLLISION_PROB
    )
    new_row_2_keys = list(new_row_2.keys())
    assert tag in new_row_2_keys[0]
    assert len(new_row_2_keys[0]) == 9


def test_coerce_json_variant(item_normalizer: JsonLItemsNormalizer) -> None:
    # for this test use case sensitive naming convention
    os.environ["SCHEMA__NAMING"] = "direct"
    item_normalizer.schema.update_normalizers()
    # update naming cached in item normalizer
    item_normalizer.naming = item_normalizer.schema.naming
    # create two columns to which json type cannot be coerced
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    new_row, new_table = item_normalizer._coerce_row("event_user", None, row)
    assert new_row == row
    item_normalizer.schema.update_table(new_table)

    # add two more json columns that should be coerced to text
    v_list = [1, 2, "3", {"json": True}]
    v_dict = {"list": [1, 2], "str": "json"}
    c_row = {"c_list": v_list, "c_dict": v_dict}
    c_new_row, c_new_table = item_normalizer._coerce_row("event_user", None, c_row)
    c_new_columns = list(c_new_table["columns"].values())
    assert c_new_columns[0]["name"] == "c_list"
    assert c_new_columns[0]["data_type"] == "json"
    assert "variant" not in c_new_columns[0]
    assert c_new_columns[1]["name"] == "c_dict"
    assert c_new_columns[1]["data_type"] == "json"
    assert "variant" not in c_new_columns[1]
    assert c_new_row["c_list"] == v_list
    item_normalizer.schema.update_table(c_new_table)

    # add same row again
    c_new_row, c_new_table = item_normalizer._coerce_row("event_user", None, c_row)
    assert c_new_table is None
    assert c_new_row["c_dict"] == v_dict

    # add json types on the same columns
    c_row_v = {"floatX": v_list, "confidenceX": v_dict, "strX": v_dict}
    # expect two new variant columns to be created
    c_new_row_v, c_new_table_v = item_normalizer._coerce_row("event_user", None, c_row_v)
    c_new_columns_v = list(c_new_table_v["columns"].values())
    # two new variant columns added
    assert len(c_new_columns_v) == 2
    assert c_new_columns_v[0]["name"] == "floatX▶v_json"
    assert c_new_columns_v[1]["name"] == "confidenceX▶v_json"
    assert c_new_columns_v[0]["variant"] is True
    assert c_new_columns_v[1]["variant"] is True
    assert c_new_row_v["floatX▶v_json"] == v_list
    assert c_new_row_v["confidenceX▶v_json"] == v_dict
    assert c_new_row_v["strX"] == json.dumps(v_dict)
    item_normalizer.schema.update_table(c_new_table_v)

    # add that row again
    c_row_v = {"floatX": v_list, "confidenceX": v_dict, "strX": v_dict}
    c_new_row_v, c_new_table_v = item_normalizer._coerce_row("event_user", None, c_row_v)
    assert c_new_table_v is None
    assert c_new_row_v["floatX▶v_json"] == v_list
    assert c_new_row_v["confidenceX▶v_json"] == v_dict
    assert c_new_row_v["strX"] == json.dumps(v_dict)


def test_supports_variant_pua_decode(item_normalizer: JsonLItemsNormalizer) -> None:
    rows = load_json_case("pua_encoded_row")
    # use actual encoding for wei
    from dlt.common.json import _WEI, _HEXBYTES

    rows[0]["_tx_transactionHash"] = rows[0]["_tx_transactionHash"].replace("", _HEXBYTES)
    rows[0]["wad"] = rows[0]["wad"].replace("", _WEI)

    normalized_row = list(
        item_normalizer.schema.normalize_data_item(rows[0], "0912uhj222", "event")
    )
    # pua encoding still present
    assert normalized_row[0][1]["wad"].startswith(_WEI)
    # decode pua
    decoded_row = {k: custom_pua_decode(v) for k, v in normalized_row[0][1].items()}
    assert isinstance(decoded_row["wad"], Wei)
    c_row, new_table = item_normalizer._coerce_row("eth", None, decoded_row)
    assert c_row["wad__v_str"] == str(2**256 - 1)
    assert new_table["columns"]["wad__v_str"]["data_type"] == "text"


def test_supports_variant(item_normalizer: JsonLItemsNormalizer) -> None:
    rows = [
        {"evm": Wei.from_int256(2137 * 10**16, decimals=18)},
        {"evm": Wei.from_int256(2**256 - 1)},
    ]
    normalized_rows: List[Any] = []
    for row in rows:
        normalized_rows.extend(
            item_normalizer.schema.normalize_data_item(row, "128812.2131", "event")
        )
    # row 1 contains Wei
    assert isinstance(normalized_rows[0][1]["evm"], Wei)
    assert normalized_rows[0][1]["evm"] == Wei("21.37")
    # row 2 contains Wei
    assert "evm" in normalized_rows[1][1]
    assert isinstance(normalized_rows[1][1]["evm"], Wei)
    assert normalized_rows[1][1]["evm"] == 2**256 - 1
    # coerce row
    c_row, new_table = item_normalizer._coerce_row("eth", None, normalized_rows[0][1])
    assert isinstance(c_row["evm"], Wei)
    assert c_row["evm"] == Wei("21.37")
    assert new_table["columns"]["evm"]["data_type"] == "wei"
    assert "variant" not in new_table["columns"]["evm"]
    item_normalizer.schema.update_table(new_table)
    # coerce row that should expand to variant
    c_row, new_table = item_normalizer._coerce_row("eth", None, normalized_rows[1][1])
    assert isinstance(c_row["evm__v_str"], str)
    assert c_row["evm__v_str"] == str(2**256 - 1)
    assert new_table["columns"]["evm__v_str"]["data_type"] == "text"
    assert new_table["columns"]["evm__v_str"]["variant"] is True


def test_supports_recursive_variant(item_normalizer: JsonLItemsNormalizer) -> None:
    class RecursiveVariant(int):
        # provide __call__ for SupportVariant
        def __call__(self) -> Any:
            if self == 1:
                return self
            else:
                return ("div2", RecursiveVariant(self // 2))

    row = {"rv": RecursiveVariant(8)}
    c_row, new_table = item_normalizer._coerce_row("rec_variant", None, row)
    # this variant keeps expanding until the value is 1, we start from 8 so there are log2(8) == 3 divisions
    col_name = "rv" + "__v_div2" * 3
    assert c_row[col_name] == 1
    assert new_table["columns"][col_name]["data_type"] == "bigint"
    assert new_table["columns"][col_name]["variant"] is True


def test_supports_variant_autovariant_conflict(item_normalizer: JsonLItemsNormalizer) -> None:
    class PureVariant(int):
        def __init__(self, v: Any) -> None:
            self.v = v

        # provide __call__ for SupportVariant
        def __call__(self) -> Any:
            if isinstance(self.v, int):
                return self.v
            if isinstance(self.v, float):
                return ("text", self.v)

    assert issubclass(PureVariant, int)
    rows = [{"pv": PureVariant(3377)}, {"pv": PureVariant(21.37)}]
    normalized_rows: List[Any] = []
    for row in rows:
        normalized_rows.extend(
            item_normalizer.schema.normalize_data_item(row, "128812.2131", "event")
        )
    assert normalized_rows[0][1]["pv"]() == 3377
    assert normalized_rows[1][1]["pv"]() == ("text", 21.37)
    # first normalized row fits into schema (pv is int)
    _, new_table = item_normalizer._coerce_row("pure_variant", None, normalized_rows[0][1])
    item_normalizer.schema.update_table(new_table)
    assert new_table["columns"]["pv"]["data_type"] == "bigint"
    _, new_table = item_normalizer._coerce_row("pure_variant", None, normalized_rows[1][1])
    # we trick the normalizer to create text variant but actually provide double value
    item_normalizer.schema.update_table(new_table)
    assert new_table["columns"]["pv__v_text"]["data_type"] == "double"

    # second row does not coerce: there's `pv__v_bool` field in it of type double but we already have a column that is text
    with pytest.raises(CannotCoerceColumnException) as exc_val:
        _, new_table = item_normalizer._coerce_row("pure_variant", None, {"pv": "no double"})
    assert exc_val.value.column_name == "pv__v_text"
    assert exc_val.value.from_type == "text"
    assert exc_val.value.to_type == "double"
    assert exc_val.value.coerced_value == "no double"


def test_coerce_new_null_value(item_normalizer: JsonLItemsNormalizer) -> None:
    row = {"timestamp": None}
    new_row, new_table = item_normalizer._coerce_row("event_user", None, row)
    # No new rows, but new column in schema
    assert "timestamp" not in new_row
    assert "data_type" not in new_table["columns"]["timestamp"]
    assert new_table["columns"]["timestamp"]["nullable"] is True
    assert new_table["columns"]["timestamp"]["x-normalizer"]["seen-null-first"] is True


def test_coerce_new_null_value_over_not_null(item_normalizer: JsonLItemsNormalizer) -> None:
    row = {"_dlt_id": None}
    with pytest.raises(CannotCoerceNullException) as exc_info:
        item_normalizer._coerce_row("event_user", None, row)
    # Make sure it was raised by _infer_column
    assert exc_info.traceback[-1].name == "_infer_column"


def test_coerce_null_value_over_existing(item_normalizer: JsonLItemsNormalizer) -> None:
    row = {"timestamp": 82178.1298812}
    new_row, new_table = item_normalizer._coerce_row("event_user", None, row)
    item_normalizer.schema.update_table(new_table)
    row = {"timestamp": None}
    new_row, new_table = item_normalizer._coerce_row("event_user", None, row)
    # do not generate table update on existing column
    assert new_table is None
    assert "timestamp" not in new_row


def test_coerce_null_value_over_existing_incomplete(item_normalizer: JsonLItemsNormalizer) -> None:
    row = {"timestamp": 82178.1298812, "null_col": None}
    _, new_table = item_normalizer._coerce_row("event_user_2", None, row)
    item_normalizer.schema.update_table(new_table)
    # same row again
    _, new_table = item_normalizer._coerce_row("event_user_2", None, row)
    # do not generate table update on existing column
    assert new_table is None


def test_coerce_null_value_over_existing_predefined(item_normalizer: JsonLItemsNormalizer) -> None:
    # create "null_col" upfront
    new_table = utils.new_table("event_user_2", columns=[{"name": "null_col"}])
    item_normalizer.schema.update_table(new_table)
    # we should get new table with "seen-null-first"
    row = {"timestamp": 82178.1298812, "null_col": None}
    _, new_table = item_normalizer._coerce_row("event_user_2", None, row)
    assert new_table["columns"]["null_col"]["x-normalizer"]["seen-null-first"] is True
    item_normalizer.schema.update_table(new_table)
    # same row again
    _, new_table = item_normalizer._coerce_row("event_user_2", None, row)
    # do not generate table update on existing column
    assert new_table is None


def test_coerce_null_value_over_not_null(item_normalizer: JsonLItemsNormalizer) -> None:
    row = {"timestamp": 82178.1298812}
    _, new_table = item_normalizer._coerce_row("event_user", None, row)
    item_normalizer.schema.update_table(new_table)
    item_normalizer.schema.get_table_columns("event_user", include_incomplete=True)["timestamp"][
        "nullable"
    ] = False
    row = {"timestamp": None}
    with pytest.raises(CannotCoerceNullException):
        item_normalizer._coerce_row("event_user", None, row)


@pytest.mark.parametrize(
    "nested_item",
    [
        [1, 2],
        [
            {
                "timestamp": 82178.1298812,
            }
        ],
    ],
    ids=["nested_item_list", "nested_item_dict"],
)
def test_coerce_null_value_in_nested_table(
    item_normalizer: JsonLItemsNormalizer, nested_item: TDataItems
) -> None:
    """Ensure that a column previously created as a child table
    does not attempt new column updates in a subsequent run when it has no values."""

    def _normalize_items_chunk(items: TDataItems) -> TSchemaUpdate:
        schema_update = item_normalizer._normalize_chunk(
            root_table_name="nested",
            items=items,
            may_have_pua=False,
            skip_write=True,
        )
        return schema_update

    # use very long column names
    col_name_a = "a" * (item_normalizer.naming.max_length + 1)
    norm_col_name_a = item_normalizer.naming.normalize_path(col_name_a)
    nested_tbl_name = item_normalizer.naming.shorten_fragments("nested", f"{norm_col_name_a}")

    col_name_b = "b" * (item_normalizer.naming.max_length + 1)
    norm_col_name_b = item_normalizer.naming.normalize_path(col_name_b)
    nested_nested_tbl_name = item_normalizer.naming.shorten_fragments(
        "nested", f"{norm_col_name_a}", f"{norm_col_name_b}"
    )

    # create parent and child tables
    schema_update = _normalize_items_chunk(
        [
            {
                "timestamp": 82178.1298812,
                col_name_a: [
                    {
                        "timestamp": 82178.1298812,
                        col_name_b: nested_item,
                    }
                ],
            },
        ]
    )
    assert "nested" in schema_update
    assert nested_tbl_name in schema_update
    assert nested_nested_tbl_name in schema_update

    # verify that empty child table columns don't create schema updates
    schema_update = _normalize_items_chunk(
        [
            {
                "timestamp": 82178.1298812,
                col_name_a: [
                    {
                        "timestamp": 82178.1298812,
                        col_name_b: None,
                    }
                ],
            },
        ]
    )
    assert not schema_update
    schema_update = _normalize_items_chunk(
        [
            {
                "timestamp": 82178.1298812,
                col_name_a: None,
            },
        ]
    )
    assert not schema_update


@pytest.mark.parametrize(
    "use_very_long_col_name", [True, False], ids=["very_long_col_name", "short_col_name"]
)
def test_coerce_null_value_as_compound_columns(
    item_normalizer: JsonLItemsNormalizer, use_very_long_col_name: bool
) -> None:
    """Ensure that a column previously created as compound column(s) in the same table
    does not attempt new column updates in a subsequent run when it has no values.

    Note: This test also shows an edge case that is not properly handled when very long
    column names are shortened. The item normalizer doesn't maintain a mapping between
    original column names and their normalized/shortened versions. When a null value is
    encountered for a long column name that was previously expanded into compound columns,
    the normalizer can't find the existing compound columns (which were created with
    shortened prefixes) and incorrectly creates a new column with "seen-null-first": True."""

    col_name = "a" * (item_normalizer.naming.max_length + 1) if use_very_long_col_name else "a"
    norm_col_name = item_normalizer.naming.normalize_path(col_name)
    shortened_compound_col_b = item_normalizer.naming.shorten_fragments(norm_col_name, "b")
    shortened_compound_col_c = item_normalizer.naming.shorten_fragments(norm_col_name, "c")

    schema_update = item_normalizer._normalize_chunk(
        root_table_name="nested",
        items=[{"id": "1", col_name: {"b": 1, "c": 2}}],
        may_have_pua=False,
        skip_write=True,
    )

    assert "nested" in schema_update
    assert list(schema_update["nested"][0]["columns"].keys()) == [
        "id",
        shortened_compound_col_b,
        shortened_compound_col_c,
        "_dlt_load_id",
        "_dlt_id",
    ]

    schema_update = item_normalizer._normalize_chunk(
        root_table_name="nested",
        items=[{"id": "1", col_name: None}],
        may_have_pua=False,
        skip_write=True,
    )

    if not use_very_long_col_name:
        assert not schema_update
    else:
        # edge case with very long column name
        assert norm_col_name in schema_update["nested"][0]["columns"]
        assert (
            schema_update["nested"][0]["columns"][norm_col_name]["x-normalizer"]["seen-null-first"]
            is True
        )


def test_infer_with_autodetection(item_normalizer: JsonLItemsNormalizer) -> None:
    # iso timestamp detection
    c = item_normalizer._infer_column("ts", pendulum.now().isoformat())
    assert c["data_type"] == "timestamp"
    item_normalizer.schema._type_detections = []
    c = item_normalizer._infer_column("ts", pendulum.now().timestamp())
    assert c["data_type"] == "double"


def test_infer_with_variant(item_normalizer: JsonLItemsNormalizer) -> None:
    c = item_normalizer._infer_column("ts", pendulum.now().timestamp(), is_variant=True)
    assert c["variant"]
    c = item_normalizer._infer_column("ts", pendulum.now().timestamp())
    assert "variant" not in c


def test_update_schema_parent_missing(item_normalizer: JsonLItemsNormalizer) -> None:
    tab1 = utils.new_table("tab1", parent_table_name="tab_parent")
    # tab_parent is missing in schema
    with pytest.raises(ParentTableNotFoundException) as exc_val:
        item_normalizer.schema.update_table(tab1)
    assert exc_val.value.parent_table_name == "tab_parent"
    assert exc_val.value.table_name == "tab1"


def test_update_schema_table_prop_conflict(item_normalizer: JsonLItemsNormalizer) -> None:
    # parent table conflict
    tab1 = utils.new_table("tab1", write_disposition="append")
    tab_parent = utils.new_table("tab_parent", write_disposition="replace")
    item_normalizer.schema.update_table(tab1)
    item_normalizer.schema.update_table(tab_parent)
    tab1_u1 = deepcopy(tab1)
    tab1_u1["parent"] = "tab_parent"
    with pytest.raises(TablePropertiesConflictException) as exc_val:
        item_normalizer.schema.update_table(tab1_u1)
    assert exc_val.value.table_name == "tab1"
    assert exc_val.value.prop_name == "parent"
    assert exc_val.value.val1 is None
    assert exc_val.value.val2 == "tab_parent"


def test_autodetect_convert_type(item_normalizer: JsonLItemsNormalizer) -> None:
    # add to wei to float converter
    item_normalizer.schema._type_detections = list(item_normalizer.schema._type_detections) + [
        "wei_to_double"
    ]
    row = {"evm": Wei(1)}
    c_row, new_table = item_normalizer._coerce_row("eth", None, row)
    assert c_row["evm"] == 1.0
    assert isinstance(c_row["evm"], float)
    assert new_table["columns"]["evm"]["data_type"] == "double"
    item_normalizer.schema.update_table(new_table)
    # add another row
    row = {"evm": Wei("21.37")}
    c_row, new_table = item_normalizer._coerce_row("eth", None, row)
    assert new_table is None
    assert c_row["evm"] == 21.37
    assert isinstance(c_row["evm"], float)

    # wei are converted to float before variants are generated
    row = {"evm": Wei.from_int256(2**256)}
    c_row, new_table = item_normalizer._coerce_row("eth", None, row)
    assert new_table is None
    assert c_row["evm"] == float(2**256)
    assert isinstance(c_row["evm"], float)

    # make sure variants behave the same

    class AlwaysWei(Decimal):
        def __call__(self) -> Any:
            return ("up", Wei(self))

    # create new column
    row = {"evm2": AlwaysWei(22)}  # type: ignore[dict-item]
    c_row, new_table = item_normalizer._coerce_row("eth", None, row)
    assert c_row["evm2__v_up"] == 22.0
    assert isinstance(c_row["evm2__v_up"], float)
    assert new_table["columns"]["evm2__v_up"]["data_type"] == "double"
    item_normalizer.schema.update_table(new_table)
    # add again
    row = {"evm2": AlwaysWei(22.2)}  # type: ignore[dict-item]
    c_row, new_table = item_normalizer._coerce_row("eth", None, row)
    assert c_row["evm2__v_up"] == 22.2
    assert isinstance(c_row["evm2__v_up"], float)
    assert new_table is None
    # create evm2 column
    row = {"evm2": 22.1}  # type: ignore[dict-item]
    _, new_table = item_normalizer._coerce_row("eth", None, row)
    assert new_table["columns"]["evm2"]["data_type"] == "double"
    item_normalizer.schema.update_table(new_table)
    # and add variant again
    row = {"evm2": AlwaysWei(22.2)}  # type: ignore[dict-item]
    # and this time variant will not be expanded
    # because the "evm2" column already has a type so it goes directly into double as a normal coercion
    c_row, new_table = item_normalizer._coerce_row("eth", None, row)
    assert c_row["evm2"] == 22.2
    assert isinstance(c_row["evm2"], float)


def test_infer_on_incomplete_column(item_normalizer: JsonLItemsNormalizer) -> None:
    # if incomplete column is present, dlt still infers column schema from the data
    # but overrides it with incomplete column
    incomplete_col = utils.new_column("I", nullable=False)
    incomplete_col["primary_key"] = True
    incomplete_col["x-special"] = "spec"  # type: ignore[typeddict-unknown-key]
    table = utils.new_table("table", columns=[incomplete_col])
    item_normalizer.schema.update_table(table, normalize_identifiers=False)
    # make sure that column is still incomplete and has no default hints
    assert item_normalizer.schema.get_table("table")["columns"]["I"] == {
        "name": "I",
        "nullable": False,
        "primary_key": True,
        "x-special": "spec",
    }

    timestamp_float = 78172.128
    # add new column with preferred
    row_1 = {
        "timestamp": timestamp_float,
        "confidence": "0.1",
        "I": "0xFF",
        "number": Decimal("128.67"),
    }
    _, new_table = item_normalizer._coerce_row("table", None, row_1)
    assert "I" in new_table["columns"]
    i_column = new_table["columns"]["I"]
    assert utils.is_complete_column(i_column)
    # has default hints and overrides
    assert i_column["nullable"] is False
    assert i_column["x-special"] == "spec"  # type: ignore[typeddict-item]
    assert i_column["primary_key"] is True
    assert i_column["data_type"] == "text"


def test_update_table_adds_at_end(item_normalizer: JsonLItemsNormalizer) -> None:
    row = {"evm": Wei(1)}
    _, new_table = item_normalizer._coerce_row("eth", None, row)
    item_normalizer.schema.update_table(new_table)
    item_normalizer.schema.update_table(
        {
            "name": new_table["name"],
            "columns": {
                "_dlt_load_id": {
                    "name": "_dlt_load_id",
                    "data_type": "text",
                    "nullable": False,
                }
            },
        }
    )
    table = item_normalizer.schema.tables["eth"]
    # place new columns at the end
    assert list(table["columns"].keys()) == ["evm", "_dlt_load_id"]


def test_keeps_old_name_in_variant_column(item_normalizer: JsonLItemsNormalizer) -> None:
    # for this test use case sensitive naming convention
    os.environ["SCHEMA__NAMING"] = "direct"
    item_normalizer.schema.update_normalizers()
    item_normalizer.naming = item_normalizer.schema.naming
    # create two columns to which json type cannot be coerced
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    _, event_user = item_normalizer._coerce_row("event_user", None, row)
    item_normalizer.schema.update_table(event_user)

    # mock a variant column
    event_user_partial = utils.new_table(
        "event_user",
        columns=[
            {"name": "floatX▶v_complex", "data_type": "json", "variant": True},
            {"name": "confidenceX▶v_complex", "data_type": "json", "variant": False},
        ],
    )
    item_normalizer.schema.update_table(event_user_partial, normalize_identifiers=False)

    # add json types on the same columns
    v_list = [1, 2, "3", {"json": True}]
    v_dict = {"list": [1, 2], "str": "json"}
    c_row_v = {"floatX": v_list, "confidenceX": v_dict}
    # expect two new variant columns to be created
    c_new_row_v, c_new_table_v = item_normalizer._coerce_row("event_user", None, c_row_v)
    c_new_columns_v = list(c_new_table_v["columns"].values())
    print(c_new_row_v)
    print(c_new_table_v)
    # floatX▶v_complex is kept (was marked with variant)
    # confidenceX▶v_json is added (confidenceX▶v_complex not marked as variant)
    assert len(c_new_columns_v) == 1
    assert c_new_columns_v[0]["name"] == "confidenceX▶v_json"
    assert c_new_columns_v[0]["variant"] is True
    # c_row_v coerced to variants
    assert c_new_row_v["floatX▶v_complex"] == v_list
    assert c_new_row_v["confidenceX▶v_json"] == v_dict


def test_preserve_column_order(
    item_normalizer: JsonLItemsNormalizer, schema_storage: SchemaStorage
) -> None:
    schema = item_normalizer.schema
    # python dicts are ordered from v3.6, add 50 column with random names
    update: List[TColumnSchema] = [
        item_normalizer._infer_column("t" + uniq_id(), pendulum.now().timestamp())
        for _ in range(50)
    ]
    schema.update_table(utils.new_table("event_test_order", columns=update))

    def verify_items(table, update) -> None:
        assert [i[0] for i in table.items()] == list(table.keys()) == [u["name"] for u in update]
        assert [i[1] for i in table.items()] == list(table.values()) == update

    table = schema.get_table_columns("event_test_order")
    verify_items(table, update)
    # save and load
    schema_storage.save_schema(schema)
    loaded_schema = schema_storage.load_schema("event")
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update)
    # add more columns
    update2: List[TColumnSchema] = [
        item_normalizer._infer_column("t" + uniq_id(), pendulum.now().timestamp())
        for _ in range(50)
    ]
    loaded_schema.update_table(utils.new_table("event_test_order", columns=update2))
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update + update2)
    # save and load
    schema_storage.save_schema(loaded_schema)
    loaded_schema = schema_storage.load_schema("event")
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update + update2)


@pytest.mark.parametrize(
    "columns,hint,value",
    [
        (
            ["_dlt_id", "_dlt_root_id", "_dlt_load_id", "_dlt_parent_id", "_dlt_list_idx"],
            "nullable",
            False,
        ),
        (["_dlt_id"], "row_key", True),
        (["_dlt_id"], "unique", True),
        (["_dlt_parent_id"], "parent_key", True),
    ],
)
def test_relational_normalizer_schema_hints(
    columns: Sequence[str],
    hint: str,
    value: bool,
    item_normalizer: JsonLItemsNormalizer,
    schema_storage: SchemaStorage,
) -> None:
    item_normalizer.schema = schema_storage.load_schema("event")
    for name in columns:
        # infer column hints
        c = item_normalizer._infer_column(name, "x")
        assert c[hint] is value  # type: ignore[literal-required]


@pytest.mark.parametrize(
    "columns,hint,value",
    [
        (
            [
                "timestamp",
                "_timestamp",
                "_dist_key",
                "_dlt_id",
                "_dlt_root_id",
                "_dlt_load_id",
                "_dlt_parent_id",
                "_dlt_list_idx",
                "sender_id",
            ],
            "nullable",
            False,
        ),
        (["confidence", "_sender_id"], "nullable", True),
        (["timestamp", "_timestamp"], "partition", True),
        (["_dist_key", "sender_id"], "cluster", True),
        (["_dlt_id"], "row_key", True),
        (["_dlt_id"], "unique", True),
        (["_dlt_parent_id"], "parent_key", True),
        (["timestamp", "_timestamp"], "sort", True),
    ],
)
def test_rasa_event_hints(
    columns: Sequence[str],
    hint: str,
    value: bool,
    item_normalizer: JsonLItemsNormalizer,
    schema_storage: SchemaStorage,
) -> None:
    item_normalizer.schema = schema_storage.load_schema("event")
    for name in columns:
        # infer column hints
        c = item_normalizer._infer_column(name, "x")
        assert c[hint] is value  # type: ignore[literal-required]


def test_filter_hints_no_table(
    item_normalizer: JsonLItemsNormalizer, schema_storage: SchemaStorage
) -> None:
    # this is empty schema without any tables
    schema = item_normalizer.schema = schema_storage.load_schema("event")
    bot_case: StrAny = load_json_case("mod_bot_case")
    # actually the empty `event_bot` table exists (holds exclusion filters)
    rows = schema.filter_row_with_hint("event_bot", "not_null", bot_case)
    assert list(rows.keys()) == []

    # must be exactly in order of fields in row: timestamp is first
    rows = schema.filter_row_with_hint("event_action", "not_null", bot_case)
    assert list(rows.keys()) == ["timestamp", "sender_id"]

    rows = schema.filter_row_with_hint("event_action", "primary_key", bot_case)
    assert list(rows.keys()) == []

    # infer table, update schema for the empty bot table
    coerced_row, update = item_normalizer._coerce_row("event_bot", None, bot_case)
    schema.update_table(update)
    # not empty anymore
    assert schema.get_table_columns("event_bot") is not None

    # make sure the column order is the same when inferring from newly created table
    rows = schema.filter_row_with_hint("event_bot", "not_null", coerced_row)
    assert list(rows.keys()) == ["timestamp", "sender_id"]
