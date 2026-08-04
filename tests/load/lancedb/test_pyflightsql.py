from typing import Any, List, Optional, Tuple

import pyarrow
import pytest

from dlt.destinations.impl.lancedb.pyflightsql import (
    STATEMENT_QUERY_TYPE_URL,
    FlightSqlCursor,
    _encode_varint,
    interpolate_parameters,
    statement_query_command,
)


pytestmark = pytest.mark.essential


def _read_varint(data: bytes, pos: int) -> Tuple[int, int]:
    value = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7


def _read_len_field(data: bytes, pos: int) -> Tuple[int, bytes, int]:
    """Reads one length-delimited protobuf field, returning (field_number, payload, next_pos)."""
    tag, pos = _read_varint(data, pos)
    assert tag & 0x07 == 2, "expected a length-delimited wire type"
    length, pos = _read_varint(data, pos)
    return tag >> 3, data[pos : pos + length], pos + length


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(0, b"\x00", id="zero"),
        pytest.param(1, b"\x01", id="one"),
        pytest.param(127, b"\x7f", id="single-byte-max"),
        pytest.param(128, b"\x80\x01", id="two-byte-min"),
        pytest.param(300, b"\xac\x02", id="two-byte"),
        pytest.param(16383, b"\xff\x7f", id="two-byte-max"),
        pytest.param(16384, b"\x80\x80\x01", id="three-byte-min"),
    ],
)
def test_encode_varint(value: int, expected: bytes) -> None:
    assert _encode_varint(value) == expected


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT 1", id="short"),
        pytest.param(
            "select * from t where id in (" + ",".join("1" * 200) + ")", id="multi-byte-len"
        ),
        pytest.param("select 'ünïcödé—λ' as s", id="unicode"),
        pytest.param("", id="empty"),
    ],
)
def test_statement_query_command_round_trip(query: str) -> None:
    command = statement_query_command(query)

    # outer message is a google.protobuf.Any: type_url then the packed value
    field_number, type_url, pos = _read_len_field(command, 0)
    assert field_number == 1
    assert type_url == STATEMENT_QUERY_TYPE_URL
    field_number, value, pos = _read_len_field(command, pos)
    assert field_number == 2
    assert pos == len(command)

    # inner message is CommandStatementQuery with the query in field 1
    field_number, encoded_query, pos = _read_len_field(value, 0)
    assert field_number == 1
    assert encoded_query.decode("utf-8") == query
    assert pos == len(value)


@pytest.mark.parametrize(
    ("query", "parameters", "expected"),
    [
        pytest.param("select 1", None, "select 1", id="no-parameters"),
        pytest.param("select 1", (), "select 1", id="empty-parameters"),
        pytest.param("select %s", (1,), "select 1", id="int"),
        pytest.param("select %s", ("a",), "select 'a'", id="str"),
        pytest.param("select %s", ("O'Brien",), "select 'O''Brien'", id="str-with-quote"),
        pytest.param("select %s", (None,), "select NULL", id="none"),
        pytest.param("select %s, %s", (1, "a"), "select 1, 'a'", id="multiple"),
    ],
)
def test_interpolate_parameters(query: str, parameters: Any, expected: str) -> None:
    assert interpolate_parameters(query, parameters) == expected


def _cursor_over(batches: List[pyarrow.RecordBatch], schema: pyarrow.Schema) -> FlightSqlCursor:
    cursor = FlightSqlCursor(None)
    cursor._schema = schema
    cursor._batches = iter(batches)
    return cursor


@pytest.fixture
def batches() -> Tuple[List[pyarrow.RecordBatch], pyarrow.Schema]:
    schema = pyarrow.schema([pyarrow.field("x", pyarrow.int64())])
    # two server batches, so chunking must both split and span them
    return [
        pyarrow.RecordBatch.from_pydict({"x": [1, 2, 3]}, schema=schema),
        pyarrow.RecordBatch.from_pydict({"x": [4, 5]}, schema=schema),
    ], schema


@pytest.mark.parametrize(
    ("chunk_size", "expected"),
    [
        pytest.param(None, [5], id="no-chunking"),
        pytest.param(1, [1, 1, 1, 1, 1], id="single-row-chunks"),
        pytest.param(2, [2, 2, 1], id="splits-batches"),
        pytest.param(3, [3, 2], id="batch-aligned"),
        pytest.param(10, [5], id="chunk-larger-than-result"),
    ],
)
def test_iter_arrow_tables_honours_chunk_size(
    batches: Tuple[List[pyarrow.RecordBatch], pyarrow.Schema],
    chunk_size: Optional[int],
    expected: List[int],
) -> None:
    cursor = _cursor_over(*batches)
    tables = list(cursor.iter_arrow_tables(chunk_size))
    assert [table.num_rows for table in tables] == expected
    assert [row for table in tables for row in table.column("x").to_pylist()] == [1, 2, 3, 4, 5]


def test_fetch_methods_page_across_batches(
    batches: Tuple[List[pyarrow.RecordBatch], pyarrow.Schema],
) -> None:
    cursor = _cursor_over(*batches)
    assert cursor.fetchone() == (1,)
    assert cursor.fetchmany(2) == [(2,), (3,)]
    assert cursor.fetchall() == [(4,), (5,)]
    assert cursor.fetchall() == []
    assert cursor.fetchone() is None


def test_description_and_empty_result(
    batches: Tuple[List[pyarrow.RecordBatch], pyarrow.Schema],
) -> None:
    _, schema = batches
    assert FlightSqlCursor(None).description is None

    cursor = _cursor_over([], schema)
    assert [column[0] for column in cursor.description] == ["x"]
    assert cursor.fetch_arrow_table().num_rows == 0
    assert cursor.fetchall() == []
