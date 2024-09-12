import io
import pytest
import time
from typing import Iterator

from dlt.common import pendulum, json
from dlt.common.data_writers.exceptions import DataWriterNotFound, SpecLookupFailed
from dlt.common.metrics import DataWriterMetrics
from dlt.common.typing import AnyFun

from dlt.common.data_writers.escape import (
    escape_redshift_identifier,
    escape_hive_identifier,
    escape_redshift_literal,
    escape_postgres_literal,
    escape_duckdb_literal,
)

# import all writers here to check if it can be done without all the dependencies
from dlt.common.data_writers.writers import (
    WRITER_SPECS,
    ArrowToCsvWriter,
    ArrowToInsertValuesWriter,
    ArrowToJsonlWriter,
    ArrowToParquetWriter,
    ArrowToTypedJsonlListWriter,
    CsvWriter,
    DataWriter,
    EMPTY_DATA_WRITER_METRICS,
    ImportFileWriter,
    InsertValuesWriter,
    JsonlWriter,
    create_import_spec,
    get_best_writer_spec,
    resolve_best_writer_spec,
    is_native_writer,
)

from tests.common.utils import load_json_case, row_to_column_schemas

ALL_LITERAL_ESCAPE = [escape_redshift_literal, escape_postgres_literal, escape_duckdb_literal]


class _StringIOWriter(DataWriter):
    _f: io.StringIO


class _BytesIOWriter(DataWriter):
    _f: io.BytesIO


@pytest.fixture
def insert_writer() -> Iterator[DataWriter]:
    from dlt.destinations import redshift

    with io.StringIO() as f:
        yield InsertValuesWriter(f, caps=redshift().capabilities())


@pytest.fixture
def jsonl_writer() -> Iterator[DataWriter]:
    with io.BytesIO() as f:
        yield JsonlWriter(f)


def test_simple_insert_writer(insert_writer: _StringIOWriter) -> None:
    rows = load_json_case("simple_row")
    insert_writer.write_all(row_to_column_schemas(rows[0]), rows)
    lines = insert_writer._f.getvalue().split("\n")
    assert lines[0].startswith("INSERT INTO {}")
    assert '","'.join(rows[0].keys()) in lines[0]
    assert lines[1] == "VALUES"
    assert len(lines) == 4


def test_simple_jsonl_writer(jsonl_writer: _BytesIOWriter) -> None:
    rows = load_json_case("simple_row")
    jsonl_writer.write_all(None, rows)
    # remove b'' at the end
    lines = jsonl_writer._f.getvalue().split(b"\n")
    assert lines[-1] == b""
    assert len(lines) == 3


def test_bytes_insert_writer(insert_writer: _StringIOWriter) -> None:
    rows = [{"bytes": b"bytes"}]
    insert_writer.write_all(row_to_column_schemas(rows[0]), rows)
    lines = insert_writer._f.getvalue().split("\n")
    assert lines[2] == "(from_hex('6279746573'));"


def test_datetime_insert_writer(insert_writer: _StringIOWriter) -> None:
    rows = [{"datetime": pendulum.from_timestamp(1658928602.575267)}]
    insert_writer.write_all(row_to_column_schemas(rows[0]), rows)
    lines = insert_writer._f.getvalue().split("\n")
    assert lines[2] == "('2022-07-27T13:30:02.575267+00:00');"


def test_date_insert_writer(insert_writer: _StringIOWriter) -> None:
    rows = [{"date": pendulum.date(1974, 8, 11)}]
    insert_writer.write_all(row_to_column_schemas(rows[0]), rows)
    lines = insert_writer._f.getvalue().split("\n")
    assert lines[2] == "('1974-08-11');"


@pytest.mark.skip("not implemented")
def test_unicode_insert_writer_postgres() -> None:
    # implements tests for the postgres encoding -> same cases as redshift
    pass


def test_unicode_insert_writer(insert_writer: _StringIOWriter) -> None:
    rows = load_json_case("weird_rows")
    insert_writer.write_all(row_to_column_schemas(rows[0]), rows)
    lines = insert_writer._f.getvalue().split("\n")
    assert lines[2].endswith("', NULL''); DROP SCHEMA Public --'),")
    assert lines[3].endswith("'イロハニホヘト チリヌルヲ ''ワカヨタレソ ツネナラム'),")
    assert lines[4].endswith("'ऄअआइ''ईउऊऋऌऍऎए'),")
    assert lines[5].endswith("hello\\nworld\t\t\t\\r\x06'),")


def test_string_literal_escape() -> None:
    assert escape_redshift_literal(", NULL'); DROP TABLE --") == "', NULL''); DROP TABLE --'"
    assert escape_redshift_literal(", NULL');\n DROP TABLE --") == "', NULL'');\\n DROP TABLE --'"
    assert escape_redshift_literal(", NULL);\n DROP TABLE --") == "', NULL);\\n DROP TABLE --'"
    assert (
        escape_redshift_literal(", NULL);\\n DROP TABLE --\\")
        == "', NULL);\\\\n DROP TABLE --\\\\'"
    )
    # assert escape_redshift_literal(b'hello_word') == "\\x68656c6c6f5f776f7264"


@pytest.mark.parametrize("escaper", ALL_LITERAL_ESCAPE)
def test_string_nested_escape(escaper: AnyFun) -> None:
    doc = {
        "nested": [1, 2, 3, "a"],
        "link": (
            "?commen\ntU\nrn=urn%3Ali%3Acomment%3A%28acti\0xA \0x0"
            " \\vity%3A69'08444473\n\n551163392%2C6n \r \x8e9085"
        ),
    }
    escaped = escaper(doc)
    # should be same as string escape
    if escaper == escape_redshift_literal:
        assert escaped == f"json_parse({escaper(json.dumps(doc))})"
    else:
        assert escaped == escaper(json.dumps(doc))


def test_identifier_escape() -> None:
    assert (
        escape_redshift_identifier(", NULL'); DROP TABLE\" -\\-")
        == '", NULL\'); DROP TABLE"" -\\\\-"'
    )


def test_identifier_escape_bigquery() -> None:
    assert (
        escape_hive_identifier(", NULL'); DROP TABLE\"` -\\-")
        == "`, NULL'); DROP TABLE\"\\` -\\\\-`"
    )


def test_string_literal_escape_unicode() -> None:
    # test on some unicode characters
    assert escape_redshift_literal(", NULL);\n DROP TABLE --") == "', NULL);\\n DROP TABLE --'"
    assert (
        escape_redshift_literal("イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム")
        == "'イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム'"
    )
    assert escape_redshift_identifier('ąćł"') == '"ąćł"""'
    assert (
        escape_redshift_identifier('イロハニホヘト チリヌルヲ "ワカヨタレソ ツネナラム')
        == '"イロハニホヘト チリヌルヲ ""ワカヨタレソ ツネナラム"'
    )


def test_data_writer_metrics_add() -> None:
    now = time.time()
    metrics = DataWriterMetrics("file", 10, 100, now, now + 10)
    add_m: DataWriterMetrics = metrics + EMPTY_DATA_WRITER_METRICS  # type: ignore[assignment]
    assert add_m == DataWriterMetrics("", 10, 100, now, now + 10)
    # will keep "file" because it is in both
    assert metrics + metrics == DataWriterMetrics("file", 20, 200, now, now + 10)
    assert sum((metrics, metrics, metrics), EMPTY_DATA_WRITER_METRICS) == DataWriterMetrics(
        "", 30, 300, now, now + 10
    )
    # time range extends when added
    add_m = metrics + DataWriterMetrics("fileX", 99, 120, now - 10, now + 20)  # type: ignore[assignment]
    assert add_m == DataWriterMetrics("", 109, 220, now - 10, now + 20)


def test_is_native_writer() -> None:
    assert is_native_writer(InsertValuesWriter)
    assert is_native_writer(ArrowToCsvWriter)
    assert is_native_writer(CsvWriter)
    # and not one with adapter
    assert is_native_writer(ArrowToJsonlWriter) is False


def test_resolve_best_writer() -> None:
    assert (
        WRITER_SPECS[
            resolve_best_writer_spec("arrow", ("insert_values", "jsonl", "parquet", "csv"))
        ]
        == ArrowToParquetWriter
    )
    assert WRITER_SPECS[resolve_best_writer_spec("object", ("jsonl",))] == JsonlWriter
    # order of what is possible matters
    assert (
        WRITER_SPECS[
            resolve_best_writer_spec("arrow", ("insert_values", "jsonl", "csv", "parquet"))
        ]
        == ArrowToCsvWriter
    )
    # jsonl is ignored because it is not native
    assert (
        WRITER_SPECS[
            resolve_best_writer_spec("arrow", ("insert_values", "jsonl", "parquet"), "jsonl")
        ]
        == ArrowToParquetWriter
    )
    # csv not possible
    with pytest.raises(ValueError):
        resolve_best_writer_spec("arrow", ("insert_values", "jsonl", "parquet"), "csv")
    # csv is native and preferred so goes over parquet
    assert (
        WRITER_SPECS[
            resolve_best_writer_spec("arrow", ("insert_values", "jsonl", "parquet", "csv"), "csv")
        ]
        == ArrowToCsvWriter
    )

    # not a native writer
    assert (
        WRITER_SPECS[
            resolve_best_writer_spec(
                "arrow",
                ("insert_values",),
            )
        ]
        == ArrowToInsertValuesWriter
    )
    assert (
        WRITER_SPECS[
            resolve_best_writer_spec("arrow", ("insert_values", "typed-jsonl"), "typed-jsonl")
        ]
        == ArrowToTypedJsonlListWriter
    )

    # no route
    with pytest.raises(SpecLookupFailed) as spec_ex:
        resolve_best_writer_spec("arrow", ("tsv",), "tsv")  # type: ignore[arg-type]
    assert spec_ex.value.possible_file_formats == ("tsv",)
    assert spec_ex.value.data_item_format == "arrow"
    assert spec_ex.value.file_format == "tsv"


def test_get_best_writer() -> None:
    assert WRITER_SPECS[get_best_writer_spec("arrow", "csv")] == ArrowToCsvWriter
    assert WRITER_SPECS[get_best_writer_spec("object", "csv")] == CsvWriter
    assert WRITER_SPECS[get_best_writer_spec("arrow", "insert_values")] == ArrowToInsertValuesWriter
    with pytest.raises(DataWriterNotFound):
        get_best_writer_spec("arrow", "tsv")  # type: ignore


def test_import_file_writer() -> None:
    spec = create_import_spec("jsonl", ["jsonl"])
    assert spec.data_item_format == "file"
    assert spec.file_format == "jsonl"
    writer = DataWriter.writer_class_from_spec(spec)
    assert writer is ImportFileWriter
    w_ = writer(None)
    with pytest.raises(NotImplementedError):
        w_.write_header(None)
