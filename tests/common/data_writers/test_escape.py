import pytest
from datetime import datetime, timezone  # noqa: I251
from zoneinfo import ZoneInfo
from typing import Any

from dlt.common import pendulum, json
from dlt.common.time import set_context_timezone
from dlt.common.typing import AnyFun

from dlt.common.data_writers.escape import (
    escape_redshift_identifier,
    escape_snowflake_identifier,
    escape_snowflake_literal,
    escape_hive_identifier,
    escape_databricks_identifier,
    escape_databricks_literal,
    escape_redshift_literal,
    escape_postgres_literal,
    escape_duckdb_literal,
    escape_bigquery_literal,
    escape_bigquery_identifier,
    format_datetime_literal,
    format_clickhouse_datetime_literal,
    format_datetime_value,
)

ALL_LITERAL_ESCAPE = [
    escape_redshift_literal,
    escape_postgres_literal,
    escape_duckdb_literal,
    escape_bigquery_literal,
]

_TZ_FORMS = {
    "naive": pendulum.naive(2024, 3, 4, 5, 6, 7, 123456),
    "utc": pendulum.datetime(2024, 3, 4, 5, 6, 7, 123456, tz="UTC"),
    "berlin": pendulum.datetime(2024, 3, 4, 5, 6, 7, 123456, tz="Europe/Berlin"),
}


def test_string_literal_escape() -> None:
    assert escape_redshift_literal(", NULL'); DROP TABLE --") == "', NULL''); DROP TABLE --'"
    assert escape_redshift_literal(", NULL');\n DROP TABLE --") == "', NULL'');\\n DROP TABLE --'"
    assert escape_redshift_literal(", NULL);\n DROP TABLE --") == "', NULL);\\n DROP TABLE --'"
    assert (
        escape_redshift_literal(", NULL);\\n DROP TABLE --\\")
        == "', NULL);\\\\n DROP TABLE --\\\\'"
    )
    # assert escape_redshift_literal(b'hello_word') == "\\x68656c6c6f5f776f7264"
    assert (
        escape_snowflake_literal('@schema."%my table"/"load_id"')
        == '\'@schema."%my table"/"load_id"\''
    )
    assert escape_snowflake_literal("file:///tmp/o'hara/f.jsonl") == "'file:///tmp/o''hara/f.jsonl'"


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


@pytest.mark.parametrize(
    "precision,tz_form,naive_input,expected",
    [
        # naive input — no_tz toggle has no effect
        (6, "naive", False, "2024-03-04 05:06:07.123456"),
        (3, "naive", False, "2024-03-04 05:06:07.123"),
        (0, "naive", False, "2024-03-04 05:06:07"),
        # tz-aware UTC — no_tz=False keeps the offset
        (6, "utc", False, "2024-03-04 05:06:07.123456+00:00"),
        (3, "utc", False, "2024-03-04 05:06:07.123+00:00"),
        (0, "utc", False, "2024-03-04 05:06:07+00:00"),
        # tz-aware UTC — no_tz=True strips the offset, value unchanged
        (6, "utc", True, "2024-03-04 05:06:07.123456"),
        (3, "utc", True, "2024-03-04 05:06:07.123"),
        # tz-aware non-UTC — no_tz=True converts to the context timezone (UTC here) then strips
        # 05:06:07 Europe/Berlin (UTC+1, CET, no DST in March before the last Sunday) -> 04:06:07 UTC
        (6, "berlin", True, "2024-03-04 04:06:07.123456"),
        (3, "berlin", True, "2024-03-04 04:06:07.123"),
        # tz-aware non-UTC — no_tz=False keeps original offset
        (6, "berlin", False, "2024-03-04 05:06:07.123456+01:00"),
    ],
    ids=lambda v: str(v),
)
def test_format_datetime_value(
    precision: int, tz_form: str, naive_input: bool, expected: str
) -> None:
    v = _TZ_FORMS[tz_form]
    value = format_datetime_value(v, precision=precision, no_tz=naive_input)
    assert value == expected
    # format_datetime_literal is a thin wrapper that adds quotes
    assert format_datetime_literal(v, precision=precision, no_tz=naive_input) == f"'{expected}'"


@pytest.mark.parametrize(
    "tz_form,no_tz,expected",
    [
        # a naive literal is the context wall clock, an aware one keeps its offset
        pytest.param("utc", True, "2024-03-04 06:06:07.123456", id="utc-input-naive-literal"),
        pytest.param("berlin", True, "2024-03-04 05:06:07.123456", id="berlin-input-naive-literal"),
        pytest.param("naive", True, "2024-03-04 05:06:07.123456", id="naive-input-untouched"),
        pytest.param(
            "berlin", False, "2024-03-04 05:06:07.123456+01:00", id="aware-literal-keeps-offset"
        ),
    ],
)
def test_format_datetime_value_in_context_timezone(
    tz_form: str, no_tz: bool, expected: str
) -> None:
    set_context_timezone(ZoneInfo("Europe/Berlin"))
    assert format_datetime_value(_TZ_FORMS[tz_form], no_tz=no_tz) == expected


@pytest.mark.parametrize(
    "context_tz,tz_form,expected_value",
    [
        pytest.param("UTC", "naive", "2024-03-04 05:06:07.123456", id="utc-ctx-naive"),
        pytest.param("UTC", "utc", "2024-03-04 05:06:07.123456", id="utc-ctx-utc"),
        # 05:06:07+01:00 is 04:06:07 UTC, and the literal must not be shifted twice
        pytest.param("UTC", "berlin", "2024-03-04 04:06:07.123456", id="utc-ctx-berlin"),
        # the literal is the context wall clock and the zone argument names the context
        pytest.param("Europe/Berlin", "naive", "2024-03-04 05:06:07.123456", id="berlin-ctx-naive"),
        pytest.param("Europe/Berlin", "utc", "2024-03-04 06:06:07.123456", id="berlin-ctx-utc"),
        pytest.param(
            "Europe/Berlin", "berlin", "2024-03-04 05:06:07.123456", id="berlin-ctx-berlin"
        ),
    ],
)
def test_format_clickhouse_datetime_literal(
    context_tz: str, tz_form: str, expected_value: str
) -> None:
    """`toDateTime64` reads the literal in the zone named by its third argument."""
    set_context_timezone(ZoneInfo(context_tz))
    v = datetime(2024, 3, 4, 5, 6, 7, 123456)
    if tz_form == "utc":
        v = v.replace(tzinfo=timezone.utc)
    elif tz_form == "berlin":
        v = v.replace(tzinfo=ZoneInfo("Europe/Berlin"))

    assert (
        format_clickhouse_datetime_literal(v)
        == f"toDateTime64('{expected_value}', 6, '{context_tz}')"
    )


def test_identifier_escape() -> None:
    # only the double-quote is doubled; backslash is literal in double-quoted identifiers
    assert (
        escape_redshift_identifier(", NULL'); DROP TABLE\" -\\-")
        == '", NULL\'); DROP TABLE"" -\\-"'
    )


def test_escape_snowflake_identifier() -> None:
    # only the double-quote is doubled; backslash stays literal so structured-type field
    # names round-trip against the data keys they must match at load time
    assert escape_snowflake_identifier('a"b') == '"a""b"'
    assert escape_snowflake_identifier("back\\slash") == '"back\\slash"'
    assert escape_snowflake_identifier("🦆 日本語 naïve") == '"🦆 日本語 naïve"'
    assert escape_snowflake_identifier("MixedCase") == '"MixedCase"'


def test_escape_snowflake_literal() -> None:
    # snowflake treats backslash as an escape char in '...' literals, so both backslash and
    # single quote must be escaped, else a backslash before a doubled quote breaks out
    assert escape_snowflake_literal("plain") == "'plain'"
    assert escape_snowflake_literal("a'b") == "'a''b'"
    assert escape_snowflake_literal("back\\slash") == "'back\\\\slash'"
    assert escape_snowflake_literal("\\'") == "'\\\\'''"
    assert escape_snowflake_literal("end\\") == "'end\\\\'"
    # json (list/dict) literals escape backslashes too
    assert escape_snowflake_literal({"k": "a\\b"}) == '\'{"k":"a\\\\\\\\b"}\''


def test_escape_hive_identifier() -> None:
    assert (
        escape_hive_identifier(", NULL'); DROP TABLE\"` -\\-")
        == "`, NULL'); DROP TABLE\"\\` -\\\\-`"
    )


def test_escape_databricks_identifier() -> None:
    # databricks doubles an embedded backtick (not backslash-escaped like hive); backslash is literal
    assert escape_databricks_identifier("a`b") == "`a``b`"
    assert escape_databricks_identifier("back\\slash") == "`back\\slash`"
    assert escape_databricks_identifier("plain") == "`plain`"


def test_escape_databricks_literal() -> None:
    assert escape_databricks_literal("a'b") == "'a\\'b'"
    assert escape_databricks_literal("back\\slash") == "'back\\\\slash'"
    # NUL is stripped (it would terminate the inlined query) instead of raising KeyError
    assert escape_databricks_literal("a\x00b") == "'ab'"
    assert escape_databricks_literal({"k": "a'b"}) == '\'{"k":"a\\\'b"}\''


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


@pytest.mark.parametrize(
    "value,expected",
    [
        (", NULL'); DROP TABLE --", "', NULL\\'); DROP TABLE --'"),
        ("", "''"),
        ("hello\tworld", "'hello\\tworld'"),
        ("bell\a", "'bell\\a'"),
        ("path\\to\\file", "'path\\\\to\\\\file'"),
        (b"hello", "FROM_BASE64('aGVsbG8=')"),
        (b"\x00\x01\x02", "FROM_BASE64('AAEC')"),
        (pendulum.datetime(2023, 1, 15, 12, 30, 45), "'2023-01-15T12:30:45+00:00'"),
        (pendulum.date(2023, 1, 15), "'2023-01-15'"),
        (
            {"key": "value", "nested": {"inner": "data"}},
            '\'{"key":"value","nested":{"inner":"data"}}\'',
        ),
        ([1, 2, 3, "four"], "'[1,2,3,\"four\"]'"),
        (True, "TRUE"),
        (False, "FALSE"),
        (None, "NULL"),
        (
            "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム",
            "'イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム'",
        ),
    ],
    ids=[
        "sql_injection_attempt",
        "empty_string",
        "tab_char",
        "bell_char",
        "backslash_path",
        "bytes_simple",
        "bytes_binary",
        "datetime",
        "date",
        "dict_json",
        "list_json",
        "bool_true",
        "bool_false",
        "null",
        "unicode_japanese",
    ],
)
def test_bigquery_literal_escape(value: Any, expected: str) -> None:
    """Test escape_bigquery_literal with various datatypes."""
    result = escape_bigquery_literal(value)
    assert result == expected


def test_escape_bigquery_identifier() -> None:
    # BigQuery identifier escaping uses backticks (same as Hive)
    assert escape_bigquery_identifier("table_name") == "`table_name`"
    assert escape_bigquery_identifier("table`name") == "`table\\`name`"
    assert escape_bigquery_identifier("table\\name") == "`table\\\\name`"
