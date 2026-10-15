import re
import base64
from typing import Any, Dict
from datetime import date, datetime, time  # noqa: I251

from dlt.common.json import json
from dlt.common.time import (
    get_context_timezone_name,
    normalize_timezone,
    reduce_pendulum_datetime_precision,
)

# use regex to escape characters in single pass
# NUL (\x00) is stripped: postgres/redshift cannot store it in text and duckdb cannot parse it
# inside an inline string literal (the query is a NUL-terminated string)
SQL_ESCAPE_DICT = {"'": "''", "\\": "\\\\", "\n": "\\n", "\r": "\\r", "\x00": ""}


def _make_sql_escape_re(escape_dict: Dict[str, str]) -> re.Pattern:  # type: ignore[type-arg]
    return re.compile(
        "|".join([re.escape(k) for k in sorted(escape_dict, key=len, reverse=True)]),
        flags=re.DOTALL,
    )


SQL_ESCAPE_RE = _make_sql_escape_re(SQL_ESCAPE_DICT)


def _escape_extended(
    v: str,
    prefix: str = "E'",
    escape_dict: Dict[str, str] = None,
    escape_re: re.Pattern = None,  # type: ignore[type-arg]
) -> str:
    escape_dict = escape_dict or SQL_ESCAPE_DICT
    escape_re = escape_re or SQL_ESCAPE_RE
    return "{}{}{}".format(prefix, escape_re.sub(lambda x: escape_dict[x.group(0)], v), "'")


def escape_redshift_literal(v: Any) -> Any:
    if isinstance(v, str):
        # https://www.postgresql.org/docs/9.3/sql-syntax-lexical.html
        # looks like this is the only thing we need to escape for Postgres > 9.1
        # redshift keeps \ as escape character which is pre 9 behavior
        return _escape_extended(v, prefix="'")
    if isinstance(v, bytes):
        return f"from_hex('{v.hex()}')"
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return "json_parse(%s)" % _escape_extended(json.dumps(v), prefix="'")
    if v is None:
        return "NULL"

    return str(v)


def escape_postgres_literal(v: Any) -> Any:
    if isinstance(v, str):
        # we escape extended string which behave like the redshift string
        return _escape_extended(v)
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(json.dumps(v))
    if isinstance(v, bytes):
        return f"'\\x{v.hex()}'"
    if v is None:
        return "NULL"

    return str(v)


def escape_duckdb_literal(v: Any) -> Any:
    if isinstance(v, str):
        # we escape extended string which behave like the redshift string
        return _escape_extended(v)
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(json.dumps(v))
    if isinstance(v, bytes):
        return f"from_base64('{base64.b64encode(v).decode('ascii')}')"
    if v is None:
        return "NULL"

    return str(v)


def escape_lancedb_literal(v: Any) -> Any:
    if isinstance(v, str):
        # we escape extended string which behave like the redshift string
        return _escape_extended(v, prefix="'")
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(json.dumps(v), prefix="'")
    # TODO: check how binaries are represented in fusion
    if isinstance(v, bytes):
        return f"from_base64('{base64.b64encode(v).decode('ascii')}')"
    if v is None:
        return "NULL"

    return str(v)


MS_SQL_ESCAPE_DICT = {
    "'": "''",
    "\n": "' + CHAR(10) + N'",
    "\r": "' + CHAR(13) + N'",
    "\t": "' + CHAR(9) + N'",
}
MS_SQL_ESCAPE_RE = _make_sql_escape_re(MS_SQL_ESCAPE_DICT)


def escape_mssql_literal(v: Any) -> Any:
    if isinstance(v, str):
        return _escape_extended(
            v, prefix="N'", escape_dict=MS_SQL_ESCAPE_DICT, escape_re=MS_SQL_ESCAPE_RE
        )
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(
            json.dumps(v), prefix="N'", escape_dict=MS_SQL_ESCAPE_DICT, escape_re=MS_SQL_ESCAPE_RE
        )
    if isinstance(v, bytes):
        from dlt.destinations.impl.mssql.mssql import VARBINARY_MAX_N

        if len(v) <= VARBINARY_MAX_N:
            n = str(len(v))
        else:
            n = "MAX"
        return f"CONVERT(VARBINARY({n}), '{v.hex()}', 2)"

    if isinstance(v, bool):
        return str(int(v))
    if v is None:
        return "NULL"
    return str(v)


def escape_redshift_identifier(v: str) -> str:
    # in double-quoted identifiers only the double-quote is special (escaped by doubling);
    # backslash is literal for postgres, redshift, snowflake, athena and dremio alike
    return '"' + v.replace('"', '""') + '"'


escape_postgres_identifier = escape_redshift_identifier
escape_athena_identifier = escape_postgres_identifier
escape_dremio_identifier = escape_postgres_identifier


def escape_hive_identifier(v: str) -> str:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    return "`" + v.replace("\\", "\\\\").replace("`", "\\`") + "`"


def escape_snowflake_identifier(v: str) -> str:
    # snowflake uppercases unquoted identifiers; quoting preserves case
    return escape_postgres_identifier(v)


def escape_snowflake_literal(v: Any) -> Any:
    """Escape string literals for Snowflake.

    Snowflake treats backslash as an escape character inside single-quoted literals,
    so both backslash and single quote are escaped (a lone backslash before a doubled
    quote would otherwise consume it and break out of the string).
    """
    if isinstance(v, str):
        return "'" + v.replace("\\", "\\\\").replace("'", "''") + "'"
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return "'" + json.dumps(v).replace("\\", "\\\\").replace("'", "''") + "'"
    if isinstance(v, bytes):
        return f"X'{v.hex()}'"
    return "NULL" if v is None else str(v)


def escape_databricks_identifier(v: str) -> str:
    # databricks escapes an embedded backtick by doubling it; backslash is literal
    return "`" + v.replace("`", "``") + "`"


# NUL stripped to match the base SQL escaping: it would terminate the inlined query string
DATABRICKS_ESCAPE_DICT = {"'": "\\'", "\\": "\\\\", "\n": "\\n", "\r": "\\r", "\x00": ""}
DATABRICKS_ESCAPE_RE = _make_sql_escape_re(DATABRICKS_ESCAPE_DICT)


def escape_databricks_literal(v: Any) -> Any:
    if isinstance(v, str):
        return _escape_extended(
            v, prefix="'", escape_dict=DATABRICKS_ESCAPE_DICT, escape_re=DATABRICKS_ESCAPE_RE
        )
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(
            json.dumps(v),
            prefix="'",
            escape_dict=DATABRICKS_ESCAPE_DICT,
            escape_re=DATABRICKS_ESCAPE_RE,
        )
    if isinstance(v, bytes):
        return f"X'{v.hex()}'"
    return "NULL" if v is None else str(v)


# https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/sql-reference/syntax.md#string
CLICKHOUSE_ESCAPE_DICT = {
    "'": "''",
    "\\": "\\\\",
    "\n": "\\n",
    "\t": "\\t",
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
}

CLICKHOUSE_ESCAPE_RE = _make_sql_escape_re(CLICKHOUSE_ESCAPE_DICT)


def escape_clickhouse_literal(v: Any) -> Any:
    if isinstance(v, str):
        return _escape_extended(
            v, prefix="'", escape_dict=CLICKHOUSE_ESCAPE_DICT, escape_re=CLICKHOUSE_ESCAPE_RE
        )
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(
            json.dumps(v),
            prefix="'",
            escape_dict=CLICKHOUSE_ESCAPE_DICT,
            escape_re=CLICKHOUSE_ESCAPE_RE,
        )
    if isinstance(v, bytes):
        return f"'{v.hex()}'"
    return "NULL" if v is None else str(v)


def escape_clickhouse_identifier(v: str) -> str:
    return "`" + v.replace("`", "``").replace("\\", "\\\\") + "`"


# https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#string_and_bytes_literals
BIGQUERY_ESCAPE_DICT = {
    "'": "\\'",
    "\\": "\\\\",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
    "\b": "\\b",
    "\f": "\\f",
    "\a": "\\a",
    "\v": "\\v",
}

BIGQUERY_ESCAPE_RE = _make_sql_escape_re(BIGQUERY_ESCAPE_DICT)


def escape_bigquery_literal(v: Any) -> Any:
    if isinstance(v, str):
        return _escape_extended(
            v, prefix="'", escape_dict=BIGQUERY_ESCAPE_DICT, escape_re=BIGQUERY_ESCAPE_RE
        )
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(
            json.dumps(v),
            prefix="'",
            escape_dict=BIGQUERY_ESCAPE_DICT,
            escape_re=BIGQUERY_ESCAPE_RE,
        )
    if isinstance(v, bytes):
        return f"FROM_BASE64('{base64.b64encode(v).decode('ascii')}')"
    if isinstance(v, bool):
        return str(v).upper()
    return "NULL" if v is None else str(v)


escape_bigquery_identifier = escape_hive_identifier


def format_datetime_value(v: datetime, precision: int = 6, no_tz: bool = False) -> str:
    """ISO datetime string at given `precision`, optionally naive."""
    if no_tz:
        # same call the loaded value goes through, so literal and stored value agree
        v = normalize_timezone(v, False)
    v = reduce_pendulum_datetime_precision(v, precision)
    if precision < 3:
        timespec = "seconds"
    elif precision < 6:
        timespec = "milliseconds"
    else:
        timespec = "microseconds"
    return v.isoformat(sep=" ", timespec=timespec)


def format_datetime_literal(v: datetime, precision: int = 6, no_tz: bool = False) -> str:
    """Quoted SQL datetime literal."""
    return "'" + format_datetime_value(v, precision, no_tz) + "'"


def format_bigquery_datetime_literal(v: datetime, precision: int = 6, no_tz: bool = False) -> str:
    """Returns BigQuery-adjusted datetime literal by prefixing required `TIMESTAMP` indicator.

    Also works for Presto-based engines.
    """
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#timestamp_literals
    return "TIMESTAMP " + format_datetime_literal(v, precision, no_tz)


def format_clickhouse_datetime_literal(v: datetime, precision: int = 6, no_tz: bool = False) -> str:
    """Returns clickhouse compatible function"""
    # the literal is naive in the context timezone, so `toDateTime64` must read it in that zone
    datetime = format_datetime_literal(v, precision, True)
    return f"toDateTime64({datetime}, {precision}, '{get_context_timezone_name()}')"
