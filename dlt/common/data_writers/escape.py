import re
import base64
from typing import Any
from datetime import date, datetime  # noqa: I251

from dlt.common.json import json

# use regex to escape characters in single pass
SQL_ESCAPE_DICT = {"'": "''", "\\": "\\\\", "\n": "\\n", "\r": "\\r"}
SQL_ESCAPE_RE = re.compile("|".join([re.escape(k) for k in sorted(SQL_ESCAPE_DICT, key=len, reverse=True)]), flags=re.DOTALL)


def _escape_extended(v: str, prefix:str = "E'") -> str:
    return "{}{}{}".format(prefix, SQL_ESCAPE_RE.sub(lambda x: SQL_ESCAPE_DICT[x.group(0)], v), "'")


def escape_redshift_literal(v: Any) -> Any:
    if isinstance(v, str):
        # https://www.postgresql.org/docs/9.3/sql-syntax-lexical.html
        # looks like this is the only thing we need to escape for Postgres > 9.1
        # redshift keeps \ as escape character which is pre 9 behavior
        return _escape_extended(v, prefix="'")
    if isinstance(v, bytes):
        return f"from_hex('{v.hex()}')"
    if isinstance(v, (datetime, date)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return "json_parse(%s)" % _escape_extended(json.dumps(v), prefix='\'')

    return str(v)


def escape_postgres_literal(v: Any) -> Any:
    if isinstance(v, str):
        # we escape extended string which behave like the redshift string
        return _escape_extended(v)
    if isinstance(v, (datetime, date)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(json.dumps(v))
    if isinstance(v, bytes):
        return f"'\\x{v.hex()}'"

    return str(v)


def escape_duckdb_literal(v: Any) -> Any:
    if isinstance(v, str):
        # we escape extended string which behave like the redshift string
        return _escape_extended(v)
    if isinstance(v, (datetime, date)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(json.dumps(v))
    if isinstance(v, bytes):
        return f"from_base64('{base64.b64encode(v).decode('ascii')}')"

    return str(v)


def escape_redshift_identifier(v: str) -> str:
    return '"' + v.replace('"', '""').replace("\\", "\\\\") + '"'


escape_postgres_identifier = escape_redshift_identifier


def escape_bigquery_identifier(v: str) -> str:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    return "`" + v.replace("\\", "\\\\").replace("`","\\`") + "`"


def escape_snowflake_identifier(v: str) -> str:
    # Snowcase uppercase all identifiers unless quoted. Match this here so queries on information schema work without issue
    # See also https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers
    return escape_postgres_identifier(v.upper())
