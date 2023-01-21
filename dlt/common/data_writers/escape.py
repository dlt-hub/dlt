import re
from typing import Any
from datetime import date, datetime  # noqa: I251

from dlt.common import json

# use regex to escape characters in single pass
SQL_ESCAPE_DICT = {"'": "''", "\\": "\\\\", "\n": "\\n", "\r": "\\r"}
SQL_ESCAPE_RE = re.compile("|".join([re.escape(k) for k in sorted(SQL_ESCAPE_DICT, key=len, reverse=True)]), flags=re.DOTALL)


def escape_redshift_literal(v: Any) -> Any:
    if isinstance(v, str):
        # https://www.postgresql.org/docs/9.3/sql-syntax-lexical.html
        # looks like this is the only thing we need to escape for Postgres > 9.1
        # redshift keeps \ as escape character which is pre 9 behavior
        return "{}{}{}".format("'", SQL_ESCAPE_RE.sub(lambda x: SQL_ESCAPE_DICT[x.group(0)], v), "'")
    if isinstance(v, bytes):
        return f"from_hex('{v.hex()}')"
    if isinstance(v, (datetime, date)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return f"json_parse('{json.dumps(v)}')"

    return str(v)


def escape_postgres_literal(v: Any) -> Any:
    if isinstance(v, str):
        # we escape extended string which behave like the redshift string
        return "{}{}{}".format("E'", SQL_ESCAPE_RE.sub(lambda x: SQL_ESCAPE_DICT[x.group(0)], v), "'")
    if isinstance(v, bytes):
        return f"'\\x{v.hex()}'"
    if isinstance(v, (datetime, date)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return f"'{json.dumps(v)}'"

    return str(v)


def escape_redshift_identifier(v: str) -> str:
    return '"' + v.replace('"', '""').replace("\\", "\\\\") + '"'


escape_postgres_identifier = escape_redshift_identifier


def escape_bigquery_identifier(v: str) -> str:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    return "`" + v.replace("\\", "\\\\").replace("`","\\`") + "`"
