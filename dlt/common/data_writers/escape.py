import re
import base64
from typing import Any, Dict
from datetime import date, datetime, time  # noqa: I251

from dlt.common.json import json

# use regex to escape characters in single pass
SQL_ESCAPE_DICT = {"'": "''", "\\": "\\\\", "\n": "\\n", "\r": "\\r"}


def _make_sql_escape_re(escape_dict: Dict[str, str]) -> re.Pattern:  # type: ignore[type-arg]
    return re.compile(
        "|".join([re.escape(k) for k in sorted(escape_dict, key=len, reverse=True)]),
        flags=re.DOTALL,
    )


SQL_ESCAPE_RE = _make_sql_escape_re(SQL_ESCAPE_DICT)


def _escape_extended(
    v: str, prefix: str = "E'", escape_dict: Dict[str, str] = None, escape_re: re.Pattern = None  # type: ignore[type-arg]
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
    return '"' + v.replace('"', '""').replace("\\", "\\\\") + '"'


escape_postgres_identifier = escape_redshift_identifier
escape_athena_identifier = escape_postgres_identifier


def escape_bigquery_identifier(v: str) -> str:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    return "`" + v.replace("\\", "\\\\").replace("`", "\\`") + "`"


def escape_snowflake_identifier(v: str) -> str:
    # Snowcase uppercase all identifiers unless quoted. Match this here so queries on information schema work without issue
    # See also https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers
    return escape_postgres_identifier(v.upper())


escape_databricks_identifier = escape_bigquery_identifier


DATABRICKS_ESCAPE_DICT = {"'": "\\'", "\\": "\\\\", "\n": "\\n", "\r": "\\r"}


def escape_databricks_literal(v: Any) -> Any:
    if isinstance(v, str):
        return _escape_extended(v, prefix="'", escape_dict=DATABRICKS_ESCAPE_DICT)
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return _escape_extended(json.dumps(v), prefix="'", escape_dict=DATABRICKS_ESCAPE_DICT)
    if isinstance(v, bytes):
        return f"X'{v.hex()}'"
    if v is None:
        return "NULL"

    return str(v)
