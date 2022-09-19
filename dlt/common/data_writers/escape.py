import re

# use regex to escape characters in single pass
SQL_ESCAPE_DICT = {"'": "''", "\\": "\\\\", "\n": "\\n", "\r": "\\r"}
SQL_ESCAPE_RE = re.compile("|".join([re.escape(k) for k in sorted(SQL_ESCAPE_DICT, key=len, reverse=True)]), flags=re.DOTALL)


def escape_redshift_literal(v: str) -> str:
    # https://www.postgresql.org/docs/9.3/sql-syntax-lexical.html
    # looks like this is the only thing we need to escape for Postgres > 9.1
    # redshift keeps \ as escape character which is pre 9 behavior
    return "{}{}{}".format("'", SQL_ESCAPE_RE.sub(lambda x: SQL_ESCAPE_DICT[x.group(0)], v), "'")


def escape_redshift_identifier(v: str) -> str:
    return '"' + v.replace('"', '""').replace("\\", "\\\\") + '"'


def escape_bigquery_identifier(v: str) -> str:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    return "`" + v.replace("\\", "\\\\").replace("`","\\`") + "`"
