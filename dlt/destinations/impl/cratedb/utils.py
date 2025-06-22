import json
import platform
from datetime import datetime, date, time
from typing import Union, Sequence, Any, Optional

from dlt.common.data_writers.escape import _escape_extended, _make_sql_escape_re

if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
import psycopg2.sql


# CrateDB does not accept the original `{"'": "''", "\\": "\\\\", "\n": "\\n", "\r": "\\r"}`?
SQL_ESCAPE_DICT = {"'": "''"}
SQL_ESCAPE_RE = _make_sql_escape_re(SQL_ESCAPE_DICT)


def _escape_extended_cratedb(v: str) -> str:
    """
    The best-practice escaper for CrateDB, discovered by trial-and-error.
    """
    return _escape_extended(v, prefix="'", escape_dict=SQL_ESCAPE_DICT, escape_re=SQL_ESCAPE_RE)


def escape_cratedb_literal(v: Any) -> Any:
    """
    Based on `escape_postgres_literal`, with a mix of `escape_redshift_literal`.

    CrateDB needs a slightly adjusted escaping of literals.
    Examples: "L'Aupillon" and "Pizzas d'Anarosa" from `sys.summits`.

    It possibly also doesn't support the `E'` prefix as employed by the PostgreSQL escaper?
    Fortunately, the Redshift escaper came to the rescue, providing a reasonable baseline.

    CrateDB also needs support when serializing container types ARRAY vs. OBJECT.
    """
    if isinstance(v, str):
        return _escape_extended_cratedb(v)
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    # CrateDB, when serializing from an incoming `json` or `jsonb` type, the type mapper
    # can't know about what's actually inside, so arrays need a special treatment.
    if isinstance(v, list):
        v = {"array": v}
    if isinstance(v, dict):
        return _escape_extended_cratedb(json.dumps(v)) + "::OBJECT(DYNAMIC)"
    if isinstance(v, bytes):
        return f"'\\x{v.hex()}'"
    if v is None:
        return "NULL"

    return str(v)


class SystemColumnWorkaround:
    """
    CrateDB reserves `_`-prefixed column names for internal purposes.

    When approaching CrateDB with such private columns which are common in
    ETL frameworks, a corresponding error will be raised.

        InvalidColumnNameException["_dlt_load_id" conflicts with system column pattern]

    This class provides utility methods to work around the problem, brutally.

    See also: https://github.com/crate/crate/issues/15161

    FIXME: Please get rid of this code by resolving the issue in CrateDB.
    """

    quirked_labels = [
        "__dlt_id",
        "__dlt_load_id",
        "__dlt_parent_id",
        "__dlt_list_idx",
        "__dlt_root_id",
    ]

    @staticmethod
    def quirk(thing: str) -> str:
        thing = (
            thing.replace("_dlt_load_id", "__dlt_load_id")
            .replace("_dlt_id", "__dlt_id")
            .replace("_dlt_parent_id", "__dlt_parent_id")
            .replace("_dlt_list_idx", "__dlt_list_idx")
            .replace("_dlt_root_id", "__dlt_root_id")
        )
        return thing

    @staticmethod
    def unquirk(thing: str) -> str:
        thing = (
            thing.replace("__dlt_load_id", "_dlt_load_id")
            .replace("__dlt_id", "_dlt_id")
            .replace("__dlt_parent_id", "_dlt_parent_id")
            .replace("__dlt_list_idx", "_dlt_list_idx")
            .replace("__dlt_root_id", "_dlt_root_id")
        )
        return thing

    @classmethod
    def patch_sql(cls, sql: Union[str, psycopg2.sql.Composed]) -> Union[str, psycopg2.sql.Composed]:
        if isinstance(sql, str):
            sql = cls.quirk(sql)
        elif isinstance(sql, psycopg2.sql.Composed):
            sql.seq[0]._wrapped = cls.quirk(sql.seq[0]._wrapped)
        else:
            raise NotImplementedError(f"Unsupported type '{type(sql)}' for augmenting SQL: {sql}")
        return sql

    @classmethod
    def patch_result(cls, result: Sequence[Sequence[Any]]) -> Optional[Sequence[Sequence[Any]]]:
        if result is None:
            return None
        row_new = []
        for row in result:
            if len(row) >= 2 and row[1] in cls.quirked_labels:
                r = list(row)
                r[1] = cls.unquirk(r[1])
                row = tuple(r)
            row_new.append(row)
        return row_new
