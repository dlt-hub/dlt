import platform
from typing import Union, Sequence

if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
import psycopg2.sql


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

    quirked_labels = ["__dlt_id", "__dlt_load_id", "__dlt_parent_id", "__dlt_list_idx", "__dlt_root_id"]

    @staticmethod
    def quirk(thing: str) -> str:
        thing = (thing
                 .replace("_dlt_load_id", "__dlt_load_id")
                 .replace("_dlt_id", "__dlt_id")
                 .replace("_dlt_parent_id", "__dlt_parent_id")
                 .replace("_dlt_list_idx", "__dlt_list_idx")
                 .replace("_dlt_root_id", "__dlt_root_id")
                 )
        return thing

    @staticmethod
    def unquirk(thing: str) -> str:
        thing = (thing
                 .replace("__dlt_load_id", "_dlt_load_id")
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
    def patch_result(cls, result: Union[Sequence[tuple], None]) -> Union[Sequence[tuple], None]:
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
