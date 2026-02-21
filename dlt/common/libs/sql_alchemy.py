from dlt.common.exceptions import MissingDependencyException
from dlt import version

try:
    from sqlalchemy import MetaData, Table, Column, create_engine, TypeDecorator  # noqa: I251
    from sqlalchemy.engine import Engine, URL, make_url, Row  # noqa: I251
    from sqlalchemy.sql import sqltypes, Select, Executable  # noqa: I251
    from sqlalchemy.sql.elements import TextClause  # noqa: I251
    from sqlalchemy.sql.sqltypes import TypeEngine  # noqa: I251
    from sqlalchemy.exc import CompileError  # noqa: I251
    from sqlalchemy.dialects.oracle import NUMBER as ORACLE_NUMBER  # noqa: I251
    from sqlalchemy.dialects.oracle.base import OracleDialect  # noqa: I251
    import sqlalchemy as sa  # noqa: I251
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt sql_database helpers ",
        [f"{version.DLT_PKG_NAME}[sql_database]"],
        "Install the `sql_database` helpers for loading from `sql_database` sources. Note that you"
        " may need to install additional SQLAlchemy dialects for your source database.",
    )

# TODO: maybe use sa.__version__?
IS_SQL_ALCHEMY_20 = hasattr(sa, "Double")

__all__ = [
    "IS_SQL_ALCHEMY_20",
    "MetaData",
    "Table",
    "Column",
    "create_engine",
    "TypeDecorator",
    "Engine",
    "URL",
    "make_url",
    "Row",
    "sqltypes",
    "Select",
    "Executable",
    "TextClause",
    "TypeEngine",
    "CompileError",
    "ORACLE_NUMBER",
    "OracleDialect",
    "sa",
]
