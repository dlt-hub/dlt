from dlt.common.exceptions import MissingDependencyException
from dlt import version

try:
    from sqlalchemy import MetaData, Table, Column, create_engine
    from sqlalchemy.engine import Engine, URL, make_url, Row
    from sqlalchemy.sql import sqltypes, Select, Executable
    from sqlalchemy.sql.elements import TextClause
    from sqlalchemy.sql.sqltypes import TypeEngine
    from sqlalchemy.exc import CompileError
    import sqlalchemy as sa
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
    "sa",
]
