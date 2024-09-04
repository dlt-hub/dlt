from typing import cast

from dlt.common.exceptions import MissingDependencyException
from dlt import version

try:
    from sqlalchemy import MetaData, Table, Column, create_engine
    from sqlalchemy.engine import Engine, URL, make_url, Row
    from sqlalchemy.sql import sqltypes, Select
    from sqlalchemy.sql.sqltypes import TypeEngine
    from sqlalchemy.exc import CompileError
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt sql_database helpers ",
        [f"{version.DLT_PKG_NAME}[sql_database]"],
        "Install the sql_database helpers for loading from sql_database sources.",
    )
