from typing import Iterator, List, Any, Union
from functools import partial

import dlt

from dlt.common.configuration.specs.postgres_credentials import ConnectionStringCredentials
from dlt.common.typing import AnyFun, DictStrAny, StrAny, TDataItem
from dlt.common.exceptions import MissingDependencyException


try:
    # import gracefully and produce nice exception that explains the user what to do
    import pandas
except ImportError:
    raise MissingDependencyException("SQL Query Source", ["pandas"], "SQL Query Source temporarily uses pandas as DB interface")

try:
    from sqlalchemy.exc import NoSuchModuleError
except ImportError:
    raise MissingDependencyException("SQL Query Source", ["sqlalchemy"], "SQL Query Source temporarily uses pandas as DB interface")


def _query_data(
    f: AnyFun
) -> Iterator[DictStrAny]:

    try:
        items = f()
    except NoSuchModuleError as m_exc:
        if "redshift.redshift_connector" in str(m_exc):
            raise MissingDependencyException("SQL Query Source", ["sqlalchemy-redshift", "redshift_connector"], "Redshift dialect support for SqlAlchemy")
        raise

    for i in items:
        record = i.to_dict("records")
        if len(record) > 0:
            yield record[0]


@dlt.resource
def query_table(
    table_name: str,
    credentials: Union[ConnectionStringCredentials, str, StrAny],
    table_schema_name: str = None,
    # index_col: Union[str, Sequence[str], None] = None,
    coerce_float: bool = True,
    parse_dates: Any = None,
    columns: List[str] = None,
    chunk_size: int = 1000
) -> Any:
    assert isinstance(credentials, ConnectionStringCredentials)
    f = partial(pandas.read_sql_table, table_name, credentials.to_native_representation(), table_schema_name, None, coerce_float, parse_dates, columns, chunksize=chunk_size)
    # if resource is returned from decorator function, it will override the hints from decorator
    return dlt.resource(_query_data(f), name=table_name)


@dlt.resource
def query_sql(
    sql: str,
    credentials: Union[ConnectionStringCredentials, str, StrAny],
    coerce_float: bool = True,
    parse_dates: Any = None,
    chunk_size: int = 1000,
    dtype: Any = None
) -> Iterator[TDataItem]:
    assert isinstance(credentials, ConnectionStringCredentials)
    f = partial(pandas.read_sql_query, sql, credentials.to_native_representation(), None, coerce_float, None, parse_dates, chunk_size, dtype)
    yield from _query_data(f)
