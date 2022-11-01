from typing import Iterator, List, Sequence, Union, Any
from functools import partial

import dlt
from dlt.common.typing import AnyFun, DictStrAny, TDataItem, TSecretValue
from dlt.pipeline.exceptions import MissingDependencyException


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
    database_url: TSecretValue,
    table_schema_name: str = None,
    index_col: Union[str, Sequence[str], None] = None,
    coerce_float: bool = True,
    parse_dates: Any = None,
    columns: List[str] = None,
    chunk_size: int = 1000
) -> Iterator[TDataItem]:
    f = partial(pandas.read_sql_table, table_name, database_url, table_schema_name, index_col, coerce_float, parse_dates, columns, chunksize=chunk_size)
    return _query_data(f)
    # pandas.read_sql(sql, con, index_col=index_col, coerce_float=coerce_float, params=params, parse_dates=parse_dates, columns=columns, chunksize=chunk_size)
