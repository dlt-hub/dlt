from typing import Iterator, Sequence, Union, Any

from dlt.common.typing import DictStrAny
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


def get_source(
    sql: Any,
    con: Any,
    index_col: Union[str, Sequence[str], None] = None,
    coerce_float: bool = True,
    params: Any = None,
    parse_dates: Any = None,
    columns: Any = None,
    chunk_size: int = 1000
) -> Iterator[DictStrAny]:

    try:
        items = pandas.read_sql(sql, con, index_col=index_col, coerce_float=coerce_float, params=params, parse_dates=parse_dates, columns=columns, chunksize=chunk_size)
    except NoSuchModuleError:
        if "redshift.redshift_connector":
            raise MissingDependencyException("SQL Query Source", ["sqlalchemy-redshift", "redshift_connector"], "Redshift dialect support for SqlAlchemy")
        raise

    for i in items:
        record = i.to_dict("records")
        if len(record) > 0:
            yield record[0]
