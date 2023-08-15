from typing import Any

from deprecated import deprecated

from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.sql_client import SqlClientBase

try:
    import pandas as pd
    from pandas.io.sql import _wrap_result
except ModuleNotFoundError:
    raise MissingDependencyException("DLT Pandas Helpers", ["pandas"])


@deprecated(reason="Use `df` method on cursor returned from client.execute_query")
def query_results_to_df(
    client: SqlClientBase[Any], query: str, index_col: Any = None, coerce_float: bool = True, parse_dates: Any = None, dtype: Any = None
) -> pd.DataFrame:
    """
    A helper function that executes a query in the destination and returns the result as Pandas `DataFrame`

    This method reuses `read_sql` method of `Pandas` with the sql client obtained from `Pipeline.sql_client` method.

    Parameters
    ----------
    client (SqlClientBase[Any]): Sql Client instance
    query (str): Query to be executed
    index_col str or list of str, optional, default: None
        Column(s) to set as index(MultiIndex).
    coerce_float (bool, optional): default: True
        Attempts to convert values of non-string, non-numeric objects (like
        decimal.Decimal) to floating point. Useful for SQL result sets.
    parse_dates : list or dict, default: None
        - List of column names to parse as dates.
        - Dict of ``{column_name: format string}`` where format string is
            strftime compatible in case of parsing string times, or is one of
            (D, s, ns, ms, us) in case of parsing integer timestamps.
        - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
            to the keyword arguments of :func:`pandas.to_datetime`
            Especially useful with databases without native Datetime support,
            such as SQLite.
    dtype : Type name or dict of columns
        Data type for data or columns. E.g. np.float64 or
        {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}.

    Returns
    -------
    DataFrame with the query results
    """
    with client.execute_query(query) as curr:
        # get column names
        columns = [c[0] for c in curr.description]
        # use existing panda function that converts results to data frame
        # TODO: we may use `_wrap_iterator` to prevent loading the full result to memory first
        pf: pd.DataFrame = _wrap_result(curr.fetchall(), columns, index_col, coerce_float, parse_dates, dtype)
    return pf
