---
sidebar_label: pandas_helper
title: helpers.pandas_helper
---

#### query\_results\_to\_df

```python
@deprecated(
    reason="Use `df` method on cursor returned from client.execute_query")
def query_results_to_df(client: SqlClientBase[Any],
                        query: str,
                        index_col: Any = None,
                        coerce_float: bool = True,
                        parse_dates: Any = None,
                        dtype: Any = None) -> pd.DataFrame
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/helpers/pandas_helper.py#L16)

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

