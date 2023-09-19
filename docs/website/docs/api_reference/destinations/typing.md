---
sidebar_label: typing
title: destinations.typing
---

## DBApiCursor Objects

```python
class DBApiCursor(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/typing.py#L24)

Protocol for DBAPI cursor

#### native\_cursor

Cursor implementation native to current destination

#### df

```python
def df(chunk_size: int = None, **kwargs: None) -> Optional[DataFrame]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/typing.py#L42)

Fetches the results as data frame. For large queries the results may be chunked

Fetches the results into a data frame. The default implementation uses helpers in `pandas.io.sql` to generate Pandas data frame.
This function will try to use native data frame generation for particular destination. For `BigQuery`: `QueryJob.to_dataframe` is used.
For `duckdb`: `DuckDBPyConnection.df'

**Arguments**:

- `chunk_size` _int, optional_ - Will chunk the results into several data frames. Defaults to None
- `**kwargs` _Any_ - Additional parameters which will be passed to native data frame generation function.
  

**Returns**:

- `Optional[DataFrame]` - A data frame with query results. If chunk_size > 0, None will be returned if there is no more data in results

