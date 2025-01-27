---
sidebar_label: factory
title: destinations.impl.duckdb.factory
---

## duckdb Objects

```python
class duckdb(Destination[DuckDbClientConfiguration, "DuckDbClient"])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/duckdb/factory.py#L126)

### \_\_init\_\_

```python
def __init__(credentials: t.Union[DuckDbCredentials, t.Dict[str, t.Any], str,
                                  "DuckDBPyConnection"] = None,
             create_indexes: bool = False,
             destination_name: t.Optional[str] = None,
             environment: t.Optional[str] = None,
             bound_to_pipeline: t.Optional[SupportsPipeline] = None,
             **kwargs: t.Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/duckdb/factory.py#L162)

Configure the DuckDB destination to use in a pipeline.

All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

**Arguments**:

- `credentials` - Credentials to connect to the duckdb database. Can be an instance of `DuckDbCredentials` or
  a path to a database file. Use :pipeline: to create a duckdb
  in the working folder of the pipeline
- `create_indexes` - Should unique indexes be created, defaults to False
- `bound_to_pipeline` - Bind the connections generates by this factory to this pipeline, to enable :pipeline: path
- `**kwargs` - Additional arguments passed to the destination config

