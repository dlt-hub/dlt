from dlt.destinations.impl.postgres.factory import postgres
from dlt.destinations.impl.snowflake.factory import snowflake
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.destinations.impl.duckdb.factory import duckdb


__all__ = [
    "postgres",
    "snowflake",
    "filesystem",
    "duckdb",
]
