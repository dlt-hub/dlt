from dlt.destinations.impl.postgres.factory import postgres
from dlt.destinations.impl.snowflake.factory import snowflake
from dlt.destinations.impl.filesystem.factory import filesystem


__all__ = [
    "postgres",
    "snowflake",
    "filesystem",
]
