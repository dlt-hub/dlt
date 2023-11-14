from dlt.destinations.impl.postgres.factory import postgres
from dlt.destinations.impl.snowflake.factory import snowflake
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.destinations.impl.duckdb.factory import duckdb
from dlt.destinations.impl.dummy.factory import dummy
from dlt.destinations.impl.mssql.factory import mssql
from dlt.destinations.impl.bigquery.factory import bigquery
from dlt.destinations.impl.athena.factory import athena
from dlt.destinations.impl.redshift.factory import redshift
from dlt.destinations.impl.qdrant.factory import qdrant
from dlt.destinations.impl.motherduck.factory import motherduck
from dlt.destinations.impl.weaviate.factory import weaviate


__all__ = [
    "postgres",
    "snowflake",
    "filesystem",
    "duckdb",
    "dummy",
    "mssql",
    "bigquery",
    "athena",
    "redshift",
    "qdrant",
    "motherduck",
    "weaviate",
]
