from typing import cast, Any

from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.reference import TDestinationReferenceArg, Destination, JobClientBase
from dlt.common.schema import Schema
from dlt.destinations.sql_client import SqlClientBase

try:
    import ibis  # type: ignore
    import sqlglot
    from ibis import BaseBackend, Expr
except ModuleNotFoundError:
    raise MissingDependencyException("dlt ibis helpers", ["ibis-framework"])


SUPPORTED_DESTINATIONS = [
    "dlt.destinations.postgres",
    "dlt.destinations.duckdb",
    "dlt.destinations.motherduck",
    "dlt.destinations.filesystem",
    "dlt.destinations.bigquery",
    "dlt.destinations.snowflake",
    "dlt.destinations.redshift",
    "dlt.destinations.mssql",
    "dlt.destinations.synapse",
    "dlt.destinations.clickhouse",
    # NOTE: Athena could theoretically work with trino backend, but according to
    # https://github.com/ibis-project/ibis/issues/7682 connecting with aws credentials
    # does not work yet.
    # "dlt.destinations.athena",
]


# Map dlt data types to ibis data types
DATA_TYPE_MAP = {
    "text": "string",
    "double": "float64",
    "bool": "boolean",
    "timestamp": "timestamp",
    "bigint": "int64",
    "binary": "binary",
    "json": "string",  # Store JSON as string in ibis
    "decimal": "decimal",
    "wei": "int64",  # Wei is a large integer
    "date": "date",
    "time": "time",
}


def create_ibis_backend(
    destination: TDestinationReferenceArg, client: JobClientBase
) -> BaseBackend:
    """Create a given ibis backend for a destination client and dataset"""

    # check if destination is supported
    destination_type = Destination.from_reference(destination).destination_type
    if destination_type not in SUPPORTED_DESTINATIONS:
        raise NotImplementedError(f"Destination of type {destination_type} not supported by ibis.")

    if destination_type in ["dlt.destinations.motherduck", "dlt.destinations.duckdb"]:
        import duckdb
        from dlt.destinations.impl.duckdb.duck import DuckDbClient

        duck_client = cast(DuckDbClient, client)
        duck = duckdb.connect(
            database=duck_client.config.credentials._conn_str(),
            read_only=duck_client.config.credentials.read_only,
            config=duck_client.config.credentials._get_conn_config(),
        )
        con = ibis.duckdb.from_connection(duck)
    elif destination_type in [
        "dlt.destinations.postgres",
        "dlt.destinations.redshift",
    ]:
        credentials = client.config.credentials.to_native_representation()
        con = ibis.connect(credentials)
    elif destination_type == "dlt.destinations.snowflake":
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        sf_client = cast(SnowflakeClient, client)
        credentials = sf_client.config.credentials.to_connector_params()
        con = ibis.snowflake.connect(**credentials)
    elif destination_type in ["dlt.destinations.mssql", "dlt.destinations.synapse"]:
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient

        mssql_client = cast(MsSqlJobClient, client)
        con = ibis.mssql.connect(
            host=mssql_client.config.credentials.host,
            port=mssql_client.config.credentials.port,
            database=mssql_client.config.credentials.database,
            user=mssql_client.config.credentials.username,
            password=mssql_client.config.credentials.password,
            driver=mssql_client.config.credentials.driver,
        )
    elif destination_type == "dlt.destinations.bigquery":
        from dlt.destinations.impl.bigquery.bigquery import BigQueryClient

        bq_client = cast(BigQueryClient, client)
        credentials = bq_client.config.credentials.to_native_credentials()
        con = ibis.bigquery.connect(
            credentials=credentials,
            project_id=bq_client.sql_client.project_id,
            location=bq_client.sql_client.location,
        )
    elif destination_type == "dlt.destinations.clickhouse":
        from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient

        ch_client = cast(ClickHouseClient, client)
        con = ibis.clickhouse.connect(
            host=ch_client.config.credentials.host,
            port=ch_client.config.credentials.http_port,
            database=ch_client.config.credentials.database,
            user=ch_client.config.credentials.username,
            password=ch_client.config.credentials.password,
            secure=bool(ch_client.config.credentials.secure),
            # compression=True,
        )
    elif destination_type == "dlt.destinations.filesystem":
        import duckdb
        from dlt.destinations.impl.filesystem.sql_client import (
            FilesystemClient,
            FilesystemSqlClient,
        )
        from dlt.destinations.impl.duckdb.factory import DuckDbCredentials

        # we create an in memory duckdb and create the ibis backend from it
        fs_client = cast(FilesystemClient, client)
        sql_client = FilesystemSqlClient(
            fs_client,
            dataset_name=fs_client.dataset_name,
            credentials=DuckDbCredentials(duckdb.connect()),
        )
        # do not use context manager to not return and close the cloned connection
        duckdb_conn = sql_client.open_connection()
        # make all tables available here
        # NOTE: we should probably have the option for the user to only select a subset of tables here
        sql_client.create_views_for_all_tables()
        # why this works now: whenever a clone of connection is made, all SET commands
        # apply only to it. old code was setting `curl` on the internal clone of sql_client
        # now we export this clone directly to ibis to it works
        con = ibis.duckdb.from_connection(duckdb_conn)

    return con


def create_unbound_ibis_table(
    sql_client: SqlClientBase[Any], schema: Schema, table_name: str
) -> Expr:
    """Create an unbound ibis table from a dlt schema"""

    if table_name not in schema.tables:
        raise Exception(
            f"Table {table_name} not found in schema. Available tables: {schema.tables.keys()}"
        )
    table_schema = schema.tables[table_name]

    # Convert dlt table schema columns to ibis schema
    ibis_schema = {
        sql_client.capabilities.casefold_identifier(col_name): DATA_TYPE_MAP[
            col_info.get("data_type", "string")
        ]
        for col_name, col_info in table_schema.get("columns", {}).items()
    }

    # normalize table name
    table_path = sql_client.make_qualified_table_name_path(table_name, escape=False)

    catalog = None
    if len(table_path) == 3:
        catalog, database, table = table_path
    else:
        database, table = table_path

    # create unbound ibis table and return in dlt wrapper
    unbound_table = ibis.table(schema=ibis_schema, name=table, database=database, catalog=catalog)

    return unbound_table
