from typing import cast

from dlt.common.exceptions import MissingDependencyException

from dlt.common.destination.reference import TDestinationReferenceArg, Destination, JobClientBase

try:
    import ibis  # type: ignore
    from ibis import BaseBackend
except ModuleNotFoundError:
    raise MissingDependencyException("dlt ibis Helpers", ["ibis"])


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

        # we create an in memory duckdb and create all tables on there
        duck = duckdb.connect(":memory:")
        fs_client = cast(FilesystemClient, client)
        creds = DuckDbCredentials(duck)
        sql_client = FilesystemSqlClient(
            fs_client, dataset_name=fs_client.dataset_name, credentials=creds
        )

        # NOTE: we should probably have the option for the user to only select a subset of tables here
        with sql_client as _:
            sql_client.create_views_for_all_tables()
        con = ibis.duckdb.from_connection(duck)

    return con
