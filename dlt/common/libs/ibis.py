from typing import cast

from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema import Schema

from dlt.common.destination.reference import TDestinationReferenceArg, Destination, JobClientBase

try:
    import ibis  # type: ignore
    from ibis import BaseBackend
except ModuleNotFoundError:
    raise MissingDependencyException("dlt ibis Helpers", ["ibis"])


SUPPORTED_DESTINATIONS = [
    "dlt.destinations.postgres",
    "dlt.destinations.duckdb",
    "dlt.destinations.filesystem",
    "dlt.destinations.bigquery",
    "dlt.destinations.snowflake",
    "dlt.destinations.redshift",
]


def create_ibis_backend(
    destination: TDestinationReferenceArg, dataset_name: str, client: JobClientBase
) -> BaseBackend:
    """Create a given ibis backend for a destination client and dataset"""
    import duckdb
    from dlt.destinations.impl.duckdb.factory import DuckDbCredentials

    # check if destination is supported
    destination_type = Destination.from_reference(destination).destination_type
    if destination_type not in SUPPORTED_DESTINATIONS:
        raise NotImplementedError(f"Destination of type {destination_type} not supported by ibis.")

    if destination_type in [
        "dlt.destinations.postgres",
        "dlt.destinations.duckdb",
        "dlt.destinations.redshift",
    ]:
        credentials = client.config.credentials.to_native_representation()
        con = ibis.connect(credentials)
    elif destination_type == "dlt.destinations.snowflake":
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        sf_client = cast(SnowflakeClient, client)
        credentials = sf_client.config.credentials.to_connector_params()
        con = ibis.snowflake.connect(**credentials)
    elif destination_type == "dlt.destinations.bigquery":
        from dlt.destinations.impl.bigquery.bigquery import BigQueryClient

        bq_client = cast(BigQueryClient, client)
        credentials = bq_client.config.credentials.to_native_credentials()
        con = ibis.bigquery.connect(
            credentials=credentials,
            project_id=bq_client.sql_client.project_id,
            location=bq_client.sql_client.location,
        )
    elif destination_type == "dlt.destinations.filesystem":
        from dlt.destinations.impl.filesystem.sql_client import (
            FilesystemClient,
            FilesystemSqlClient,
        )

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
