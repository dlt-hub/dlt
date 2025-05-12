from typing import cast, Any

from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination import TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.storages.configuration import FilesystemConfiguration

from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration
from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration
from dlt.destinations.impl.motherduck.configuration import MotherDuckClientConfiguration
from dlt.destinations.impl.postgres.configuration import PostgresClientConfiguration
from dlt.destinations.impl.redshift.configuration import RedshiftClientConfiguration
from dlt.destinations.impl.snowflake.configuration import SnowflakeClientConfiguration
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.impl.clickhouse.configuration import ClickHouseClientConfiguration


try:
    import ibis
    import sqlglot
    from ibis import BaseBackend, Expr, Table
except ModuleNotFoundError:
    raise MissingDependencyException("dlt ibis helpers", ["ibis-framework"])


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
    """Create a given ibis backend for a destination client and dataset."""

    # ensure destination is a Destination instance
    if not isinstance(destination, Destination):
        destination = Destination.from_reference(destination)

    if issubclass(destination.spec, DuckDbClientConfiguration) or issubclass(
        destination.spec, MotherDuckClientConfiguration
    ):
        from dlt.destinations.impl.duckdb.duck import DuckDbClient
        import duckdb

        assert isinstance(client, DuckDbClient)
        duck = duckdb.connect(
            database=client.config.credentials._conn_str(),
            read_only=client.config.credentials.read_only,
            config=client.config.credentials._get_conn_config(),
        )
        con = ibis.duckdb.from_connection(duck)
        # make sure we can access tables from current dataset without qualification
        dataset_name = client.sql_client.fully_qualified_dataset_name()
        con.raw_sql(f"SET search_path = '{dataset_name}';")
    elif issubclass(destination.spec, PostgresClientConfiguration):
        from dlt.destinations.impl.postgres.postgres import PostgresClient
        from dlt.destinations.impl.redshift.redshift import RedshiftClient

        assert isinstance(client, (PostgresClient, RedshiftClient))
        if destination.spec is RedshiftClientConfiguration:
            # patch psycopg
            try:
                import psycopg  # type: ignore[import-not-found, unused-ignore]

                old_fetch = psycopg.types.TypeInfo.fetch

                def _ignore_hstore(conn: Any, name: Any) -> Any:
                    if name == "hstore":
                        raise TypeError("HSTORE")
                    return old_fetch(conn, name)

                psycopg.types.TypeInfo.fetch = _ignore_hstore  # type: ignore[method-assign, unused-ignore]
            except Exception:
                pass
            # check ibis version and raise an error if it's >= 0.10.4
            ibis_version = tuple(map(int, ibis.__version__.split(".")))
            if ibis_version >= (0, 10, 4):
                raise NotImplementedError(
                    "Redshift is not properly supported by ibis as of version 0.10.4. "
                    "Please use an older version of ibis."
                )
        credentials = client.config.credentials.copy()
        # schema must be passed at path, `schema` argument does not work (overridden, probably a bug)
        credentials.database = credentials.database + "/" + client.sql_client.dataset_name
        con = ibis.connect(credentials.to_native_representation())
    elif issubclass(destination.spec, SnowflakeClientConfiguration):
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        assert isinstance(client, SnowflakeClient)
        sn_credentials = client.config.credentials.to_connector_params()
        dataset_name = client.sql_client.fully_qualified_dataset_name()
        con = ibis.snowflake.connect(
            schema=dataset_name, **sn_credentials, create_object_udfs=False
        )
    elif issubclass(destination.spec, MsSqlClientConfiguration):
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient

        assert isinstance(client, MsSqlJobClient)
        ms_credentials = client.config.credentials.to_native_representation()
        con = ibis.connect(ms_credentials, driver=client.config.credentials.driver)
    elif issubclass(destination.spec, BigQueryClientConfiguration):
        from dlt.destinations.impl.bigquery.bigquery import BigQueryClient

        assert isinstance(client, BigQueryClient)
        credentials = client.config.credentials.to_native_credentials()
        con = ibis.bigquery.connect(
            credentials=credentials,
            project_id=client.sql_client.project_id,
            dataset_id=client.sql_client.fully_qualified_dataset_name(escape=False),
            location=client.sql_client.location,
        )
    elif issubclass(destination.spec, ClickHouseClientConfiguration):
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
    elif issubclass(destination.spec, DatabricksClientConfiguration):
        from dlt.destinations.impl.databricks.databricks import DatabricksClient

        bricks_client = cast(DatabricksClient, client)
        con = ibis.databricks.connect(
            **bricks_client.config.credentials.to_connector_params(),
            schema=bricks_client.sql_client.dataset_name,
        )
    elif issubclass(destination.spec, AthenaClientConfiguration):
        from dlt.destinations.impl.athena.athena import AthenaClient

        athena_client = cast(AthenaClient, client)
        con = ibis.athena.connect(
            schema_name=athena_client.sql_client.dataset_name,
            **athena_client.config.to_connector_params(),
        )
    # TODO: allow for sqlalchemy mysql and sqlite here
    elif issubclass(destination.spec, FilesystemConfiguration):
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
            cache_db=DuckDbCredentials(duckdb.connect()),
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
    else:
        # NOTE: Athena could theoretically work with trino backend, but according to
        # https://github.com/ibis-project/ibis/issues/7682 connecting with aws credentials
        # does not work yet.
        raise NotImplementedError(
            f"Destination of type {Destination.from_reference(destination).destination_type} not"
            " supported by ibis."
        )

    return con


def create_unbound_ibis_table(
    sql_client: SqlClientBase[Any], schema: Schema, table_name: str
) -> Expr:
    """Create an unbound ibis table from a dlt schema. Tables not in schema will be created
    without columns.
    """
    # allow to create empty tables without schema to unify behavior with default relation
    if table_name not in schema.tables:
        schema.update_table(new_table(table_name))
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
