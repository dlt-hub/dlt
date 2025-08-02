from typing import cast, Any

from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination import TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase
from dlt.common.schema import Schema
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.libs.sqlglot import TSqlGlotDialect

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
from dlt.destinations.impl.synapse.configuration import SynapseClientConfiguration

try:
    import ibis
    import sqlglot
    from ibis import BaseBackend, Expr, Table
    import ibis.backends.sql.compilers as sc
    from ibis.backends.sql.compilers.base import SQLGlotCompiler
except ImportError:
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
        # open connection, apply all settings and pragmas
        duck_conn = client.config.credentials.borrow_conn()
        # move main connection ownership to ibis
        con = ibis.duckdb.from_connection(client.config.credentials.move_conn())
        client.config.credentials.return_conn(duck_conn)

        # make sure we can access tables from current dataset without qualification
        dataset_name = client.sql_client.fully_qualified_dataset_name()
        con.raw_sql(f"SET search_path = '{dataset_name}'")
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
    elif issubclass(destination.spec, MsSqlClientConfiguration) and not issubclass(
        destination.spec, SynapseClientConfiguration
    ):  # exclude synapse
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient

        assert isinstance(client, MsSqlJobClient)
        ms_credentials = client.config.credentials.to_native_representation()
        ms_credentials = ms_credentials.replace("synapse://", "mssql://")
        con = ibis.connect(ms_credentials, driver=client.config.credentials.driver)
    elif issubclass(destination.spec, BigQueryClientConfiguration):
        from dlt.destinations.impl.bigquery.bigquery import BigQueryClient

        assert isinstance(client, BigQueryClient)
        credentials = client.config.credentials.to_native_credentials()
        con = ibis.bigquery.connect(
            credentials=credentials,
            project_id=client.sql_client.project_id,
            dataset_id=client.sql_client.fully_qualified_dataset_name(quote=False),
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
    # elif issubclass(destination.spec, DatabricksClientConfiguration):
    #     from dlt.destinations.impl.databricks.databricks import DatabricksClient

    #     bricks_client = cast(DatabricksClient, client)
    #     con = ibis.databricks.connect(
    #         **bricks_client.config.credentials.to_connector_params(),
    #         schema=bricks_client.sql_client.dataset_name,
    #     )
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
            f"Destination type `{Destination.from_reference(destination).destination_type}` is not"
            " supported."
        )

    return con


def create_unbound_ibis_table(schema: Schema, dataset_name: str, table_name: str) -> Table:
    """Create an unbound ibis table from a dlt schema. No additional identifiers normalization, quoting
    or escaping is performed.
    """
    table_schema = schema.tables[table_name]

    # Convert dlt table schema columns to ibis schema
    ibis_schema = {
        col_name: DATA_TYPE_MAP[col_info.get("data_type", "text")]
        for col_name, col_info in table_schema.get("columns", {}).items()
    }

    # create unbound ibis table and return in dlt wrapper
    unbound_table = ibis.table(schema=ibis_schema, name=table_name, database=dataset_name)

    return unbound_table


def get_compiler_for_dialect(dialect: TSqlGlotDialect) -> SQLGlotCompiler:
    """Get the compiler for a given dialect."""

    ibis_dialect: str = dialect
    if dialect == "tsql":
        ibis_dialect = "mssql"
    if dialect == "redshift":
        ibis_dialect = "postgres"

    try:
        compiler_provider = getattr(sc, ibis_dialect)
    except AttributeError:
        # default is duckdb
        compiler_provider = sc.duckdb

    if (compiler := getattr(compiler_provider, "compiler", None)) is None:
        raise NotImplementedError(f"{compiler_provider} is not a SQL backend")

    return compiler


def compile_ibis_to_sqlglot(ibis_expr: Expr, dialect: TSqlGlotDialect) -> sqlglot.expressions.Query:
    """Compile an ibis expression to a sqlglot query."""
    compiler = get_compiler_for_dialect(dialect)
    return cast(sqlglot.expressions.Query, compiler.to_sqlglot(ibis_expr))
