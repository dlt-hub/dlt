from typing import cast, Any

from dlt.common.exceptions import MissingDependencyException, ValueErrorWithKnownValues
from dlt.common.destination import TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase
from dlt.common.schema import Schema
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.libs.sqlglot import TSqlGlotDialect

from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration
from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration
from dlt.destinations.impl.ducklake.configuration import DuckLakeClientConfiguration
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
    import sqlglot.expressions as sge
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
    destination: TDestinationReferenceArg, client: JobClientBase, read_only: bool = False
) -> BaseBackend:
    """Create a given ibis backend for a destination client and dataset."""

    # ensure destination is a Destination instance
    if not isinstance(destination, Destination):
        destination = Destination.from_reference(destination)

    if issubclass(destination.spec, DuckDbClientConfiguration) or issubclass(
        destination.spec, MotherDuckClientConfiguration
    ):
        from dlt.destinations.impl.duckdb.duck import DuckDbClient

        assert isinstance(client, DuckDbClient)
        # do not set read_only flag on motherduck, it is managed on server side
        if not issubclass(destination.spec, MotherDuckClientConfiguration):
            client.config.credentials.read_only = read_only
        # this will open connection to duckdb, take a clone and close the clone
        with client:
            # make sure we can access tables from current dataset without qualification
            # also prevents empty duckdb files from being created
            client.sql_client.use_dataset()
            # move main connection ownership to ibis
            con = ibis.duckdb.from_connection(client.config.credentials.conn_pool.move_conn())
    elif issubclass(destination.spec, DuckLakeClientConfiguration):
        from dlt.destinations.impl.ducklake.ducklake import DuckLakeClient

        assert isinstance(client, DuckLakeClient)
        # open connection but do not close it, ducklake always creates a separate connection
        # and will not close it in destructor
        conn = client.sql_client.open_connection()
        try:
            # make sure we can access tables from current dataset without qualification
            # also prevents empty duckdb files from being created
            client.sql_client.use_dataset()
        except Exception:
            # close explicitly, won't be done by the conn pool
            client.sql_client.close_connection()
            raise
        con = ibis.duckdb.from_connection(conn)
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
        from dlt.destinations.impl.filesystem.sql_client import (
            FilesystemClient,
            FilesystemSqlClient,
        )

        # we create an in memory duckdb and create the ibis backend from it
        fs_client = cast(FilesystemClient, client)
        sql_client = fs_client.sql_client
        assert isinstance(sql_client, FilesystemSqlClient)
        # do not use context manager to not return and close the cloned connection
        duckdb_conn = sql_client.open_connection()
        # make all tables available here
        # NOTE: we should probably have the option for the user to only select a subset of tables here
        sql_client.create_views_for_all_tables()
        # why this works now: whenever a clone of connection is made, all SET commands
        # apply only to it. old code was setting `curl` on the internal clone of sql_client
        # now we export this clone directly to ibis to it works
        con = ibis.duckdb.from_connection(duckdb_conn)
        # disable destructor
        fs_client.sql_client = None
        sql_client.memory_db = None
        del sql_client
    else:
        # NOTE: Athena could theoretically work with trino backend, but according to
        # https://github.com/ibis-project/ibis/issues/7682 connecting with aws credentials
        # does not work yet.
        raise NotImplementedError(
            f"Destination type `{Destination.from_reference(destination).destination_type}` is not"
            " supported by the Ibis backend."
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


def _get_ibis_to_sqlglot_compiler(dialect: TSqlGlotDialect) -> SQLGlotCompiler:
    """Get the compiler for a given dialect."""
    if dialect == "athena":
        compiler = sc.AthenaCompiler()
    elif dialect == "bigquery":
        compiler = sc.BigQueryCompiler()
    elif dialect == "clickhouse":
        compiler = sc.ClickHouseCompiler()
    elif dialect == "databricks":
        compiler = sc.DatabricksCompiler()
    elif dialect == "druid":
        compiler = sc.DruidCompiler()
    elif dialect == "duckdb":
        compiler = sc.DuckDBCompiler()
    elif dialect == "mysql":
        compiler = sc.MySQLCompiler()
    elif dialect == "oracle":
        compiler = sc.OracleCompiler()
    elif dialect == "postgres":
        compiler = sc.PostgresCompiler()
    elif dialect == "presto":
        compiler = sc.TrinoCompiler()
    elif dialect == "redshift":
        compiler = sc.PostgresCompiler()
    elif dialect == "risingwave":
        compiler = sc.RisingWaveCompiler()
    elif dialect == "snowflake":
        compiler = sc.SnowflakeCompiler()
    # NOTE I'm unsure if both `spark` and `spark2` are supported by the same compiler
    elif dialect == "spark":
        compiler = sc.PySparkCompiler()
    elif dialect == "spark2":
        compiler = sc.PySparkCompiler()
    elif dialect == "sqlite":
        compiler = sc.SQLiteCompiler()
    elif dialect == "trino":
        compiler = sc.TrinoCompiler()
    elif dialect == "tsql":
        compiler = sc.MSSQLCompiler()
    else:
        compiler = sc.DuckDBCompiler()

    return compiler


def compile_ibis_to_sqlglot(ibis_expr: Expr, dialect: TSqlGlotDialect) -> sge.Query:
    """Compile an ibis expression to a sqlglot query."""
    compiler = _get_ibis_to_sqlglot_compiler(dialect)
    return cast(sge.Query, compiler.to_sqlglot(ibis_expr))
