import ibis.backends.sql.compilers as sc
from dlt.common.destination import TDestinationReferenceArg, Destination


# TODO move this code to the right place
def _get_sqlalchemy_compiler(destination: Destination):
    """
    reference: https://docs.sqlalchemy.org/en/20/dialects/
    """
    # Dialects included in SQLAlchemy
    # SQLite via SQLAlchemy is fully-tested with dlt
    if ... == "sqlite":
        compiler = sc.SQLiteCompiler()
    # MySQL via SQLAlchemy is fully-tested with dlt
    elif ... == "mysql":
        compiler = sc.MySQLCompiler()

    elif ... == "oracle":
        compiler = sc.OracleCompiler()

    elif ... == "mariadb":
        raise NotImplementedError

    elif ... == "postgres":
        compiler = sc.PostgresCompiler()

    elif ... == "mssql":
        compiler = sc.MSSQLCompiler()

    # SQLAlchemy extension dialects
    elif ... == "druid":
        compiler = sc.DruidCompiler()

    elif ... == "hive":
        compiler = sc.TrinoCompiler()

    elif ... == "clickhouse":
        compiler = sc.ClickHouseCompiler()

    elif ... == "databricks":
        compiler = sc.DatabricksCompiler()

    elif ... == "bigquery":
        compiler = sc.BigQueryCompiler()

    elif ... == "impala":
        compiler = sc.ImpalaCompiler()

    elif ... == "snowflake":
        compiler = sc.SnowflakeCompiler()

    else:
        raise NotImplementedError

    return compiler


def get_sqlglot_compiler(destination: TDestinationReferenceArg):
    # ensure destination is a Destination instance
    if not isinstance(destination, Destination):
        destination = Destination.from_reference(destination)
    assert isinstance(destination, Destination)

    if destination.destination_name == "duckdb":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "filesystem":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "motherduck":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "postgres":
        compiler = sc.PostgresCompiler()

    elif destination.destination_name == "clickhouse":
        compiler = sc.ClickHouseCompiler()

    elif destination.destination_name == "snowflake":
        compiler = sc.SnowflakeCompiler()

    elif destination.destination_name == "databricks":
        compiler = sc.DatabricksCompiler()

    elif destination.destination_name == "mssql":
        compiler = sc.MSSQLCompiler()

    # NOTE synapse might differ from MSSQL
    elif destination.destination_name == "synapse":
        compiler = sc.MSSQLCompiler()

    elif destination.destination_name == "bigquery":
        compiler = sc.BigQueryCompiler()

    elif destination.destination_name == "athena":
        compiler = sc.AthenaCompiler()

    # NOTE Dremio uses a presto/trino-based language
    elif destination.destination_name == "dremio":
        compiler = sc.TrinoCompiler()

    # TODO parse the SQLAlchemy dialect
    elif destination.destination_name == "sqlalchemy":
        compiler = _get_sqlalchemy_compiler(destination)

    else:
        raise NotImplementedError(
            f"Destination of type {Destination.from_reference(destination).destination_type} not"
            " supported by ibis."
        )

    return compiler
