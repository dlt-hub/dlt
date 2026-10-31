from typing import cast, Sequence

from dlt.common.exceptions import MissingDependencyException, ValueErrorWithKnownValues
from dlt.common.destination import TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase
from dlt.common.schema import Schema
from dlt.common.libs.sqlglot import TSqlGlotDialect

try:
    import ibis  # noqa: I251
    import sqlglot
    import sqlglot.expressions as sge
    from ibis import BaseBackend, Expr, Table  # noqa: I251
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
    destination: TDestinationReferenceArg,
    client: JobClientBase,
    read_only: bool = False,
    schemas: Sequence[Schema] = (),
) -> BaseBackend:
    """Create a given ibis backend for a destination client and dataset."""
    if not isinstance(destination, Destination):
        destination = Destination.from_reference(destination)
    return destination.create_ibis_backend(client, read_only=read_only, schemas=schemas)


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
    elif dialect == "fabric":
        compiler = sc.MSSQLCompiler()
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
