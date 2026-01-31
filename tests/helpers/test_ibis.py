import pytest
import ibis.backends.sql.compilers as sc

from dlt.helpers.ibis import _get_ibis_to_sqlglot_compiler


@pytest.mark.parametrize(
    "dialect, expected_compiler",
    [
        ("athena", sc.AthenaCompiler),
        ("bigquery", sc.BigQueryCompiler),
        ("clickhouse", sc.ClickHouseCompiler),
        ("databricks", sc.DatabricksCompiler),
        ("doris", sc.DuckDBCompiler),  # default value
        ("drill", sc.DuckDBCompiler),  # default value
        ("druid", sc.DruidCompiler),
        ("duckdb", sc.DuckDBCompiler),
        ("dune", sc.DuckDBCompiler),  # default value
        ("hive", sc.DuckDBCompiler),  # default value
        ("materialize", sc.DuckDBCompiler),  # default value
        ("mysql", sc.MySQLCompiler),
        ("oracle", sc.OracleCompiler),
        ("postgres", sc.PostgresCompiler),
        ("presto", sc.TrinoCompiler),
        ("prql", sc.DuckDBCompiler),  # default value
        ("redshift", sc.PostgresCompiler),
        ("risingwave", sc.RisingWaveCompiler),
        ("snowflake", sc.SnowflakeCompiler),
        ("spark", sc.PySparkCompiler),
        ("spark2", sc.PySparkCompiler),
        ("sqlite", sc.SQLiteCompiler),
        ("starrocks", sc.DuckDBCompiler),  # default value
        ("tableau", sc.DuckDBCompiler),  # default value
        ("teradata", sc.DuckDBCompiler),  # default value
        ("trino", sc.TrinoCompiler),
        ("tsql", sc.MSSQLCompiler),
    ],
)
def test_get_ibis_to_sqlglot_compiler(dialect, expected_compiler) -> None:
    compiler = _get_ibis_to_sqlglot_compiler(dialect)
    assert isinstance(compiler, expected_compiler)
