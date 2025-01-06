import os
import pyarrow as pa
import pyarrow.parquet as pq
import sqlglot  # type: ignore
import tempfile
import unicodedata
from typing import Any, Dict, Literal, NamedTuple, Optional

from dlt.common.schema.utils import is_valid_schema_name

try:
    # Import sklearn because claude often uses it.
    import sklearn  # type: ignore
except ImportError:
    pass

from fastmcp import FastMCP  # type: ignore

from dlt.common import logger


profile: Optional[str] = None  # will run in local mode


class DltMCP(FastMCP):
    def _setup_handlers(self) -> None:
        super()._setup_handlers()
        # NOTE: handler is disabled in base class
        self._mcp_server.list_resource_templates()(self.list_resource_templates)

    def run(self, transport: Literal["stdio", "sse"] = "stdio") -> None:
        global profile

        # if the profile is not set, assume we are in local mode
        if not profile:
            os.chdir(os.path.split((os.path.dirname(__file__)))[0])

        # Get the current context (after the cwd is changed)
        ctx = dlt_pdl.context()

        if not profile:
            # If profile not set use default profile.
            profile = ctx.config.settings.get("default_profile", "dev")

        logger.info(f"Running mcp server with profile {profile} cwd {os.getcwd()}...")
        super().run(transport)


mcp = DltMCP("dlt+ catalog", log_level="WARNING")


class CacheEntry(NamedTuple):
    result: pa.Table
    query: str
    input_cache_entry: Optional[str]


result_cache: Dict[str, CacheEntry] = {}
RECENT_CACHE_KEY = "_recent"
RECENT_COUNT = 0
ANALYSIS_PROMPTS = """
You are an AI assistant and your goal is to explore the data found in the catalog in order to perform the data analysis
requested by the user. Assistant must asses the complexity of the tasks in order to choose the right
tools.

1. the user specified the following task
%s

2. Explore the available datasets and tables to locate where the data of interest may be
a. List available datasets with available_datasets tool
b. List available tables for each dataset with available_tables tool
c. Guess which tables may hold interesting data

3. Obtain schemas for the tables of interest
a. Use table_schema tool to get the schema of the table
b. Remember the SQL dialect that you will use to query each table
c. When constructing SQL statements always use "normalized_name" for tables and columns
d. Schema contains data types as Apache Arrow types. You'll need to convert them to database types of particular dialect yourself

3. Assess the complexity of the analysis before doing SQL queries
a. if you can solve the problem with an SQL query that returns few hundred rows go to step 4
b. If you need large data, or you need to process this data in Python (as dataframe) or you need to reuse results, go to step 5

4. Use the query_sql to query table(s) to return few hundred rows.
a. Read the tool description on how to write SELECT queries
b. Run the tool and get the csv with results
e. You can use "recent_result" tool to get csv with the most recent result from query_sql
f. You have access to parquet file with query result, `query_sql` returns the path to it

5. Analyze large data with SQL queries and Python data frame transformations
a. You will use "bookmark_sql" tool to execute SELECT and bookmark the result on the server
b. You will use "transform_bookmark_and_return" to transform the results under bookmark and return them as csv
c. You can always use "read_result_from_bookmark" tool to get the result under bookmark as csv.
d. You can transform the same bookmark several times
e. You can use "recent_result" tool to get csv with the most recent result from bookmark_sql and transform_bookmark_and_return
f. You have access to parquet file with query and transformation result, `bookmark_sql` and `read_result_from_bookmark` return the path to it

6. Present the analysis to the user and ask the user if to create visualization of your analysis

7. Offer drill-down options to the user

"""


@mcp.prompt()
def data_analysis(query: str) -> Any:
    return [{"role": "user", "content": ANALYSIS_PROMPTS % query}]


@mcp.tool(
    description="List all available dlt datasets in the dlt+ catalog",
)
def available_datasets() -> str:
    catalog_ = dlt_pdl.catalog(profile=profile)
    # use `datasets` prop when it becomes available
    datasets = catalog_._project_config.datasets
    if datasets:
        return "\n".join(datasets.keys())
    else:
        return ""


@mcp.tool(
    description="All available tables in the dataset",
)
def available_tables(dataset: str) -> Any:
    # TODO: cache catalog and all schemas in datasets so we can create datasets quickly
    # we can perhaps cache the datasets too (server is async)
    catalog_ = dlt_pdl.catalog(profile=profile)
    return getattr(catalog_, dataset).__str__()


@mcp.tool(description="Print first 10 rows in the table")
def table_head(dataset: str, table: str) -> Any:
    catalog_ = dlt_pdl.catalog(profile=profile)
    ibis_conn = getattr(catalog_, dataset).ibis()
    ibis_table = ibis_conn.table(table, database=dataset)
    return ibis_table.limit(10).execute()


@mcp.tool(
    description="""
Executes sql statement on a given dataset as returns the result as | delimited csv. Use this tool
for simple analysis where the number of rows is small ie. below 100.

SQL dialect: Use table and column names discovered in `available_tables` and `table_schema` tools.
Use SQL dialect as indicated in `table_schema`. Do not qualify table names with schema names.
"""
)
def query_sql(dataset: str, sql: str) -> Any:
    return _execute_sql(dataset, sql)


def _execute_sql(dataset: str, sql: str, bookmark: Optional[str] = None) -> Any:
    # Use sqlglot to parse. reject any MDL statement you can find.
    parsed = sqlglot.parse(sql)
    if any(
        isinstance(expr, (sqlglot.exp.Insert, sqlglot.exp.Update, sqlglot.exp.Delete))
        for expr in parsed
    ):
        raise ValueError("Data modification statements are not allowed")

    catalog_ = dlt_pdl.catalog(profile=profile)

    table = catalog_.dataset(dataset)(sql).arrow()
    return _return_or_cache(table, sql, bookmark)


def _cache_arrow(
    table: pa.Table,
    query: Optional[str] = None,
    save_bookmark: Optional[str] = None,
    input_bookmark: Optional[str] = None,
) -> str:
    if not is_valid_schema_name(save_bookmark):
        raise ValueError(
            f"Invalid bookmark name: {save_bookmark}. Only strings that are valid Python identifiers are accepted."
        )
    info = f"Result with {len(table)} row(s) bookmarked under {save_bookmark}\n"
    result_cache[save_bookmark] = CacheEntry(table, query, input_bookmark)
    # write bookmark as pq, this is EXPERIMENTAL to see if exposing data frames directly makes sense.
    if save_bookmark == RECENT_CACHE_KEY:
        # Do not overwrite recent cache.
        global RECENT_COUNT

        RECENT_COUNT += 1
        save_bookmark += f".{RECENT_COUNT}"

    pq_file = os.path.join(tempfile.gettempdir(), save_bookmark + ".parquet")
    pq.write_table(table, pq_file)
    info += f"Result is also available as parquet file in {pq_file}\n"

    return info


def _return_or_cache(
    table: pa.Table,
    query: Optional[str] = None,
    save_bookmark: Optional[str] = None,
    input_bookmark: Optional[str] = None,
) -> Any:
    info = _cache_arrow(table, query, RECENT_CACHE_KEY, input_bookmark)  # cache last result
    if save_bookmark:
        return _cache_arrow(table, query, save_bookmark, input_bookmark)
    else:
        return _return_df(table, info)


@mcp.tool(
    description="""
Returns table schema as dlt (data load tool) table schema. Additionally provides SQL
dialect that should be used in `sql_query` tool. For each column, it provides Apache Arrow type
in addition to dlt type. Table and column names are provided both as original and normalized and
escaped to be used in SQL queries (`normalized_name`). The return format is yaml.
"""
)
def table_schema(dataset: str, table: str) -> str:
    from dlt.common.schema.utils import to_pretty_yaml

    return to_pretty_yaml(_make_table_schema(dataset, table))


def _make_table_schema(dataset: str, table: str) -> Any:
    from dlt.common.libs.pyarrow import get_py_arrow_datatype
    from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

    catalog_ = dlt_pdl.catalog(profile=profile)
    dataset_ = catalog_[dataset]
    # get schema and clone the table

    schema = dataset_.schema
    table_schema: Dict[str, Any] = schema.get_table(table)
    # Add SQL dialect which is a destination type
    if isinstance(dataset_.sql_client, DuckDbSqlClient):
        dialect = "duckdb"
    else:
        assert dataset_._destination is not None
        dialect = dataset_._destination.destination_type
    table_schema["sql_dialect"] = dialect
    # normalize names
    table_schema["normalized_name"] = dataset_.sql_client.escape_column_name(
        schema.naming.normalize_tables_path(table_schema["name"])
    )
    for col_schema in table_schema["columns"].values():
        col_schema["normalized_name"] = dataset_.sql_client.escape_column_name(
            schema.naming.normalize_tables_path(col_schema["name"])
        )
        col_schema["arrow_data_type"] = str(
            get_py_arrow_datatype(col_schema, dataset_.sql_client.capabilities, "UTC")
        )
    stored_schema = schema.to_dict(remove_defaults=True, bump_version=False)
    return stored_schema["tables"][table]


def _return_df(table: pa.Table, info: str = "") -> Any:
    # just copy metadata
    df = table.to_pandas().copy(deep=False)

    # Remove non-ascii characters from the columns.
    # Those hang claude desktop - server at least on Windows.
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda x: unicodedata.normalize("NFKD", str(x))
            .encode("ascii", "ignore")
            .decode("ascii")
            .replace("\n", " ")
            .replace("\r", " ")
        )
    if info:
        info += "csv delimited with | containing header starts in next line:\n"
    return info + df.to_csv(index=False, sep="|")


def run_local() -> None:
    """Runs mcp from within the package."""
    mcp.run()
