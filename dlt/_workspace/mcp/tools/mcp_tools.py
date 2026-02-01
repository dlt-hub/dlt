from typing import Any, Dict, NamedTuple, Optional, Callable, cast
from abc import ABC, abstractmethod

import sqlglot

import pyarrow as pa
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.server import AnyUrl
from mcp.server.fastmcp.resources import FunctionResource

from dlt import Pipeline
from dlt import Dataset
from dlt._workspace.mcp.tools.helpers import format_csv
from dlt.common.schema.utils import is_valid_schema_name


class CacheEntry(NamedTuple):
    result: pa.Table
    query: str
    input_cache_entry: Optional[str]


class BaseMCPTools(ABC):
    RECENT_CACHE_KEY = "_recent"

    def __init__(self) -> None:
        self.result_cache: Dict[str, CacheEntry] = {}
        self._recent_count = 0

    @abstractmethod
    def register_with(self, mcp_server: FastMCP) -> None:
        """Register tools with the MCP server."""
        pass

    def register_resource(
        self,
        mcp_server: FastMCP,
        fn: Callable[[], Any],
        uri: str,
        name: str,
        description: str,
        mime_type: str,
    ) -> None:
        resource = FunctionResource(
            uri=AnyUrl(uri),
            name=name,
            description=description,
            mime_type=mime_type,
            fn=fn,
        )
        mcp_server.add_resource(resource)

    def register_resource_template(
        self,
        mcp_server: FastMCP,
        fn: Callable[..., Any],
        uri_template: str,
        name: str,
        description: str,
        mime_type: str,
    ) -> None:
        mcp_server._resource_manager.add_template(fn, uri_template, name, description, mime_type)

    def recent_result_resource(self) -> str:
        return self._return_from_cache(self.RECENT_CACHE_KEY)

    def bookmark_resource(self, bookmark: str) -> str:
        return self._return_from_cache(bookmark)

    def _make_table_schema(self, dataset: Dataset, table_name: str) -> Dict[str, Any]:
        """
        Construct and return a normalized table schema dictionary for the specified table.
        """
        from dlt.common.libs.pyarrow import get_py_arrow_datatype
        from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

        # get schema and clone the table
        schema = dataset.schema
        table_schema = cast(Dict[str, Any], schema.get_table(table_name))
        # add sql dialect which is destination type
        if isinstance(dataset.sql_client, DuckDbSqlClient):
            dialect = "duckdb"
        else:
            assert dataset._destination is not None
            dialect = dataset._destination.destination_type
        table_schema["sql_dialect"] = dialect
        # normalize names
        table_schema["normalized_name"] = dataset.sql_client.escape_column_name(
            schema.naming.normalize_tables_path(table_schema["name"])
        )
        for col_schema in table_schema["columns"].values():
            col_schema["normalized_name"] = dataset.sql_client.escape_column_name(
                schema.naming.normalize_tables_path(col_schema["name"])
            )
            col_schema["arrow_data_type"] = str(
                get_py_arrow_datatype(col_schema, dataset.sql_client.capabilities, "UTC")
            )
        stored_schema = schema.to_dict(remove_defaults=True, bump_version=False)
        return stored_schema["tables"][table_name]  # type: ignore[return-value]

    def _execute_sql(
        self,
        dataset: Dataset,
        sql: str,
        bookmark: Optional[str] = None,
    ) -> str:
        # use sqlglot to parse. reject any MDL statement you can find
        parsed = sqlglot.parse(sql)
        if any(
            isinstance(expr, (sqlglot.exp.Insert, sqlglot.exp.Update, sqlglot.exp.Delete))
            for expr in parsed
        ):
            raise ValueError("Data modification statements are not allowed")

        table = dataset(sql).arrow()
        return self._return_or_cache(table, sql, bookmark)

    def _return_or_cache(
        self,
        table: pa.Table,
        query: Optional[str] = None,
        save_bookmark: Optional[str] = None,
        input_bookmark: Optional[str] = None,
    ) -> str:
        info = self._cache_arrow(
            table, query, self.RECENT_CACHE_KEY, input_bookmark
        )  # cache last result
        if save_bookmark:
            return self._cache_arrow(table, query, save_bookmark, input_bookmark)
        else:
            return format_csv(table, info)

    # def _return_df(self, table: pa.Table, info: str = "") -> str:
    #     # just copy metadata
    #     df = table.to_pandas().copy(deep=False)

    #     # Remove non ascii characters from the columns. Those hang claude desktop - server
    #     # at least on Windows
    #     for col in df.select_dtypes(include=["object"]).columns:
    #         df[col] = df[col].apply(
    #             lambda x: unicodedata.normalize("NFKD", str(x))
    #             .encode("ascii", "ignore")
    #             .decode("ascii")
    #             .replace("\n", " ")
    #             .replace("\r", " ")
    #         )
    #     if info:
    #         info += "csv delimited with | containing header starts in next line:\n"
    #     return str(info + df.to_csv(index=False, sep="|"))

    def _cache_arrow(
        self,
        table: pa.Table,
        query: Optional[str] = None,
        save_bookmark: Optional[str] = None,
        input_bookmark: Optional[str] = None,
    ) -> str:
        # info = ""
        if not is_valid_schema_name(save_bookmark):
            raise ValueError(
                f"Invalid bookmark name: {save_bookmark}. Only strings that are valid Python "
                "identifiers are accepted."
            )
        info = f"Result with {len(table)} row(s) bookmarked under {save_bookmark}\n"
        self.result_cache[save_bookmark] = CacheEntry(table, query, input_bookmark)
        # write bookmark as pq, this is EXPERIMENTAL to see if exposing data frames directly
        # makes sense
        if save_bookmark == self.RECENT_CACHE_KEY:
            # do not overwrite recents
            self._recent_count += 1
            save_bookmark = save_bookmark + f".{self._recent_count}"

        return info

    def _return_from_cache(self, cache_url: str) -> str:
        if not (cache_entry := self.result_cache.get(cache_url)):
            raise ValueError(f"{cache_url} bookmark not found")
        return format_csv(cache_entry.result)

    def read_result_from_bookmark(self, bookmark: str) -> str:
        return self._return_from_cache(bookmark)

    def recent_result(self) -> str:
        return self._return_from_cache(self.RECENT_CACHE_KEY)


class PipelineMCPTools(BaseMCPTools):
    def __init__(self, pipeline: Pipeline):
        super().__init__()
        self.pipeline = pipeline

    def register_with(self, mcp_server: FastMCP) -> None:
        pipeline_name = self.pipeline.pipeline_name
        mcp_server.add_tool(
            self.available_tables,
            name="available_tables",
            description=f"All available tables in the pipeline {pipeline_name}",
        )
        mcp_server.add_tool(
            self.table_head,
            name="table_head",
            description=f"Get the first 10 rows of the table in the pipeline {pipeline_name}",
        )
        mcp_server.add_tool(
            self.table_schema,
            name="table_schema",
            description=f"Get the schema of the table in the pipeline {pipeline_name}",
        )
        mcp_server.add_tool(
            self.query_sql,
            name="query_sql",
            description=(
                f"Executes sql statement on a given pipeline {pipeline_name} as returns the result "
                "as | delimited csv. Use this tool for simple analysis where the number of rows is "
                "small ie. below 100. SQL dialect: Use table and column names discovered in "
                "`available_tables` and `table_schema` tools. Use SQL dialect as indicated in "
                "`table_schema`. Do not qualify table names with schema names."
            ),
        )
        mcp_server.add_tool(
            self.bookmark_sql,
            name="bookmark_sql",
            description=(
                f"Executes sql statement on a pipeline {pipeline_name} and bookmarks it under "
                "given bookmark for further processing. Use this tool when you need to select "
                "and transform a large result or when you want to reuse results of the query. "
                "To obtain full result use `read_result_from_bookmark` or `recent_result` tools."
            ),
        )
        mcp_server.add_tool(
            self.read_result_from_bookmark,
            name="read_result_from_bookmark",
            description="Read the result of the bookmark and return it as '|' delimited CSV",
        )
        mcp_server.add_tool(
            self.recent_result,
            name="recent_result",
            description="Read the most recent result and return it as '|' delimited CSV",
        )

    def available_tables(self) -> Dict[str, Any]:
        return {
            "schemas": {
                schema_name: [table["name"] for table in schema.data_tables()]
                for schema_name, schema in self.pipeline.schemas.items()
            }
        }

    def table_head(self, table_name: str) -> str:
        return format_csv(self.pipeline.dataset()[table_name].head(10).arrow())

    def table_schema(self, table: str) -> Dict[str, Any]:
        return self._make_table_schema(self.pipeline.dataset(), table)

    def query_sql(self, sql: str) -> str:
        return self._execute_sql(self.pipeline.dataset(), sql)

    def bookmark_sql(self, sql: str, bookmark: str) -> str:
        return self._execute_sql(self.pipeline.dataset(), sql, bookmark)
