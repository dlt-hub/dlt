from typing import Any, Generator, Sequence, Type, Union, TYPE_CHECKING
from contextlib import contextmanager

import sqlglot

from dlt.common.destination.dataset import (
    SupportsReadableRelation,
)
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import Self
from dlt.transformations import lineage

from dlt.destinations.dataset.exceptions import (
    ReadableRelationHasQueryException,
    ReadableRelationUnknownColumnException,
)

if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any


class BaseReadableDBAPIRelation(SupportsReadableRelation, WithSqlClient):
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""

        self._dataset = readable_dataset

        # wire protocol functions
        self.df = self._wrap_func("df")  # type: ignore
        self.arrow = self._wrap_func("arrow")  # type: ignore
        self.fetchall = self._wrap_func("fetchall")  # type: ignore
        self.fetchmany = self._wrap_func("fetchmany")  # type: ignore
        self.fetchone = self._wrap_func("fetchone")  # type: ignore

        self.iter_df = self._wrap_iter("iter_df")  # type: ignore
        self.iter_arrow = self._wrap_iter("iter_arrow")  # type: ignore
        self.iter_fetch = self._wrap_iter("iter_fetch")  # type: ignore

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        return self._dataset.sql_client

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        return self._dataset.sql_client_class

    def query(self) -> Any:
        # NOTE: converted from property to method due to:
        #   if this property raises AttributeError, __getattr__ will get called 🤯
        #   this leads to infinite recursion as __getattr_ calls this property
        #   also it does a heavy computation inside so it should be a method
        raise NotImplementedError("No query in ReadableDBAPIRelation")

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self.sql_client as client:
            with client.execute_query(self.query()) as cursor:
                if columns_schema := self.compute_columns_schema():
                    cursor.columns_schema = columns_schema
                yield cursor

    def _wrap_iter(self, func_name: str) -> Any:
        """wrap SupportsReadableRelation generators in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                yield from getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def _wrap_func(self, func_name: str) -> Any:
        """wrap SupportsReadableRelation functions in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                return getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def compute_columns_schema(
        self,
        allow_unknown_columns: bool = True,
        allow_anonymous_columns: bool = True,
        allow_fail: bool = True,
        **kwargs: Any,
    ) -> TTableSchemaColumns:
        """Provides the expected columns schema for the query

        Args:
            allow_unknown_columns (bool): If False, raise if any column types are not known
            allow_anonymous_columns (bool): If False, raise if any columns have auto assigned names
            allow_fail (bool): If False, will raise if for some reason no columns can be computed
        """

        # NOTE: if we do not have a schema, we cannot compute the columns schema
        if self._dataset.schema is None or (
            hasattr(self, "_table_name")
            and self._table_name
            and self._table_name not in self._dataset.schema.tables.keys()
        ):
            return {}

        # TODO: sqlalchemy does not work with their internal types, so we go via duckdb
        # TODO: snowflake has some bug in sqlglot lineage, so we go via duckdb
        # TODO: clickhouse has some bug in sqlglot lineage, so we go via duckdb
        caps = self._dataset.sql_client.capabilities
        dialect: str = caps.sqlglot_dialect
        query = self.query()
        if self._dataset._destination.destination_type in [
            "dlt.destinations.sqlalchemy",
            "dlt.destinations.snowflake",
            "dlt.destinations.clickhouse",
        ]:
            query = sqlglot.transpile(query, read=dialect, write="duckdb")[0]
            dialect = "duckdb"

        # TODO: maybe store the SQLGlot schema on the dataset
        # TODO: support joins between datasets
        d = self._dataset
        sqlglot_schema = lineage.create_sqlglot_schema(d.sql_client, d.schema)
        return lineage.compute_columns_schema(
            query,
            sqlglot_schema,
            dialect,
            infer_sqlglot_schema=allow_unknown_columns,
            allow_anonymous_columns=allow_anonymous_columns,
            allow_partial=allow_fail,
        )

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        return self.compute_columns_schema()

    @columns_schema.setter
    def columns_schema(self, new_value: TTableSchemaColumns) -> None:
        raise NotImplementedError("Columns Schema may not be set")


class ReadableDBAPIRelation(BaseReadableDBAPIRelation):
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        provided_query: Any = None,
        table_name: str = None,
        limit: int = None,
        selected_columns: Sequence[str] = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""

        # NOTE: we can keep an assertion here, this class will not be created by the user
        assert bool(table_name) != bool(
            provided_query
        ), "Please provide either an sql query OR a table_name"

        super().__init__(readable_dataset=readable_dataset)

        self._provided_query = provided_query
        self._table_name = table_name
        self._limit = limit
        self._selected_columns = selected_columns

    def query(self) -> Any:
        """build the query"""
        # TODO reimplement this using SQLGLot instead of passing strings
        if self._provided_query:
            return self._provided_query

        dataset_schema = self._dataset.schema

        table_name = self.sql_client.make_qualified_table_name(
            dataset_schema.naming.normalize_path(self._table_name)
        )

        maybe_limit_clause_1 = ""
        maybe_limit_clause_2 = ""
        if self._limit:
            maybe_limit_clause_1, maybe_limit_clause_2 = self.sql_client._limit_clause_sql(
                self._limit
            )

        selector = "*"
        if self._selected_columns:
            selector = ",".join(
                [
                    self.sql_client.escape_column_name(
                        dataset_schema.naming.normalize_tables_path(c)
                    )
                    for c in self._selected_columns
                ]
            )

        return f"SELECT {maybe_limit_clause_1} {selector} FROM {table_name} {maybe_limit_clause_2}"

    def __copy__(self) -> Self:
        return self.__class__(
            readable_dataset=self._dataset,
            provided_query=self._provided_query,
            table_name=self._table_name,
            limit=self._limit,
            selected_columns=self._selected_columns,
        )

    def limit(self, limit: int, **kwargs: Any) -> Self:
        if self._provided_query:
            raise ReadableRelationHasQueryException("limit")
        rel = self.__copy__()
        rel._limit = limit
        return rel

    def select(self, *columns: str) -> Self:
        if self._provided_query:
            raise ReadableRelationHasQueryException("select")
        rel = self.__copy__()
        rel._selected_columns = columns
        rel.compute_columns_schema()
        return rel

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> Self:
        if isinstance(columns, str):
            return self.select(columns)
        elif isinstance(columns, Sequence):
            return self.select(*columns)
        else:
            raise TypeError(f"Invalid argument type: {type(columns).__name__}")

    def head(self, limit: int = 5) -> Self:
        return self.limit(limit)
