from typing import Any, Generator, Sequence, Union, TYPE_CHECKING

from contextlib import contextmanager


from dlt.common.destination.reference import (
    SupportsReadableRelation,
)

from dlt.destinations.dataset.exceptions import (
    ReadableRelationHasQueryException,
    ReadableRelationUnknownColumnException,
)

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.sql_client import SqlClientBase
from dlt.common.schema import Schema

if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any


class BaseReadableDBAPIRelation(SupportsReadableRelation):
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
    def schema(self) -> Schema:
        return self._dataset.schema

    @property
    def query(self) -> Any:
        raise NotImplementedError("No query in ReadableDBAPIRelation")

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self.sql_client as client:
            # this hacky code is needed for mssql to disable autocommit, read iterators
            # will not work otherwise. in the future we should be able to create a readony
            # client which will do this automatically
            if hasattr(self.sql_client, "_conn") and hasattr(self.sql_client._conn, "autocommit"):
                self.sql_client._conn.autocommit = False
            with client.execute_query(self.query) as cursor:
                if columns_schema := self.columns_schema:
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

    @property
    def query(self) -> Any:
        """build the query"""
        if self._provided_query:
            return self._provided_query

        table_name = self.sql_client.make_qualified_table_name(
            self.schema.naming.normalize_path(self._table_name)
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
                    self.sql_client.escape_column_name(self.schema.naming.normalize_tables_path(c))
                    for c in self._selected_columns
                ]
            )

        return f"SELECT {maybe_limit_clause_1} {selector} FROM {table_name} {maybe_limit_clause_2}"

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        return self.compute_columns_schema()

    @columns_schema.setter
    def columns_schema(self, new_value: TTableSchemaColumns) -> None:
        raise NotImplementedError("columns schema in ReadableDBAPIRelation can only be computed")

    def compute_columns_schema(self) -> TTableSchemaColumns:
        """provide schema columns for the cursor, may be filtered by selected columns"""

        columns_schema = (
            self.schema.tables.get(self._table_name, {}).get("columns", {}) if self.schema else {}
        )

        if not columns_schema:
            return None
        if not self._selected_columns:
            return columns_schema

        filtered_columns: TTableSchemaColumns = {}
        for sc in self._selected_columns:
            sc = self.schema.naming.normalize_path(sc)
            if sc not in columns_schema.keys():
                raise ReadableRelationUnknownColumnException(sc)
            filtered_columns[sc] = columns_schema[sc]

        return filtered_columns

    def __copy__(self) -> "ReadableDBAPIRelation":
        return self.__class__(
            readable_dataset=self._dataset,
            provided_query=self._provided_query,
            table_name=self._table_name,
            limit=self._limit,
            selected_columns=self._selected_columns,
        )

    def limit(self, limit: int, **kwargs: Any) -> "ReadableDBAPIRelation":
        if self._provided_query:
            raise ReadableRelationHasQueryException("limit")
        rel = self.__copy__()
        rel._limit = limit
        return rel

    def select(self, *columns: str) -> "ReadableDBAPIRelation":
        if self._provided_query:
            raise ReadableRelationHasQueryException("select")
        rel = self.__copy__()
        rel._selected_columns = columns
        # NOTE: the line below will ensure that no unknown columns are selected if
        # schema is known
        rel.compute_columns_schema()
        return rel

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> "SupportsReadableRelation":
        if isinstance(columns, str):
            return self.select(columns)
        elif isinstance(columns, Sequence):
            return self.select(*columns)
        else:
            raise TypeError(f"Invalid argument type: {type(columns).__name__}")

    def head(self, limit: int = 5) -> "ReadableDBAPIRelation":
        return self.limit(limit)
