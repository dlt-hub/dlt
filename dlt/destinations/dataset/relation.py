from textwrap import indent
from typing import Any, Dict, Generator, Optional, Sequence, Tuple, Type, Union, TYPE_CHECKING
from contextlib import contextmanager

import sqlglot
import sqlglot.expressions as sge

from dlt.common.destination.dataset import (
    SupportsReadableRelation,
)

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import Self
from dlt.common.exceptions import TypeErrorWithKnownTypes
from dlt.transformations import lineage
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.dataset.exceptions import (
    ReadableRelationHasQueryException,
)
from dlt.destinations.dataset.utils import normalize_query

if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any


class BaseReadableDBAPIRelation(SupportsReadableRelation, WithSqlClient):
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        normalize_query: bool = True,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""

        # provided properties
        self._dataset = readable_dataset
        self._should_normalize_query: bool = normalize_query

        # derived / cached properties
        self._opened_sql_client: SqlClientBase[Any] = None
        self._columns_schema: TTableSchemaColumns = None
        self._qualified_query: sge.Query = None
        self._normalized_query: sge.Query = None

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

    def query(self, pretty: bool = False) -> Any:
        # NOTE: converted from property to method due to:
        #   if this property raises AttributeError, __getattr__ will get called 🤯
        #   this leads to infinite recursion as __getattr_ calls this property
        #   also it does a heavy computation inside so it should be a method
        if not self._should_normalize_query:
            return self._query()

        return self.normalized_query.sql(
            dialect=self._dataset.sql_client.capabilities.sqlglot_dialect, pretty=pretty
        )

    def _query(self) -> Any:
        """Returns a compliant with dlt schema in the relation.
        1. identifiers are not case folded
        2. star top level selects are not expanded
        3. dlt schema compat aliases are not added to top level selects
        """
        raise NotImplementedError("No query in ReadableDBAPIRelation")

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        try:
            self._opened_sql_client = self.sql_client

            # we allow computing the schema to fail if query normalization is disabled
            # this is useful for raw sql query access, testing and debugging
            try:
                columns_schema = self.columns_schema
            except lineage.LineageFailedException:
                if self._should_normalize_query:
                    raise
                else:
                    columns_schema = None

            # case 1: client is already opened and managed from outside
            if self.sql_client.native_connection:
                with self.sql_client.execute_query(self.query()) as cursor:
                    if columns_schema:
                        cursor.columns_schema = columns_schema
                    yield cursor
            # case 2: client is not opened, we need to manage it
            else:
                with self.sql_client as client:
                    with client.execute_query(self.query()) as cursor:
                        if columns_schema:
                            cursor.columns_schema = columns_schema
                        yield cursor
        finally:
            self._opened_sql_client = None

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
        infer_sqlglot_schema: bool = True,
        allow_anonymous_columns: bool = True,
        allow_partial: bool = True,
        **kwargs: Any,
    ) -> TTableSchemaColumns:
        return self._compute_columns_schema(
            infer_sqlglot_schema, allow_anonymous_columns, allow_partial, **kwargs
        )[0]

    def _compute_columns_schema(
        self,
        infer_sqlglot_schema: bool = False,
        allow_anonymous_columns: bool = True,
        allow_partial: bool = False,
        **kwargs: Any,
    ) -> Tuple[TTableSchemaColumns, Optional[sge.Query]]:
        """Provides the expected columns schema for the query

        Args:
            infer_sqlglot_schema (bool): If False, raise if any column types are not known
            allow_anonymous_columns (bool): If False, raise if any columns have auto assigned names
            allow_partial (bool): If False, will raise if for some reason no columns can be computed
        """
        # TODO: the docstrings above seem not right!
        # NOTE: if we do not have a schema, we cannot compute the columns schema
        # TODO: it is impossible to not have a schema, new schema will be created if dataset is not
        #       on the destination. try to remove the condition
        if self._dataset.schema is None or (
            hasattr(self, "_table_name")
            and self._table_name
            and self._table_name not in self._dataset.schema.tables.keys()
        ):
            return {}, None

        dialect: str = self._dataset.sql_client.capabilities.sqlglot_dialect

        return lineage.compute_columns_schema(
            # use dlt schema compliant query so lineage will work correctly on non case folded identifiers
            self._query(),
            self._dataset.sqlglot_schema,
            dialect,
            infer_sqlglot_schema=infer_sqlglot_schema,
            allow_anonymous_columns=allow_anonymous_columns,
            allow_partial=allow_partial,
        )

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        if self._columns_schema is None:
            self._columns_schema, self._qualified_query = self._compute_columns_schema()
        return self._columns_schema

    @columns_schema.setter
    def columns_schema(self, new_value: TTableSchemaColumns) -> None:
        raise NotImplementedError("Columns Schema may not be set")

    @property
    def qualified_query(self) -> sge.Query:
        if self._qualified_query is None:
            self._columns_schema, self._qualified_query = self._compute_columns_schema()
        return self._qualified_query

    @property
    def normalized_query(self) -> sge.Query:
        if self._normalized_query is None:
            self._normalized_query = normalize_query(
                self._dataset.sqlglot_schema, self.qualified_query, self._dataset.sql_client
            )
        return self._normalized_query

    def __str__(self) -> str:
        # TODO: have base table name for each relation is a good idea. consider to have it in the interface
        # TODO: merge detection of "simple" transformation that preserve table schema
        table_name = getattr(self, "_table_name", None)
        msg = ""
        for column in self.columns_schema.values():
            # TODO: show x-annotation hints
            msg += f"{column['name']} {column['data_type']}\n"
        msg = f"{table_name}:\n{indent(msg, prefix='  ')}" if table_name else msg
        return msg


class ReadableDBAPIRelation(BaseReadableDBAPIRelation):
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        provided_query: Any = None,
        table_name: str = None,
        limit: int = None,
        selected_columns: Sequence[str] = None,
        normalize_query: bool = True,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""

        # NOTE: we can keep an assertion here, this class will not be created by the user
        assert bool(table_name) != bool(
            provided_query
        ), "Please provide either an sql query OR a table_name"

        super().__init__(readable_dataset=readable_dataset, normalize_query=normalize_query)

        self._provided_query = provided_query
        self._table_name = table_name
        self._limit = limit
        self._selected_columns = selected_columns

    def _query(self) -> Any:
        # TODO reimplement this using SQLGLot instead of passing strings
        if self._provided_query:
            return self._provided_query

        dataset_schema = self._dataset.schema

        table_name = (
            self._table_name
        )  # self.sql_client.make_qualified_table_name(self._table_name, quote=False, casefold=False)

        maybe_limit_clause_1 = ""
        maybe_limit_clause_2 = ""
        if self._limit:
            maybe_limit_clause_1, maybe_limit_clause_2 = self.sql_client._limit_clause_sql(
                self._limit
            )

        selected_columns = (
            self._selected_columns
            if self._selected_columns
            else dataset_schema.get_table_columns(self._table_name).keys()
        )
        selector = ",".join(selected_columns)

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

    def __getitem__(self, columns: Sequence[str]) -> Self:
        # NOTE remember that `issubclass(str, Sequence) is True`
        if isinstance(columns, str):
            raise TypeError(
                f"Received invalid value `columns={columns}` of type"
                f" {type(columns).__name__}`. Valid types are: ['Sequence[str]']"
            )
        elif isinstance(columns, Sequence):
            return self.select(*columns)

        raise TypeError(
            f"Received invalid value `columns={columns}` of type"
            f" {type(columns).__name__}`. Valid types are: ['Sequence[str]']"
        )

    def head(self, limit: int = 5) -> Self:
        return self.limit(limit)
