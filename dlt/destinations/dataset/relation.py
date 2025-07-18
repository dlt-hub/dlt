from typing import Any, Generator, Optional, Sequence, Tuple, Type, TYPE_CHECKING

from textwrap import indent
from typing import (
    overload,
    Any,
    get_args,
    Generator,
    Optional,
    Sequence,
    Tuple,
    Type,
    Literal,
    Union,
    TYPE_CHECKING,
)
from contextlib import contextmanager
from textwrap import indent
from dlt.common.utils import simple_repr, without_none

from sqlglot import maybe_parse
from sqlglot.optimizer.merge_subqueries import merge_subqueries
from sqlglot.expressions import ExpOrStr as SqlglotExprOrStr

import sqlglot.expressions as sge

from dlt.common.destination.dataset import Relation, TFilterOperation

from dlt.common.libs.sqlglot import to_sqlglot_type, build_typed_literal, TSqlGlotDialect
from dlt.common.schema.typing import TTableSchemaColumns, TTableSchema
from dlt.common.typing import Self, TSortOrder
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.transformations import lineage
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.queries import normalize_query, build_select_expr
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.dataset import DataAccess

try:
    from dlt.helpers.ibis import Expr as IbisExpr
    from dlt.helpers.ibis import compile_ibis_to_sqlglot
except (ImportError, MissingDependencyException):
    IbisExpr = None

if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
    from dlt.helpers.ibis import Expr as IbisExpr
else:
    ReadableDBAPIDataset = Any
    IbisTable = Any
    BaseReadableDBAPIDataset = Any


_FILTER_OP_MAP = {
    "eq": sge.EQ,
    "ne": sge.NEQ,
    "gt": sge.GT,
    "lt": sge.LT,
    "gte": sge.GTE,
    "lte": sge.LTE,
    "in": sge.In,
    "not_in": sge.Not,
}


class ReadableDBAPIRelation(Relation, WithSqlClient):
    @overload
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        query: Union[str, sge.Query],
        query_dialect: Optional[str] = None,
        _execute_raw_query: bool = False,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        table_name: str,
    ) -> None: ...

    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        query: Optional[Union[str, sge.Query, IbisExpr]] = None,
        query_dialect: Optional[str] = None,
        table_name: Optional[str] = None,
        _execute_raw_query: bool = False,
    ) -> None:
        """Create a lazy evaluated relation for the dataset of a destination"""

        # provided properties
        self._dataset = readable_dataset
        self.__execute_raw_query: bool = _execute_raw_query

        # derived / cached properties
        self._opened_sql_client: SqlClientBase[Any] = None
        self._columns_schema: TTableSchemaColumns = None
        self.__qualified_query: sge.Query = None
        self.__normalized_query: sge.Query = None

        # parse incoming query object
        self._sqlglot_expression: sge.Query = None
        if IbisExpr and isinstance(query, IbisExpr):
            self._sqlglot_expression = compile_ibis_to_sqlglot(query, self.query_dialect())
        elif query:
            self._sqlglot_expression = maybe_parse(
                query,
                dialect=query_dialect or self.query_dialect(),
            )
        else:
            self._sqlglot_expression = build_select_expr(
                table_name=table_name,
                selected_columns=list(self._dataset.schema.get_table_columns(table_name).keys()),
            )

    #
    # forward DataAccess protocol methods
    #
    def _wrap_iter(self, func_name: str) -> Any:
        """wrap Relation generators in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                yield from getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def _wrap_func(self, func_name: str) -> Any:
        """wrap Relation functions in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                return getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def df(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_func("df")(*args, **kwargs)

    def arrow(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_func("arrow")(*args, **kwargs)

    def fetchall(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_func("fetchall")(*args, **kwargs)

    def fetchmany(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_func("fetchmany")(*args, **kwargs)

    def fetchone(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_func("fetchone")(*args, **kwargs)

    def iter_df(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_iter("iter_df")(*args, **kwargs)

    def iter_arrow(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_iter("iter_arrow")(*args, **kwargs)

    def iter_fetch(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrap_iter("iter_fetch")(*args, **kwargs)

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        if self._columns_schema is None:
            self._columns_schema, self.__qualified_query = self._compute_columns_schema()
        return self._columns_schema

    @property
    def schema(self) -> TTableSchema:
        computed_columns, _ = self._compute_columns_schema(
            infer_sqlglot_schema=True,
            allow_anonymous_columns=True,
            allow_partial=True,
        )
        return {"columns": computed_columns}

    @schema.setter
    def schema(self, new_value: TTableSchema) -> None:
        raise NotImplementedError("Schema may not be set")

    @property
    def columns(self) -> list[str]:
        return list(self.schema.get("columns", {}).keys())

    def _ipython_key_completions_(self) -> list[str]:
        return self.columns

    #
    # WithSqlClient interface
    #
    @property
    def sql_client(self) -> SqlClientBase[Any]:
        return self._dataset.sql_client

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        return self._dataset.sql_client_class

    #
    # Cursor Management
    #
    @contextmanager
    def cursor(self) -> Generator[DataAccess, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        try:
            self._opened_sql_client = self.sql_client

            # we only compute the columns schema if we are not executing the raw query
            if self.__execute_raw_query:
                columns_schema = None
            else:
                columns_schema = self.columns_schema

            # case 1: client is already opened and managed from outside
            if self.sql_client.native_connection:
                with self.sql_client.execute_query(self.to_sql()) as cursor:
                    if columns_schema:
                        cursor.columns_schema = columns_schema
                    yield cursor
            # case 2: client is not opened, we need to manage it
            else:
                with self.sql_client as client:
                    with client.execute_query(self.to_sql()) as cursor:
                        if columns_schema:
                            cursor.columns_schema = columns_schema
                        yield cursor
        finally:
            self._opened_sql_client = None

    #
    # Query  / Expression Management
    #
    def to_sql(self, pretty: bool = False) -> str:
        """Returns an executable sql query string in the correct sql dialect for this relation"""

        if self.__execute_raw_query:
            query = self._sqlglot_expression
        else:
            query = self._normalized_query

        if not isinstance(query, sge.Query):
            raise ValueError(
                f"Query `{query}` received for `{self.__class__.__name__}`. "
                "Must be an SQL SELECT statement."
            )

        return query.sql(dialect=self.query_dialect(), pretty=pretty)

    def query_dialect(self) -> TSqlGlotDialect:
        return self._dataset.sqlglot_dialect

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
            allow_partial (bool): If False, will raise in case of parsing errors, missing table reference, unresolved `SELECT *`, etc.
        """
        # NOTE: if we do not have a schema, we cannot compute the columns schema
        if self._dataset.schema is None:
            return {}, None

        return lineage.compute_columns_schema(
            # use dlt schema compliant query so lineage will work correctly on non case folded identifiers
            self._sqlglot_expression,
            self._dataset.sqlglot_schema,
            dialect=self.query_dialect(),
            infer_sqlglot_schema=infer_sqlglot_schema,
            allow_anonymous_columns=allow_anonymous_columns,
            allow_partial=allow_partial,
        )

    @property
    def _qualified_query(self) -> sge.Query:
        if self.__qualified_query is None:
            self._columns_schema, self.__qualified_query = self._compute_columns_schema()
        return self.__qualified_query

    @property
    def _normalized_query(self) -> sge.Query:
        """Computes and returns the normalized query"""
        if self.__normalized_query is None:
            self.__normalized_query = normalize_query(
                self._dataset.sqlglot_schema,
                self._qualified_query,
                self._dataset.sql_client,
            )
        return self.__normalized_query

    #
    # Relation protocol methods
    #

    def limit(self, limit: int, **kwargs: Any) -> Self:
        rel = self.__copy__()
        rel._sqlglot_expression = rel._sqlglot_expression.limit(limit)
        return rel

    def head(self, limit: int = 5) -> Self:
        return self.limit(limit)

    def select(self, *columns: str) -> Self:
        proj = [sge.Column(this=sge.to_identifier(col, quoted=True)) for col in columns]
        subquery = self._sqlglot_expression.subquery()
        new_expr = sge.select(*proj).from_(subquery)
        rel = self.__copy__()
        rel._sqlglot_expression = merge_subqueries(new_expr)
        rel.compute_columns_schema()
        return rel

    def order_by(self, column_name: str, direction: TSortOrder = "asc") -> Self:
        if direction not in ["asc", "desc"]:
            raise ValueError(
                f"`{direction}` is an invalid sort order, allowed values are: `asc` and `desc`"
            )
        order_expr = sge.Ordered(
            this=sge.Column(
                this=sge.to_identifier(column_name, quoted=True),
            ),
            desc=(direction == "desc"),
        )
        rel = self.__copy__()
        rel._sqlglot_expression = rel._sqlglot_expression.order_by(order_expr)
        return rel

    def _apply_agg(self, agg_cls: type[sge.AggFunc]) -> Self:
        if len(self._sqlglot_expression.selects) != 1:
            raise ValueError(
                f"{agg_cls.__name__.lower()}() requires a query with exactly one select expression."
                " Consider selecting the column you want to aggregate."
            )
        selected_col = self._sqlglot_expression.selects[0]
        expr = agg_cls(this=selected_col.this if hasattr(selected_col, "this") else selected_col)
        rel = self.__copy__()
        rel._sqlglot_expression.set("expressions", [expr])
        return rel

    def max(self) -> Self:  # noqa: A003
        return self._apply_agg(sge.Max)

    def min(self) -> Self:  # noqa: A003
        return self._apply_agg(sge.Min)

    @overload
    def where(self, column_or_expr: SqlglotExprOrStr) -> Self: ...

    @overload
    def where(
        self,
        column_or_expr: str,
        operator: TFilterOperation,
        value: Any,
    ) -> Self: ...

    def where(
        self,
        column_or_expr: SqlglotExprOrStr,
        operator: Optional[TFilterOperation] = None,
        value: Optional[Any] = None,
    ) -> Self:
        rel = self.__copy__()

        if not isinstance(rel._sqlglot_expression, sge.Select):
            raise ValueError(
                f"Query `{rel._sqlglot_expression}` received for `{rel.__class__.__name__}`. "
                "Must be an SQL SELECT statement."
            )

        if not operator and not value:
            rel._sqlglot_expression = rel._sqlglot_expression.where(
                column_or_expr, dialect=self.query_dialect()
            )
            return rel

        assert isinstance(column_or_expr, str)
        column_name = column_or_expr

        if isinstance(operator, str):
            try:
                condition_cls = _FILTER_OP_MAP[operator]
            except KeyError:
                raise ValueErrorWithKnownValues(
                    key="operator",
                    value_received=operator,
                    valid_values=list(_FILTER_OP_MAP.keys()),
                )

        sqlgot_type = to_sqlglot_type(
            dlt_type=self.columns_schema[column_name].get("data_type"),
            precision=self.columns_schema[column_name].get("precision"),
            timezone=self.columns_schema[column_name].get("timezone"),
            nullable=self.columns_schema[column_name].get("nullable"),
        )

        value_expr = build_typed_literal(value, sqlgot_type)

        column = sge.Column(this=sge.to_identifier(column_name, quoted=True))

        condition: sge.Expression = None
        if operator == "in":
            exprs = value_expr.expressions if isinstance(value_expr, sge.Tuple) else [value_expr]
            condition = sge.In(this=column, expressions=exprs)
        elif operator == "not_in":
            exprs = value_expr.expressions if isinstance(value_expr, sge.Tuple) else [value_expr]
            condition = sge.Not(this=sge.In(this=column, expressions=exprs))
        else:
            condition = condition_cls(this=column, expression=value_expr)

        rel._sqlglot_expression = rel._sqlglot_expression.where(condition)
        return rel

    @overload
    def filter(self, column_or_expr: SqlglotExprOrStr) -> Self: ...  # noqa: A003

    @overload
    def filter(  # noqa: A003
        self,
        column_or_expr: str,
        operator: TFilterOperation,
        value: Any,
    ) -> Self: ...

    def filter(  # noqa: A003
        self,
        column_or_expr: SqlglotExprOrStr,
        operator: Optional[TFilterOperation] = None,
        value: Optional[Any] = None,
    ) -> Self:
        if not operator and not value:
            return self.where(column_or_expr=column_or_expr)
        assert isinstance(column_or_expr, str)
        return self.where(column_or_expr=column_or_expr, operator=operator, value=value)

    def scalar(self) -> Any:
        row = self.fetchmany(2)
        if not row:
            return None
        if len(row) != 1:
            raise ValueError(
                "Expected scalar result (single row, single column), got more than one row"
            )
        if len(row[0]) != 1:
            raise ValueError(
                "Expected scalar result (single row, single column), got 1 row with"
                f" {len(row[0])} columns"
            )
        return row[0][0]

    def __getitem__(self, columns: Sequence[str]) -> Self:
        # NOTE remember that `issubclass(str, Sequence) is True`
        if isinstance(columns, str):
            columns = [columns]
        elif not isinstance(columns, Sequence):
            raise TypeError(
                f"Received value `{columns=:}` of type `{type(columns).__name__}`."
                " Valid types are: `[Sequence[str]]`"
            )

        unknown_columns = [col for col in columns if col not in self.columns]
        if unknown_columns:
            raise KeyError(
                f"Columns `{unknown_columns}` not found on dataset. Available columns:"
                f" {self.columns}"
            )

        return self.select(*columns)

    #
    # Builtins
    #
    def __str__(self) -> str:
        # TODO: merge detection of "simple" transformation that preserve table schema
        msg = f"Relation query: \n{indent(self.to_sql(pretty=True), prefix='  ')}\n"
        msg += "Columns:\n"
        for column in self.columns_schema.values():
            # TODO: show x-annotation hints
            msg += f"{indent(column['name'], prefix='  ')} {column['data_type']}\n"
        return msg

    def __repr__(self) -> str:
        # schema may not be set
        kwargs = {
            "dataset": repr(self._dataset),
            "query": self.to_sql(pretty=True),
        }
        return simple_repr("dlt.Relation", **without_none(kwargs))

    def __copy__(self) -> Self:
        return self.__class__(
            readable_dataset=self._dataset,
            query=self._sqlglot_expression,
        )
