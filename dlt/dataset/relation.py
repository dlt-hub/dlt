from __future__ import annotations

from typing import overload, Union, Any, Generator, Optional, Sequence, Type, TYPE_CHECKING
from textwrap import indent
from contextlib import contextmanager
from dlt.common.utils import simple_repr, without_none

from sqlglot import maybe_parse
from sqlglot.optimizer.merge_subqueries import merge_subqueries
from sqlglot.expressions import ExpOrStr as SqlglotExprOrStr

import sqlglot.expressions as sge

import dlt
from dlt.common.destination.dataset import TFilterOperation
from dlt.common.libs.sqlglot import to_sqlglot_type, build_typed_literal, TSqlGlotDialect
from dlt.common.libs.utils import is_instance_lib
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns
from dlt.common.typing import Self, TSortOrder
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.dataset import lineage
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.queries import _normalize_query, build_select_expr
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.dataset import SupportsDataAccess


if TYPE_CHECKING:
    from ibis import ir
    from dlt.helpers.ibis import Expr as IbisExpr


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


class Relation(WithSqlClient):
    @overload
    def __init__(
        self,
        *,
        dataset: dlt.Dataset,
        query: Union[str, sge.Query],
        query_dialect: Optional[str] = None,
        _execute_raw_query: bool = False,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        dataset: dlt.Dataset,
        table_name: str,
    ) -> None: ...

    def __init__(
        self,
        *,
        dataset: dlt.Dataset,
        query: Optional[Union[str, sge.Query, IbisExpr]] = None,
        query_dialect: Optional[str] = None,
        table_name: Optional[str] = None,
        _execute_raw_query: bool = False,
    ) -> None:
        """Create a lazy evaluated relation for the dataset of a destination"""
        if table_name is None and query is None:
            raise ValueError(
                "`dlt.Relation` needs to receive minimally `table_name` or `query` at"
                " initialization."
            )

        self._dataset = dataset
        self._query = query
        self._query_dialect = query_dialect
        self._table_name = table_name
        self._execute_raw_query: bool = _execute_raw_query

        self._opened_sql_client: SqlClientBase[Any] = None
        self._sqlglot_expression: sge.Query = None
        self._schema: Optional[TTableSchemaColumns] = None

    def _wrap_iter(self, func_name: str) -> Any:
        """wrap Relation generators in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self._cursor() as cursor:
                yield from getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def _wrap_func(self, func_name: str) -> Any:
        """wrap Relation functions in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self._cursor() as cursor:
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
        """dlt columns schema. Convenience method for `dlt.schema["columns"]`"""
        return self.schema.get("columns", {})

    @property
    def schema(self) -> TTableSchema:
        """dlt table schema associated with the relation.

        This infers the schema from the relation's content. It's likely to include less
        information than retrieving the schema from the pipeline or the dataset if the table
        already exists.
        """
        if self._schema is None:
            schema, _ = _get_relation_output_columns_schema(
                self,
                infer_sqlglot_schema=True,
                allow_anonymous_columns=True,
                allow_partial=True,
            )
            self._schema = schema

        assert self._schema is not None
        # TODO use lineage features to propagate table-level dlt annotations
        return {"columns": self._schema}

    @schema.setter
    def schema(self, new_value: Any) -> None:
        """Disable schema setter."""
        raise NotImplementedError("Schema may not be set")

    @property
    def columns(self) -> list[str]:
        """List of column names found on the table."""
        return list(self.columns_schema.keys())

    def _ipython_key_completions_(self) -> list[str]:
        """Provide column names as completion suggestion in interactive environments."""
        return self.columns

    # TODO can we narrow type from `sge.Query` to `sge.Select`?
    @property
    def sqlglot_expression(self) -> sge.Query:
        """SQLGlot expression"""
        if isinstance(self._sqlglot_expression, sge.Query):
            return self._sqlglot_expression

        if isinstance(self._query, (str, sge.Query)):
            expression = maybe_parse(
                self._query, dialect=self._query_dialect or self.destination_dialect
            )
        elif isinstance(self._table_name, str):
            expression = build_select_expr(table_name=self._table_name)
        elif is_instance_lib(self._query, class_ref="ibis.Expr"):
            from dlt.helpers.ibis import ibis

            assert isinstance(self._query, ibis.Expr)

            from dlt.helpers.ibis import compile_ibis_to_sqlglot

            expression = compile_ibis_to_sqlglot(self._query, self.destination_dialect)
        else:
            raise RuntimeError(
                "`dlt.Relation` is missing `table_name` and `query` to resolve the SQLGlot"
                " expression. This is an unexpected error."
            )

        self._sqlglot_expression = expression
        return self._sqlglot_expression

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        return self._dataset.sql_client

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        return self._dataset.sql_client_class

    @contextmanager
    def _cursor(self) -> Generator[SupportsDataAccess, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        try:
            self._opened_sql_client = self.sql_client

            # we only compute the columns schema if we are not executing the raw query
            if self._execute_raw_query:
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

    def to_sql(self, pretty: bool = False, *, _raw_query: bool = False) -> str:
        """Get the normalize query string in the correct sql dialect for this relation"""

        if self._execute_raw_query or _raw_query:
            query = self.sqlglot_expression
        else:
            _, _qualified_query = _get_relation_output_columns_schema(self)
            query = _normalize_query(
                qualified_query=_qualified_query,
                sqlglot_schema=self._dataset.sqlglot_schema,
                sql_client=self.sql_client,
                casefold_identifier=self.sql_client.capabilities.casefold_identifier,
            )

        if not isinstance(query, sge.Query):
            raise ValueError(
                f"Query `{query}` received for `{self.__class__.__name__}`. "
                "Must be an SQL SELECT statement."
            )

        return query.sql(dialect=self.destination_dialect, pretty=pretty)

    # TODO this method needs to have the same name as `dlt.extract.hints::SqlModel.query_dialect`;
    # the current implementation doesn't disambiguate "query dialect" and "destination dialect",
    # i.e., the input and output the SQL transpilation
    # These methods are called in `dlt.normalize.items_normalizers::ModelItemsNormalizer.__call__()`
    # and should be fixed; then remove this property
    @property
    def query_dialect(self) -> TSqlGlotDialect:
        return self.destination_dialect

    @property
    def destination_dialect(self) -> TSqlGlotDialect:
        """SQLGlot dialect used by the destination.

        This is the target dialect when transpiling SQL queries.
        """
        return self._dataset.destination_dialect

    def to_ibis(self) -> ir.Table:
        """Create an `ibis.Table` expression from the current relation.

        If the `dlt.Relation` was initialized with a `table_name`, it will return an
        `ibis.Table` directly. If the `dlt.Relation` was transformed via `.where()`, `.select()`,
        etc., it will apply the operations in a single step as an opaque SQLQuery Ibis operation.
        """
        from dlt.common.libs.ibis import _DltBackend

        backend = _DltBackend.from_dataset(self._dataset)

        if self._table_name:
            ibis_table = backend.table(self._table_name)
        else:
            # pass raw query before any identifiers are expanded, quoted or normalized
            ibis_table = backend.sql(self.sqlglot_expression.sql(dialect=self.destination_dialect))

        return ibis_table

    def limit(self, limit: int) -> Self:
        """Create a `Relation` using a `LIMIT` clause."""
        rel = self.__copy__()
        rel._sqlglot_expression = rel.sqlglot_expression.limit(limit)
        return rel

    def head(self, limit: int = 5) -> Self:
        """Create a `Relation` using a `LIMIT` clause. Defaults to `limit=5`

        This proxies `Relation.limit()`.
        """
        return self.limit(limit)

    def select(self, *columns: str) -> Self:
        """CReate a `Relation` with the selected columns using a `SELECT` clause."""
        proj = [sge.Column(this=sge.to_identifier(col, quoted=True)) for col in columns]
        subquery = self.sqlglot_expression.subquery()
        new_expr = sge.select(*proj).from_(subquery)
        rel = self.__copy__()
        rel._sqlglot_expression = merge_subqueries(new_expr)
        return rel

    def order_by(self, column_name: str, direction: TSortOrder = "asc") -> Self:
        """Create a `Relation` ordering results using a `ORDER BY` clause.

        Args:
            column_name (str): The column to order by.
            direction (TSortOrder, optional): The direction to order by: "asc"/"desc". Defaults to "asc".

        Returns:
            Self: A new Relation with the `ORDER BY` clause applied.
        """
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
        rel._sqlglot_expression = rel.sqlglot_expression.order_by(order_expr)
        return rel

    # NOTE we currently force to have one column selected; we could be more flexible
    # and rewrite the query to compute the AGG of all selected columns
    # `SELECT AGG(col1), AGG(col2), ... FROM table``
    def _apply_agg(self, agg_cls: type[sge.AggFunc]) -> Self:
        """Create a `Relation` with the aggregate function applied.

        Exactly one column must be selected.
        """
        if len(self.sqlglot_expression.selects) != 1:
            raise ValueError(
                f"{agg_cls.__name__.lower()}() requires a query with exactly one select expression."
                " Consider selecting the column you want to aggregate."
            )
        selected_col = self.sqlglot_expression.selects[0]
        expr = agg_cls(this=selected_col.this if hasattr(selected_col, "this") else selected_col)
        rel = self.__copy__()
        rel.sqlglot_expression.set("expressions", [expr])
        return rel

    def max(self) -> Self:  # noqa: A003
        """Create a `Relation` with the `MAX` aggregate applied.

        Exactly one column must be selected.
        """
        return self._apply_agg(sge.Max)

    def min(self) -> Self:  # noqa: A003
        """Create a `Relation` with the `MIN` aggregate applied.

        Exactly one column must be selected.
        """
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
        """Create a `Relation` filtering results using a `WHERE` clause.

        This is identical to `Relation.filter()`.

        Args:
            column_name (str): The column to filter on.
            operator (TFilterOperation): The operator to use. Available operations are: eq, ne, gt, lt, gte, lte, in, not_in
            value (Any): The value to filter on.

        Returns:
            Self: A new Relation with the WHERE clause applied.
        """
        rel = self.__copy__()

        if not isinstance(rel.sqlglot_expression, sge.Select):
            raise ValueError(
                f"Query `{rel.sqlglot_expression}` received for `{rel.__class__.__name__}`. "
                "Must be an SQL SELECT statement."
            )

        if not operator and not value:
            rel._sqlglot_expression = rel.sqlglot_expression.where(
                column_or_expr, dialect=self.destination_dialect
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

        rel._sqlglot_expression = rel.sqlglot_expression.where(condition)
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
        """Create a `Relation` filtering results using a `WHERE` clause.

        This is identical to `Relation.where()`.

        Args:
            column_name (str): The column to filter on.
            operator (TFilterOperation): The operator to use. Available operations are: eq, ne, gt, lt, gte, lte, in, not_in
            value (Any): The value to filter on.

        Returns:
            Self: A new Relation with the WHERE clause applied.
        """
        if not operator and not value:
            return self.where(column_or_expr=column_or_expr)
        assert isinstance(column_or_expr, str)
        return self.where(column_or_expr=column_or_expr, operator=operator, value=value)

    # TODO move this to the WithSqlClient / data accessor mixin.
    def fetchscalar(self) -> Any:
        """Execute the relation and return the first value of first column as a Python primitive"""
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
        """Create a new Relation with the specified columns selected.

        This proxies `Relation.select()`.
        """
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

    def __str__(self) -> str:
        # TODO: merge detection of "simple" transformation that preserve table schema
        msg = f"Relation query:\n{indent(self.to_sql(pretty=True), prefix='  ')}\n"
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
        return self.__class__(dataset=self._dataset, query=self.sqlglot_expression)


def _get_relation_output_columns_schema(
    relation: dlt.Relation,
    *,
    infer_sqlglot_schema: bool = False,
    allow_anonymous_columns: bool = True,
    allow_partial: bool = False,
) -> tuple[TTableSchemaColumns, sge.Query]:
    columns_schema, normalized_query = lineage.compute_columns_schema(
        # use dlt schema compliant query so lineage will work correctly on non case folded identifiers
        relation.sqlglot_expression,
        relation._dataset.sqlglot_schema,
        dialect=relation.destination_dialect,
        infer_sqlglot_schema=infer_sqlglot_schema,
        allow_anonymous_columns=allow_anonymous_columns,
        allow_partial=allow_partial,
    )
    return columns_schema, normalized_query
