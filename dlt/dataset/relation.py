from __future__ import annotations
from collections.abc import Collection, Sequence
from functools import reduce

from functools import partial
from typing import (
    overload,
    Union,
    Any,
    Generator,
    Optional,
    Type,
    TYPE_CHECKING,
    Literal,
)
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
from dlt.common.schema.typing import (
    TTableSchema,
    TTableSchemaColumns,
    LOADS_TABLE_NAME,
    C_DLT_LOADS_TABLE_LOAD_ID,
    C_DLT_LOAD_ID,
    TTableReference,
    TTableReferenceStandalone,
)
from dlt.common.schema import utils as schema_utils, TSchemaTables
from dlt.common.typing import Self, TSortOrder, TypedDict
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.dataset import lineage
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.queries import bind_query, build_select_expr
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.dataset import SupportsDataAccess


if TYPE_CHECKING:
    from ibis import ir
    from dlt.common.libs.pandas import pandas as pd
    from dlt.common.libs.pyarrow import pyarrow as pa
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


class _JoinRef(TypedDict):
    table: str
    referenced_table: str
    columns: Union[list[str], Sequence[str]]
    referenced_columns: Sequence[str]
    direction: Literal["LEFT", "RIGHT"]


def _to_join_ref(ref: TTableReference, direction: Literal["LEFT", "RIGHT"]) -> _JoinRef:
    if "table" not in ref or ref["table"] is None or "referenced_table" not in ref:
        raise ValueError(
            f"Malformed table reference for join: {ref} - missing 'table' or 'referenced_table'"
        )
    if (
        len(ref.get("columns", [])) == 0
        or len(ref.get("referenced_columns", [])) == 0
        or len(ref.get("columns", [])) != len(ref.get("referenced_columns", []))
    ):
        raise ValueError(
            f"Malformed table reference for join: {ref} - 'columns' or 'referenced_columns' are"
            " empty"
        )
    return _JoinRef(
        table=ref["table"],
        referenced_table=ref["referenced_table"],
        columns=ref.get("columns", []),
        referenced_columns=ref.get("referenced_columns", []),
        direction=direction,
    )


def _resolve_parent_reference_chain(schema: dlt.Schema, left: str, right: str) -> list[_JoinRef]:
    """Resolve the reference chain between two tables.

    References always point child -> parent (child has foreign key to parent).
    By using LEFT/RIGHT joins appropriately, we avoid reversing references:
    - Child -> Parent: Use RIGHT JOIN (all rows from parent/right side)
    - Parent -> Child: Use LEFT JOIN (all rows from parent/left side)

    Returns:
        List of (reference, join_type) tuples where join_type is:
        - "RIGHT" when left is child and right is parent (natural ref direction)
        - "LEFT" when left is parent and right is child (opposite ref direction)
    """
    upward_chain_from_left = schema_utils.get_all_parent_references_to_root(schema.tables, left)
    upward_chain_from_right = schema_utils.get_all_parent_references_to_root(schema.tables, right)

    for idx, left_ref in enumerate(upward_chain_from_left):
        if "referenced_table" not in left_ref or "table" not in left_ref:
            break
        if left_ref["referenced_table"] == right:
            # right is a parent of left: natural direction (for references), use RIGHT JOIN
            return [_to_join_ref(ref, "RIGHT") for ref in upward_chain_from_left[: idx + 1]]

    for idx, right_ref in enumerate(upward_chain_from_right):
        if "referenced_table" not in right_ref or "table" not in right_ref:
            break
        if right_ref["referenced_table"] == left:
            # left is a parent of right: reverse chain, use LEFT JOIN
            return [
                _to_join_ref(ref, "LEFT") for ref in reversed(upward_chain_from_right[: idx + 1])
            ]

    raise ValueError(f"Unable to resolve reference chain between {left} and {right}")


def _resolve_reference_chain(schema: dlt.Schema, left: str, right: str) -> list[_JoinRef]:
    """Resolve references between two tables and determine join type per reference.

    Returns:
        List of _JoinRef where directions is:
        - "RIGHT" when joining from child to parent (natural ref direction)
        - "LEFT" when joining from parent to child
    """
    if left == right:
        raise ValueError(f"Cannot join a table to itself: {left}")
    # Check direct references first
    for ref in schema.references:
        if ref.get("table") == left and ref.get("referenced_table") == right:
            # Natural direction: left (child) -> right (parent), use RIGHT JOIN
            return [_to_join_ref(TTableReference(**ref), "RIGHT")]
        if ref.get("table") == right and ref.get("referenced_table") == left:
            # Opposite direction: left (parent) <- right (child), use LEFT JOIN
            return [_to_join_ref(TTableReference(**ref), "LEFT")]

    # Fall back to parent-child reference chain
    return _resolve_parent_reference_chain(schema, left, right)


def _build_join_condition(
    ref: _JoinRef,
    left_alias: str = "l",
    right_alias: str = "r",
    is_parent_on_left: bool = False,
) -> sge.Expression:
    """Build join ON condition."""
    conditions: list[sge.Expression] = []

    # Determine which columns go on which side
    if is_parent_on_left:
        left_cols = ref["referenced_columns"]
        right_cols = ref["columns"]
    else:
        left_cols = ref["columns"]
        right_cols = ref["referenced_columns"]

    for left_col, right_col in zip(left_cols, right_cols):
        condition = sge.EQ(
            this=sge.Column(
                this=sge.to_identifier(left_col, quoted=True),
                table=sge.to_identifier(left_alias, quoted=False),
            ),
            expression=sge.Column(
                this=sge.to_identifier(right_col, quoted=True),
                table=sge.to_identifier(right_alias, quoted=False),
            ),
        )
        conditions.append(condition)

    if len(conditions) == 1:
        return conditions[0]

    return reduce(lambda x, y: sge.And(this=x, expression=y), conditions)


def _build_join(
    refs_with_types: list[_JoinRef],
    *,
    base_alias: str = "t0",
    start_index: int = 1,
) -> list[sge.Join]:
    """Build SQL joins for the given references with their join types.

    Args:
        refs_with_types: List of (reference, join_type) tuples where join_type is "LEFT" or "RIGHT"

    Returns:
        List of SQLGlot join expressions
    """
    joins: list[sge.Join] = []
    left_alias = base_alias
    alias_index = start_index

    for ref in refs_with_types:
        # LEFT join = parent->child (swap columns, join to child table)
        # RIGHT join = child->parent (natural columns, join to parent table)
        is_left_join = ref["direction"] == "LEFT"
        joined_table = ref["table"] if is_left_join else ref["referenced_table"]
        right_alias = f"t{alias_index}"

        join = sge.Join(
            this=sge.Table(
                this=sge.to_identifier(joined_table, quoted=True),
                alias=sge.TableAlias(this=sge.to_identifier(right_alias, quoted=False)),
            ),
            kind=ref["direction"],
        ).on(
            _build_join_condition(
                ref,
                left_alias=left_alias,
                right_alias=right_alias,
                # left join means parent -> child join
                is_parent_on_left=is_left_join,
            )
        )
        joins.append(join)
        left_alias = right_alias
        alias_index += 1

    return joins


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

        # Track the original base table for chained join validation
        self._origin_table_name: Optional[str] = table_name
        # necessary to allow for chained joins while keeping correct cardinality
        self._joined_table_aliases: Optional[dict[str, str]] = (
            {table_name: "t0"} if table_name else None
        )
        self._next_join_alias_index: Optional[int] = 1 if table_name else None
        self._is_joinable_graph = True if table_name else False

        self._opened_sql_client: SqlClientBase[Any] = None
        self._sqlglot_expression: sge.Query = None
        self._schema: Optional[TTableSchemaColumns] = None

    def df(self, *args: Any, **kwargs: Any) -> pd.DataFrame | None:
        with self._cursor() as cursor:
            return cursor.df(*args, **kwargs)

    def arrow(self, *args: Any, **kwargs: Any) -> pa.Table | None:
        with self._cursor() as cursor:
            return cursor.arrow(*args, **kwargs)

    def fetchall(self, *args: Any, **kwargs: Any) -> list[tuple[Any, ...]]:
        with self._cursor() as cursor:
            return cursor.fetchall(*args, **kwargs)

    def fetchmany(self, *args: Any, **kwargs: Any) -> list[tuple[Any, ...]]:
        with self._cursor() as cursor:
            return cursor.fetchmany(*args, **kwargs)

    def fetchone(self, *args: Any, **kwargs: Any) -> tuple[Any, ...] | None:
        with self._cursor() as cursor:
            return cursor.fetchone(*args, **kwargs)

    def iter_df(self, *args: Any, **kwargs: Any) -> Generator[pd.DataFrame, None, None]:
        with self._cursor() as cursor:
            yield from cursor.iter_df(*args, **kwargs)

    # TODO maybe it should return record batches
    def iter_arrow(self, *args: Any, **kwargs: Any) -> Generator[pa.Table, None, None]:
        with self._cursor() as cursor:
            yield from cursor.iter_arrow(*args, **kwargs)

    def iter_fetch(self, *args: Any, **kwargs: Any) -> Generator[list[tuple[Any, ...]], None, None]:
        with self._cursor() as cursor:
            yield from cursor.iter_fetch(*args, **kwargs)

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

    @property
    def origin_table_name(self) -> Optional[str]:
        """Original base table name for chained joins, if available."""
        return self._origin_table_name

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
            query = bind_query(
                qualified_query=_qualified_query,
                sqlglot_schema=self._dataset.sqlglot_schema,
                expand_table_name=partial(
                    self.sql_client.make_qualified_table_name_path,
                    quote=False,
                    casefold=False,
                ),
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

    def select(self, *columns: str, _allow_merge_subqueries: bool = True) -> Self:
        """Create a `Relation` with the selected columns using a `SELECT` clause."""
        proj = [sge.Column(this=sge.to_identifier(col, quoted=True)) for col in columns]
        subquery = self.sqlglot_expression.subquery()
        new_expr = sge.select(*proj).from_(subquery)
        rel = self.__copy__()
        if _allow_merge_subqueries:
            rel._sqlglot_expression = merge_subqueries(new_expr)
        else:
            rel._sqlglot_expression = new_expr
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

    def join(self, other: str | Self) -> Self:
        """Join this relation to another table using schema references.

        Joins are resolved from this relation's origin table through the schema
        reference chain. Child->parent hops use RIGHT JOIN and parent->child hops
        use LEFT JOIN to preserve parent-side rows.

        Args:
            other: Table name or join-graph relation to join.

        Returns:
            A new relation with the join(s) applied.

        Raises:
            ValueError: If this relation is not joinable, `other` is invalid, the
                target table is missing, or no reference chain can be resolved.
        """
        if not self._origin_table_name or not self._is_joinable_graph:
            raise ValueError("Reference-based join requires a base or join-graph relation")

        if isinstance(other, Relation):
            if not other._is_joinable_graph:
                raise ValueError(
                    f"Cannot ensure joinability of relation `{other}` as it is not a join-graph"
                    " relation, please join before applying transformations like `select` or"
                    " `_apply_agg`"
                )
            other_table = other._origin_table_name
        else:
            other_table = other

        if not other_table or not isinstance(other_table, str):
            raise ValueError("`other` must be a table name or a base table relation")

        if other_table not in self._dataset.schema.tables:
            raise ValueError(f"Table `{other_table}` not found in dataset schema")

        schema = self._dataset.schema
        refs_with_types = _resolve_reference_chain(schema, self._origin_table_name, other_table)
        joined_tables = (
            self._joined_table_aliases.copy()
            if self._joined_table_aliases
            else {self._origin_table_name: "t0"}
        )
        next_alias_index = (
            self._next_join_alias_index
            if self._next_join_alias_index is not None
            else len(joined_tables)
        )

        base_alias = joined_tables[self._origin_table_name]
        refs_to_add: list[_JoinRef] = []
        # part of the reference chain might already be joined
        # in that case we join to the first existing alias we find
        for ref in refs_with_types:
            joined_table = ref["table"] if ref["direction"] == "LEFT" else ref["referenced_table"]
            if existing_alias := joined_tables.get(joined_table):
                base_alias = existing_alias
                continue
            refs_to_add.append(ref)

        if isinstance(self._sqlglot_expression, sge.Select) and self._sqlglot_expression.args.get(
            "joins"
        ):
            # this is a chained join, preserve existing joins
            query = self._sqlglot_expression.copy()
            existing_joins = query.args.get("joins", [])
        else:
            query = sge.Select(expressions=[sge.Star()]).from_(
                sge.Table(
                    this=sge.to_identifier(self._origin_table_name, quoted=True),
                    alias=sge.TableAlias(this="t0"),
                )
            )
            existing_joins = []

        start_index = max(len(existing_joins) + 1, next_alias_index)
        if refs_to_add:
            for join in _build_join(refs_to_add, base_alias=base_alias, start_index=start_index):
                query = query.join(join)
            for ref in refs_to_add:
                joined_table = (
                    ref["table"] if ref["direction"] == "LEFT" else ref["referenced_table"]
                )
                joined_tables[joined_table] = f"t{start_index}"
                start_index += 1

        rel = self.__copy__()
        # setup to allow further joins
        rel._sqlglot_expression = query
        rel._joined_table_aliases = joined_tables
        rel._next_join_alias_index = start_index if refs_to_add else next_alias_index
        rel._origin_table_name = self._origin_table_name
        rel._is_joinable_graph = True
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

    # TODO could be refactored to join any column from `_dlt_loads` table
    def with_load_id_col(self) -> dlt.Relation:
        """Return the relation with the `_dlt_load_id` included.

        There are 3 cases:
        - If the relation is a root table, this is a no-op
        - If the relation has a root key, join relation to root table
        - If the relation has a parent key, iteratively join the root to the relation
        - Else raise

        This should only raise if the `dlt.Schema` was tempered, breaking the
        dlt-generated root and parent relationships.
        """
        table_schema = self._dataset.schema.tables[self._table_name]

        if self._dataset.schema.naming.normalize_identifier(C_DLT_LOAD_ID) in self.columns:
            return self
        elif schema_utils.has_column_with_prop(table_schema, "root_key"):
            return _add_load_id_via_root_key(self)
        elif schema_utils.has_column_with_prop(table_schema, "parent_key"):
            return _add_load_id_via_parent_key(self)
        else:
            raise ValueError

    def from_loads(
        self,
        load_ids: Collection[str],
        add_load_id_column: bool = False,
    ) -> dlt.Relation:
        """Filter the table to rows associated with `load_ids`.

        This resolves the `_dlt_load_id` column then filters rows of the
        current relation. `include_load_id` allows to keep the `_dlt_load_id` column
        or exclude it after filtering.
        """
        if not self._table_name:
            raise ValueError(
                "`filter_loads()` only works on relations created via .table()."
                " It can't be applied to arbitrary relation."
            )

        initial_columns = self.columns
        normalized_load_id = self._dataset.schema.naming.normalize_identifier(C_DLT_LOAD_ID)
        filtered_rel_with_load_id = self.with_load_id_col().where(
            normalized_load_id, "in", load_ids
        )
        return (
            filtered_rel_with_load_id
            if add_load_id_column
            else filtered_rel_with_load_id.select(*initial_columns, _allow_merge_subqueries=False)
        )

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
        rel = self.__class__(dataset=self._dataset, query=self.sqlglot_expression)
        rel._origin_table_name = self._origin_table_name
        rel._joined_table_aliases = (
            self._joined_table_aliases.copy() if self._joined_table_aliases else None
        )
        rel._next_join_alias_index = self._next_join_alias_index
        # by default relations are not joinable after transforms
        rel._is_joinable_graph = False
        return rel


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


def _add_load_id_via_root_key_query(
    table_name: str,
    root_table_name: str,
    child_root_key: str,
    root_row_key: str,
    *,
    normalized_load_id: str = C_DLT_LOAD_ID,
) -> sge.Select:
    child_table = sge.Table(
        this=sge.to_identifier(table_name, quoted=True),
        alias=sge.TableAlias(this=sge.to_identifier("child", quoted=False)),
    )
    root_table_expr = sge.Table(
        this=sge.to_identifier(root_table_name, quoted=True),
        alias=sge.TableAlias(this=sge.to_identifier("root", quoted=False)),
    )

    # Build column list: child.*, root._dlt_load_id
    columns = [
        sge.Column(table=sge.to_identifier("child", quoted=False), this=sge.Star()),
        sge.Column(
            table=sge.to_identifier("root", quoted=False),
            this=sge.to_identifier(normalized_load_id, quoted=True),
        ),
    ]

    join_condition = sge.EQ(
        this=sge.Column(
            table=sge.to_identifier("child", quoted=False),
            this=sge.to_identifier(child_root_key, quoted=True),
        ),
        expression=sge.Column(
            table=sge.to_identifier("root", quoted=False),
            this=sge.to_identifier(root_row_key, quoted=True),
        ),
    )

    query = (
        sge.Select(expressions=columns)
        .from_(child_table)
        .join(root_table_expr, on=join_condition, join_type="INNER")
    )
    return query


def _add_load_id_via_root_key(relation: dlt.Relation) -> dlt.Relation:
    """Return the input relation with the `_dlt_load_id` column added.

    This is done by joining the `root_table._dlt_id` with the `table._dlt_root_id`
    """
    origin_table_name: str = relation._table_name
    tables_schema = relation._dataset.schema.tables
    root_table = schema_utils.get_root_table(tables_schema, origin_table_name)
    root_table_name = root_table["name"]

    ref = schema_utils.create_root_child_reference(tables_schema, origin_table_name)
    child_root_key = ref["columns"][0]
    root_row_key = ref["referenced_columns"][0]

    # Construct SELECT with INNER JOIN
    query = _add_load_id_via_root_key_query(
        table_name=origin_table_name,
        root_table_name=root_table_name,
        child_root_key=child_root_key,
        root_row_key=root_row_key,
        normalized_load_id=relation._dataset.schema.naming.normalize_identifier(C_DLT_LOAD_ID),
    )

    rel = relation.__copy__()
    rel._sqlglot_expression = query
    return rel


def _add_load_id_via_parent_key_query(
    table_name: str, table_schemas: TSchemaTables, normalized_load_id: str = C_DLT_LOAD_ID
) -> sge.Select:
    # The reference_chain goes from root to the queried table
    # Each reference contains: child table -> parent table relationship
    # We need to build joins from the queried table back to root
    reference_chain = []
    root_table_name = schema_utils.get_root_table(table_schemas, table_name)["name"]
    for reference in schema_utils.get_all_parent_child_references_from_root(
        table_schemas, root_table_name
    ):
        reference_chain.append(reference)
        if reference["table"] == table_name:
            break

    queried_table = sge.Table(
        this=sge.to_identifier(table_name, quoted=True),
        alias=sge.TableAlias(this=sge.to_identifier("t0", quoted=False)),
    )

    # build final table query with all columns explicitly + _dlt_load_id from root table
    columns = [
        sge.Column(
            table=sge.to_identifier("t0", quoted=False),
            this=sge.to_identifier(col_name, quoted=True),
        )
        for col_name in table_schemas[table_name]["columns"]
    ]
    columns.append(
        sge.Column(
            table=sge.to_identifier(f"t{len(reference_chain)}", quoted=False),
            this=sge.to_identifier(normalized_load_id, quoted=True),
        )
    )
    query = sge.Select(expressions=columns).from_(queried_table)

    # loop through references from table to root to append INNER JOINs
    for i, ref in enumerate(reversed(reference_chain)):
        parent_alias = f"t{i + 1}"
        parent_table = sge.Table(
            this=sge.to_identifier(ref["referenced_table"], quoted=True),
            alias=sge.TableAlias(this=sge.to_identifier(parent_alias, quoted=False)),
        )
        join_condition = sge.EQ(
            this=sge.Column(
                table=sge.to_identifier(f"t{i}", quoted=False),
                this=sge.to_identifier(ref["columns"][0], quoted=True),
            ),
            expression=sge.Column(
                table=sge.to_identifier(parent_alias, quoted=False),
                this=sge.to_identifier(ref["referenced_columns"][0], quoted=True),
            ),
        )
        query = query.join(parent_table, on=join_condition, join_type="INNER")

    return query


def _add_load_id_via_parent_key(relation: dlt.Relation) -> dlt.Relation:
    """Return the input relation with the `_dlt_load_id` column added.

    This is done by iteratively joining the `root_table._dlt_id` with `child._dlt_parent_id`
    until the input relation is reached.
    """
    origin_table_name: str = relation._table_name
    table_schemas = relation._dataset.schema.tables

    query = _add_load_id_via_parent_key_query(
        table_name=origin_table_name,
        table_schemas=table_schemas,
        normalized_load_id=relation._dataset.schema.naming.normalize_identifier(C_DLT_LOAD_ID),
    )

    rel = relation.__copy__()
    rel._sqlglot_expression = query
    return rel
