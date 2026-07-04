from __future__ import annotations
from collections.abc import Collection, Sequence
from typing import (
    overload,
    Union,
    Any,
    Generator,
    Optional,
    Type,
    TYPE_CHECKING,
    Literal,
    get_args,
)
from textwrap import indent
from contextlib import contextmanager
from dlt.common.utils import simple_repr, without_none

from sqlglot import maybe_parse
from sqlglot.optimizer.merge_subqueries import merge_subqueries
from sqlglot.expressions import ExpOrStr as SqlglotExprOrStr
from sqlglot.schema import Schema as SQLGlotSchema

import sqlglot.expressions as sge

import dlt
from dlt.common.destination.dataset import TFilterOperation
from dlt.common.libs.sqlglot import (
    to_sqlglot_type,
    build_typed_literal,
    migrate_order_and_limit,
    DLT_SUBQUERY_NAME,
    TSqlGlotDialect,
    has_pure_column_projection,
)
from dlt.common.libs import is_instance_lib
from dlt.common.schema.typing import (
    TTableSchema,
    TTableSchemaColumns,
    C_DLT_LOAD_ID,
)
from dlt.common.schema import utils as schema_utils
from dlt.common.typing import Self, TSortOrder, TypedDict
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.dataset import lineage
from dlt.destinations.sql_client import SqlClientBase, WithSchemas, WithSqlClient
from dlt.destinations.queries import bind_query, build_select_expr, make_expand_table_name
from dlt.common.destination.dataset import SupportsDataAccess
from dlt.dataset._incremental import (
    _build_incremental_aggregate,
    _build_incremental_condition,
    _maybe_warn_on_cursor_missing_raise,
    _parse_incremental_cursor_path,
    _raise_incomplete_cursor_column,
    _RelationIncrementalContext,
    _sqlglot_type_for_column,
)
from dlt.dataset._join import (
    _apply_join,
    _apply_explicit_join,
    _extract_joined_table_aliases,
    _JoinTarget,
    _left_source_qualifier,
    _qualify_unscoped_tables_with_dataset,
)


if TYPE_CHECKING:
    from dlt.common.libs.ibis import ir
    from dlt.common.libs.pandas import pandas as pd
    from dlt.extract.incremental import Incremental
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


TJoinType = Literal["left", "right", "inner", "full"]


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
        self._incremental_ctx: Optional[_RelationIncrementalContext] = None
        self._foreign_schemas: dict[str, list[dlt.Schema]] = {}
        self._foreign_physical_names: dict[str, str] = {}

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
        """Get the query string in the destination dialect. `pretty` flattens subqueries and formats the SQL."""

        if self._execute_raw_query or _raw_query:
            query = self.sqlglot_expression
        else:
            _, _qualified_query = _get_relation_output_columns_schema(self)
            if pretty:
                # optimize only for readable output; executed SQL stays as constructed
                _qualified_query = _optimize_query(_qualified_query)
            query = bind_query(
                qualified_query=_qualified_query,
                sqlglot_schema=self._relation_sqlglot_schema(),
                expand_table_name=make_expand_table_name(
                    self.sql_client, self._logical_to_physical_dataset_map()
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

        if self._table_name and self._query is None:
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
        """Create a `Relation` with the selected columns using a `SELECT` clause."""
        proj = [sge.Column(this=sge.to_identifier(col, quoted=True)) for col in columns]
        rel = self.__copy__()
        if has_pure_column_projection(self.sqlglot_expression):
            expr = self.sqlglot_expression.copy()
            expr.set("expressions", proj)
            rel._sqlglot_expression = expr
            return rel
        # a defining projection (aliases, expressions, distinct, group) must become a derived table
        qualifier = _left_source_qualifier(self.sqlglot_expression) or DLT_SUBQUERY_NAME
        subquery = self.sqlglot_expression.subquery(qualifier)
        new_expr = sge.select(*proj).from_(subquery)
        migrate_order_and_limit(subquery.this, new_expr, qualifier)
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
            this=sge.Column(this=sge.to_identifier(column_name, quoted=True)),
            desc=(direction == "desc"),
        )
        rel = self.__copy__()
        rel._sqlglot_expression = rel.sqlglot_expression.order_by(order_expr)
        return rel

    @overload
    def join(
        self,
        other: str | Self,
        *,
        kind: TJoinType = "inner",
        alias: Optional[str] = None,
    ) -> Self: ...

    @overload
    def join(
        self,
        other: str | Self,
        on: str | sge.Expression,
        *,
        kind: TJoinType = "inner",
        alias: Optional[str] = None,
    ) -> Self: ...

    def join(
        self,
        other: str | Self,
        on: str | sge.Expression | None = None,
        *,
        kind: TJoinType = "inner",
        alias: Optional[str] = None,
    ) -> Self:
        """Join this relation to another table.

        Without `on`, join conditions are discovered automatically from the
        schema's reference chain (parent/child/root relationships created by
        dlt during loading). With `on`, an explicit join predicate is used
        instead — this also enables cross-dataset joins.

        Args:
            other: Table name or Relation to join. For cross-dataset joins,
                pass a Relation from a different `dlt.Dataset`.
            on: Explicit join condition as an SQL string or sqlglot expression.
                Required for cross-dataset joins and joins between tables
                without dlt schema references. Column and table names in the
                predicate must use their dlt schema (normalized) names.
            kind: Type of SQL join: ``"inner"``, ``"left"``, ``"right"``,
                or ``"full"``.
            alias: Projection prefix for the joined table's columns. Columns
                from ``other`` appear as ``{alias}__{column}``. Defaults to
                the target table name.

        Returns:
            A new relation with the join applied and the target table's
            columns appended to the projection.

        Raises:
            ValueError: If the join cannot be resolved.

        Example:
            >>> # auto join (schema references)
            >>> dataset["orders"].join("users")

            >>> # explicit ON
            >>> dataset["orders"].join("users", on="orders._dlt_parent_id = users._dlt_id")

            >>> # cross-dataset join
            >>> local["orders"].join(
            ...     foreign["products"],
            ...     on="orders.product_id = products.id",
            ... )
        """
        if alias == "":
            raise ValueError("`alias` must be a non-empty string when provided.")

        if kind not in get_args(TJoinType):
            raise ValueErrorWithKnownValues(
                key="kind", value_received=kind, valid_values=list(get_args(TJoinType))
            )

        if isinstance(on, str) and not on.strip():
            raise ValueError("`on` must be a non-empty SQL expression.")

        target = self._resolve_join_target(other, on=on)
        target_is_foreign = target.dataset_name != self._dataset.dataset_name

        projection_prefix = alias or target.table_name

        if on is None:
            if not self._table_name:
                raise ValueError("This relation has no base table to resolve references.")
            if target_is_foreign:
                raise ValueError("`on` is required when joining relations from different datasets.")
            if target.table_name not in self._dataset.schema.tables:
                raise ValueError(f"Table `{target.table_name}` not found in dataset schema")
            query = _apply_join(
                self.sqlglot_expression,
                schema=self._dataset.schema,
                left_table=self._table_name,
                right_table=target.table_name,
                projection_prefix=projection_prefix,
                kind=kind,
                dataset_name=self._dataset.dataset_name,
            )
        else:
            query = _apply_explicit_join(
                self.sqlglot_expression,
                target,
                on=on,
                projection_prefix=projection_prefix,
                kind=kind,
                destination_dialect=self.destination_dialect,
                left_dataset_name=self._dataset.dataset_name,
            )

        _qualify_unscoped_tables_with_dataset(query, self._dataset.dataset_name)

        rel = self.__copy__()
        rel._sqlglot_expression = query

        # carry the RHS relation's foreign schemas
        if isinstance(other, dlt.Relation):
            for ds_name, schemas in other._foreign_schemas.items():
                if ds_name == self._dataset.dataset_name:
                    continue
                rel._foreign_schemas[ds_name] = list(schemas)
                if ds_name in other._foreign_physical_names:
                    rel._foreign_physical_names[ds_name] = other._foreign_physical_names[ds_name]
        if target_is_foreign:
            rel._foreign_schemas[target.dataset_name] = list(target.schemas)
            if target.physical_dataset_name is not None:
                rel._foreign_physical_names[target.dataset_name] = target.physical_dataset_name

        return rel

    def _resolve_join_target(
        self,
        other: Union[str, Self],
        *,
        on: Union[str, sge.Expression, None] = None,
    ) -> _JoinTarget:
        """Resolve the right-hand side of a join into a `_JoinTarget`."""
        if isinstance(other, dlt.Relation):
            this_dataset = self._dataset
            target_dataset = other._dataset
            if not (
                this_dataset.destination_client.config.can_read_from(
                    target_dataset.destination_client.config
                )
            ):
                raise ValueError(
                    "Cannot join relations from different physical destinations: dataset"
                    f" '{this_dataset.dataset_name}' on"
                    f" '{this_dataset.destination_client.config}' vs dataset"
                    f" '{target_dataset.dataset_name}' on"
                    f" '{target_dataset.destination_client.config}'"
                )

            # unreachable until `can_read_from` is relaxed for attached locations: same-named
            # datasets on two locations cannot be disambiguated by the dataset name alone
            if (
                this_dataset.dataset_name == target_dataset.dataset_name
                and this_dataset.destination_client.config.physical_location()
                != target_dataset.destination_client.config.physical_location()
            ):
                raise ValueError(
                    "Cannot join datasets with the same name located on two different destinations"
                )

            is_foreign = not self._dataset._is_same_dataset(target_dataset)
            if is_foreign and (
                isinstance(self.sql_client, WithSchemas)
                # TODO: drop the sqlite check once we ATTACH foreign datasets
                or getattr(self.sql_client, "dialect_name", None) == "sqlite"
            ):
                raise ValueError(
                    "Cross-dataset joins are not supported on the"
                    f" `{self._dataset._destination.destination_name}` destination."
                )

            target_table = other._table_name
            is_transformed = other._query is not None
            if target_table and not is_transformed:
                # pristine base-table Relation: look up columns from schema
                target_columns = _find_table_columns(target_dataset.schemas, target_table)
            elif target_table and is_transformed:
                # transformed Relation that still tracks its origin table
                # (e.g., .where(), .select()); use its actual output columns
                target_columns = other.columns_schema
            else:
                # no base table at all (e.g., from .query())
                if on is None:
                    raise ValueError(f"Relation `{other}` has no base table to resolve references.")
                target_table = _left_source_qualifier(other.sqlglot_expression) or "subquery"
                target_columns = other.columns_schema
            return _JoinTarget(
                dataset_name=target_dataset.dataset_name,
                table_name=target_table,
                columns=target_columns,
                schemas=target_dataset.schemas,
                subquery=other.sqlglot_expression if is_transformed else None,
                physical_dataset_name=(
                    target_dataset.sql_client.dataset_name if is_foreign else None
                ),
            )

        if isinstance(other, str):
            if "." in other:
                ds_name, tbl_name = other.split(".", 1)
            else:
                ds_name, tbl_name = self._dataset.dataset_name, other

            if ds_name == self._dataset.dataset_name:
                return _JoinTarget(
                    dataset_name=ds_name,
                    table_name=tbl_name,
                    columns=_find_table_columns(self._dataset.schemas, tbl_name),
                    schemas=self._dataset.schemas,
                )
            if ds_name in self._foreign_schemas:
                foreign_schemas = self._foreign_schemas[ds_name]
                return _JoinTarget(
                    dataset_name=ds_name,
                    table_name=tbl_name,
                    columns=_find_table_columns(foreign_schemas, tbl_name),
                    schemas=foreign_schemas,
                    physical_dataset_name=self._foreign_physical_names.get(ds_name),
                )
            raise ValueError(
                f"Dataset `{ds_name}` is not registered. Pass a Relation from the "
                "foreign dataset to automatically register its schema."
            )

        raise ValueError(
            f"`other` must be a table name or a `dlt.Relation`, got `{type(other).__name__}`."
        )

    def incremental(self, incremental: Incremental[Any]) -> Self:
        """Filter this relation to a cursor range using an Incremental.

        Translates the `Incremental` bounds (`initial_value`/`end_value`, `range_start`/
        `range_end`, `last_value_func`) into a SQL `WHERE` clause. When the cursor
        path is `table.column`, joins the referenced table via the dataset schema
        without adding its columns to the projection, then filters on the joined
        column. If the target is already joined, the existing JOIN is reused.

        Args:
            incremental (Incremental[Any]): The incremental whose cursor path and
                range define the filter. `last_value_func` must be `min` or `max`.

        Returns:
            Self: A new relation with the incremental filter applied.
        """
        if self._incremental_ctx is not None:
            raise ValueError(
                "`.incremental()` has already been applied to this relation with "
                f"cursor `{self._incremental_ctx.incremental.cursor_path}`."
            )

        table_name, column_name = _parse_incremental_cursor_path(incremental.cursor_path)
        naming = self._dataset.schema.naming
        column_name = naming.normalize_identifier(column_name)

        if table_name is None:
            relation_columns = self.columns_schema
            if column_name not in relation_columns:
                _raise_incomplete_cursor_column(incremental.cursor_path, "this relation")
            return self._apply_incremental(
                incremental=incremental,
                target_query=self.sqlglot_expression,
                column_ref=sge.Column(this=sge.to_identifier(column_name, quoted=True)),
                column_lookup_columns=relation_columns,
            )

        if not self._table_name:
            raise ValueError(
                f"Incremental cursor `{incremental.cursor_path}` references table "
                f"`{table_name}` but the relation has no base table to resolve joins. "
                "Call `.incremental()` on `dataset.table(...)`, not on a `.query(...)`."
            )
        table_name = naming.normalize_table_identifier(table_name)
        if table_name not in self._dataset.schema.tables:
            raise ValueError(
                f"Incremental cursor target table `{table_name}` not found in dataset schema."
            )
        target_columns = self._dataset.schema.get_table_columns(table_name)
        if column_name not in target_columns:
            _raise_incomplete_cursor_column(incremental.cursor_path, f"table `{table_name}`")
        if self._table_name not in _extract_joined_table_aliases(
            self.sqlglot_expression, self._dataset.dataset_name
        ):
            raise ValueError(
                f"Incremental cursor `{incremental.cursor_path}` requires base table "
                f"`{self._table_name}` to stay directly in the FROM clause to resolve the join "
                f"to `{table_name}`, but this relation embeds it in a subquery (e.g. a projection "
                "with aliases or aggregates, or a raw `dataset(query)` relation). Use a cursor on "
                f"a column of `{self._table_name}` instead, or apply `.incremental()` before the "
                "step that wrapped the base table."
            )

        query = _apply_join(
            self.sqlglot_expression,
            schema=self._dataset.schema,
            left_table=self._table_name,
            right_table=table_name,
            projection_prefix=table_name,
            kind="inner",
            project=False,
            dataset_name=self._dataset.dataset_name,
        )
        target_qualifier = _extract_joined_table_aliases(query, self._dataset.dataset_name)[
            table_name
        ]
        return self._apply_incremental(
            incremental=incremental,
            target_query=query,
            column_ref=sge.Column(
                this=sge.to_identifier(column_name, quoted=True),
                table=sge.to_identifier(target_qualifier, quoted=False),
            ),
            column_lookup_columns=target_columns,
        )

    def _apply_incremental(
        self,
        *,
        incremental: Incremental[Any],
        target_query: sge.Query,
        column_ref: sge.Column,
        column_lookup_columns: TTableSchemaColumns,
    ) -> Self:
        """Build the WHERE for `incremental`."""
        column_name = column_ref.name
        sqlglot_type = _sqlglot_type_for_column(column_lookup_columns, column_name)
        _maybe_warn_on_cursor_missing_raise(incremental, column_lookup_columns, column_name)
        condition = _build_incremental_condition(
            incremental,
            column_ref,
            sqlglot_type,
            destination_capabilities=self.sql_client.capabilities,
        )

        rel = self.__copy__()
        rel._sqlglot_expression = (
            target_query.where(condition) if condition is not None else target_query
        )
        rel._incremental_ctx = _RelationIncrementalContext(
            incremental=incremental,
            cursor_column=column_ref.copy(),
        )
        return rel

    @property
    def is_incremental(self) -> bool:
        """True if any clause on this relation was produced by `.incremental()`."""
        return self._incremental_ctx is not None

    def _incremental_aggregate_relation(self) -> Optional[Self]:
        """Return a relation computing `<last_value_func>(cursor)` over this relation
        or `None` if this relation is not incremental.
        """
        if self._incremental_ctx is None:
            return None
        agg_query = _build_incremental_aggregate(
            self.sqlglot_expression,
            self._incremental_ctx,
            destination_capabilities=self.sql_client.capabilities,
        )
        rel = self.__copy__()
        rel._sqlglot_expression = agg_query
        # derived relation — do not re-advance state from the aggregate itself.
        rel._incremental_ctx = None
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

        This only works on relations created via `.table()`.

        If the relation already includes `_dlt_load_id`, it is returned unchanged.
        Otherwise, the root table is joined to add the column to the current relation.

        Raises:
            ValueError: If called on a non-table relation, a root table without
                `_dlt_load_id`, or a relation whose root load ID column cannot be located.
        """
        if not self._table_name or self._query is not None:
            raise ValueError(
                "`with_load_id_col()` only works on relations created via .table()."
                " It can't be applied to arbitrary relation."
            )

        normalized_load_id = self._dataset.schema.naming.normalize_identifier(C_DLT_LOAD_ID)

        if normalized_load_id in self.columns:
            return self

        root_table_name = schema_utils.get_root_table(
            self._dataset.schema.tables, self._table_name
        )["name"]
        if root_table_name == self._table_name:
            raise ValueError(
                f"{root_table_name} is a root table, but load id column is not present."
            )

        join_alias = "_dlt_root"
        joined = self.join(root_table_name, alias=join_alias)
        joined_expression = joined.sqlglot_expression.copy()
        left_projection = joined_expression.selects[: len(self.sqlglot_expression.selects)]
        load_id_output_name = f"{join_alias}__{normalized_load_id}"
        load_id_expr = next(
            (expr for expr in joined_expression.selects if expr.output_name == load_id_output_name),
            None,
        )
        if load_id_expr is None:
            raise ValueError(f"Could not locate column {normalized_load_id}")

        joined_expression.set("expressions", [*left_projection, load_id_expr.this.copy()])

        rel = self.__copy__()
        rel._sqlglot_expression = joined_expression
        return rel

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
        if not self._table_name or self._query is not None:
            raise ValueError(
                "`from_loads()` only works on relations created via .table()."
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
            else filtered_rel_with_load_id.select(*initial_columns)
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
        rel._table_name = self._table_name
        rel._incremental_ctx = self._incremental_ctx
        rel._foreign_schemas = {k: list(v) for k, v in self._foreign_schemas.items()}
        rel._foreign_physical_names = dict(self._foreign_physical_names)
        return rel

    def _relation_sqlglot_schema(self) -> SQLGlotSchema:
        schema_map: dict[str, Sequence[dlt.Schema]] = {
            self._dataset.dataset_name: list(self._dataset.schemas),
            **self._foreign_schemas,
        }
        return lineage.create_sqlglot_schema(schema_map, dialect=self.destination_dialect)

    def _logical_to_physical_dataset_map(self) -> dict[str, str]:
        """Map each logical dataset qualifier used in the query to its physical name."""
        return {
            self._dataset.dataset_name: self.sql_client.dataset_name,
            **self._foreign_physical_names,
        }


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
        relation._relation_sqlglot_schema(),
        dialect=relation.destination_dialect,
        infer_sqlglot_schema=infer_sqlglot_schema,
        allow_anonymous_columns=allow_anonymous_columns,
        allow_partial=allow_partial,
    )
    return columns_schema, normalized_query


def _find_table_columns(schemas: Sequence[dlt.Schema], table_name: str) -> TTableSchemaColumns:
    """Find the columns schema for a table across a sequence of schemas."""
    for schema in schemas:
        if table_name in schema.tables:
            return schema.get_table_columns(table_name)
    raise ValueError(f"Table `{table_name}` not found in dataset schema")


def _optimize_query(qualified_query: sge.Query) -> sge.Query:
    """Flatten a qualified query for readable SQL output."""
    return merge_subqueries(qualified_query)
