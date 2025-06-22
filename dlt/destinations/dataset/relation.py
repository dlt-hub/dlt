from typing import (
    Any,
    cast,
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
from collections.abc import Iterable

from enum import Enum, auto

from sqlglot import parse_one
import sqlglot.expressions as sge

from dlt.common.destination.dataset import (
    SupportsReadableRelation,
)

from dlt.common.libs.sqlglot import to_sqlglot_type
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import Self
from dlt.common.exceptions import TypeErrorWithKnownTypes
from dlt.transformations import lineage
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.queries import normalize_query, build_select_expr

if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any


class FilterOp(Enum):
    EQ = auto()
    NE = auto()
    GT = auto()
    LT = auto()
    GTE = auto()
    LTE = auto()
    IN = auto()
    NOT_IN = auto()


_FILTER_OP_MAP = {
    FilterOp.EQ: sge.EQ,
    FilterOp.NE: sge.NEQ,
    FilterOp.GT: sge.GT,
    FilterOp.LT: sge.LT,
    FilterOp.GTE: sge.GTE,
    FilterOp.LTE: sge.LTE,
    FilterOp.IN: sge.In,
    FilterOp.NOT_IN: sge.Not,
}


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
            allow_partial (bool): If False, will raise in case of parsing errors, missing table reference, unresolved `SELECT *`, etc.
        """
        # NOTE: if we do not have a schema, we cannot compute the columns schema
        if self._dataset.schema is None:
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
        sqlglot_expression: sge.Select = None,
    ) -> None:
        """Create a lazy evaluated relation for the dataset of a destination"""

        # NOTE: we can keep an assertion here, this class will not be created by the user
        assert (
            sum(x is not None for x in (provided_query, table_name, sqlglot_expression)) == 1
        ), "Please provide either an sql query, a table_name or a an explicit sqlglot expression"
        super().__init__(readable_dataset=readable_dataset, normalize_query=normalize_query)

        self._provided_query = provided_query
        self._selected_columns = selected_columns

        self._sqlglot_expression: sge.Select = None
        if sqlglot_expression:
            self._sqlglot_expression = sqlglot_expression
        elif provided_query:
            self._sqlglot_expression = cast(
                sge.Select,
                parse_one(
                    provided_query,
                    read=self.sql_client.capabilities.sqlglot_dialect,
                ),
            )
        else:
            self._sqlglot_expression = build_select_expr(
                table_name=table_name,
                selected_columns=(
                    list(selected_columns)
                    if selected_columns
                    else list(self._dataset.schema.get_table_columns(table_name).keys())
                ),
            )
            if limit:
                self._sqlglot_expression = self._sqlglot_expression.limit(limit)

    def _query(self) -> Any:
        return self._sqlglot_expression.sql(
            dialect=self._dataset.sql_client.capabilities.sqlglot_dialect
        )

    def __copy__(self) -> Self:
        return self.__class__(
            readable_dataset=self._dataset,
            selected_columns=self._selected_columns,
            sqlglot_expression=self._sqlglot_expression,
        )

    def limit(self, limit: int, **kwargs: Any) -> Self:
        rel = self.__copy__()
        rel._sqlglot_expression = rel._sqlglot_expression.limit(limit)
        return rel

    def select(self, *columns: str) -> Self:
        rel = self.__copy__()
        rel._selected_columns = columns
        new_proj = [sge.Column(this=sge.to_identifier(col, quoted=True)) for col in columns]
        rel._sqlglot_expression.set("expressions", new_proj)
        rel.compute_columns_schema()
        return rel

    def order_by(self, column_name: str, direction: Literal["asc", "desc"] = "asc") -> Self:
        order_expr = sge.Ordered(
            this=sge.Column(
                this=sge.to_identifier(column_name, quoted=True),
            ),
            desc=(direction == "desc"),
        )
        rel = self.__copy__()
        rel._sqlglot_expression = rel._sqlglot_expression.order_by(order_expr)
        return rel

    def where(
        self,
        column_name: str,
        operator: Union[FilterOp, str],
        value: Any,
    ) -> Self:
        if isinstance(operator, str):
            try:
                operator = FilterOp[operator.upper()]
            except KeyError:
                raise ValueError(
                    f"Invalid operator '{operator}'. Expected one of:"
                    f" {[op.name.lower() for op in FilterOp]}"
                )

        def _build_typed_literal(v: Any, sqlglot_type: sge.DataType) -> sge.Expression:
            """
            Create a literal and CAST it to the requested sqlglot DataType.
            """
            lit: sge.Expression
            if v is None:
                lit = sge.Null()
            elif isinstance(v, str):
                lit = sge.Literal.string(v)
            elif isinstance(v, (int, float)):
                lit = sge.Literal.number(v)
            elif isinstance(v, (bytes, bytearray)):
                lit = sge.Literal.string(v.hex())
            else:
                lit = sge.Literal.string(str(v))
            return sge.Cast(this=lit, to=sqlglot_type.copy()) if sqlglot_type is not None else lit

        sqlgot_type = to_sqlglot_type(
            dlt_type=self.columns_schema[column_name].get("data_type"),
            precision=self.columns_schema[column_name].get("precision"),
            timezone=self.columns_schema[column_name].get("timezone"),
            nullable=self.columns_schema[column_name].get("nullable"),
        )

        value_expr: Union[sge.Expression, sge.Tuple]
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            value_expr = sge.Tuple(
                expressions=[_build_typed_literal(v, sqlgot_type) for v in value]
            )
        else:
            value_expr = _build_typed_literal(value, sqlgot_type)

        column = sge.Column(this=sge.to_identifier(column_name, quoted=True))

        condition: sge.Expression = None
        if operator == FilterOp.IN:
            exprs = value_expr.expressions if isinstance(value_expr, sge.Tuple) else [value_expr]
            condition = sge.In(this=column, expressions=exprs)
        elif operator == FilterOp.NOT_IN:
            exprs = value_expr.expressions if isinstance(value_expr, sge.Tuple) else [value_expr]
            condition = sge.Not(this=sge.In(this=column, expressions=exprs))
        else:
            condition_cls = _FILTER_OP_MAP[operator]
            condition = condition_cls(this=column, expression=value_expr)

        rel = self.__copy__()
        rel._sqlglot_expression = rel._sqlglot_expression.where(condition)
        return rel

    def filter(  # noqa: A003
        self,
        column_name: str,
        operator: Union[FilterOp, str],
        value: Any,
    ) -> Self:
        return self.where(column_name=column_name, operator=operator, value=value)

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
