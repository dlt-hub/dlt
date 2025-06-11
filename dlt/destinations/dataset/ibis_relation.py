from typing import TYPE_CHECKING, Any, Union, Sequence
from functools import partial
import sqlglot

from dlt.destinations.dataset.relation import BaseReadableDBAPIRelation


if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
    from dlt.helpers.ibis import Table as IbisTable, Expr as IbisEpr
else:
    IbisTable = object
    IbisEpr = Any


# NOTE: some dialects are not supported by ibis, but by sqlglot, these need to
# be transpiled with an intermediary step
TRANSPILE_VIA_DEFAULT = [
    "redshift",
    "presto",
]


class ReadableIbisRelation(BaseReadableDBAPIRelation):
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        ibis_object: IbisEpr = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        super().__init__(readable_dataset=readable_dataset)
        self._ibis_object = ibis_object

    def _query(self) -> Any:
        from dlt.helpers.ibis import ibis

        ibis_expr = self._ibis_object
        caps = self._dataset.sql_client.capabilities
        target_dialect = caps.sqlglot_dialect

        # render sql directly if possible
        # NOTE: ibis is optimized for reading a real schema from db and pushing back optimized sql
        #  - it quotes all identifiers, there's no option to get unqoted query
        #  - it converts full lists of column names back into star
        #  - it optimizes the query inside, adds meaningless aliases to all tables etc.
        if target_dialect not in TRANSPILE_VIA_DEFAULT:
            if target_dialect == "tsql":
                # NOTE: Ibis uses the product name "mssql" as the dialect instead of the official "tsql".
                return ibis.to_sql(ibis_expr, dialect="mssql")
            else:
                return ibis.to_sql(ibis_expr, dialect=target_dialect)

        # here we need to transpile to ibis default and transpile back to target with sqlglot
        # NOTE: ibis defaults to the duckdb dialect, if a dialect is not passed
        sql = ibis.to_sql(ibis_expr)
        sql = sqlglot.transpile(sql, write=target_dialect)[0]
        return sql

    def _proxy_expression_method(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Proxy method calls to the underlying ibis expression, allowing to wrap the resulting expression in a new relation"""

        # Get the method from the expression
        method = getattr(self._ibis_object, method_name)

        # unwrap args and kwargs if they are relations
        args = tuple(
            arg._ibis_object if isinstance(arg, ReadableIbisRelation) else arg for arg in args
        )
        kwargs = {
            k: v._ibis_object if isinstance(v, ReadableIbisRelation) else v
            for k, v in kwargs.items()
        }

        # Call it with provided args
        result = method(*args, **kwargs)

        # If result is an ibis expression, wrap it in a new relation else return raw result
        return self.__class__(readable_dataset=self._dataset, ibis_object=result)

    def __getattr__(self, name: str) -> Any:
        """Wrap all callable attributes of the expression"""

        attr = getattr(self._ibis_object, name, None)

        if attr is None:
            raise AttributeError(
                f"`{self._ibis_object.__class__.__name__}` object has no attribute `{name}`"
            )

        if not callable(attr):
            # NOTE: This case usually is a column
            return self.__class__(readable_dataset=self._dataset, ibis_object=attr)

        return partial(self._proxy_expression_method, name)

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> "ReadableIbisRelation":
        if not isinstance(columns, (str, Sequence)):
            raise TypeError(
                "Expected a sequence of column names `Sequence[str]` or a single column `str`. "
                f"Received invalid argument of type: `{type(columns).__name__}`"
            )

        expr = self._ibis_object[columns]
        return self.__class__(
            readable_dataset=self._dataset,
            ibis_object=expr,
        )

    # forward ibis methods defined on interface
    def limit(self, limit: int, **kwargs: Any) -> "ReadableIbisRelation":
        """limit the result to 'limit' items"""
        return self._proxy_expression_method("limit", limit, **kwargs)  # type: ignore

    def head(self, limit: int = 5) -> "ReadableIbisRelation":
        """limit the result to 5 items by default"""
        return self._proxy_expression_method("head", limit)  # type: ignore

    def select(self, *columns: str) -> "ReadableIbisRelation":
        """set which columns will be selected"""
        return self._proxy_expression_method("select", *columns)  # type: ignore

    # forward ibis comparison and math operators
    def __lt__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__lt__", other)  # type: ignore

    def __gt__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__gt__", other)  # type: ignore

    def __ge__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__ge__", other)  # type: ignore

    def __le__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__le__", other)  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return self._proxy_expression_method("__eq__", other)  # type: ignore

    def __ne__(self, other: Any) -> bool:
        return self._proxy_expression_method("__ne__", other)  # type: ignore

    def __and__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__and__", other)  # type: ignore

    def __or__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__or__", other)  # type: ignore

    def __mul__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__mul__", other)  # type: ignore

    def __div__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__div__", other)  # type: ignore

    def __add__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__add__", other)  # type: ignore

    def __sub__(self, other: Any) -> "ReadableIbisRelation":
        return self._proxy_expression_method("__sub__", other)  # type: ignore

    def __repr__(self) -> str:
        return self._ibis_object.__repr__()  # type: ignore[no-any-return]

    def __str__(self) -> str:
        return self._ibis_object.__str__()  # type: ignore[no-any-return]
