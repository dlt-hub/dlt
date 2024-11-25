from typing import TYPE_CHECKING, Any, Union, Sequence

from functools import partial

from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.dataset.relation import BaseReadableDBAPIRelation
from dlt.common.schema.typing import TTableSchemaColumns


if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any

try:
    from dlt.common.libs.ibis import Expr
except MissingDependencyException:
    Expr = Any

# map dlt destination to sqlglot dialect
DIALECT_MAP = {
    "dlt.destinations.duckdb": "duckdb",  # works
    "dlt.destinations.motherduck": "duckdb",  # works
    "dlt.destinations.clickhouse": "clickhouse",  # works
    "dlt.destinations.databricks": "databricks",  # works
    "dlt.destinations.bigquery": "bigquery",  # works
    "dlt.destinations.postgres": "postgres",  # works
    "dlt.destinations.redshift": "redshift",  # works
    "dlt.destinations.snowflake": "snowflake",  # works
    "dlt.destinations.mssql": "tsql",  # works
    "dlt.destinations.synapse": "tsql",  # works
    "dlt.destinations.athena": "trino",  # works
    "dlt.destinations.filesystem": "duckdb",  # works
    "dlt.destinations.dremio": "presto",  # works
    # NOTE: can we discover the current dialect in sqlalchemy?
    "dlt.destinations.sqlalchemy": "mysql",  # may work
}

# NOTE: some dialects are not supported by ibis, but by sqlglot, these need to
# be transpiled with a intermediary step
TRANSPILE_VIA_MAP = {
    "tsql": "postgres",
    "databricks": "postgres",
    "clickhouse": "postgres",
    "redshift": "postgres",
    "presto": "postgres",
}


# TODO: provide ibis expression typing for the readable relation
class ReadableIbisRelation(BaseReadableDBAPIRelation):
    def __init__(
        self,
        *,
        readable_dataset: ReadableDBAPIDataset,
        expression: Expr = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        super().__init__(readable_dataset=readable_dataset)
        self._expression = expression

    @property
    def query(self) -> Any:
        """build the query"""

        from dlt.common.libs.ibis import ibis, sqlglot

        destination_type = self._dataset._destination.destination_type
        target_dialect = DIALECT_MAP[destination_type]

        # render sql directly if possible
        if target_dialect not in TRANSPILE_VIA_MAP:
            return ibis.to_sql(self._expression, dialect=target_dialect)

        # here we need to transpile first
        transpile_via = TRANSPILE_VIA_MAP[target_dialect]
        sql = ibis.to_sql(self._expression, dialect=transpile_via)
        sql = sqlglot.transpile(sql, read=transpile_via, write=target_dialect)[0]
        return sql

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        return self.compute_columns_schema()

    @columns_schema.setter
    def columns_schema(self, new_value: TTableSchemaColumns) -> None:
        raise NotImplementedError("columns schema in ReadableDBAPIRelation can only be computed")

    def compute_columns_schema(self) -> TTableSchemaColumns:
        """provide schema columns for the cursor, may be filtered by selected columns"""
        # TODO: provide column lineage tracing with sqlglot lineage
        return None

    def _proxy_expression_method(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Proxy method calls to the underlying ibis expression, allowing to wrap the resulting expression in a new relation"""

        # Get the method from the expression
        method = getattr(self._expression, method_name)

        # unwrap args and kwargs if they are relations
        args = tuple(
            arg._expression if isinstance(arg, ReadableIbisRelation) else arg for arg in args
        )
        kwargs = {
            k: v._expression if isinstance(v, ReadableIbisRelation) else v
            for k, v in kwargs.items()
        }

        # casefold string params, we assume these are column names
        args = tuple(
            self.sql_client.capabilities.casefold_identifier(arg) if isinstance(arg, str) else arg
            for arg in args
        )
        kwargs = {
            k: self.sql_client.capabilities.casefold_identifier(v) if isinstance(v, str) else v
            for k, v in kwargs.items()
        }

        # Call it with provided args
        result = method(*args, **kwargs)

        # If result is an ibis expression, wrap it in a new relation else return raw result
        if isinstance(result, Expr):
            return self.__class__(readable_dataset=self._dataset, expression=result)
        return result

    def __getattr__(self, name: str) -> Any:
        """Wrap all callable attributes of the expression"""

        attr = getattr(self._expression, name, None)

        # try casefolded name for ibis columns access
        if attr is None:
            name = self.sql_client.capabilities.casefold_identifier(name)
            attr = getattr(self._expression, name, None)

        if attr is None:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        if not callable(attr):
            return attr
        return partial(self._proxy_expression_method, name)

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> "ReadableIbisRelation":
        # casefold column-names
        columns = [self.sql_client.capabilities.casefold_identifier(col) for col in columns]
        expr = self._expression[columns]
        return self.__class__(readable_dataset=self._dataset, expression=expr)

    # forward ibis methods defined on interface
    def limit(self, limit: int) -> "ReadableIbisRelation":
        """limit the result to 'limit' items"""
        return self._proxy_expression_method("limit", limit)  # type: ignore

    def head(self, limit: int = 5) -> "ReadableIbisRelation":
        """limit the result to 5 items by default"""
        return self._proxy_expression_method("head", limit)  # type: ignore

    def select(self, *columns: str) -> "ReadableIbisRelation":
        """set which columns will be selected"""
        return self._proxy_expression_method("select", *columns)  # type: ignore
