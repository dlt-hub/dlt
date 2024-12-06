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
    from dlt.helpers.ibis import Expr
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


class ReadableIbisRelation(BaseReadableDBAPIRelation):
    def __init__(
        self,
        *,
        readable_dataset: ReadableDBAPIDataset,
        ibis_object: Any = None,
        columns_schema: TTableSchemaColumns = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        super().__init__(readable_dataset=readable_dataset)
        self._ibis_object = ibis_object
        self._columns_schema = columns_schema

    @property
    def query(self) -> Any:
        """build the query"""

        from dlt.helpers.ibis import ibis, sqlglot

        destination_type = self._dataset._destination.destination_type
        target_dialect = DIALECT_MAP[destination_type]

        # render sql directly if possible
        if target_dialect not in TRANSPILE_VIA_MAP:
            return ibis.to_sql(self._ibis_object, dialect=target_dialect)

        # here we need to transpile first
        transpile_via = TRANSPILE_VIA_MAP[target_dialect]
        sql = ibis.to_sql(self._ibis_object, dialect=transpile_via)
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
        return self._columns_schema

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

        # calculate columns schema for the result, some operations we know will not change the schema
        # and select will just reduce the amount of column
        columns_schema = None
        if method_name == "select":
            columns_schema = self._get_filtered_columns_schema(args)
        elif method_name in ["filter", "limit", "order_by", "head"]:
            columns_schema = self._columns_schema

        # If result is an ibis expression, wrap it in a new relation else return raw result
        return self.__class__(
            readable_dataset=self._dataset, ibis_object=result, columns_schema=columns_schema
        )

    def __getattr__(self, name: str) -> Any:
        """Wrap all callable attributes of the expression"""

        attr = getattr(self._ibis_object, name, None)

        # try casefolded name for ibis columns access
        if attr is None:
            name = self.sql_client.capabilities.casefold_identifier(name)
            attr = getattr(self._ibis_object, name, None)

        if attr is None:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        if not callable(attr):
            # NOTE: we don't need to forward columns schema for non-callable attributes, these are usually columns
            return self.__class__(readable_dataset=self._dataset, ibis_object=attr)

        return partial(self._proxy_expression_method, name)

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> "ReadableIbisRelation":
        # casefold column-names
        columns = [columns] if isinstance(columns, str) else columns
        columns = [self.sql_client.capabilities.casefold_identifier(col) for col in columns]
        expr = self._ibis_object[columns]
        return self.__class__(
            readable_dataset=self._dataset,
            ibis_object=expr,
            columns_schema=self._get_filtered_columns_schema(columns),
        )

    def _get_filtered_columns_schema(self, columns: Sequence[str]) -> TTableSchemaColumns:
        if not self._columns_schema:
            return None
        try:
            return {col: self._columns_schema[col] for col in columns}
        except KeyError:
            # NOTE: select statements can contain new columns not present in the original schema
            # here we just break the column schema inheritance chain
            return None

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
