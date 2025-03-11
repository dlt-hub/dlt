from collections.abc import Sequence
from functools import partial
from typing import TYPE_CHECKING, Any, Union

from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.utils import (
    get_root_table,
    get_root_to_table_chain,
    get_row_key,
    get_parent_key,
    get_root_key,
    is_root_table,
)
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.dataset.relation import BaseReadableDBAPIRelation


if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any

try:
    from dlt.helpers.ibis import Table
except MissingDependencyException:
    Table = Any

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
        table_name: str = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""
        super().__init__(readable_dataset=readable_dataset)
        self._ibis_object = ibis_object
        self._columns_schema = columns_schema
        self._table_name = table_name

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

    @property
    def table_name(self) -> str:
        return self._table_name

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
            name = self._dataset._sql_client.capabilities.casefold_identifier(name)
            attr = getattr(self._ibis_object, name, None)

        if attr is None:
            raise AttributeError(
                f"'{self._ibis_object.__class__.__name__}' object has no attribute '{name}'"
            )

        if not callable(attr):
            # NOTE: we don't need to forward columns schema for non-callable attributes, these are usually columns
            return self.__class__(readable_dataset=self._dataset, ibis_object=attr)

        return partial(self._proxy_expression_method, name)

    def __getitem__(self, *columns: str) -> "ReadableIbisRelation":
        """Proxy method to select columns on an Ibis expression.

        This supports 3 notations:
        ```
        self["foo"]  # Column type
        self["foo", "bar"]  # Table type
        self[["foo", "bar"]]  # Table type
        ```
        Ibis reference: https://ibis-project.org/tutorials/ibis-for-pandas-users#selecting-columns
        """
        # casefold column-names
        # self["foo"]
        if len(columns) == 1 and isinstance(columns[0], str):
            col = self.sql_client.capabilities.casefold_identifier(columns[0])
            cols = [col]
            expr = self._ibis_object[col]
        # self[["foo", "bar"]]
        elif len(columns) == 1 and isinstance(columns[0], Sequence):
            cols = [self.sql_client.capabilities.casefold_identifier(col) for col in columns[0]]
            expr = self._ibis_object[cols]
        # self["foo", "bar"]
        elif all(isinstance(col, str) for col in columns):
            cols = [self.sql_client.capabilities.casefold_identifier(col) for col in columns]
            expr = self._ibis_object[cols]
        else:
            raise ValueError(
                "ReadableIbisRelation can be accessed using `rel['foo']` to retrieve a column, or `rel['foo', 'bar']` and `rel[['foo', 'bar']]` to access a table"
            )

        return self.__class__(
            readable_dataset=self._dataset,
            ibis_object=expr,
            columns_schema=self._get_filtered_columns_schema(cols),
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
        
    def _join_to_root_table(self) -> "ReadableIbisRelation":
        """Join the current table to the root table. If the current table is root, it's a no-op."""
        LOAD_ID_COL = "_dlt_load_id"

        table_schema = self.schema.tables[self.table_name]
        if is_root_table(table_schema):
            return self

        root_key = get_root_key(table_schema)
        # TODO setup another case that traverse parent-row keys to join nested tables without root_key
        if root_key is None:
            raise KeyError(
                "ReadableIbisRelation requires a `root_key` hint to join non-root tables. "
                "Set `root_key=True` on the source or use `write_disposition='merge'`."
            )
        root_table_schema = get_root_table(self.schema.tables, self.table_name)
        root_row_key = get_row_key(root_table_schema)
        root_table = self._dataset.table(root_table_schema["name"])

        joined_table = self.inner_join(
            root_table.select(LOAD_ID_COL, root_row_key),
            self[root_key] == root_table[root_row_key],
        )
        # `self` selects all columns from the original table
        return joined_table.select(self, LOAD_ID_COL)
    
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

    # TODO ensure same defaults in ReadableDBAPIDataset and ReadableIbisRelation; and docstrings
    def list_load_ids(
        self, status: Union[int, list[int], None] = 0, limit: int | None = None
    ) -> "ReadableIbisRelation":
        load_table = self._dataset.table(self.schema.loads_table_name)
        if status is not None:
            status = [status] if isinstance(status, int) else status
            load_table = load_table.filter(load_table["status"].isin(status))

        load_table = load_table.order_by(load_table["load_id"].desc())
        # limit needs to be applied after sorting
        if limit is not None:
            load_table = load_table.limit(limit)

        return load_table.load_id

    def latest_load_id(self, status: Union[int, list[int], None] = 0) -> "ReadableIbisRelation":
        """Latest `load_id` with matching load status (0 is success). If `status` is None, match any status."""
        load_table = self._dataset.table(self.schema.loads_table_name)
        if status is not None:
            status = [status] if isinstance(status, int) else status
            load_table = load_table.filter(load_table["status"].isin(status))

        return load_table.load_id.max()

    def filter_by_load_ids(self, load_ids: Union[str, list[str]]) -> "ReadableIbisRelation":
        """Filter on matching `load_ids`."""
        load_ids = [load_ids] if isinstance(load_ids, str) else load_ids
        table = self._join_to_root_table()
        return table.filter(table["_dlt_load_id"].isin(load_ids))

    def filter_by_load_status(
        self, status: Union[int, list[int], None] = 0
    ) -> "ReadableIbisRelation":
        """Filter to rows with a specific load status."""
        if status is None:
            return self

        load_ids = self.list_load_ids(status=status)
        table = self._join_to_root_table()
        return table.filter(table["_dlt_load_id"].isin(load_ids))

    def filter_by_latest_load_id(
        self, status: Union[int, list[int], None] = 0
    ) -> "ReadableIbisRelation":
        """Filter on the most recent `load_id` with a specific load status.

        If `status` is None, don't filter by status.
        """
        table = self._join_to_root_table()
        return table.filter(table["_dlt_load_id"] == self.latest_load_id(status=status))

    def filter_by_load_id_gt(
        self, load_id: str, status: Union[int, list[int], None] = 0
    ) -> "ReadableIbisRelation":
        load_table = self._dataset.table(self.schema.loads_table_name)

        conditions = [load_table["load_id"] > load_id]
        if status is not None:
            status = [status] if isinstance(status, int) else status
            conditions.append(load_table["status"].isin(status))
        load_table = load_table.filter(*conditions)

        table = self._join_to_root_table()   
        joined_table = table.inner_join(
            load_table,
            table["_dlt_load_id"] == load_table["load_id"],
        )
        return joined_table.select(table)

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
