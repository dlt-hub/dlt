from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Union

from functools import partial

from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema import Schema
from dlt.common.schema.utils import get_root_table, get_root_to_table_chain
from dlt.common.schema.typing import TTableSchemaColumns, TTableSchema
from dlt.destinations.dataset.relation import BaseReadableDBAPIRelation

if TYPE_CHECKING:
    from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any

try:
    from dlt.helpers.ibis import Table
except MissingDependencyException:
    Table = Any


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

        target_dialect = self._dataset._destination.capabilities().sqlglot_dialect

        # render sql directly if possible
        if target_dialect not in TRANSPILE_VIA_DEFAULT:
            if target_dialect == "tsql":
                # NOTE: Ibis uses the product name "mssql" as the dialect instead of the official "tsql".
                return ibis.to_sql(self._ibis_object, dialect="mssql")
            else:
                return ibis.to_sql(self._ibis_object, dialect=target_dialect)

        # here we need to transpile to ibis default and transpile back to target with sqlglot
        # NOTE: ibis defaults to the default pretty dialect, if a dialect is not passed
        sql = ibis.to_sql(self._ibis_object)
        sql = sqlglot.transpile(sql, write=target_dialect)[0]
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

    def _filter_using_load_table(
        self, key: str, values_to_include: list[Any]
    ) -> "ReadableIbisRelation":
        """Filter the load table and propagate from root table to current table."""
        from dlt.helpers.ibis import create_unbound_ibis_table

        load_table = create_unbound_ibis_table(
            self.sql_client, schema=self.schema, table_name=self.schema.loads_table_name
        )
        load_table = load_table.filter(load_table.__getattr__(key).isin(values_to_include))
        # TODO is _dlt_load_id a reserved name?
        return self._filter_using_root_table(
            key="_dlt_load_id", values_to_include=load_table.load_id
        )

    def _filter_using_root_table(
        self,
        key: str,
        values_to_include: list[Any],
    ) -> "ReadableIbisRelation":
        """Filter the root table and propagate to current table."""
        from dlt.helpers.ibis import create_unbound_ibis_table

        # Case 1: if current table is root table, you can filter directly and exit early
        dlt_root_table = get_root_table(self.schema.tables, self.table_name)
        if dlt_root_table["name"] == self.table_name:
            # doing ibis.expr.types.Table[COL_NAME] returns a column, but ReadableIbisRelation[COL_NAME] returns a table
            # we need to use `__getattr__` to simulate Table.COL_NAME via ReadableIbisRelation.COL_NAME
            return self.filter(self.__getattr__(key).isin(values_to_include))  # type: ignore[no-any-return]

        root_table = create_unbound_ibis_table(
            self.sql_client, schema=self.schema, table_name=dlt_root_table["name"]
        )
        root_table = root_table.filter(root_table.__getattr__(key).isin(values_to_include))

        # Case 2: There is a root_key (e.g., root_key=True on Source, write_disposition="merge")
        root_key = None
        for col_name, col in self.columns_schema.items():
            if col.get("root_key") is True:
                root_key = col_name

        if root_key is not None:
            root_row_key = next(
                col_name for col_name, col in dlt_root_table["columns"].items() if col.get("row_key") is True
            )
            return self.filter(
                self.__getattr__(root_key).isin(root_table.__getattr__(root_row_key))
            )

        # Case 3: there is no root key so we traverse the chain of parent-child from root to current table
        return self._filter_nested_table(root_table)

    def _filter_nested_table(self, filtered_root_table: Any) -> "ReadableIbisRelation":
        """Filter the current table based on a filtered root table.

        This takes the Ibis Table expression `filtered_root_table`, which has filtered rows,
        and propagate the selection of `_dlt_id` on the root table via `row_key -> parent_key`
        recursively.

        For tables `root -> child1 -> child2`, we need to:
        ```
        filtered_root_table = root.filter(root._dlt_load_id.isin(
            ["foo", "bar"]
        ))._dlt_id  # root row_key

        child2.filter(child2._dlt_parent_id.isin(  # child2 parent_key
            child1.filter(child1._dlt_parent_id.isin(  # child1 parent_key
                filtered_root_table
            ))._dlt_id  # child1 row_key
        ))
        ```

        Takes as input and returns a `ibis.expr.types.Table`
        """
        from dlt.helpers.ibis import create_unbound_ibis_table

        filtered_table = filtered_root_table
        parent_row_key = None
        for table in get_root_to_table_chain(self.schema.tables, self.table_name):
            # the root table is already filtered, only set the parent_row_key
            if parent_row_key is None:
                parent_row_key = next(
                    col_name
                    for col_name, col in table["columns"].items()
                    if col.get("row_key") is True
                )
                continue

            parent_key = None
            row_key = None
            for col_name, col in table["columns"].items():
                if col.get("parent_key") is True:
                    parent_key = col_name
                if col.get("row_key") is True:
                    row_key = col_name

            # should always match a column because `get_root_to_table_chain()` returns tables based on parent_key / row_key
            assert parent_key is not None
            assert row_key is not None

            ibis_table = create_unbound_ibis_table(
                self.sql_client, schema=self.schema, table_name=table["name"]
            )
            filter_clause = ibis_table[parent_key].isin(filtered_table[parent_row_key])
            # TODO the only operation to proxy if required
            filtered_table = ibis_table.filter(filter_clause)

            parent_row_key = row_key

        return self.__class__(
            readable_dataset=self._dataset,
            ibis_object=filtered_table,
            columns_schema=self.columns_schema,
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

    def filter_by_load_ids(self, load_ids: Union[str, list[str]]) -> "ReadableIbisRelation":
        """Filter on matching `load_ids`."""
        load_ids = [load_ids] if isinstance(load_ids, str) else load_ids
        return self._filter_using_load_table(key="load_id", values_to_include=load_ids)

    def filter_by_load_status(
        self, status: Union[int, list[int], None] = 0
    ) -> "ReadableIbisRelation":
        """Filter to rows with a specific load status."""
        if status is None:
            return self

        status = [status] if isinstance(status, int) else status
        return self._filter_using_load_table(key="status", values_to_include=status)

    def filter_by_latest_load_id(
        self, status: Union[int, list[int], None] = 0
    ) -> "ReadableIbisRelation":
        """Filter on the most recent `load_id` with a specific load status.

        If `status` is None, don't filter by status.
        """
        from dlt.helpers.ibis import create_unbound_ibis_table

        # filter load table
        load_table = create_unbound_ibis_table(
            self.sql_client, schema=self.schema, table_name=self.schema.loads_table_name
        )
        if status is not None:
            status_list = [status] if isinstance(status, int) else status
            load_table = load_table.filter(load_table.status.isin(status_list))

        latest_load_id = load_table.load_id.max()
        return self._filter_using_load_table(key="load_id", values_to_include=[latest_load_id])

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
