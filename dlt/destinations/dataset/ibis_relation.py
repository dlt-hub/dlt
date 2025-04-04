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


# NOTE: some dialects are not supported by ibis, but by sqlglot, these need to
# be transpiled with an intermediary step
TRANSPILE_VIA_DEFAULT = [
    "redshift",
    "presto",
]

EXECUTION_METHODS = (
    "execute",
    "to_csv",
    "to_delta",
    "to_json",
    "to_pandas",
    "to_pandas_batches",
    "to_parquet",
    "to_parquet_dir",
    "to_polars",
    "to_pyarrow",
    "to_pyarrow_batches",
    "to_torch",
    "to_xlsx",
)


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

    def to_ibis(self) -> Expr:
        return self._ibis_object

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

    def _proxy_execution_method(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        assert method_name in EXECUTION_METHODS
        # NOTE should the connection be explicitly closed?
        con = self._dataset.ibis()

        # we can't use `con.__getattr__` to proxy methods because this is explicitly prevented by Ibis
        if method_name == "execute":
            result = con.execute(self._ibis_object, *args, **kwargs)

        elif method_name == "to_csv":
            result = con.to_csv(self._ibis_object, *args, **kwargs)

        elif method_name == "to_delta":
            # reimplements the Ibis code: https://github.com/ibis-project/ibis/blob/8e813b0a73c6898273ffc688dc1eebfe56029a6d/ibis/backends/__init__.py#L554
            # because it uses `.to_pyarrow_batches` under the hood, which we can't proxy
            try:
                from deltalake.writer import write_deltalake
            except MissingDependencyException:
                raise MissingDependencyException(
                    "ReadableIbisRelation",
                    ["deltalake"],
                    "Install `dlt[deltalake]` to use `.to_delta()`",
                )

            # `path` is required, but it can be can be positional or keyword
            path = args[0] if args else None
            if path is None:
                path = kwargs.pop("path")
            params = kwargs.pop("params", None)

            with con.to_pyarrow_batches(self._ibis_object, params=params) as batch_reader:
                result = write_deltalake(path, batch_reader, **kwargs)

        elif method_name == "to_json":
            result = con.to_json(self._ibis_object, *args, **kwargs)

        elif method_name == "to_pandas":
            result = con.to_pandas(self._ibis_object, *args, **kwargs)

        elif method_name == "to_pandas_batches":
            result = con.to_pandas_batches(self._ibis_object, *args, **kwargs)

        elif method_name == "to_parquet":
            result = con.to_parquet(self._ibis_object, *args, **kwargs)

        elif method_name == "to_parquet_dir":
            # reimplements the Ibis code https://github.com/ibis-project/ibis/blob/8e813b0a73c6898273ffc688dc1eebfe56029a6d/ibis/backends/__init__.py#L508
            # because it uses `.to_pyarrow_batches` under the hood, which we can't proxy
            try:
                import pyarrow.dataset as ds
            except MissingDependencyException:
                raise MissingDependencyException(
                    "ReadableIbisRelation",
                    ["pyarrow"],
                    "Install `pyarrow` to use `.to_parquet_dir()`",
                )

            # `directory` is required, but it can be can be positional or keyword
            directory = args[0] if args else None
            if directory is None:
                directory = kwargs.pop("directory")
            params = kwargs.pop("params", None)

            with con.to_pyarrow_batches(self._ibis_object, params=params) as batch_reader:
                result = ds.write_dataset(
                    batch_reader, base_dir=directory, format="parquet", **kwargs
                )

        elif method_name == "to_polars":
            result = con.to_polars(self._ibis_object, *args, **kwargs)

        elif method_name == "to_pyarrow":
            result = con.to_pyarrow(self._ibis_object, *args, **kwargs)

        elif method_name == "to_pyarrow_batches":
            result = con.to_pyarrow_batches(self._ibis_object, *args, **kwargs)

        elif method_name == "to_torch":
            result = con.to_torch(self._ibis_object, *args, **kwargs)

        elif method_name == "to_xlsx":
            result = con.to_xlsx(self._ibis_object, *args, **kwargs)

        else:
            raise ValueError(
                f"Method `{method_name}` is unsupported. If you think this is an error, please open"
                " a GitHub issue."
            )

        con.disconnect()

        return result

    def _proxy_expression_method(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Proxy method calls to the underlying ibis expression, allowing to wrap the resulting expression in a new relation"""

        if method_name in EXECUTION_METHODS:
            return self._proxy_execution_method(method_name, *args, **kwargs)

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
