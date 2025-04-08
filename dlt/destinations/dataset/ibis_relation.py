from __future__ import annotations
import contextlib
from functools import partial
from typing import TYPE_CHECKING, Any, Union

# ibis imports follow the convention used in the library
import ibis
import ibis.backends.sql.compilers as sc
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import sqlglot as sg
from ibis.backends import _get_backend_names, NoUrl, NoExampleLoader
from ibis.backends.sql import SQLBackend
from ibis.formats import TypeMapper

import dlt
import dlt.helpers.ibis
from dlt.common.schema import Schema as DltSchema
from dlt.common.destination import TDestinationReferenceArg, Destination
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchemaColumns, TDataType
from dlt.destinations.dataset import ReadableDBAPIDataset
from dlt.destinations.dataset.relation import BaseReadableDBAPIRelation
from dlt.destinations.dataset.factory import dataset as dataset_factory

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pandas as pd
    import pyarrow as pa
    import pyarrow_hotfix  # noqa: F401

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

# TODO move this as a destination capability; the destination should declare if it's compatible
SUPPORTED_DESTINATIONS = (
    "postgres",
    "snowflake",
    "filesystem",  # uses duckdb
    "mssql",
    "bigquery",
    "redshift",  # uses postgres
    "motherduck",  # uses duckdb
    ""
)

ALL_IBIS_BACKENDS = set(_get_backend_names())


# TODO complete implementation and register to singledispatch `sch.infer.register`
class DltType(TypeMapper):
    @classmethod
    def from_ibis(cls):
        raise NotImplementedError


    @classmethod
    def to_ibis(cls, typ: TDataType, nullable: bool | None = None) -> dt.DataType:
        nullable = True if nullable is None else bool(nullable)
        return ibis.dtype(dlt.helpers.ibis.DATA_TYPE_MAP[typ], nullable=nullable)
    

def _get_sqlalchemy_compiler(destination: Destination):
    """
    reference: https://docs.sqlalchemy.org/en/20/dialects/
    """
    # Dialects included in SQLAlchemy
    # SQLite via SQLAlchemy is fully-tested with dlt
    if ... == "sqlite":
        compiler = sc.SQLiteCompiler()
    # MySQL via SQLAlchemy is fully-tested with dlt
    elif ... == "mysql":
        compiler = sc.MySQLCompiler()

    elif ... == "oracle":
        compiler = sc.OracleCompiler()

    elif ... == "mariadb":
        raise NotImplementedError

    elif ... == "postgres":
        compiler = sc.PostgresCompiler()

    elif ... == "mssql":
        compiler = sc.MSSQLCompiler()

    # SQLAlchemy extension dialects
    elif ... == "druid":
        compiler = sc.DruidCompiler()

    elif ... == "hive":
        compiler = sc.TrinoCompiler()
    
    elif ... == "clickhouse":
        compiler = sc.ClickHouseCompiler()

    elif ... == "databricks":
        compiler = sc.DatabricksCompiler()

    elif ... == "bigquery":
        compiler = sc.BigQueryCompiler()

    elif ... == "impala":
        compiler = sc.ImpalaCompiler()

    elif ... == "snowflake":
        compiler = sc.SnowflakeCompiler()

    else:
        raise NotImplementedError
    
    return compiler



def get_ibis_to_sqlglot_compiler(destination: TDestinationReferenceArg): 
    # ensure destination is a Destination instance
    if not isinstance(destination, Destination):
        destination = Destination.from_reference(destination)
    assert isinstance(destination, Destination)

    if destination.destination_name == "duckdb":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "filesystem":
        compiler = sc.DuckDBCompiler()

    elif destination.destination_name == "motherduck":
        compiler = sc.DuckDBCompiler()
    
    elif destination.destination_name == "postgres":
        compiler = sc.PostgresCompiler()
    
    elif destination.destination_name == "clickhouse":
        compiler = sc.ClickHouseCompiler()
    
    elif destination.destination_name == "snowflake":
        compiler = sc.SnowflakeCompiler()

    elif destination.destination_name == "databricks":
        compiler = sc.DatabricksCompiler()

    elif destination.destination_name == "mssql":
        compiler = sc.MSSQLCompiler()

    # NOTE synapse might differ from MSSQL
    elif destination.destination_name == "synapse":
        compiler = sc.MSSQLCompiler()

    elif destination.destination_name == "bigquery":
        compiler = sc.BigQueryCompiler()

    elif destination.destination_name == "athena":
        compiler = sc.AthenaCompiler()

    # NOTE Dremio uses a presto/trino-based language
    elif destination.destination_name == "dremio":
        compiler = sc.TrinoCompiler()

    # TODO parse the SQLAlchemy dialect
    elif destination.destination_name == "sqlalchemy":
        compiler = _get_sqlalchemy_compiler(destination)

    else:
        raise NotImplementedError(
            f"Destination of type {Destination.from_reference(destination).destination_type} not"
            " supported by ibis."
        )
    
    return compiler


# TODO support `database` kwarg (equiv. `dataset_name`) to enable DltBackend to access multiple database
# NOTE this should support all SQLGlot dialects https://sqlglot.com/sqlglot/dialects.html#Dialect

class DltBackend(SQLBackend, NoUrl, NoExampleLoader):
    name = "dlt"
    supports_temporary_tables = False
    supports_python_udfs = False

    @property
    def version(self) -> str:
        import importlib.metadata
        return importlib.metadata.version("dlt")
    
    # NOTE can change signature
    def do_connect(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[DltSchema, str, None] = None,
        **config: Any,
    ) -> None:
        self._dataset = dataset_factory(
            destination=destination,
            dataset_name=dataset_name,
            schema=schema,
        )
        self.compiler = get_ibis_to_sqlglot_compiler(self._dataset._destination)

    @classmethod
    def from_dataset(cls, dataset: ReadableDBAPIDataset) -> DltBackend:
        new_backend = cls()
        new_backend._dataset = dataset
        new_backend.compiler = get_ibis_to_sqlglot_compiler(new_backend._dataset._destination)
        return new_backend

    @classmethod
    def from_pipeline(cls, pipeline: dlt.Pipeline) -> DltBackend:
        new_backend = cls()
        new_backend._dataset = pipeline.dataset()
        new_backend.compiler = get_ibis_to_sqlglot_compiler(new_backend._dataset._destination)
        return new_backend

    @contextlib.contextmanager
    def _safe_raw_sql(self, *args, **kwargs):
        yield self.raw_sql(*args, **kwargs)

    def raw_sql(self, query: str | sg.Expression, **kwargs: Any) -> Any:
        """Execute SQL string or SQLGlot expression using the dlt destination SQL client"""
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.name)

        with self._dataset.sql_client as client:
            result = client.execute_sql(query)

        return result
    
    def _register_udfs(self, *args, **kwargs) -> None:
        """Override SQLBackend method to avoid round-trip to Ibis SQL compiler"""
    
    def list_tables(self, *args, **kwargs) -> list[str]:
        """Return the list of table names"""
        return list(self._dataset.schema.tables.keys())
    
    def get_schema(self, table_name: str, *args, **kwargs) -> sch.Schema:
        """Get the Ibis table schema"""
        return sch.Schema(  
            {
                name: DltType.to_ibis(column["data_type"], column.get("nullable"))  # type: ignore
                for name, column in self._dataset.schema.get_table(table_name)["columns"].items()  # type: ignore
            }  # type: ignore
        )
    
    def table(self, name: str) -> ir.Table:
        """Construct a table expression"""
        # TODO maybe there's a more straighforward way to retrieve catalog and db
        sql_client = self._dataset.sql_client
        catalog = sql_client.catalog_name()
        database =  sql_client.capabilities.casefold_identifier(sql_client.dataset_name)

        table_schema = self.get_schema(name)
        return ops.DatabaseTable(
            name,
            schema=table_schema,
            source=self,
            namespace=ops.Namespace(catalog=catalog, database=database)
        ).to_expr()

    # TODO use the new dlt `model` format with INSERT statement
    # for non-SQL (e.g., filesystem) use JobClient 
    def create_table(
        self,
        name: str,
        /,
        obj: pd.DataFrame | pa.Table | ir.Table | None = None,
        *,
        schema: sch.Schema | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> ir.Table:
        raise NotImplementedError
    
    def _get_schema_using_query(self, *args, **kwargs):
        """Required to subclass SQLBackend"""
        raise NotImplementedError

    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        """Required to subclass SQLBackend"""
        raise NotImplementedError


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
