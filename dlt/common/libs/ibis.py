from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Union

# ibis imports follow the convention used in the ibis source code
import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import sqlglot as sg
from ibis.backends import _get_backend_names, NoUrl, NoExampleLoader
from ibis.backends.sql import SQLBackend
from ibis.formats import TypeMapper

import dlt
from dlt.helpers.ibis import _get_ibis_to_sqlglot_compiler
from dlt.common.schema import Schema as DltSchema
from dlt.common.destination import TDestinationReferenceArg
from dlt.common.schema.typing import TDataType
from dlt.destinations.dataset.factory import dataset as dataset_factory

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


# TODO move this as a destination capability; the destination should declare if it's compatible
SUPPORTED_DESTINATIONS = (
    "postgres",
    "snowflake",
    "filesystem",  # uses duckdb
    "mssql",
    "bigquery",
    "redshift",  # uses postgres
    "motherduck",  # uses duckdb
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
        self.compiler = _get_ibis_to_sqlglot_compiler(self._dataset._destination)

    @classmethod
    def from_dataset(cls, dataset: dlt.Dataset) -> DltBackend:
        new_backend = cls()
        new_backend._dataset = dataset
        new_backend.compiler = _get_ibis_to_sqlglot_compiler(new_backend._dataset._destination)
        return new_backend

    @classmethod
    def from_pipeline(cls, pipeline: dlt.Pipeline) -> DltBackend:
        new_backend = cls()
        new_backend._dataset = pipeline.dataset()
        new_backend.compiler = _get_ibis_to_sqlglot_compiler(new_backend._dataset._destination)
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