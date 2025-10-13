from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Iterator, Union
from typing_extensions import override

import dlt
from dlt.helpers.ibis import create_ibis_backend, _get_ibis_to_sqlglot_compiler
from dlt.common.schema import Schema as DltSchema
from dlt.common.destination import TDestinationReferenceArg
from dlt.common.schema.typing import TDataType, TTableSchema
from dlt.common.exceptions import MissingDependencyException

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from ibis import BaseBackend

try:
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
except ImportError:
    raise MissingDependencyException("dlt ibis helpers", ["ibis-framework"])

# TODO move `dlt.helpers.ibis` content to this module


# TODO complete implementation and register to singledispatch `sch.infer.register`
class DltType(TypeMapper):
    @classmethod
    def from_ibis(cls) -> TDataType:
        raise NotImplementedError

    @classmethod
    def to_ibis(cls, typ: TDataType, nullable: bool | None = None) -> dt.DataType:
        nullable = True if nullable is None else bool(nullable)
        return ibis.dtype(dlt.helpers.ibis.DATA_TYPE_MAP[typ], nullable=nullable)


# TODO support `database` kwarg (equiv. `dataset_name`) to enable DltBackend to access multiple database
# NOTE most of the code below was derived from the Ibis BaseBackend and SQLBackend implementation
class _DltBackend(SQLBackend, NoUrl, NoExampleLoader):
    """Ibis backend that delegates execution to dlt's native SQL engine.

    To make Ibis expressions executable, they need to be bound to a table on the
    backend (i.e., data that exists somewhere). By default, Ibis backends work by
    creating and maintaining a connection to the backend.

    This `DltBackend` is "lazier" and doesn't make a connection to the backend where
    the data lives ("backend" in Ibis, "destination" in dlt). Instead, it uses dlt
    metadata to register what table exists and create bound expressions.

    For example, if you use Ibis with the Snowflake backend, you will get use Ibis'
    implementation. If you use the DltBackend with a Snowflake destination, you will
    use the dlt SQL client for Snowflake. The SQL query created should be the same
    because dlt internally uses the `ibis -> sqlglot` compiler, but the actual
    execution might differ. This is especially likely when calling `.execute()` or
    other methods that return data in memory.
    """

    name: str = "dlt"
    supports_temporary_tables = False
    supports_python_udfs = False

    @classmethod
    def from_dataset(cls, dataset: dlt.Dataset) -> _DltBackend:
        """Create an Ibis `DltBackend` from a `dlt.Dataset`

        This enables `dlt.Relation.to_ibis()` to create bound tables.
        """
        # NOTE it's essential to keep this lazy, no round-trip to destination
        # sync with destination should be made through the dataset.
        new_backend = cls()
        new_backend._dataset = dataset
        new_backend.compiler = _get_ibis_to_sqlglot_compiler(new_backend._dataset._destination)  # type: ignore[arg-type]
        return new_backend

    def to_native_ibis(self, *, read_only: bool = False) -> BaseBackend:
        return get_native_ibis_backend(self._dataset, read_only=read_only)

    @property
    # @override
    def version(self) -> str:
        import importlib.metadata

        return importlib.metadata.version("dlt")

    # NOTE can change signature
    # @override
    def do_connect(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[DltSchema, str, None] = None,
        **config: Any,
    ) -> None:
        self._dataset = dlt.dataset(
            destination=destination,
            dataset_name=dataset_name,
            schema=schema,
        )
        self.compiler = _get_ibis_to_sqlglot_compiler(self._dataset._destination)  # type: ignore[arg-type]

    @contextlib.contextmanager
    # @override
    def _safe_raw_sql(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        yield self.raw_sql(*args, **kwargs)

    # @override
    def raw_sql(self, query: Union[str, sg.Expression], **kwargs: Any) -> Any:
        """Execute SQL string or SQLGlot expression using the dlt destination SQL client"""
        with contextlib.suppress(AttributeError):
            if isinstance(query, sg.Expression):
                query = query.sql(dialect=self.compiler.dialect)
            else:
                query = sg.transpile(query, write=self.compiler.dialect)[0]

        assert isinstance(query, str)
        with self._dataset.sql_client as client:
            result = client.execute_sql(query)

        return result

    # required for marimo DataSources UI to work
    @property
    def current_database(self) -> str:
        return self._dataset.dataset_name

    # required for marimo DataSources UI to work
    # @override
    def list_tables(
        self, *, like: str | None = None, database: tuple[str, str] | str | None = None
    ) -> list[str]:
        """Return the list of table names"""
        return list(self._dataset.schema.tables.keys())

    # required for marimo DataSources UI to work
    # @override
    def get_schema(self, table_name: str, *args: Any, **kwargs: Any) -> sch.Schema:
        """Get the Ibis table schema"""
        return _to_ibis_schema(self._dataset.table(table_name).schema)

    # required for marimo DataSources UI to work
    # @override
    def _get_schema_using_query(self, query: str) -> sch.Schema:
        """Required to subclass SQLBackend"""
        return _to_ibis_schema(self._dataset(query).schema)

    # required for marimo DataSources UI to work
    # @override
    def table(self, name: str, /, *, database: tuple[str, str] | str | None = None) -> ir.Table:
        """Construct a table expression"""
        # TODO maybe there's a more straighforward way to retrieve catalog and db
        sql_client = self._dataset.sql_client
        catalog = sql_client.catalog_name()
        database = sql_client.capabilities.casefold_identifier(sql_client.dataset_name)

        table_schema = self.get_schema(name)
        return ops.DatabaseTable(
            name,
            schema=table_schema,
            source=self,
            namespace=ops.Namespace(catalog=catalog, database=database),
        ).to_expr()

    # TODO use the new dlt `model` format with INSERT statement
    # for non-SQL (e.g., filesystem) use JobClient
    # @override
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

    # @override
    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        """Required to subclass SQLBackend"""
        raise NotImplementedError

    # @override
    def _register_udfs(self, expr: ir.Expr) -> None:
        """Override SQLBackend method to avoid round-trip to Ibis SQL compiler"""


def _to_ibis_schema(table_schema: TTableSchema) -> sch.Schema:
    return sch.Schema(
        {
            name: DltType.to_ibis(column["data_type"], column.get("nullable"))
            for name, column in table_schema["columns"].items()
        }
    )


def get_native_ibis_backend(dataset: dlt.Dataset, *, read_only: bool = False) -> BaseBackend:
    return create_ibis_backend(
        destination=dataset._destination,
        client=dataset.destination_client,
        read_only=read_only,
    )
