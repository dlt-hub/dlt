from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Generator, Mapping, Optional, Union

import dlt
from dlt.common.destination.dataset import SupportsDataAccess
from dlt.helpers.ibis import DATA_TYPE_MAP, create_ibis_backend, _get_ibis_to_sqlglot_compiler
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
    from ibis import util as ibis_util, options as ibis_options
    import ibis.expr.datatypes as dt
    import ibis.expr.operations as ops
    import ibis.expr.schema as sch
    import ibis.expr.types as ir
    import sqlglot as sg
    import ibis.backends.sql.compilers as sc
    from ibis.backends import NoUrl, NoExampleLoader
    from ibis.backends.sql import SQLBackend
    from ibis.formats import TypeMapper
    from sqlglot import expressions as sge
except ImportError:
    raise MissingDependencyException("dlt ibis helpers", ["ibis-framework"])

# TODO move `dlt.helpers.ibis` content to this module


# TODO complete implementation and register to singledispatch `sch.infer.register`
class DltType(TypeMapper):
    @classmethod
    def from_ibis(cls) -> TDataType:
        raise NotImplementedError

    @classmethod
    def to_ibis(cls, typ: TDataType, nullable: Optional[bool] = None) -> dt.DataType:
        nullable = True if nullable is None else bool(nullable)
        return ibis.dtype(DATA_TYPE_MAP[typ], nullable=nullable)


# def _transpile(query: sge.ExpOrStr, *, target_dialect: type[sg.Dialect]) -> str:
#     if isinstance(query, sg.Expression):
#         query = query.sql(dialect=target_dialect)
#     elif isinstance(query, str):
#         query = sg.transpile(query, write=target_dialect)[0]
#     else:
#         raise TypeErrorWithKnownTypes(
#             key="query", value_received=query, valid_types=["str", "sqlglot.Expression"]
#         )

#     return query


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
        new_backend.compiler = _get_ibis_to_sqlglot_compiler(dataset.destination_dialect)
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
        self.compiler = _get_ibis_to_sqlglot_compiler(self._dataset.destination_dialect)

    def disconnect(self) -> None:
        # no need to disconnect, no connections are persisted
        pass

    @contextlib.contextmanager
    def _safe_raw_sql(
        self, query: Union[sge.ExpOrStr, str, ir.Expr], **kwargs: Any
    ) -> Generator[SupportsDataAccess, None, None]:
        # just re-yield our cursor which is dbapi compatible
        with self._dataset.query(query)._cursor() as cur:
            yield cur

    def compile(  # noqa
        self,
        expr: ir.Expr,
        /,
        *,
        limit: Union[int, str, None] = None,
        params: Optional[Mapping[ir.Scalar, Any]] = None,
        pretty: bool = False,
    ) -> str:
        # this reuses dlt.Relation to generate destination-specific SQL from destination agnostic
        # expr
        r_ = self._dataset.query(expr)
        if limit:
            if limit == "default":
                limit = ibis_options.sql.default_limit
            if limit:
                r_ = r_.limit(int(limit))
        sql = r_.to_sql(pretty=pretty)
        self._log(sql)
        # TODO: allow for `params`
        return sql

    # required for marimo DataSources UI to work
    @property
    def current_database(self) -> str:
        return self._dataset.dataset_name

    # required for marimo DataSources UI to work
    def list_tables(
        self, *, like: Optional[str] = None, database: Union[tuple[str, str], str, None] = None
    ) -> list[str]:
        """Return the list of table names"""
        return list(self._dataset.schema.tables.keys())

    # required for marimo DataSources UI to work
    def get_schema(self, table_name: str, *args: Any, **kwargs: Any) -> sch.Schema:
        return _to_ibis_schema(self._dataset.table(table_name).schema)

    # required for marimo DataSources UI to work
    def _get_schema_using_query(self, query: str) -> sch.Schema:
        return _to_ibis_schema(self._dataset(query).schema)

    # required for marimo DataSources UI to work
    def table(
        self, name: str, /, *, database: Union[tuple[str, str], str, None] = None
    ) -> ir.Table:
        table_schema = self.get_schema(name)
        return ops.DatabaseTable(
            name,
            schema=table_schema,
            source=self,
            namespace=ops.Namespace(database=self.current_database),
        ).to_expr()

    # TODO use the new dlt `model` format with INSERT statement
    # for non-SQL (e.g., filesystem) use JobClient
    def create_table(
        self,
        name: str,
        /,
        obj: Union[pd.DataFrame, pa.Table, ir.Table, None] = None,
        *,
        schema: Optional[sch.Schema] = None,
        database: Optional[str] = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> ir.Table:
        raise NotImplementedError

    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        """Required to subclass SQLBackend"""
        raise NotImplementedError

    def _register_udfs(self, expr: ir.Expr) -> None:
        """Override SQLBackend method to avoid round-trip to Ibis SQL compiler"""

    def _fetch_from_cursor(self, cursor: SupportsDataAccess, schema: sch.Schema) -> pd.DataFrame:
        # SqlBackend implementation is clearly wrong - it passes cursor to `from_records`
        # which expects data
        # as a bonus - this will provide native pandas reading for destinations that support it

        from ibis.formats.pandas import PandasData

        df = PandasData.convert_table(cursor.df(), schema)
        return df

    @ibis_util.experimental
    def to_pyarrow(
        self,
        expr: ir.Expr,
        /,
        *,
        params: Optional[Mapping[ir.Scalar, Any]] = None,
        limit: Union[int, str, None] = None,
        **kwargs: Any,
    ) -> pa.Table:
        self._run_pre_execute_hooks(expr)

        table_expr = expr.as_table()
        schema = table_expr.schema()
        arrow_schema = schema.to_pyarrow()

        with self._safe_raw_sql(self.compile(expr, limit=limit, params=params)) as cur:
            table = cur.arrow()

        return expr.__pyarrow_result__(
            table.rename_columns(list(table_expr.columns)).cast(arrow_schema)
        )

    # TODO: implement native arrow batches
    # @util.experimental
    # def to_pyarrow_batches(


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
