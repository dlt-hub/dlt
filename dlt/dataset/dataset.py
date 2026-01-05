from __future__ import annotations

from types import TracebackType
from typing import Any, Optional, Type, Union, TYPE_CHECKING, Literal, overload

from sqlglot.schema import Schema as SQLGlotSchema
import sqlglot.expressions as sge

import dlt
from dlt.common.destination.exceptions import OpenTableClientNotAvailable
from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.common.json import json
from dlt.common.destination.reference import AnyDestination, TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase, SupportsOpenTables, WithStateSync
from dlt.common.schema import Schema
from dlt.common.typing import Self
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.common.utils import simple_repr, without_none
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.dataset import lineage
from dlt.dataset.utils import get_destination_clients
from dlt.destinations.queries import build_row_counts_expr
from dlt.common.destination.exceptions import SqlClientNotAvailable

if TYPE_CHECKING:
    from ibis import ir
    from ibis import BaseBackend as IbisBackend


class Dataset:
    """Access to dataframes and arrow tables in the destination dataset via dbapi"""

    def __init__(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[dlt.Schema, str, None] = None,
    ) -> None:
        self._destination_reference = destination
        self._destination: AnyDestination = Destination.from_reference(destination)
        self._dataset_name = dataset_name
        self._schema: Union[dlt.Schema, str, None] = schema
        # self._sqlglot_schema: SQLGlotSchema = None
        self._sql_client: SqlClientBase[Any] = None
        self._opened_sql_client: SqlClientBase[Any] = None
        self._table_client: SupportsOpenTables = None

    def ibis(self, read_only: bool = False) -> IbisBackend:
        """Get an ibis backend for the dataset.

        This creates a connection to the destination.

        The `read_only` flag is currently only supported for duckdb destination.
        """
        from dlt.helpers.ibis import create_ibis_backend

        return create_ibis_backend(
            destination=self._destination,
            client=self.destination_client,
            read_only=read_only,
        )

    @property
    def schema(self) -> dlt.Schema:
        """dlt schema associated with the dataset.

        If no provided at dataset initialization, it is fetched from the destination. Fallbacks
        to local dlt pipeline metadata.
        """
        if isinstance(self._schema, dlt.Schema):
            return self._schema

        maybe_schema: Optional[dlt.Schema] = None

        if isinstance(self._schema, str):
            maybe_schema = _get_dataset_schema_from_destination_using_schema_name(
                self, self._schema
            )

        if not maybe_schema:
            maybe_schema = _get_dataset_schema_from_destination_using_dataset_name(self)

        if not maybe_schema:
            # uses local dlt pipeline data instead of destination
            maybe_schema = dlt.Schema(self.dataset_name)

        assert isinstance(maybe_schema, dlt.Schema)
        self._schema = maybe_schema
        return self._schema

    @property
    def tables(self) -> list[str]:
        """List of table names found in the dataset.

        This only includes "completed tables". In other words, during the lifetime of a `pipeline.run()`
        execution, tables may exist on the destination, but will only appear on the dataset once
        `pipeline.run()` is done.
        """
        # return only completed tables
        return self.schema.data_table_names() + self.schema.dlt_table_names()

    def _ipython_key_completions_(self) -> list[str]:
        """Provide table names as completion suggestion in interactive environments."""
        return self.tables

    @property
    def sqlglot_schema(self) -> SQLGlotSchema:
        """SQLGlot schema of the dataset derived from the dlt schema."""
        # NOTE: no cache for now, it is probably more expensive to compute the current schema hash
        # to see wether this is stale than to compute a new sqlglot schema
        return lineage.create_sqlglot_schema(
            self.schema, self.dataset_name, dialect=self.destination_dialect
        )

    @property
    def destination_dialect(self) -> TSqlGlotDialect:
        """SQLGlot dialect of the dataset destination.

        This is the target dialect when transpiling SQL queries.
        """
        return self.sql_client.capabilities.sqlglot_dialect

    @property
    def dataset_name(self) -> str:
        """Name of the dataset"""
        return self._dataset_name

    # TODO why do we need `_opened_sql_client` and `_sql_client`? One seems used by
    # the `dlt.Dataset` context manager and the other by `dlt.Relation`
    @property
    def sql_client(self) -> SqlClientBase[Any]:
        # return the opened sql client if it exists
        if self._opened_sql_client:
            return self._opened_sql_client
        if not self._sql_client:
            self._sql_client = get_dataset_sql_client(self)
        return self._sql_client

    # TODO remove this; this seems only implemented to pass to `Relation` for it
    # to respect the ABC requirements of `WithSqlClient` mixin
    # also, it's odd that we need to instantiate an SQL client to get its class
    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        return self.sql_client.__class__

    # TODO if `destination_client` returns a new client each time, it shouldn't be a property
    @property
    def destination_client(self) -> JobClientBase:
        return get_dataset_destination_client(self)

    # TODO should this public? This should only be accessed via `.__open__()`
    @property
    def open_table_client(self) -> SupportsOpenTables:
        if not self._table_client:
            client = get_dataset_destination_client(self)
            if isinstance(client, SupportsOpenTables):
                self._table_client = client
            else:
                raise OpenTableClientNotAvailable(
                    self.dataset_name, self._destination.destination_name
                )
        return self._table_client

    # TODO remove method; need to update `dlthub` to avoid conflict
    # this is only used by `dlt.hub.transformation` currently
    def is_same_physical_destination(self, other: dlt.Dataset) -> bool:
        """
        Returns true if the other dataset is on the same physical destination
        helpful if we want to run sql queries without extracting the data
        """
        return is_same_physical_destination(self, other)

    def query(
        self,
        query: Union[str, sge.Select, ir.Expr],
        query_dialect: Optional[TSqlGlotDialect] = None,
        *,
        _execute_raw_query: bool = False,
    ) -> dlt.Relation:
        """Create a `dlt.Relation` from an SQL query, SQLGlot expression or Ibis expression.

        Args:
            query (Union[str, sge.Select, ir.Expr]): The query that defines the relation.
            query_dialect (Optional[TSqlGlotDialect]): The dialect of the query. If specified, it will be used to transpile the query
                to the destination's dialect. Otherwise, the query is assumed to be the destination's dialect (accessible via `Dataset.sqlglot_dialect`)

        Returns:
            dlt.Relation: The relation for the query
        """
        return dlt.Relation(
            dataset=self,
            query=query,
            query_dialect=query_dialect,
            _execute_raw_query=_execute_raw_query,
        )

    # NOTE could simply accept `*args, **kwargs` and pass to `.query()` but would decrease readability
    #
    def __call__(
        self,
        query: Union[str, sge.Select, ir.Expr],
        query_dialect: Optional[TSqlGlotDialect] = None,
        *,
        _execute_raw_query: bool = False,
    ) -> dlt.Relation:
        """Convenience method to proxy `Dataset.query()`. See this method for details."""
        return self.query(query, query_dialect, _execute_raw_query=_execute_raw_query)

    def table(self, table_name: str, **kwargs: Any) -> dlt.Relation:
        """Get a `dlt.Relation` associated with a table from the dataset."""

        # NOTE dataset only provides access to tables known in dlt schema
        # raw query execution could access tables unknown by dlt
        if table_name not in self.tables:
            # TODO: raise TableNotFound
            raise ValueError(f"Table `{table_name}` not found. Available table(s): {self.tables}")

        # TODO remove in due time;
        if kwargs.get("table_type") == "ibis":
            raise DeprecationWarning(
                "Calling `.table(..., table_type='ibis') is deprecated. Instead, call"
                " `.table('foo').to_ibis()` to create a `dlt.Relation` and then retrieve the"
                " Ibis Table."
            )

        # fallback to the standard dbapi relation
        return dlt.Relation(dataset=self, table_name=table_name)

    def row_counts(
        self,
        *,
        data_tables: bool = True,
        dlt_tables: bool = False,
        table_names: Optional[list[str]] = None,
        load_id: Optional[str] = None,
    ) -> dlt.Relation:
        """Create a `dlt.Relation` with the query to get the row counts of all tables in the dataset.

        Args:
            data_tables (bool): Whether to include data tables. Defaults to True.
            dlt_tables (bool): Whether to include dlt tables. Defaults to False.
            table_names (Optional[list[str]]): The names of the tables to include. Defaults to None. Will override data_tables and dlt_tables if set
            load_id (Optional[str]): If set, only count rows associated with a given load id. Will exclude tables that do not have a load id.
        Returns:
            dlt.Relation: Relation for the query that computes the requested row count.
        """

        selected_tables = table_names or []
        if not selected_tables:
            if data_tables:
                selected_tables += self.schema.data_table_names(seen_data_only=True)
            if dlt_tables:
                selected_tables += self.schema.dlt_table_names()

        # filter tables so only ones with dlt_load_id column are included
        dlt_load_id_col = None
        if load_id:
            dlt_load_id_col = self.schema.naming.normalize_identifier(C_DLT_LOAD_ID)
            selected_tables = [
                table
                for table in selected_tables
                if dlt_load_id_col in self.schema.tables[table]["columns"].keys()
            ]

        union_all_expr = None

        for table_name in selected_tables:
            counts_expr = build_row_counts_expr(
                table_name=table_name,
                dlt_load_id_col=dlt_load_id_col,
                load_id=load_id,
            )
            if union_all_expr is None:
                union_all_expr = counts_expr
            else:
                union_all_expr = union_all_expr.union(counts_expr, distinct=False)

        return self.query(query=union_all_expr)

    def __getitem__(self, table_name: str) -> dlt.Relation:
        """Get a `dlt.Relation` for a table via dictionary notation.

        This proxies `Dataset.table()`.
        """
        if table_name not in self.tables:
            raise KeyError(
                f"Table `{table_name}` not found on dataset. Available tables: `{self.tables}`"
            )
        try:
            return self.table(table_name)
        # TODO: expect TableNotFound in the future
        except ValueError as exc:
            raise KeyError(table_name, str(exc))

    # TODO this creates all sorts of bugs in dynamic scenarios where `getattr()` is used
    # Table names that collide with `Dataset` methods will shadow attributes.
    # You get much weaker IDE autocompletion than `.__getitem__()` or `.table()`
    # Potential fix:
    # super().__getattr__(name); if None: or try/except: return self.table()
    def __getattr__(self, name: str) -> Any:
        """Get a `dlt.Relation` for a table via dictionary notation.

        This proxies `Dataset.table()`.
        """
        try:
            return self.table(name)
        # TODO: expect TableNotFound in the future
        except ValueError as exc:
            raise AttributeError(name, str(exc))

    def __enter__(self) -> Self:
        """Context manager to keep the connection to the destination open between queries"""
        assert (
            not self._opened_sql_client
        ), "context manager can't be used when sql client is initialized"
        # return sql_client wrapped so it will not call close on __exit__ as dataset is managing connections
        # use internal class

        # create a new sql client
        self._opened_sql_client = get_dataset_sql_client(self)

        NoCloseClient = type(
            "NoCloseClient",
            (self._opened_sql_client.__class__,),
            {
                "__exit__": (
                    lambda self, exc_type, exc_val, exc_tb: None
                )  # No-operation: do not close the connection
            },
        )

        self._opened_sql_client.__class__ = NoCloseClient

        self._opened_sql_client.open_connection()
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        """Context manager to keep the connection to the destination open between queries"""
        self._opened_sql_client.close_connection()
        self._opened_sql_client = None

    def __repr__(self) -> str:
        # schema may not be set
        kwargs = {
            "dataset_name": self.dataset_name,
            "destination": repr(self._destination),
            "schema": repr(self.schema) if self.schema else None,
        }
        return simple_repr("dlt.dataset", **without_none(kwargs))

    def __str__(self) -> str:
        # obtain schema. this eventually loads it from destination (if only name was provided)
        _client = self.destination_client
        # str(config) provides displayable physical location
        destination_info = f"{self._destination.destination_name}[{str(_client.config)}]"
        # new schema is created if dataset is not found or schema.name is not materialized yet
        if self.schema.is_new:
            schema_info = "\nDataset is not available at the destination.\n"
        else:
            schema_info = f"with tables in dlt schema `{self.schema.name}`:\n"

        msg = f"Dataset `{self.dataset_name}` at `{destination_info}` {schema_info}"
        if self.schema:
            tables = [name for name in self.schema.data_table_names()]
            if tables:
                msg += ", ".join(tables)
            else:
                msg += "No data tables found in schema."
        return msg


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
) -> Dataset:
    return Dataset(destination, dataset_name, schema)


def get_dataset_destination_client(dataset: dlt.Dataset) -> JobClientBase:
    return get_destination_clients(
        schema=dataset.schema,
        destination=dataset._destination,
        destination_dataset_name=dataset.dataset_name,
    )[0]


def get_dataset_sql_client(dataset: dlt.Dataset) -> SqlClientBase[Any]:
    client = get_dataset_destination_client(dataset)
    if isinstance(client, WithSqlClient):
        return client.sql_client
    else:
        raise SqlClientNotAvailable("dataset", dataset.dataset_name, client.config.destination_type)


def is_same_physical_destination(dataset1: dlt.Dataset, dataset2: dlt.Dataset) -> bool:
    """Check if both datasets are at the same physical destination.

    This is done by comparing the fingerprint of both destination configs. There
    are potential false positive if two different config give access to the same destination.
    """
    return str(dataset1.destination_client.config) == str(dataset2.destination_client.config)


def _get_dataset_schema_from_destination_using_schema_name(
    dataset: dlt.Dataset, schema_name: str
) -> Optional[dlt.Schema]:
    schema = None
    with get_destination_clients(
        schema=dlt.Schema(schema_name),
        destination=dataset._destination,
        destination_dataset_name=dataset.dataset_name,
    )[0] as client:
        if isinstance(client, WithStateSync):
            stored_schema = client.get_stored_schema(schema_name)
            if stored_schema:
                schema = dlt.Schema.from_stored_schema(json.loads(stored_schema.schema))
            else:
                schema = dlt.Schema(schema_name)

    return schema


def _get_dataset_schema_from_destination_using_dataset_name(
    dataset: dlt.Dataset,
) -> Optional[dlt.Schema]:
    schema = None
    with get_destination_clients(
        schema=dlt.Schema(dataset.dataset_name),
        destination=dataset._destination,
        destination_dataset_name=dataset.dataset_name,
    )[0] as client:
        if isinstance(client, WithStateSync):
            stored_schema = client.get_stored_schema()
            if stored_schema:
                schema = dlt.Schema.from_stored_schema(json.loads(stored_schema.schema))

    return schema
