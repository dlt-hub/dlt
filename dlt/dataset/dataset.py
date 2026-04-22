from __future__ import annotations

from types import TracebackType
from typing import (
    Any,
    Collection,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    Tuple,
    Union,
    TYPE_CHECKING,
    Literal,
    overload,
)

from sqlglot.schema import Schema as SQLGlotSchema
import sqlglot.expressions as sge

import dlt
from dlt.common.destination.exceptions import OpenTableClientNotAvailable
from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.common.json import json
from dlt.common.versioned_state import decompress_state
from dlt.common.destination.reference import AnyDestination, TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase, SupportsOpenTables, WithStateSync
from dlt.common.schema import Schema
from dlt.common.typing import Self
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    C_DLT_LOADS_TABLE_LOAD_ID,
    LOADS_TABLE_NAME,
)
from dlt.common.utils import extend_list_deduplicated, simple_repr, without_none
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient, WithSchemas
from dlt.dataset import lineage
from dlt.dataset.utils import get_destination_clients
from dlt.destinations.queries import build_row_counts_expr
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    SqlClientNotAvailable,
)

if TYPE_CHECKING:
    from dlt.common.libs.ibis import ir
    from dlt.common.libs.ibis import BaseBackend as IbisBackend


class Dataset:
    """Access to dataframes and arrow tables in the destination dataset via dbapi"""

    def __init__(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[dlt.Schema, str, Sequence[dlt.Schema], None] = None,
    ) -> None:
        self._destination_reference = destination
        self._destination: AnyDestination = Destination.from_reference(destination)
        self._dataset_name = dataset_name
        self._pipeline_name: Optional[str] = None
        """If set, used to resolve default schema from pipeline state"""

        self._schema_arg: Union[dlt.Schema, str, Sequence[dlt.Schema], None] = schema
        self._schemas: Dict[str, dlt.Schema] = {}
        self._default_schema_name: Optional[str] = None
        self._resolved: bool = False

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
            schemas=self.schemas,
        )

    def _resolve_schemas(self) -> None:
        """Resolve all schemas on first access."""
        if self._resolved:
            return

        schema_arg = self._schema_arg

        if isinstance(schema_arg, Sequence) and not isinstance(schema_arg, str):
            if not schema_arg:
                raise ValueError("schema sequence must not be empty")
            for s in schema_arg:
                self._schemas[s.name] = s
            self._default_schema_name = schema_arg[0].name
        elif isinstance(schema_arg, dlt.Schema):
            self._schemas[schema_arg.name] = schema_arg
            self._default_schema_name = schema_arg.name
        else:
            schemas: Optional[List[dlt.Schema]] = None

            if isinstance(schema_arg, str):
                schema = _get_dataset_schema_from_destination_using_schema_name(self, schema_arg)
                if schema:
                    schemas = [schema]

            if not schemas:
                schemas = _get_dataset_schemas_from_dataset_dlt_tables(self)

            if not schemas:
                schemas = [dlt.Schema(self.dataset_name)]

            for s in schemas:
                self._schemas[s.name] = s
            self._default_schema_name = schemas[0].name

        self._resolved = True

    @property
    def schema(self) -> dlt.Schema:
        """Default dlt schema associated with the dataset.

        If not provided at dataset initialization, it is fetched from the destination.
        Falls back to local dlt pipeline metadata.
        """
        self._resolve_schemas()
        return self._schemas[self._default_schema_name]

    @property
    def schemas(self) -> Tuple[dlt.Schema, ...]:
        """All local schemas in this dataset, default schema first."""
        self._resolve_schemas()
        return tuple(self._schemas.values())

    @property
    def tables(self) -> list[str]:
        """List of table names found in the dataset.

        This only includes "completed tables". In other words, during the lifetime of a `pipeline.run()`
        execution, tables may exist on the destination, but will only appear on the dataset once
        `pipeline.run()` is done.
        """
        result: list[str] = []
        for schema in self.schemas:
            extend_list_deduplicated(result, schema.data_table_names() + schema.dlt_table_names())
        return result

    def _ipython_key_completions_(self) -> list[str]:
        """Provide table names as completion suggestion in interactive environments."""
        return self.tables

    @property
    def sqlglot_schema(self) -> SQLGlotSchema:
        """SQLGlot schema of the dataset derived from all dlt schemas."""
        # NOTE: no cache for now, it is probably more expensive to compute the current schema hash
        # to see wether this is stale than to compute a new sqlglot schema
        return lineage.create_sqlglot_schema(
            {self.dataset_name: list(self.schemas)}, dialect=self.destination_dialect
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

    def table(
        self, table_name: str, *, load_ids: Optional[Collection[str]] = None, **kwargs: Any
    ) -> dlt.Relation:
        """Get a `dlt.Relation` associated with a table from the dataset."""
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

        if load_ids:
            return dlt.Relation(dataset=self, table_name=table_name).from_loads(load_ids)
        else:
            return dlt.Relation(dataset=self, table_name=table_name)

    def loads_table(self) -> dlt.Relation:
        """Get `_dlt_loads` table from the dataset."""
        return dlt.Relation(dataset=self, table_name=self.schema.loads_table_name)

    def load_ids(self, schema_name: Optional[str] = None) -> list[str]:
        """Retrieved the list of load ids for this dataset.

        Args:
            schema_name: Filter by this schema. Defaults to the default schema.
        """
        return _get_load_ids(self, schema_name=schema_name)

    def latest_load_id(self, schema_name: Optional[str] = None) -> Optional[str]:
        """Retrieved the latest load id for this dataset.

        Args:
            schema_name: Filter by this schema. Defaults to the default schema.
        """
        return _get_latest_load_id(self, schema_name=schema_name)

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
                for schema in self.schemas:
                    extend_list_deduplicated(
                        selected_tables, schema.data_table_names(seen_data_only=True)
                    )
            if dlt_tables:
                for schema in self.schemas:
                    extend_list_deduplicated(selected_tables, schema.dlt_table_names())

        # filter tables so only ones with dlt_load_id column are included
        # normalize per-schema since naming conventions may differ
        load_id_cols: Dict[str, str] = {}
        if load_id:
            for schema in self.schemas:
                dlt_load_id_col = schema.naming.normalize_identifier(C_DLT_LOAD_ID)
                for t_name, t_schema in schema.tables.items():
                    if dlt_load_id_col in t_schema.get("columns", {}):
                        load_id_cols[t_name] = dlt_load_id_col
            selected_tables = [t for t in selected_tables if t in load_id_cols]

        union_all_expr: Optional[sge.Query] = None

        for table_name in selected_tables:
            counts_expr = build_row_counts_expr(
                table_name=table_name,
                dlt_load_id_col=load_id_cols.get(table_name),
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
        # do not intercept dunder attributes (needed for copy/pickle/deepcopy)
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
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
        _client = self.destination_client
        destination_info = f"{self._destination.destination_name}[{str(_client.config)}]"
        header = f"Dataset `{self.dataset_name}` at `{destination_info}` "

        if self.schema.is_new:
            return header + "\nDataset is not available at the destination.\n"

        schemas = self.schemas
        if len(schemas) == 1:
            lines = [header + f"with schema `{schemas[0].name}`:"]
        else:
            lines = [header + "with schemas:"]
        for s in schemas:
            tables = ", ".join(s.data_table_names()) or "No data tables found."
            prefix = f"  {s.name}: " if len(schemas) > 1 else "  "
            lines.append(prefix + tables)
        return "\n".join(lines)


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, Sequence[Schema], None] = None,
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
        sql_client = client.sql_client
        if isinstance(sql_client, WithSchemas):
            sql_client.set_schemas(dataset.schemas)
        return sql_client
    else:
        raise SqlClientNotAvailable("dataset", dataset.dataset_name, client.config.destination_type)


def is_same_physical_destination(dataset1: dlt.Dataset, dataset2: dlt.Dataset) -> bool:
    """Check if tables from both datasets can be joined in a single query."""
    # NOTE: the name is historical -- this actually checks join compatibility via
    # can_join_with(), which may return True even when the physical storage
    # locations differ (e.g. filesystem destinations backed by different buckets).
    return dataset1.destination_client.config.can_join_with(dataset2.destination_client.config)


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


def _get_dataset_schemas_from_dataset_dlt_tables(
    dataset: dlt.Dataset,
) -> Optional[List[dlt.Schema]]:
    """Resolves all schemas from destination.

    Uses the pipeline state's ``schema_names`` list to load every schema stored
    on the destination, with the default schema first. Falls back to the most
    recently stored schema when no pipeline state is available.
    """
    with get_destination_clients(
        schema=dlt.Schema(dataset.dataset_name),
        destination=dataset._destination,
        destination_dataset_name=dataset.dataset_name,
    )[0] as client:
        if isinstance(client, WithStateSync):
            # try to resolve via pipeline state for correct default schema
            pipeline_name = dataset._pipeline_name
            if pipeline_name:
                try:
                    stored_state = client.get_stored_state(pipeline_name)
                except DestinationUndefinedEntity:
                    # state tables may not exist if pipeline never ran
                    stored_state = None
                if stored_state:
                    state = decompress_state(stored_state.state)
                    default_schema_name = state.get("default_schema_name")
                    schema_names = state.get("schema_names") or (
                        [default_schema_name] if default_schema_name else []
                    )
                    if schema_names:
                        # ensure default schema is first
                        # since schema_names from the state is sorted alphabetically
                        if default_schema_name and default_schema_name in schema_names:
                            ordered = [default_schema_name] + [
                                n for n in schema_names if n != default_schema_name
                            ]
                        else:
                            ordered = list(schema_names)
                        schemas: List[dlt.Schema] = []
                        for name in ordered:
                            stored_schema = client.get_stored_schema(name)
                            if stored_schema:
                                schemas.append(
                                    dlt.Schema.from_stored_schema(json.loads(stored_schema.schema))
                                )
                        if schemas:
                            return schemas
            # fall back to most recently stored schema
            stored_schema = client.get_stored_schema()
            if stored_schema:
                return [dlt.Schema.from_stored_schema(json.loads(stored_schema.schema))]
    return None


def _get_load_ids(dataset: dlt.Dataset, schema_name: Optional[str] = None) -> list[str]:
    """Get a list of load ids for a single schema.

    Args:
        schema_name: Filter by this schema. Defaults to the default schema.
    """
    loads_table = dataset.loads_table()
    name = schema_name or dataset.schema.name
    normalized_schema_col = dataset.schema.naming.normalize_identifier("schema_name")
    normalized_load_id_col = dataset.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID)
    query = (
        loads_table.where(normalized_schema_col, "eq", name)
        .select(normalized_load_id_col)
        .order_by(normalized_load_id_col, "asc")
    )
    load_ids: list[str] = [load_id[0] for load_id in query.fetchall()]
    return load_ids


def _get_latest_load_id(dataset: dlt.Dataset, schema_name: Optional[str] = None) -> Optional[str]:
    """Get the latest load id for a single schema.

    Args:
        schema_name: Filter by this schema. Defaults to the default schema.
    """
    loads_table = dataset.loads_table()
    name = schema_name or dataset.schema.name
    normalized_schema_col = dataset.schema.naming.normalize_identifier("schema_name")
    normalized_load_id_col = dataset.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID)
    query = (
        loads_table.where(normalized_schema_col, "eq", name).select(normalized_load_id_col).max()
    )
    load_id = query.fetchone()
    return load_id[0] if load_id else None  # type: ignore[no-any-return]
