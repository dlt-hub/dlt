from types import TracebackType
from typing import Any, Type, Union, TYPE_CHECKING, List, Sequence, Set, Optional

from sqlglot import Schema as SQLGlotSchema
import sqlglot.expressions as sge

from dlt.common.destination.exceptions import OpenTableClientNotAvailable
from dlt.common.json import json
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.reference import TDestinationReferenceArg, Destination
from dlt.common.destination.client import (
    JobClientBase,
    SupportsOpenTables,
    WithStateSync,
)
from dlt.common.destination.dataset import (
    SupportsReadableRelation,
    SupportsReadableDataset,
)
from dlt.common.destination.typing import TDatasetType
from dlt.common.schema import Schema
from dlt.common.typing import Self
from dlt.common.exceptions import DltException
from dlt.common.schema.utils import (
    get_root_table,
    get_first_column_name_with_prop,
    is_nested_table,
)

from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.transformations import lineage
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation
from dlt.destinations.dataset.relation import (
    ReadableDBAPIRelation,
    BaseReadableDBAPIRelation,
)
from dlt.destinations.dataset.utils import get_destination_clients

if TYPE_CHECKING:
    from dlt.helpers.ibis import BaseBackend as IbisBackend
else:
    IbisBackend = Any


# TODO this constant should probably live elsewhere
LOAD_ID_COL_ON_LOADS_TABLE = "load_id"


def _filter_by_status(query: sge.Select, *, status: Union[int, Sequence[int], None]) -> sge.Select:
    """Add `status` filter to a SGLGlot Select expression."""
    if isinstance(status, int):
        query = query.where(sge.to_identifier("status").eq(status))
    elif isinstance(status, Sequence):
        if len(status) < 1:
            raise DltException(
                f"Invalid argument `status`. Reason: `len(status) < 1`. Received `{status}`"
            )
        elif not all(isinstance(s, int) for s in status):
            raise DltException(
                f"Invalid argument `status`. Reason: not all values are `int`. Received `{status}`"
            )
        query = query.where(sge.to_identifier("status").isin(*status))
    elif status is not None:
        raise DltException(
            f"The `status` argument should be `int`, `Sequence[int]` or `None`. Received `{status}`"
        )
    return query


# TODO use @overload to type hint valid signatures
def _filter_min_max(
    query: sge.Select,
    *,
    lt: Optional[Any] = None,
    le: Optional[Any] = None,
    gt: Optional[Any] = None,
    ge: Optional[Any] = None,
) -> sge.Select:
    # TODO add exception handling to catch invalid user input (e.g., `le < ge`)
    if ge is not None and gt is not None:
        raise ValueError()

    if le is not None and lt is not None:
        raise ValueError()

    load_id_col = sge.to_identifier(LOAD_ID_COL_ON_LOADS_TABLE)
    conditions = []

    # between is a special condition: (ge, le)
    if ge is not None and le is not None:
        cond = load_id_col.between(low=ge, high=le)
        conditions.append(cond)
    else:
        # 8 - 1 combinations: (gt,), (ge,), (lt,), (le,), (gt, lt), (gt, le), (ge, lt); and between() == (ge, le)
        if ge is not None or gt is not None:
            cond = load_id_col >= ge if ge is not None else load_id_col > gt
            conditions.append(cond)

        if le is not None or lt is not None:
            cond = load_id_col <= le if le is not None else load_id_col < lt
            conditions.append(cond)

    query = query.where(*conditions)
    return query


def _set_limit(query: sge.Select, *, limit: Optional[int]) -> sge.Select:
    if isinstance(limit, int):
        if limit < 1:
            raise DltException(f"Invalid argument `limit`. Reason: `limit < 1`. Received `{limit}`")
        query = query.limit(limit)
    elif limit is not None:
        raise DltException(f"The `limit` argument should be `int` or `None`. Received `{limit}`")
    return query


# TODO use @overload to document valid signature
# for example, passing min, max, and limit is ambiguous and shouldn't be possible
def _list_load_ids_expr(
    loads_table_name: str = "_dlt_loads",
    lt: Optional[Any] = None,
    le: Optional[Any] = None,
    gt: Optional[Any] = None,
    ge: Optional[Any] = None,
    status: Union[int, Sequence[int], None] = None,
    limit: Optional[int] = None,
) -> sge.Select:
    query = (
        sge.select(sge.to_identifier(LOAD_ID_COL_ON_LOADS_TABLE))
        .from_(loads_table_name)
        .order_by(f"{LOAD_ID_COL_ON_LOADS_TABLE} DESC")
    )
    query = _filter_by_status(query, status=status)
    query = _filter_min_max(query, lt=lt, le=le, gt=gt, ge=ge)
    query = _set_limit(query, limit=limit)
    return query


def _latest_load_id_expr(
    loads_table_name: str = "_dlt_loads", status: Union[int, Sequence[int], None] = None
) -> sge.Select:
    query = sge.select(
        sge.func("max", sge.to_identifier(LOAD_ID_COL_ON_LOADS_TABLE)).as_(
            LOAD_ID_COL_ON_LOADS_TABLE
        )
    ).from_(loads_table_name)
    query = _filter_by_status(query, status=status)
    return query


def _filter_root_table_expr(
    table_name: str,
    load_ids: Union[Sequence[str], Set[str]],
) -> sge.Select:
    query = sge.select("*").from_(sge.to_identifier(table_name))

    # nullable means it's an empty collection; this will match no rows; we return an empty set
    if not load_ids:
        query = query.where("1 = 0")
    else:
        query = query.where(sge.to_identifier(C_DLT_LOAD_ID).isin(*load_ids))

    return query


def _filter_child_table_expr(
    table_name: str,
    table_root_key: str,
    root_table_name: str,
    root_table_row_key: str,
    load_ids: Union[Sequence[str], Set[str]],
) -> sge.Select:
    root_table_expr = _filter_root_table_expr(table_name=root_table_name, load_ids=load_ids)
    root_alias = f"{root_table_name}_filtered"
    join_condition = f"{table_name}.{table_root_key} = {root_alias}.{root_table_row_key}"
    query = (
        sge.select(f"{table_name}.*")
        .from_(sge.to_identifier(table_name))
        .join(
            root_table_expr,
            on=join_condition,
            join_type="inner",
            join_alias=root_alias,
        )
    )
    return query


class ReadableDBAPIDataset(SupportsReadableDataset[ReadableIbisRelation]):
    """Access to dataframes and arrow tables in the destination dataset via dbapi"""

    def __init__(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[Schema, str, None] = None,
        dataset_type: TDatasetType = "auto",
    ) -> None:
        self._destination_reference_arg = destination
        self._destination = Destination.from_reference(destination)
        self._provided_schema = schema
        self._dataset_name = dataset_name
        self._sql_client: SqlClientBase[Any] = None
        self._table_client: SupportsOpenTables = None
        self._schema: Schema = None
        self._load_ids: Optional[set[str]] = None
        # resolve dataset type
        if dataset_type in ("auto", "ibis"):
            try:
                from dlt.helpers.ibis import ibis

                dataset_type = "ibis"
            except MissingDependencyException:
                # if ibis is explicitly requested, reraise
                if dataset_type == "ibis":
                    raise
        self._dataset_type: TDatasetType = dataset_type

    def ibis(self) -> IbisBackend:
        """return a connected ibis backend"""
        from dlt.helpers.ibis import create_ibis_backend

        return create_ibis_backend(
            self._destination,
            self._destination_client(self.schema),
        )

    @property
    def schema(self) -> Schema:
        # NOTE: if this property raises AttributeError, __getattr__ will get called 🤯
        #   this leads to infinite recursion as __getattr_ calls this property
        if not self._schema:
            self._ensure_client_and_schema()
        return self._schema

    @property
    def sqlglot_schema(self) -> SQLGlotSchema:
        return lineage.create_sqlglot_schema(self.schema)

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        if not self._sql_client:
            self._ensure_client_and_schema()
        return self._sql_client

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        return self.sql_client.__class__

    @property
    def destination_client(self) -> JobClientBase:
        if not self._sql_client:
            self._ensure_client_and_schema()
        return self._destination_client(self._schema)

    @property
    def open_table_client(self) -> SupportsOpenTables:
        if not self._sql_client:
            self._ensure_client_and_schema()
        if not self._table_client:
            raise OpenTableClientNotAvailable(
                self._dataset_name, self._destination.destination_name
            )
        return self._table_client

    def is_same_physical_destination(self, other: "ReadableDBAPIDataset") -> bool:
        """
        Returns true if the other dataset is on the same physical destination
        helpful if we want to run sql queries without extracting the data
        """
        return str(self.destination_client.config) == str(other.destination_client.config)

    def _destination_client(self, schema: Schema) -> JobClientBase:
        return get_destination_clients(
            schema,
            destination=self._destination,
            destination_dataset_name=self._dataset_name,
        )[0]

    def _ensure_client_and_schema(self) -> None:
        """Lazy load schema and client"""

        # full schema given, nothing to do
        if not self._schema and isinstance(self._provided_schema, Schema):
            self._schema = self._provided_schema

        # schema name given, resolve it from destination by name
        elif not self._schema and isinstance(self._provided_schema, str):
            with self._destination_client(Schema(self._provided_schema)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema(self._provided_schema)
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))
                    else:
                        self._schema = Schema(self._provided_schema)

        # no schema name given, load newest schema from destination
        elif not self._schema:
            with self._destination_client(Schema(self._dataset_name)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema()
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))

        # default to empty schema with dataset name
        if not self._schema:
            self._schema = Schema(self._dataset_name)

        # here we create the client bound to the resolved schema
        if not self._sql_client:
            with self._destination_client(self._schema) as destination_client:
                if isinstance(destination_client, WithSqlClient):
                    self._sql_client = destination_client.sql_client
                else:
                    raise Exception(
                        f"Destination {destination_client.config.destination_type} does not support"
                        " SqlClient."
                    )
                if isinstance(destination_client, SupportsOpenTables):
                    self._table_client = destination_client

    def __call__(self, query: Any) -> ReadableDBAPIRelation:
        # TODO: accept other query types and return a right relation: sqlglot (DBAPI) and ibis (Expr)
        return ReadableDBAPIRelation(
            readable_dataset=self, provided_query=query, load_ids=self._load_ids
        )

    def table(self, table_name: str) -> ReadableIbisRelation:
        # we can create an ibis powered relation if ibis is available
        relation: BaseReadableDBAPIRelation
        if self._dataset_type == "ibis":
            from dlt.helpers.ibis import create_unbound_ibis_table
            from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation

            unbound_table = create_unbound_ibis_table(self.sql_client, self.schema, table_name)
            relation = ReadableIbisRelation(
                readable_dataset=self,
                ibis_object=unbound_table,
            )
            return relation

        # no selection should return all rows
        if self._load_ids is None:
            # fallback to the standard dbapi relation
            return ReadableDBAPIRelation(
                readable_dataset=self,
                table_name=table_name,
            )  # type: ignore[return-value]

        table_schema = self.schema.tables[table_name]
        # case 1: table is root
        if not is_nested_table(table_schema):
            filtered_table = _filter_root_table_expr(table_name=table_name, load_ids=self._load_ids)
            # TODO modify ReadableDBAPIRelation to take this filtered table as CTE
            # TODO in the relation, "table_name" refers to the filtered table instead of full table
        else:
            root_key = get_first_column_name_with_prop(table_schema, column_prop="root_key")
            # unsupported case: table is child / not root, but doesn't have `root_key`
            # could be implemented by traversing parent-row keys to join nested tables
            # TODO could return the full table if incremental fails instead of raising
            if root_key is None:
                raise DltException(
                    f"No `root_key` found for child table `{table_name}`. To use incremental"
                    " selection, set `root_key=True` on the source or use"
                    " `write_disposition='merge'"
                )

            # case 2: table is child / not root; use `root_key` to join
            root_table_schema = get_root_table(tables=self.schema.tables, table_name=table_name)
            root_table_name: str = root_table_schema["name"]
            root_table_row_key: str = get_first_column_name_with_prop(
                root_table_schema, column_prop="row_key"
            )

            filtered_table = _filter_child_table_expr(
                table_name=table_name,
                table_root_key=root_key,
                root_table_name=root_table_name,
                root_table_row_key=root_table_row_key,
                load_ids=self._load_ids,
            )

        return ReadableDBAPIRelation(
            readable_dataset=self, provided_query=filtered_table
        )  # type: ignore[return-value]

    def incremental(
        self,
        *,
        lt: Optional[Any] = None,
        le: Optional[Any] = None,
        gt: Optional[Any] = None,
        ge: Optional[Any] = None,
        overwrite: bool = False,
    ) -> Self:
        if self._load_ids is not None and overwrite is False:
            raise DltException(
                "The method `.incremental()` was previously called and selected `load_ids` for"
                " dataset. Use `.incremental(overwrite=True)` to update the incremental selection."
            )

        if all(arg is None for arg in [lt, le, gt, ge]):
            raise DltException(
                "The method `.incremental()` was called without arguments. "
                "Pass at least one argument for `lt`, `le`, `gt`, `ge` with `_dlt_load_id` values."
            )

        load_id_query = _list_load_ids_expr(
            loads_table_name=self.schema.loads_table_name,
            lt=lt,
            le=le,
            gt=gt,
            ge=ge,
        )
        # TODO vectorize this operation if we can assume pyarrow
        self._load_ids = set([row[0] for row in self(load_id_query).fetchall()])
        return self

    def list_load_ids(
        self,
        *,
        lt: Optional[Any] = None,
        le: Optional[Any] = None,
        gt: Optional[Any] = None,
        ge: Optional[Any] = None,
        status: Union[int, Sequence[int], None] = None,
        limit: Optional[int] = None,
    ) -> "ReadableDBAPIRelation":
        """Get the list of load id in descending order (from recent to old). This operation is eager"""
        query_expr = _list_load_ids_expr(
            loads_table_name=self.schema.loads_table_name,
            lt=lt,
            le=le,
            gt=gt,
            ge=ge,
            status=status,
            limit=limit,
        )
        return ReadableDBAPIRelation(readable_dataset=self, provided_query=query_expr.sql())

    def latest_load_id(
        self, *, status: Union[int, Sequence[int], None] = None
    ) -> ReadableDBAPIRelation:
        """Get the latest load id. This operation is eager.

        This is equivalent to `.list_load_ids(limit=1)` but returns a single value instead of
        a tuple. The implementation also includes performance improvements on larger datasets.
        """
        query_expr = _latest_load_id_expr(
            loads_table_name=self.schema.loads_table_name, status=status
        )
        return ReadableDBAPIRelation(readable_dataset=self, provided_query=query_expr.sql())

    def row_counts(
        self,
        *,
        data_tables: bool = True,
        dlt_tables: bool = False,
        table_names: List[str] = None,
    ) -> SupportsReadableRelation:
        """Returns a dictionary of table names and their row counts, returns counts of all data tables by default"""
        """If table_names is provided, only the tables in the list are returned regardless of the data_tables and dlt_tables flags"""

        selected_tables = table_names or []
        if not selected_tables:
            if data_tables:
                selected_tables += self.schema.data_table_names(seen_data_only=True)
            if dlt_tables:
                selected_tables += self.schema.dlt_table_names()

        expr = self.__row_count_expr(selected_tables)
        return self(expr.sql())

    def __row_count_expr(self, selected_tables: list[str]) -> sge.Expression:
        queries = []
        for table in selected_tables:
            sub_query = (
                sge.select(
                    sge.Literal.string(table).as_("table_name"),
                    sge.func("count", "*").as_("row_count"),
                )
                # TODO could use unqualified name and use SQLGlot dialect handling
                # when converting expression to a string in `.sql()`
                .from_(self.sql_client.make_qualified_table_name(table))
            )
            queries.append(sub_query)

        query = queries[0] if len(queries) <= 1 else sge.union(*queries, distinct=False)
        return query

    def __getitem__(self, table_name: str) -> ReadableIbisRelation:
        """access of table via dict notation"""
        return self.table(table_name)

    def __getattr__(self, table_name: str) -> ReadableIbisRelation:
        """access of table via property notation"""
        return self.table(table_name)

    def __enter__(self) -> Self:
        """Context manager used to open and close sql client and internal connection"""
        assert not self._sql_client, "context manager can't be used when sql client is initialized"
        if not self._sql_client:
            self._ensure_client_and_schema()
            # return sql_client wrapped so it will not call close on __exit__ as dataset is managing connections
            # use internal class

            NoCloseClient = type(
                "NoCloseClient",
                (self._sql_client.__class__,),
                {
                    "__exit__": (
                        lambda self, exc_type, exc_val, exc_tb: None
                    )  # No-operation: do not close the connection
                },
            )

            self._sql_client.__class__ = NoCloseClient

        self.sql_client.open_connection()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self.sql_client.close_connection()
        self._sql_client = None
