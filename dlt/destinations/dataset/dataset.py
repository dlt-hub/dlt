from typing import Any, Union, TYPE_CHECKING, List

from dlt.common.destination.client import JobClientBase, WithStateSync
from dlt.common.destination.dataset import SupportsReadableRelation, SupportsReadableDataset
from dlt.common.destination.reference import TDestinationReferenceArg, Destination
from dlt.common.destination.typing import TDatasetType
from dlt.common.exceptions import MissingDependencyException
from dlt.common.json import json
from dlt.common.schema import Schema
from dlt.common.schema.utils import get_root_table
from dlt.destinations.dataset.relation import ReadableDBAPIRelation
from dlt.destinations.dataset.utils import get_destination_clients
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient

if TYPE_CHECKING:
    try:
        from dlt.helpers.ibis import BaseBackend as IbisBackend
    except MissingDependencyException:
        IbisBackend = Any
else:
    IbisBackend = Any


class ReadableDBAPIDataset(SupportsReadableDataset):
    """Access to dataframes and arrowtables in the destination dataset via dbapi"""

    def __init__(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[Schema, str, None] = None,
        dataset_type: TDatasetType = "auto",
    ) -> None:
        self._destination = Destination.from_reference(destination)
        self._provided_schema = schema
        self._dataset_name = dataset_name
        self._sql_client: SqlClientBase[Any] = None
        self._schema: Schema = None
        self._dataset_type = dataset_type

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
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        if not self._sql_client:
            self._ensure_client_and_schema()
        return self._sql_client

    @property
    def destination_client(self) -> JobClientBase:
        if not self._sql_client:
            self._ensure_client_and_schema()
        return self._destination_client(self._schema)

    def _destination_client(self, schema: Schema) -> JobClientBase:
        return get_destination_clients(
            schema, destination=self._destination, destination_dataset_name=self._dataset_name
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
            destination_client = self._destination_client(self._schema)
            if isinstance(destination_client, WithSqlClient):
                self._sql_client = destination_client.sql_client
            else:
                raise Exception(
                    f"Destination {destination_client.config.destination_type} does not support"
                    " SqlClient."
                )

    def __call__(self, query: Any) -> ReadableDBAPIRelation:
        return ReadableDBAPIRelation(readable_dataset=self, provided_query=query)  # type: ignore[abstract]

    def _filter_using_root_table(
        self,
        table_name: str,
        key: str,
        values_to_include: Union[list[Any], list[ReadableDBAPIRelation], ReadableDBAPIRelation],
    ) -> ReadableDBAPIRelation:
        """Filter the root table and propagate to current table.
        
        Case 1: current table is root, then filter an return
        Case 2: current table is not root, then join on root key

        NOTE. The Ibis backend supports a 3rd case, where parent-child relationships are traversed
        Such recursive query is difficult to express in raw SQL.
        """
        # Case 1: if current table is root table, you can filter directly and exit early
        dlt_root_table = get_root_table(self.schema.tables, table_name)
        if dlt_root_table["name"] == self.table_name:
            # TODO use secure SQL interpolation
            # TODO use `sql_client` utils to use fully qualified names that respect destination
            query = f"SELECT * FROM {table_name} WHERE {key} in {values_to_include}"
            return self(query)

        # Case 2: There is a root_key to join root with current table
        # (e.g., root_key=True on Source, write_disposition="merge")
        root_key = None
        for col_name, col in self.columns_schema.items():
            if col.get("root_key") is True:
                root_key = col_name

        if root_key is not None:
            # TODO improve error handling
            raise ValueError(f"Table `{table_name}` has no `root_key` to join with the load table.")

        root_row_key = next(
            col_name for col_name, col in dlt_root_table["columns"].items() if col.get("row_key") is True
        )
        query = (
            "SELECT * "
            f"FROM {table_name}"
            f"WHERE current.{root_key} IN SELECT {root_row_key} FROM {dlt_root_table['name']})"
        )
        return self(query)    
    
    def table(self, table_name: str) -> SupportsReadableRelation:
        # we can create an ibis powered relation if ibis is available
        if table_name in self.schema.tables and self._dataset_type in ("auto", "ibis"):
            try:
                from dlt.helpers.ibis import create_unbound_ibis_table
                from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation

                unbound_table = create_unbound_ibis_table(self.sql_client, self.schema, table_name)
                return ReadableIbisRelation(  # type: ignore[abstract]
                    readable_dataset=self,
                    ibis_object=unbound_table,
                    columns_schema=self.schema.tables[table_name]["columns"],
                    table_name=table_name,
                )
            except MissingDependencyException:
                # if ibis is explicitly requested, reraise
                if self._dataset_type == "ibis":
                    raise

        # fallback to the standard dbapi relation
        return ReadableDBAPIRelation(
            readable_dataset=self,
            table_name=table_name,
        )  # type: ignore[abstract]

    def row_counts(
        self, *, data_tables: bool = True, dlt_tables: bool = False, table_names: List[str] = None
    ) -> SupportsReadableRelation:
        """Returns a dictionary of table names and their row counts, returns counts of all data tables by default"""
        """If table_names is provided, only the tables in the list are returned regardless of the data_tables and dlt_tables flags"""

        selected_tables = table_names or []
        if not selected_tables:
            if data_tables:
                selected_tables += self.schema.data_table_names(seen_data_only=True)
            if dlt_tables:
                selected_tables += self.schema.dlt_table_names()

        # Build UNION ALL query to get row counts for all selected tables
        queries = []
        for table in selected_tables:
            queries.append(
                f"SELECT '{table}' as table_name, COUNT(*) as row_count FROM"
                f" {self.sql_client.make_qualified_table_name(table)}"
            )

        query = " UNION ALL ".join(queries)

        # Execute query and build result dict
        return self(query)

    def list_load_ids(
        self, status: Union[int, list[int], None] = 0, limit: Union[int, None] = 10
    ) -> SupportsReadableRelation:
        """Return the list most recent `load_id`s in descending order.

        If no `load_id` is found, return empty list.
        """
        # TODO protect from SQL injection
        query = "SELECT load_id FROM {self.schema.loads_table_name}"

        if status is not None:
            status_list = [status] if isinstance(status, int) else status
            query += f"WHERE status IN {status_list}"
        
        query += "ORDER BY load_id DESC"
        
        if limit is not None:
            query += f"LIMIT {limit}"

        return self(query)

    def latest_load_id(self, status: Union[int, list[int], None] = 0) -> SupportsReadableRelation:
        """Return the latest `load_id`."""
        query = "SELECT max(load_id) FROM {self.schema.loads_table_name}"
        if status is not None:
            status_list = [status] if isinstance(status, int) else status
            query += f"WHERE status IN {status_list}"

        return self(query)
    
    def filter_by_load_ids(self, table_name: str, load_ids: Union[str, list[str]]) -> ReadableDBAPIRelation:
        """Filter on matching `load_ids`."""
        load_ids = [load_ids] if isinstance(load_ids, str) else load_ids
        return self._filter_using_root_table(
            table_name=table_name, key="_dlt_load_id", values_to_include=load_ids
        )

    def filter_by_load_status(
        self, table_name: str, status: Union[int, list[int], None] = 0
    ) -> ReadableDBAPIRelation:
        """Filter to rows with a specific load status."""
        if status is None:
            return self

        load_ids = [row[0] for row in self.list_load_ids(status=status).fetchall()]
        return self._filter_using_root_table(
            table_name=table_name, key="_dlt_load_id", values_to_include=load_ids
        )

    def filter_by_latest_load_id(
        self, table_name: str, status: Union[int, list[int], None] = 0
    ) -> ReadableDBAPIRelation:
        """Filter on the most recent `load_id` with a specific load status.

        If `status` is None, don't filter by status.
        """
        latest_load_id = self.latest_load_id(status=status).fetchall()[0][0]
        return self._filter_using_root_table(
            table_name=table_name, key="_dlt_load_id", values_to_include=[latest_load_id]
        )
    
    def filter_by_load_id_gt(
        self, table_name: str, load_id: str, status: Union[int, list[int], None] = 0
    ) -> ReadableDBAPIRelation:
        load_ids = [
            row[0] for row in self.list_load_ids(status=status, limit=None).fetchall()
            if row[0] > load_id
        ]
        return self._filter_using_root_table(
            table_name=table_name, key="_dlt_load_id", values_to_include=load_ids
        )

    def __getitem__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        return self.table(table_name)

    def __getattr__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return self.table(table_name)
