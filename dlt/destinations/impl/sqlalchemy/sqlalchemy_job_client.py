from typing import Iterable, Optional, Sequence, List, Tuple
from contextlib import suppress

import sqlalchemy as sa

from dlt.common.json import json
from dlt.common import logger
from dlt.common import pendulum
from dlt.common.destination.client import (
    JobClientBase,
    LoadJob,
    StorageSchemaInfo,
    StateInfo,
    PreparedTableSchema,
    FollowupJobRequest,
)
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset, SqlLoadJob
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema import Schema, TTableSchema, TColumnSchema, TSchemaTables
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    C_DLT_LOADS_TABLE_LOAD_ID,
    TColumnType,
    TTableSchemaColumns,
)
from dlt.common.schema.utils import (
    pipeline_state_table,
    normalize_table_identifiers,
    is_complete_column,
    get_columns_names_with_prop,
)
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyClientConfiguration
from dlt.destinations.impl.sqlalchemy.load_jobs import (
    SqlalchemyJsonLInsertJob,
    SqlalchemyParquetInsertJob,
    SqlalchemyReplaceJob,
    SqlalchemyMergeFollowupJob,
)


class SqlalchemyJobClient(SqlJobClientWithStagingDataset):
    sql_client: SqlalchemyClient  # type: ignore[assignment]

    def __init__(
        self,
        schema: Schema,
        config: SqlalchemyClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        self.sql_client = SqlalchemyClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )

        self.schema = schema
        self.capabilities = capabilities
        self.config: SqlalchemyClientConfiguration = config
        self.type_mapper = self.capabilities.get_type_mapper(self.sql_client.dialect)

    def _to_table_object(self, schema_table: PreparedTableSchema) -> sa.Table:
        existing = self.sql_client.get_existing_table(schema_table["name"])
        if existing is not None:
            existing_col_names = set(col.name for col in existing.columns)
            new_col_names = set(schema_table["columns"])
            # Re-generate the table if columns have changed
            if existing_col_names == new_col_names:
                return existing

        # build the list of Column objects from the schema
        table_columns = [
            self._to_column_object(col, schema_table)
            for col in schema_table["columns"].values()
            if is_complete_column(col)
        ]

        if self.config.create_primary_keys:
            # if a primary key list is provided in the schema, add a PrimaryKeyConstraint.
            pk_columns = get_columns_names_with_prop(schema_table, "primary_key")
            if pk_columns:
                table_columns.append(sa.PrimaryKeyConstraint(*pk_columns))  # type: ignore[arg-type]

        return sa.Table(
            schema_table["name"],
            self.sql_client.metadata,
            *table_columns,
            extend_existing=True,
            schema=self.sql_client.dataset_name,
        )

    def _to_column_object(
        self, schema_column: TColumnSchema, table: PreparedTableSchema
    ) -> sa.Column:
        col_ = sa.Column(
            schema_column["name"],
            self.type_mapper.to_destination_type(schema_column, table),
            nullable=schema_column.get("nullable", True),
        )
        if self.config.create_unique_indexes:
            col_.unique = schema_column.get("unique", False)
        return col_

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        root_table = table_chain[0]
        if root_table["x-replace-strategy"] in ["insert-from-staging", "staging-optimized"]:  # type: ignore[typeddict-item]
            # Make sure all tables are generated in metadata before creating the job
            for table in table_chain:
                self._to_table_object(table)
            return [SqlalchemyReplaceJob.from_table_chain(table_chain, self.sql_client)]
        return []

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        # Ensure all tables exist in metadata before generating sql job
        for table in table_chain:
            self._to_table_object(table)
        return [SqlalchemyMergeFollowupJob.from_table_chain(table_chain, self.sql_client)]

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if job is not None:
            return job
        parsed_file = ParsedLoadJobFileName.parse(file_path)
        if parsed_file.file_format == "typed-jsonl":
            table_obj = self._to_table_object(table)
            return SqlalchemyJsonLInsertJob(file_path, table_obj)
        elif parsed_file.file_format == "parquet":
            table_obj = self._to_table_object(table)
            return SqlalchemyParquetInsertJob(file_path, table_obj)
        return None

    def complete_load(self, load_id: str) -> None:
        loads_table = self._to_table_object(self.schema.tables[self.schema.loads_table_name])  # type: ignore[arg-type]
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            loads_table.insert().values(
                (
                    load_id,
                    self.schema.name,
                    0,
                    now_ts,
                    self.schema.version_hash,
                )
            )
        )

    def _get_table_key(self, name: str, schema: Optional[str]) -> str:
        if schema is None:
            return name
        else:
            return schema + "." + name

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        metadata = sa.MetaData()
        for table_name in table_names:
            table_obj = self.sql_client.reflect_table(table_name, metadata)
            if table_obj is None:
                yield table_name, {}
                continue
            yield table_name, {
                col.name: {
                    "name": col.name,
                    "nullable": col.nullable,
                    **self.type_mapper.from_destination_type(col.type, None, None),
                }
                for col in table_obj.columns
            }

    def update_stored_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
    ) -> Optional[TSchemaTables]:
        # super().update_stored_schema(only_tables, expected_update)
        JobClientBase.update_stored_schema(self, only_tables, expected_update)

        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is not None:
            logger.info(
                "Schema with hash %s inserted at %s found in storage, no upgrade required",
                self.schema.stored_version_hash,
                schema_info.inserted_at,
            )
            return {}
        else:
            logger.info(
                "Schema with hash %s not found in storage, upgrading",
                self.schema.stored_version_hash,
            )

        # Create all schema tables in metadata
        for table_name in only_tables or self.schema.tables:
            self._to_table_object(self.schema.tables[table_name])  # type: ignore[arg-type]

        schema_update: TSchemaTables = {}
        tables_to_create: List[sa.Table] = []
        columns_to_add: List[sa.Column] = []

        for table_name in only_tables or self.schema.tables:
            table = self.schema.tables[table_name]
            table_obj, new_columns, exists = self.sql_client.compare_storage_table(table["name"])
            if not new_columns:  # Nothing to do, don't create table without columns
                continue
            if not exists:
                logger.debug(f"Will create table {table_name} with new columns {len(new_columns)}")
                tables_to_create.append(table_obj)
            else:
                logger.debug(f"Will ALTER table {table_name} with new columns {len(new_columns)}")
                columns_to_add.extend(new_columns)
            partial_table = self.prepare_load_table(table_name)
            new_column_names = set(col.name for col in new_columns)
            partial_table["columns"] = {
                col_name: col_def
                for col_name, col_def in partial_table["columns"].items()
                if col_name in new_column_names
            }
            schema_update[table_name] = partial_table

        with self.sql_client.begin_transaction():
            for table_obj in tables_to_create:
                self.sql_client.create_table(table_obj)
            self.sql_client.alter_table_add_columns(columns_to_add)
            self._update_schema_in_storage(self.schema)

        return schema_update

    def _delete_schema_in_storage(self, schema: Schema) -> None:
        version_table = schema.tables[schema.version_table_name]
        table_obj = self._to_table_object(version_table)  # type: ignore[arg-type]
        schema_name_col = schema.naming.normalize_identifier("schema_name")
        self.sql_client.execute_sql(
            table_obj.delete().where(table_obj.c[schema_name_col] == schema.name)
        )

    def _update_schema_in_storage(self, schema: Schema) -> None:
        version_table = schema.tables[schema.version_table_name]
        table_obj = self._to_table_object(version_table)  # type: ignore[arg-type]
        schema_str = json.dumps(schema.to_dict())

        schema_mapping = StorageSchemaInfo(
            version=schema.version,
            engine_version=schema.ENGINE_VERSION,
            schema_name=schema.name,
            version_hash=schema.stored_version_hash,
            schema=schema_str,
            inserted_at=pendulum.now(),
        ).to_normalized_mapping(schema.naming)

        self.sql_client.execute_sql(table_obj.insert().values(schema_mapping))

    def _get_stored_schema(
        self,
        version_hash: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Optional[StorageSchemaInfo]:
        version_table = self.schema.tables[self.schema.version_table_name]
        table_obj = self._to_table_object(version_table)  # type: ignore[arg-type]
        with suppress(DatabaseUndefinedRelation):
            q = sa.select(table_obj)
            if version_hash is not None:
                version_hash_col = self.schema.naming.normalize_identifier("version_hash")
                q = q.where(table_obj.c[version_hash_col] == version_hash)
            if schema_name is not None:
                schema_name_col = self.schema.naming.normalize_identifier("schema_name")
                q = q.where(table_obj.c[schema_name_col] == schema_name)
            inserted_at_col = self.schema.naming.normalize_identifier("inserted_at")
            q = q.order_by(table_obj.c[inserted_at_col].desc())
            with self.sql_client.execute_query(q) as cur:
                row = cur.fetchone()
                if row is None:
                    return None

                # TODO: Decode compressed schema str if needed
                return StorageSchemaInfo.from_normalized_mapping(
                    row._mapping, self.schema.naming  # type: ignore[attr-defined]
                )

    def get_stored_schema_by_hash(self, version_hash: str) -> Optional[StorageSchemaInfo]:
        return self._get_stored_schema(version_hash)

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Get the latest stored schema"""
        return self._get_stored_schema(schema_name=schema_name)

    def get_stored_state(self, pipeline_name: str) -> StateInfo:
        state_table = self.schema.tables.get(
            self.schema.state_table_name
        ) or normalize_table_identifiers(pipeline_state_table(), self.schema.naming)
        state_table_obj = self._to_table_object(state_table)  # type: ignore[arg-type]
        loads_table = self.schema.tables[self.schema.loads_table_name]
        loads_table_obj = self._to_table_object(loads_table)  # type: ignore[arg-type]

        c_load_id, c_dlt_load_id, c_pipeline_name, c_status = map(
            self.schema.naming.normalize_identifier,
            (C_DLT_LOADS_TABLE_LOAD_ID, C_DLT_LOAD_ID, "pipeline_name", "status"),
        )

        query = (
            sa.select(state_table_obj)
            .join(loads_table_obj, loads_table_obj.c[c_load_id] == state_table_obj.c[c_dlt_load_id])
            .where(
                sa.and_(
                    state_table_obj.c[c_pipeline_name] == pipeline_name,
                    loads_table_obj.c[c_status] == 0,
                )
            )
            .order_by(loads_table_obj.c[c_load_id].desc())
        )

        with self.sql_client.execute_query(query) as cur:
            row = cur.fetchone()
            if not row:
                return None
            mapping = dict(row._mapping)  # type: ignore[attr-defined]

        return StateInfo.from_normalized_mapping(mapping, self.schema.naming)

    def _from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        raise NotImplementedError()

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableSchema = None) -> str:
        raise NotImplementedError()
