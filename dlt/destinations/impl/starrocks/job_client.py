import asyncio
import secrets

from dlt.destinations.job_client_impl import CopyRemoteFileLoadJob
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from typing import IO, Dict, Any, Iterator, Sequence, Iterable, Optional
from dlt.common.destination.client import JobClientBase
from dlt.common import logger
from dlt.common.schema import Schema, TTableSchema, TColumnSchema, TSchemaTables
from dlt.common.schema.typing import TColumnType, TTableSchemaColumns
from dlt.common.schema.utils import (
    pipeline_state_table,
    normalize_table_identifiers,
    is_complete_column,
    get_columns_names_with_prop,
)
from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults
import sqlalchemy as sa
import aiohttp

from dlt.common.destination.client import (
    LoadJob,
    PreparedTableSchema,
    SupportsStagingDestination
)
from dlt.common.destination.client import (
    RunnableLoadJob,
    HasFollowupJobs,
    CredentialsConfiguration,
)
from dlt.common.storages import FileStorage
from dlt.common.json import json, PY_DATETIME_DECODERS
from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient
from dlt.destinations.exceptions import (
    DatabaseTransientException
)


class StarrocksObjectStorageLoadJob(CopyRemoteFileLoadJob):
    def __init__(self, file_path: str, table: sa.Table, load_id: str, staging_credentials: Optional[CredentialsConfiguration], staging_kwargs: Optional[Dict]) -> None:
        super().__init__(file_path, staging_credentials)
        self._job_client: "StarrocksJobClient" = None
        self.table = table
        self.load_id = load_id
        self._staging_kwargs = staging_kwargs
        
    def run(self) -> None:
        _sql_client = self._job_client.sql_client
        # Copy the table to the current dataset (i.e. staging) if needed
        # This is a no-op if the table is already in the correct schema
        table = self.table.to_metadata(
            self.table.metadata, schema=_sql_client.dataset_name  # type: ignore[attr-defined]
        )

        bucket_path = (
            ReferenceFollowupJobRequest.resolve_reference(self._file_path)
            if ReferenceFollowupJobRequest.is_reference_job(self._file_path)
            else ""
        )

        file_format = bucket_path.split('.')[-1]
        if file_format != 'parquet':
            raise DatabaseTransientException(Exception('Only parquet files currently supported for load from Object Storage'))
        
        # if self._staging_iam_role:
        #     credentials = f"IAM_ROLE '{self._staging_iam_role}'"
        if self._staging_credentials and isinstance(
            self._staging_credentials, AwsCredentialsWithoutDefaults
        ):
            connection_dict = {
                'aws.s3.access_key': self._staging_credentials.aws_access_key_id,
                'aws.s3.secret_key': self._staging_credentials.aws_secret_access_key,
                'format': file_format,
                'path': bucket_path,
                'aws.s3.endpoint': self._staging_credentials.endpoint_url,
                'aws.s3.enable_ssl': str(self._staging_kwargs.get('use_ssl', 'true')).lower(),
                'aws.s3.enable_path_style_access': self._staging_kwargs.get('enable_path_style_access', 'true')
            }

            aws_connection_details = ',\n'.join([ f'"{i}" = "{connection_dict[i]}"'  for i in connection_dict ])

            stmt = f'''
                    INSERT INTO {self._job_client.sql_client.dataset_name}.{self.table.name}
                    SELECT * FROM FILES (
                        {aws_connection_details}
                    )
                    '''
            logger.info(stmt)

            with _sql_client.begin_transaction():
                _sql_client.execute_sql(stmt)
            
        else:
            raise DatabaseTransientException(Exception('Cannot Broker Load without staging credentials'))
    
class StarrocksStreamLoadJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(self, file_path: str, table: sa.Table, load_id: str) -> None:
        super().__init__(file_path)
        self._job_client: "StarrocksJobClient" = None
        self.table = table
        self.load_id = load_id

    async def stream_load(self):
        c = self._job_client.config.credentials
        # url = f'http://{c.http_host}:{c.http_port}/api/{self._job_client.sql_client.dataset_name}/{self.table.name}/_stream_load'
        url_base = f'http://{c.http_host}:{c.http_port}/api/transaction/'
        auth = aiohttp.BasicAuth(login=c.username, password=c.password)
        async with aiohttp.ClientSession(auth=auth) as session:
            headers = {
                "db": self._job_client.sql_client.dataset_name,
                "table": self.table.name,
                "format": "JSON",
                "strip_outer_array": "true"
            }
            
            for chunk in self._iter_data_item_chunks():
                label = self.load_id.replace('.', '-') + '-' + secrets.token_hex(2)
                headers['label'] = label

                async with session.post(url_base + 'begin', expect100 = True, headers = headers) as resp:
                    resp_dict = json.loads(await resp.text())
                    if resp.status != 200 or resp_dict["Status"] != "OK":
                        raise DatabaseTransientException(Exception('Failed to start Stream Load transaction'))
                
                async with session.put(url_base + 'load', expect100 = True, headers = headers, data = json.dumps(chunk)) as resp:
                    resp_dict = json.loads(await resp.text())
                    if resp.status != 200 or resp_dict["Status"] != "OK":
                        raise DatabaseTransientException(Exception('Failed to send data to Stream Load transaction'))

                async with session.post(url_base + 'commit', expect100 = True, headers = headers) as resp:
                    if resp.status != 200 or resp_dict["Status"] != "OK":
                        # print(resp_dict)
                        raise DatabaseTransientException(Exception('Failed to commit Stream Load transaction'))
        
    def _open_load_file(self) -> IO[bytes]:
        return FileStorage.open_zipsafe_ro(self._file_path, "rb")

    def _iter_data_items(self) -> Iterator[Dict[str, Any]]:
        all_cols = {col.name: None for col in self.table.columns}
        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            for line in f:
                # Decode date/time to py datetime objects. Some drivers have issues with pendulum objects
                for item in json.typed_loadb(line, decoders=PY_DATETIME_DECODERS):
                    # Fill any missing columns in item with None. Bulk insert fails when items have different keys
                    if item.keys() != all_cols.keys():
                        yield {**all_cols, **item}
                    else:
                        yield item

    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        max_rows = self._job_client.capabilities.max_rows_per_insert or math.inf
        # Limit by max query length should not be needed,
        # bulk insert generates an INSERT template with a single VALUES tuple of placeholders
        # If any dialects don't do that we need to check the str length of the query
        # TODO: Max params may not be needed. Limits only apply to placeholders in sql string (mysql/sqlite)
        max_params = self._job_client.capabilities.max_query_parameters or math.inf
        chunk: List[Dict[str, Any]] = []
        params_count = 0
        for item in self._iter_data_items():
            if len(chunk) + 1 == max_rows or params_count + len(item) > max_params:
                # Rotate chunk
                yield chunk
                chunk = []
                params_count = 0
            params_count += len(item)
            chunk.append(item)

        if chunk:
            yield chunk

    def run(self) -> None:
        _sql_client = self._job_client.sql_client
        # Copy the table to the current dataset (i.e. staging) if needed
        # This is a no-op if the table is already in the correct schema
        table = self.table.to_metadata(
            self.table.metadata, schema=_sql_client.dataset_name  # type: ignore[attr-defined]
        )

        # l = asyncio.get_running_loop()
        asyncio.run(self.stream_load())

class StarrocksJobClient(SqlalchemyJobClient, SupportsStagingDestination):
    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load

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

        pk_columns = get_columns_names_with_prop(schema_table, "primary_key")

        if pk_columns:
            # some databases (e.g. starrocks) requires primary key columns to be before other columns
            table_columns_reordered = [ self._to_column_object(schema_table['columns'][c], schema_table) for c in pk_columns ]
            table_columns_reordered.extend([ c for c in table_columns if c.name not in pk_columns ])
            table_columns = table_columns_reordered
            table_columns.append(sa.PrimaryKeyConstraint(*pk_columns))  # type: ignore[arg-type]

        return sa.Table(
            schema_table["name"],
            self.sql_client.metadata,
            *table_columns,
            extend_existing=True,
            schema=self.sql_client.dataset_name,
        )

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
                for c in table_obj.constraints:
                    if isinstance(c, sa.PrimaryKeyConstraint) and len(c.columns) > 0:
                        table_obj.kwargs['starrocks_primary_key'] = ', '.join([i.name for i in c.columns])

                self.sql_client.create_table(table_obj)
            self.sql_client.alter_table_add_columns(columns_to_add)
            self._update_schema_in_storage(self.schema)

        return schema_update

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = None
        
        if file_path.endswith(".typed-jsonl"):
            table_obj = self._to_table_object(table)
            job = StarrocksStreamLoadJob(file_path, table_obj, load_id)
        elif file_path.endswith(".parquet") or file_path.endswith(".reference"):
            table_obj = self._to_table_object(table)
            job = StarrocksObjectStorageLoadJob(
                    file_path,
                    table_obj,
                    load_id,
                    staging_credentials = self.config.staging_config.credentials,
                    staging_kwargs = self.config.staging_config.kwargs)
        
        if job is not None:
            return job

        logger.warn('Falling back to sqlalchemy')
        
        job = super().create_load_job(table, file_path, load_id, restore)
        return job
