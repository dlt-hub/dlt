import os
from typing import ClassVar, Sequence, List, Dict, Any, Optional, cast
from copy import deepcopy
from textwrap import dedent
from urllib.parse import urlparse, urlunparse

from dlt import current

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    SupportsStagingDestination,
    NewLoadJob,
)

from dlt.common.schema import TTableSchema, TColumnSchema, Schema, TColumnHint
from dlt.common.schema.utils import (
    table_schema_has_type,
    get_inherited_table_hint,
    is_complete_column,
)

from dlt.common.configuration.specs import AzureCredentialsWithoutDefaults

from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations.sql_jobs import SqlStagingCopyJob, SqlJobParams
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase, LoadJob, CopyRemoteFileLoadJob
from dlt.destinations.exceptions import LoadJobTerminalException

from dlt.destinations.impl.mssql.mssql import (
    MsSqlTypeMapper,
    MsSqlClient,
    VARCHAR_MAX_N,
    VARBINARY_MAX_N,
)

from dlt.destinations.impl.synapse import capabilities
from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient
from dlt.destinations.impl.synapse.configuration import SynapseClientConfiguration
from dlt.destinations.impl.synapse.synapse_adapter import (
    TABLE_INDEX_TYPE_HINT,
    TTableIndexType,
)


HINT_TO_SYNAPSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY NONCLUSTERED NOT ENFORCED",
    "unique": "UNIQUE NOT ENFORCED",
}
TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR: Dict[TTableIndexType, str] = {
    "heap": "HEAP",
    "clustered_columnstore_index": "CLUSTERED COLUMNSTORE INDEX",
}


class SynapseClient(MsSqlClient, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: SynapseClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: SynapseClientConfiguration = config
        self.sql_client = SynapseSqlClient(
            config.normalize_dataset_name(schema), config.credentials
        )

        self.active_hints = deepcopy(HINT_TO_SYNAPSE_ATTR)
        if not self.config.create_indexes:
            self.active_hints.pop("primary_key", None)
            self.active_hints.pop("unique", None)

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        table = self.prepare_load_table(table_name, staging=self.in_staging_mode)
        table_index_type = cast(TTableIndexType, table.get(TABLE_INDEX_TYPE_HINT))
        if self.in_staging_mode:
            final_table = self.prepare_load_table(table_name, staging=False)
            final_table_index_type = cast(TTableIndexType, final_table.get(TABLE_INDEX_TYPE_HINT))
        else:
            final_table_index_type = table_index_type
        if final_table_index_type == "clustered_columnstore_index":
            # Even if the staging table has index type "heap", we still adjust
            # the column data types to prevent errors when writing into the
            # final table that has index type "clustered_columnstore_index".
            new_columns = self._get_columstore_valid_columns(new_columns)

        _sql_result = SqlJobClientBase._get_table_update_sql(
            self, table_name, new_columns, generate_alter
        )
        if not generate_alter:
            table_index_type_attr = TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR[table_index_type]
            sql_result = [_sql_result[0] + f"\n WITH ( {table_index_type_attr} );"]
        else:
            sql_result = _sql_result
        return sql_result

    def _get_columstore_valid_columns(
        self, columns: Sequence[TColumnSchema]
    ) -> Sequence[TColumnSchema]:
        return [self._get_columstore_valid_column(c) for c in columns]

    def _get_columstore_valid_column(self, c: TColumnSchema) -> TColumnSchema:
        """
        Returns TColumnSchema that maps to a Synapse data type that can participate in a columnstore index.

        varchar(max), nvarchar(max), and varbinary(max) are replaced with
        varchar(n), nvarchar(n), and varbinary(n), respectively, where
        n equals the user-specified precision, or the maximum allowed
        value if the user did not specify a precision.
        """
        varchar_source_types = [
            sct
            for sct, dbt in MsSqlTypeMapper.sct_to_unbound_dbt.items()
            if dbt in ("varchar(max)", "nvarchar(max)")
        ]
        varbinary_source_types = [
            sct
            for sct, dbt in MsSqlTypeMapper.sct_to_unbound_dbt.items()
            if dbt == "varbinary(max)"
        ]
        if c["data_type"] in varchar_source_types and "precision" not in c:
            return {**c, **{"precision": VARCHAR_MAX_N}}
        elif c["data_type"] in varbinary_source_types and "precision" not in c:
            return {**c, **{"precision": VARBINARY_MAX_N}}
        return c

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[NewLoadJob]:
        return SqlJobClientBase._create_replace_followup_jobs(self, table_chain)

    def prepare_load_table(self, table_name: str, staging: bool = False) -> TTableSchema:
        table = super().prepare_load_table(table_name, staging)
        if staging and self.config.replace_strategy == "insert-from-staging":
            # Staging tables should always be heap tables, because "when you are
            # temporarily landing data in dedicated SQL pool, you may find that
            # using a heap table makes the overall process faster."
            table[TABLE_INDEX_TYPE_HINT] = "heap"  # type: ignore[typeddict-unknown-key]
        elif table_name in self.schema.dlt_table_names():
            # dlt tables should always be heap tables, because "for small lookup
            # tables, less than 60 million rows, consider using HEAP or clustered
            # index for faster query performance."
            table[TABLE_INDEX_TYPE_HINT] = "heap"  # type: ignore[typeddict-unknown-key]
        # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index#heap-tables
        else:
            if TABLE_INDEX_TYPE_HINT not in table:
                # If present in parent table, fetch hint from there.
                table[TABLE_INDEX_TYPE_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                    self.schema.tables, table_name, TABLE_INDEX_TYPE_HINT, allow_none=True
                )
        if table[TABLE_INDEX_TYPE_HINT] is None:  # type: ignore[typeddict-item]
            # Hint still not defined, fall back to default.
            table[TABLE_INDEX_TYPE_HINT] = self.config.default_table_index_type  # type: ignore[typeddict-unknown-key]
        return table

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        job = super().start_file_load(table, file_path, load_id)
        if not job:
            assert NewReferenceJob.is_reference_job(
                file_path
            ), "Synapse must use staging to load files"
            job = SynapseCopyFileLoadJob(
                table,
                file_path,
                self.sql_client,
                cast(AzureCredentialsWithoutDefaults, self.config.staging_config.credentials),
                self.config.staging_use_msi,
            )
        return job


class SynapseCopyFileLoadJob(CopyRemoteFileLoadJob):
    def __init__(
        self,
        table: TTableSchema,
        file_path: str,
        sql_client: SqlClientBase[Any],
        staging_credentials: Optional[AzureCredentialsWithoutDefaults] = None,
        staging_use_msi: bool = False,
    ) -> None:
        self.staging_use_msi = staging_use_msi
        super().__init__(table, file_path, sql_client, staging_credentials)

    def execute(self, table: TTableSchema, bucket_path: str) -> None:
        # get format
        ext = os.path.splitext(bucket_path)[1][1:]
        if ext == "parquet":
            if table_schema_has_type(table, "time"):
                # Synapse interprets Parquet TIME columns as bigint, resulting in
                # an incompatibility error.
                raise LoadJobTerminalException(
                    self.file_name(),
                    "Synapse cannot load TIME columns from Parquet files. Switch to direct INSERT"
                    " file format or convert `datetime.time` objects in your data to `str` or"
                    " `datetime.datetime`",
                )
            file_type = "PARQUET"

            # dlt-generated DDL statements will still create the table, but
            # enabling AUTO_CREATE_TABLE prevents a MalformedInputException.
            auto_create_table = "ON"
        else:
            raise ValueError(f"Unsupported file type {ext} for Synapse.")

        staging_credentials = self._staging_credentials
        assert staging_credentials is not None
        assert isinstance(staging_credentials, AzureCredentialsWithoutDefaults)
        azure_storage_account_name = staging_credentials.azure_storage_account_name
        https_path = self._get_https_path(bucket_path, azure_storage_account_name)
        table_name = table["name"]

        if self.staging_use_msi:
            credential = "IDENTITY = 'Managed Identity'"
        else:
            sas_token = staging_credentials.azure_storage_sas_token
            credential = f"IDENTITY = 'Shared Access Signature', SECRET = '{sas_token}'"

        # Copy data from staging file into Synapse table.
        with self._sql_client.begin_transaction():
            dataset_name = self._sql_client.dataset_name
            sql = dedent(f"""
                COPY INTO [{dataset_name}].[{table_name}]
                FROM '{https_path}'
                WITH (
                    FILE_TYPE = '{file_type}',
                    CREDENTIAL = ({credential}),
                    AUTO_CREATE_TABLE = '{auto_create_table}'
                )
            """)
            self._sql_client.execute_sql(sql)

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

    def _get_https_path(self, bucket_path: str, storage_account_name: str) -> str:
        """
        Converts a path in the form of az://<container_name>/<path> to
        https://<storage_account_name>.blob.core.windows.net/<container_name>/<path>
        as required by Synapse.
        """
        bucket_url = urlparse(bucket_path)
        # "blob" endpoint has better performance than "dfs" endoint
        # https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest#external-locations
        endpoint = "blob"
        _path = "/" + bucket_url.netloc + bucket_url.path
        https_url = bucket_url._replace(
            scheme="https",
            netloc=f"{storage_account_name}.{endpoint}.core.windows.net",
            path=_path,
        )
        return urlunparse(https_url)
