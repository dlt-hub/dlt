"""Fabric Warehouse job client implementation - based on Synapse with COPY INTO support"""

import os
from typing import ClassVar, Sequence, List, Dict
from copy import deepcopy
from textwrap import dedent
from urllib.parse import urlparse

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema import Schema
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import LoadJob
from dlt.destinations.impl.synapse.synapse import (
    SynapseClient,
    HINT_TO_SYNAPSE_ATTR,
    SynapseCopyFileLoadJob,
)
from dlt.destinations.impl.fabric.configuration import FabricClientConfiguration
from dlt.destinations.impl.fabric.sql_client import FabricSqlClient
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import (
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
)


class FabricCopyFileLoadJob(SynapseCopyFileLoadJob):
    """Custom COPY INTO job for Fabric that removes AUTO_CREATE_TABLE parameter"""

    # Class-level cache for initialized Service Principal tokens to avoid rate limiting
    _token_initialized_cache: ClassVar[Dict[str, bool]] = {}

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client
        # Get file type
        ext = os.path.splitext(self._bucket_path)[1][1:]
        if ext == "parquet":
            file_type = "PARQUET"
        else:
            raise ValueError(f"Unsupported file type `{ext}` for Fabric.")

        staging_credentials = self._staging_credentials
        assert staging_credentials is not None
        assert isinstance(
            staging_credentials,
            (AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults),
        )
        azure_storage_account_name = staging_credentials.azure_storage_account_name
        https_path = self._get_https_path(self._bucket_path, azure_storage_account_name)
        table_name = self._load_table["name"]

        # Check if this is OneLake storage
        bucket_url = urlparse(self._bucket_path)
        is_onelake = (
            bucket_url.scheme == "abfss" and "onelake.dfs.fabric.microsoft.com" in bucket_url.netloc
        )

        # For OneLake with Service Principal, initialize Fabric token first
        # Token initialization is cached to avoid rate limiting on multiple file loads
        if is_onelake and isinstance(
            staging_credentials, AzureServicePrincipalCredentialsWithoutDefaults
        ):
            self._ensure_fabric_token_initialized(staging_credentials)

        # Build WITH clause options
        with_options = [f"FILE_TYPE = '{file_type}'"]

        if not is_onelake:
            # For Azure Storage (non-OneLake), add credential
            if isinstance(staging_credentials, AzureCredentialsWithoutDefaults):
                sas_token = staging_credentials.azure_storage_sas_token
                credential = f"IDENTITY = 'Shared Access Signature', SECRET = '{sas_token}'"
            else:
                raise ConfigurationException(
                    f"Credentials of type `{type(staging_credentials)}` not supported"
                    " when loading data from staging into Fabric using `COPY INTO`."
                )
            with_options.append(f"CREDENTIAL = ({credential})")

        # Copy data from staging file into Fabric table
        with self._sql_client.begin_transaction():
            dataset_name = self._sql_client.dataset_name
            with_clause = ",\n                    ".join(with_options)
            sql = dedent(f"""
                COPY INTO [{dataset_name}].[{table_name}]
                FROM '{https_path}'
                WITH (
                    {with_clause}
                )
            """)
            self._sql_client.execute_sql(sql)

    def _ensure_fabric_token_initialized(
        self, credentials: AzureServicePrincipalCredentialsWithoutDefaults
    ) -> None:
        """
        Ensure Fabric token is initialized for Service Principal, using cache to avoid rate limiting.
        This is required before the SP can access OneLake through COPY INTO or OPENROWSET.

        Token initialization is cached per client_id to prevent excessive API calls during bulk loads.
        """
        cache_key = credentials.azure_client_id

        # Check if we've already initialized the token for this client
        if cache_key in self._token_initialized_cache:
            return

        try:
            import requests
            from azure.identity import ClientSecretCredential
        except ImportError:
            raise ConfigurationException(
                "azure-identity and requests packages are required for Service Principal "
                "authentication with Fabric OneLake. Install them with: "
                "pip install azure-identity requests"
            )

        # Create credential and get token
        cred = ClientSecretCredential(
            tenant_id=credentials.azure_tenant_id,
            client_id=credentials.azure_client_id,
            client_secret=credentials.azure_client_secret,
        )
        token = cred.get_token("https://api.fabric.microsoft.com/.default").token

        # Call Fabric API to initialize token (list workspaces as a simple test)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers)

        if resp.status_code != 200:
            raise ConfigurationException(
                "Failed to initialize Fabric token for Service Principal. "
                f"Status: {resp.status_code}, Response: {resp.text}"
            )

        # Cache successful initialization
        self._token_initialized_cache[cache_key] = True

    def _get_https_path(self, bucket_path: str, storage_account_name: str) -> str:
        """
        For OneLake paths (abfss://), convert to https://onelake.dfs.fabric.microsoft.com format.
        For regular Azure Storage (az://), use the standard blob endpoint conversion.
        """
        bucket_url = urlparse(bucket_path)

        # Check if this is a OneLake path (abfss:// scheme or onelake in the hostname)
        if bucket_url.scheme == "abfss" and "onelake.dfs.fabric.microsoft.com" in bucket_url.netloc:
            # OneLake abfss path: abfss://workspace@onelake.dfs.fabric.microsoft.com/item/path/file
            # Convert to: https://onelake.dfs.fabric.microsoft.com/workspace/item/path/file
            workspace = bucket_url.netloc.split("@")[0]
            path = bucket_url.path
            return f"https://onelake.dfs.fabric.microsoft.com/{workspace}{path}"
        else:
            # Regular Azure Storage path - use parent implementation
            return super()._get_https_path(bucket_path, storage_account_name)


class FabricClient(SynapseClient):
    """Custom job client for Fabric Warehouse that uses varchar instead of nvarchar and supports COPY INTO"""

    def __init__(
        self,
        schema: Schema,
        config: FabricClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # Call grandparent init (MsSqlJobClient) but set up Fabric-specific client
        # We'll initialize our own sql_client below
        super(SynapseClient, self).__init__(schema, config, capabilities)  # type: ignore[arg-type]
        self.config: FabricClientConfiguration = config  # type: ignore[assignment]

        # Create Fabric-specific SQL client
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient

        dataset_name, staging_dataset_name = MsSqlJobClient.create_dataset_names(schema, config)
        self.sql_client = FabricSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )

        self.active_hints = deepcopy(HINT_TO_SYNAPSE_ATTR)
        if not self.config.create_indexes:
            self.active_hints.pop("primary_key", None)
            self.active_hints.pop("unique", None)

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        """Override to use varchar instead of nvarchar for unique text columns"""
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # Fabric does not support nvarchar - use varchar with max length 900 for unique columns
            db_type = "varchar(%i)" % (c.get("precision") or 900)
        else:
            db_type = self.type_mapper.to_destination_type(c, table)

        # Don't add COLLATE clause here - let the database default handle it
        # The warehouse-level collation will be applied automatically

        hints_str = self._get_column_hints_sql(c)
        column_name = self.sql_client.escape_column_name(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c.get('nullable', True))}"

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        """Override to ensure proper table configuration for Fabric

        Note: Fabric doesn't support table indexing - it automatically manages storage.
        """
        table = super(SynapseClient, self).prepare_load_table(table_name)

        # For _dlt_version table, convert all text columns to varchar(max) to avoid
        # pyodbc binding them as legacy text/ntext types which don't support UTF-8 collations
        if table_name == self.schema.version_table_name:
            for column in table["columns"].values():
                if column.get("data_type") == "text":
                    # Override type mapper behavior for this specific table
                    # Use varchar(max) with explicit precision to avoid text/ntext binding
                    column["precision"] = 2147483647  # max value for varchar(max)

        return table

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Override to handle file loading - Fabric requires staging for parquet files

        Fabric doesn't use ADBC for direct parquet loading. Instead, it requires staging
        storage (OneLake or Azure Blob) and uses COPY INTO for efficient bulk loading.
        """
        from dlt.common.storages.load_package import ParsedLoadJobFileName
        from dlt.destinations.job_impl import ReferenceFollowupJobRequest

        # Check for reference jobs (staging files)
        if ReferenceFollowupJobRequest.is_reference_job(file_path):
            job = FabricCopyFileLoadJob(
                file_path,
                self.config.staging_config.credentials,  # type: ignore[arg-type]
            )
            return job

        # For non-reference jobs, only handle insert-values and sql files
        parsed_file = ParsedLoadJobFileName.parse(file_path)
        if parsed_file.file_format in ("insert_values", "sql"):
            # Use parent's insert job handling
            return super(SynapseClient, self).create_load_job(table, file_path, load_id, restore)

        # Parquet and other file formats require staging
        raise ValueError(
            f"Fabric requires staging storage for {parsed_file.file_format} files. "
            "Configure staging with filesystem destination (OneLake or Azure Blob Storage)."
        )

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        """Override to remove WITH clause - Fabric automatically manages storage"""
        # Fabric doesn't support WITH (HEAP) or WITH (CLUSTERED COLUMNSTORE INDEX)
        # Just return the base SQL without any WITH clause
        return SqlJobClientBase._get_table_update_sql(self, table_name, new_columns, generate_alter)
