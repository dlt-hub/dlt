---
title: Configuration Reference
description: Reference of all configuration options available in dlt
keywords: [configuration, reference]
---

# Configuration Reference

This page contains a reference of most configuration options and objects available in DLT.

## Destination Configurations
### AthenaClientConfiguration
Configuration for the Athena destination

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[AwsCredentials](#awscredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`query_result_bucket`** - _str_ <br /> 
* **`athena_work_group`** - _str_ <br /> 
* **`aws_data_catalog`** - _str_ <br /> 
* **`connection_params`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`force_iceberg`** - _bool_ <br /> 
* **`table_location_layout`** - _str_ <br /> 
* **`table_properties`** - _typing.Dict[str, str]_ <br /> 
* **`db_location`** - _str_ <br /> 

### BigQueryClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[GcpServiceAccountCredentials](#gcpserviceaccountcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`location`** - _str_ <br /> 
* **`project_id`** - _str_ <br /> Note, that this is BigQuery project_id which could be different from credentials.project_id
* **`has_case_sensitive_identifiers`** - _bool_ <br /> If True then dlt expects to load data into case sensitive dataset
* **`should_set_case_sensitivity_on_new_dataset`** - _bool_ <br /> If True, dlt will set case sensitivity flag on created datasets that corresponds to naming convention
* **`http_timeout`** - _float_ <br /> connection timeout for http request to BigQuery api
* **`file_upload_timeout`** - _float_ <br /> a timeout for file upload when loading local files
* **`retry_deadline`** - _float_ <br /> How long to retry the operation in case of error, the backoff 60 s.
* **`batch_size`** - _int_ <br /> Number of rows in streaming insert batch
* **`autodetect_schema`** - _bool_ <br /> Allow BigQuery to autodetect schemas and create data tables
* **`ignore_unknown_values`** - _bool_ <br /> Ignore unknown values in the data

### ClickHouseClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[ClickHouseCredentials](#clickhousecredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`dataset_table_separator`** - _str_ <br /> Separator for dataset table names, defaults to '___', i.e. 'database.dataset___table'.
* **`table_engine_type`** - _merge_tree | shared_merge_tree | replicated_merge_tree_ <br /> The default table engine to use. Defaults to `merge_tree`. Other implemented options are `shared_merge_tree` and `replicated_merge_tree`.
* **`dataset_sentinel_table_name`** - _str_ <br /> Special table to mark dataset as existing
* **`staging_use_https`** - _bool_ <br /> Connect to the staging buckets via https

### CustomDestinationClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[CredentialsConfiguration](#credentialsconfiguration)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`destination_callable`** - _str | typing.Callable[[typing.Union[typing.Any, typing.List[typing.Any], str], dlt.common.schema.typing.TTableSchema], NoneType]_ <br /> 
* **`loader_file_format`** - _jsonl | typed-jsonl | insert_values | parquet | csv | reference | model_ <br /> 
* **`batch_size`** - _int_ <br /> 
* **`skip_dlt_columns_and_tables`** - _bool_ <br /> 
* **`max_table_nesting`** - _int_ <br /> 

### DatabricksClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[DatabricksCredentials](#databrickscredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`staging_credentials_name`** - _str_ <br /> 
* **`is_staging_external_location`** - _bool_ <br /> If true, the temporary credentials are not propagated to the COPY command
* **`staging_volume_name`** - _str_ <br /> Name of the Databricks managed volume for temporary storage, e.g., catalog_name.database_name.volume_name. Defaults to '_dlt_temp_load_volume' if not set.
* **`keep_staged_files`** - _bool_ <br /> Tells if to keep the files in internal (volume) stage

### DestinationClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[CredentialsConfiguration](#credentialsconfiguration)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`

### DestinationClientDwhConfiguration
Configuration of a destination that supports datasets/schemas

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[CredentialsConfiguration](#credentialsconfiguration)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.

### DestinationClientDwhWithStagingConfiguration
Configuration of a destination that can take data from staging destination

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[CredentialsConfiguration](#credentialsconfiguration)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.

### DestinationClientStagingConfiguration
Configuration of a staging destination, able to store files with desired `layout` at `bucket_url`.

    Also supports datasets and can act as standalone destination.
    

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[CredentialsConfiguration](#credentialsconfiguration)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`as_staging_destination`** - _bool_ <br /> 
* **`bucket_url`** - _str_ <br /> 
* **`layout`** - _str_ <br /> 

### DremioClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[DremioCredentials](#dremiocredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`staging_data_source`** - _str_ <br /> The name of the staging data source

### DuckDbClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[DuckDbCredentials](#duckdbcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`local_dir`** - _str_ <br /> 
* **`pipeline_name`** - _str_ <br /> 
* **`pipeline_working_dir`** - _str_ <br /> 
* **`legacy_db_path`** - _str_ <br /> 
* **`create_indexes`** - _bool_ <br /> 

### DummyClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[DummyClientCredentials](#dummyclientcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`loader_file_format`** - _jsonl | typed-jsonl | insert_values | parquet | csv | reference | model_ <br /> 
* **`fail_schema_update`** - _bool_ <br /> 
* **`fail_prob`** - _float_ <br /> probability of terminal fail
* **`retry_prob`** - _float_ <br /> probability of job retry
* **`completed_prob`** - _float_ <br /> probability of successful job completion
* **`exception_prob`** - _float_ <br /> probability of exception transient exception when running job
* **`timeout`** - _float_ <br /> timeout time
* **`fail_terminally_in_init`** - _bool_ <br /> raise terminal exception in job init
* **`fail_transiently_in_init`** - _bool_ <br /> raise transient exception in job init
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> truncate tables on staging destination
* **`create_followup_jobs`** - _bool_ <br /> create followup job for individual jobs
* **`fail_followup_job_creation`** - _bool_ <br /> Raise generic exception during followupjob creation
* **`fail_table_chain_followup_job_creation`** - _bool_ <br /> Raise generic exception during tablechain followupjob creation
* **`create_followup_table_chain_sql_jobs`** - _bool_ <br /> create a table chain merge job which is guaranteed to fail
* **`create_followup_table_chain_reference_jobs`** - _bool_ <br /> create table chain jobs which succeed

### FilesystemConfigurationWithLocalFiles
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[AwsCredentials](#awscredentials) | [GcpServiceAccountCredentials](#gcpserviceaccountcredentials) | [AzureCredentialsWithoutDefaults](#azurecredentialswithoutdefaults) | [AzureServicePrincipalCredentialsWithoutDefaults](#azureserviceprincipalcredentialswithoutdefaults) | [AzureCredentials](#azurecredentials) | [AzureServicePrincipalCredentials](#azureserviceprincipalcredentials) | [GcpOAuthCredentials](#gcpoauthcredentials) | [SFTPCredentials](#sftpcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`local_dir`** - _str_ <br /> 
* **`pipeline_name`** - _str_ <br /> 
* **`pipeline_working_dir`** - _str_ <br /> 
* **`legacy_db_path`** - _str_ <br /> 
* **`bucket_url`** - _str_ <br /> 
* **`read_only`** - _bool_ <br /> Indicates read only filesystem access. Will enable caching
* **`kwargs`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to fsspec constructor ie. dict(use_ssl=True) for s3fs
* **`client_kwargs`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to underlying fsspec native client ie. dict(verify="public.crt) for botocore
* **`deltalake_storage_options`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`deltalake_configuration`** - _typing.Dict[str, typing.Optional[str]]_ <br /> 

### FilesystemDestinationClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[AwsCredentials](#awscredentials) | [GcpServiceAccountCredentials](#gcpserviceaccountcredentials) | [AzureCredentialsWithoutDefaults](#azurecredentialswithoutdefaults) | [AzureServicePrincipalCredentialsWithoutDefaults](#azureserviceprincipalcredentialswithoutdefaults) | [AzureCredentials](#azurecredentials) | [AzureServicePrincipalCredentials](#azureserviceprincipalcredentials) | [GcpOAuthCredentials](#gcpoauthcredentials) | [SFTPCredentials](#sftpcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`as_staging_destination`** - _bool_ <br /> 
* **`bucket_url`** - _str_ <br /> 
* **`layout`** - _str_ <br /> 
* **`local_dir`** - _str_ <br /> 
* **`pipeline_name`** - _str_ <br /> 
* **`pipeline_working_dir`** - _str_ <br /> 
* **`legacy_db_path`** - _str_ <br /> 
* **`read_only`** - _bool_ <br /> Indicates read only filesystem access. Will enable caching
* **`kwargs`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to fsspec constructor ie. dict(use_ssl=True) for s3fs
* **`client_kwargs`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to underlying fsspec native client ie. dict(verify="public.crt) for botocore
* **`deltalake_storage_options`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`deltalake_configuration`** - _typing.Dict[str, typing.Optional[str]]_ <br /> 
* **`current_datetime`** - _class 'pendulum.datetime.DateTime' | typing.Callable[[], pendulum.datetime.DateTime]_ <br /> 
* **`extra_placeholders`** - _typing.Dict[str, typing.Union[str, int, pendulum.datetime.DateTime, typing.Callable[[str, str, str, str, str], str]]]_ <br /> 
* **`max_state_files`** - _int_ <br /> Maximum number of pipeline state files to keep; 0 or negative value disables cleanup.
* **`always_refresh_views`** - _bool_ <br /> Always refresh table scanner views by setting the newest table metadata or globbing table files

### LanceDBClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[LanceDBCredentials](#lancedbcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`local_dir`** - _str_ <br /> 
* **`pipeline_name`** - _str_ <br /> 
* **`pipeline_working_dir`** - _str_ <br /> 
* **`legacy_db_path`** - _str_ <br /> 
* **`lance_uri`** - _str_ <br /> LanceDB database URI. Defaults to local, on-disk instance.
* **`dataset_separator`** - _str_ <br /> Character for the dataset separator.
* **`options`** - _[LanceDBClientOptions](#lancedbclientoptions)_ <br /> LanceDB client options.
* **`embedding_model_provider`** - _gemini-text | bedrock-text | cohere | gte-text | imagebind | instructor | open-clip | openai | sentence-transformers | huggingface | colbert | ollama_ <br /> Embedding provider used for generating embeddings. Default is "cohere". You can find the full list of
* **`embedding_model_provider_host`** - _str_ <br /> Full host URL with protocol and port (e.g. 'http://localhost:11434'). Uses LanceDB's default if not specified, assuming the provider accepts this parameter.
* **`embedding_model`** - _str_ <br /> The model used by the embedding provider for generating embeddings.
* **`embedding_model_dimensions`** - _int_ <br /> The dimensions of the embeddings generated. In most cases it will be automatically inferred, by LanceDB,
* **`vector_field_name`** - _str_ <br /> Name of the special field to store the vector embeddings.
* **`sentinel_table_name`** - _str_ <br /> Name of the sentinel table that encapsulates datasets. Since LanceDB has no

### MotherDuckClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[MotherDuckCredentials](#motherduckcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`create_indexes`** - _bool_ <br /> 

### MsSqlClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[MsSqlCredentials](#mssqlcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`create_indexes`** - _bool_ <br /> 
* **`has_case_sensitive_identifiers`** - _bool_ <br /> 

### PostgresClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[PostgresCredentials](#postgrescredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`create_indexes`** - _bool_ <br /> 
* **`csv_format`** - _[CsvFormatConfiguration](#csvformatconfiguration)_ <br /> Optional csv format configuration

### QdrantClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[QdrantCredentials](#qdrantcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`local_dir`** - _str_ <br /> 
* **`pipeline_name`** - _str_ <br /> 
* **`pipeline_working_dir`** - _str_ <br /> 
* **`legacy_db_path`** - _str_ <br /> 
* **`qd_location`** - _str_ <br /> 
* **`qd_path`** - _str_ <br /> Persistence path for QdrantLocal. Default: `None`
* **`dataset_separator`** - _str_ <br /> 
* **`embedding_batch_size`** - _int_ <br /> 
* **`embedding_parallelism`** - _int_ <br /> 
* **`upload_batch_size`** - _int_ <br /> 
* **`upload_parallelism`** - _int_ <br /> 
* **`upload_max_retries`** - _int_ <br /> 
* **`options`** - _[QdrantClientOptions](#qdrantclientoptions)_ <br /> 
* **`model`** - _str_ <br /> 

### RedshiftClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[RedshiftCredentials](#redshiftcredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`create_indexes`** - _bool_ <br /> 
* **`csv_format`** - _[CsvFormatConfiguration](#csvformatconfiguration)_ <br /> Optional csv format configuration
* **`staging_iam_role`** - _str_ <br /> 
* **`has_case_sensitive_identifiers`** - _bool_ <br /> 

### SnowflakeClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[SnowflakeCredentials](#snowflakecredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`stage_name`** - _str_ <br /> Use an existing named stage instead of the default. Default uses the implicit table stage per table
* **`keep_staged_files`** - _bool_ <br /> Whether to keep or delete the staged files after COPY INTO succeeds
* **`csv_format`** - _[CsvFormatConfiguration](#csvformatconfiguration)_ <br /> Optional csv format configuration
* **`query_tag`** - _str_ <br /> A tag with placeholders to tag sessions executing jobs
* **`create_indexes`** - _bool_ <br /> Whether UNIQUE or PRIMARY KEY constrains should be created
* **`use_vectorized_scanner`** - _bool_ <br /> Whether to use or not use the vectorized scanner in COPY INTO

### SqlalchemyClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[SqlalchemyCredentials](#sqlalchemycredentials)_ <br /> SQLAlchemy connection string
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`create_unique_indexes`** - _bool_ <br /> Whether UNIQUE constrains should be created
* **`create_primary_keys`** - _bool_ <br /> Whether PRIMARY KEY constrains should be created
* **`engine_args`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to `sqlalchemy.create_engine`

### SynapseClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[SynapseCredentials](#synapsecredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`staging_config`** - _[DestinationClientStagingConfiguration](#destinationclientstagingconfiguration)_ <br /> configuration of the staging, if present, injected at runtime
* **`truncate_tables_on_staging_destination_before_load`** - _bool_ <br /> If dlt should truncate the tables on staging destination before loading data.
* **`create_indexes`** - _bool_ <br /> Whether `primary_key` and `unique` column hints are applied.
* **`has_case_sensitive_identifiers`** - _bool_ <br /> 
* **`default_table_index_type`** - _heap | clustered_columnstore_index_ <br /> 
* **`staging_use_msi`** - _bool_ <br /> Whether the managed identity of the Synapse workspace is used to authorize access to the staging Storage Account.

### WeaviateClientConfiguration
None

* **`destination_type`** - _str_ <br /> Type of this destination, e.g. `postgres` or `duckdb`
* **`credentials`** - _[WeaviateCredentials](#weaviatecredentials)_ <br /> Credentials for this destination
* **`destination_name`** - _str_ <br /> Name of the destination, e.g. `my_postgres` or `my_duckdb`, will be the same as destination_type if not set
* **`environment`** - _str_ <br /> Environment of the destination, e.g. `dev` or `prod`
* **`dataset_name`** - _str_ <br /> dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
* **`default_schema_name`** - _str_ <br /> name of default schema to be used to name effective dataset to load data to
* **`replace_strategy`** - _truncate-and-insert | insert-from-staging | staging-optimized_ <br /> How to handle replace disposition for this destination, uses first strategy from caps if not declared
* **`staging_dataset_name_layout`** - _str_ <br /> Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional
* **`enable_dataset_name_normalization`** - _bool_ <br /> Whether to normalize the dataset name. Affects staging dataset as well.
* **`info_tables_query_threshold`** - _int_ <br /> Threshold for information schema tables query, if exceeded tables will be filtered in code.
* **`batch_size`** - _int_ <br /> 
* **`batch_workers`** - _int_ <br /> 
* **`batch_consistency`** - _ONE | QUORUM | ALL_ <br /> 
* **`batch_retries`** - _int_ <br /> 
* **`conn_timeout`** - _float_ <br /> 
* **`read_timeout`** - _float_ <br /> 
* **`startup_period`** - _int_ <br /> 
* **`dataset_separator`** - _str_ <br /> 
* **`vectorizer`** - _str_ <br /> 
* **`module_config`** - _typing.Dict[str, typing.Dict[str, str]]_ <br /> 

## Credential Configurations
### AwsCredentials
None

* **`aws_access_key_id`** - _str_ <br /> 
* **`aws_secret_access_key`** - _str_ <br /> 
* **`aws_session_token`** - _str_ <br /> 
* **`profile_name`** - _str_ <br /> 
* **`region_name`** - _str_ <br /> 
* **`endpoint_url`** - _str_ <br /> 
* **`s3_url_style`** - _str_ <br /> Only needed for duckdb sql_client s3 access, for minio this needs to be set to path for example.

### AwsCredentialsWithoutDefaults
None

* **`aws_access_key_id`** - _str_ <br /> 
* **`aws_secret_access_key`** - _str_ <br /> 
* **`aws_session_token`** - _str_ <br /> 
* **`profile_name`** - _str_ <br /> 
* **`region_name`** - _str_ <br /> 
* **`endpoint_url`** - _str_ <br /> 
* **`s3_url_style`** - _str_ <br /> Only needed for duckdb sql_client s3 access, for minio this needs to be set to path for example.

### AzureCredentials
None

* **`azure_storage_account_name`** - _str_ <br /> 
* **`azure_account_host`** - _str_ <br /> Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net
* **`azure_storage_account_key`** - _str_ <br /> 
* **`azure_storage_sas_token`** - _str_ <br /> 
* **`azure_sas_token_permissions`** - _str_ <br /> Permissions to use when generating a SAS token. Ignored when sas token is provided directly

### AzureCredentialsBase
None

* **`azure_storage_account_name`** - _str_ <br /> 
* **`azure_account_host`** - _str_ <br /> Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net

### AzureCredentialsWithoutDefaults
Credentials for Azure Blob Storage, compatible with adlfs

* **`azure_storage_account_name`** - _str_ <br /> 
* **`azure_account_host`** - _str_ <br /> Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net
* **`azure_storage_account_key`** - _str_ <br /> 
* **`azure_storage_sas_token`** - _str_ <br /> 
* **`azure_sas_token_permissions`** - _str_ <br /> Permissions to use when generating a SAS token. Ignored when sas token is provided directly

### AzureServicePrincipalCredentials
None

* **`azure_storage_account_name`** - _str_ <br /> 
* **`azure_account_host`** - _str_ <br /> Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net
* **`azure_tenant_id`** - _str_ <br /> 
* **`azure_client_id`** - _str_ <br /> 
* **`azure_client_secret`** - _str_ <br /> 

### AzureServicePrincipalCredentialsWithoutDefaults
None

* **`azure_storage_account_name`** - _str_ <br /> 
* **`azure_account_host`** - _str_ <br /> Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net
* **`azure_tenant_id`** - _str_ <br /> 
* **`azure_client_id`** - _str_ <br /> 
* **`azure_client_secret`** - _str_ <br /> 

### ClickHouseCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> database connect to. Defaults to 'default'.
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> Database user. Defaults to 'default'.
* **`host`** - _str_ <br /> Host with running ClickHouse server.
* **`port`** - _int_ <br /> Native port ClickHouse server is bound to. Defaults to 9440.
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`http_port`** - _int_ <br /> HTTP Port to connect to ClickHouse server's HTTP interface.
* **`secure`** - _0 | 1_ <br /> Enables TLS encryption when connecting to ClickHouse Server. 0 means no encryption, 1 means encrypted.
* **`connect_timeout`** - _int_ <br /> Timeout for establishing connection. Defaults to 10 seconds.
* **`send_receive_timeout`** - _int_ <br /> Timeout for sending and receiving data. Defaults to 300 seconds.

### ConnectionStringCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 

### CredentialsConfiguration
Base class for all credentials. Credentials are configurations that may be stored only by providers supporting secrets.


### DatabricksCredentials
None

* **`catalog`** - _str_ <br /> 
* **`server_hostname`** - _str_ <br /> 
* **`http_path`** - _str_ <br /> 
* **`access_token`** - _str_ <br /> 
* **`client_id`** - _str_ <br /> 
* **`client_secret`** - _str_ <br /> 
* **`http_headers`** - _typing.Dict[str, str]_ <br /> 
* **`session_configuration`** - _typing.Dict[str, typing.Any]_ <br /> Dict of session parameters that will be passed to `databricks.sql.connect`
* **`connection_parameters`** - _typing.Dict[str, typing.Any]_ <br /> Additional keyword arguments that are passed to `databricks.sql.connect`
* **`socket_timeout`** - _int_ <br /> 
* **`user_agent_entry`** - _str_ <br /> 

### DremioCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 

### DuckDbBaseCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`read_only`** - _bool_ <br /> 

### DuckDbCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`read_only`** - _bool_ <br /> 

### DummyClientCredentials
None


### GcpCredentials
None

* **`token_uri`** - _str_ <br /> 
* **`auth_uri`** - _str_ <br /> 
* **`project_id`** - _str_ <br /> 

### GcpDefaultCredentials
None

* **`token_uri`** - _str_ <br /> 
* **`auth_uri`** - _str_ <br /> 
* **`project_id`** - _str_ <br /> 

### GcpOAuthCredentials
None

* **`client_id`** - _str_ <br /> 
* **`client_secret`** - _str_ <br /> 
* **`refresh_token`** - _str_ <br /> 
* **`scopes`** - _typing.List[str]_ <br /> 
* **`token`** - _str_ <br /> Access token
* **`token_uri`** - _str_ <br /> 
* **`auth_uri`** - _str_ <br /> 
* **`project_id`** - _str_ <br /> 
* **`client_type`** - _str_ <br /> 

### GcpOAuthCredentialsWithoutDefaults
None

* **`client_id`** - _str_ <br /> 
* **`client_secret`** - _str_ <br /> 
* **`refresh_token`** - _str_ <br /> 
* **`scopes`** - _typing.List[str]_ <br /> 
* **`token`** - _str_ <br /> Access token
* **`token_uri`** - _str_ <br /> 
* **`auth_uri`** - _str_ <br /> 
* **`project_id`** - _str_ <br /> 
* **`client_type`** - _str_ <br /> 

### GcpServiceAccountCredentials
None

* **`token_uri`** - _str_ <br /> 
* **`auth_uri`** - _str_ <br /> 
* **`project_id`** - _str_ <br /> 
* **`private_key`** - _str_ <br /> 
* **`private_key_id`** - _str_ <br /> 
* **`client_email`** - _str_ <br /> 
* **`type`** - _str_ <br /> 

### GcpServiceAccountCredentialsWithoutDefaults
None

* **`token_uri`** - _str_ <br /> 
* **`auth_uri`** - _str_ <br /> 
* **`project_id`** - _str_ <br /> 
* **`private_key`** - _str_ <br /> 
* **`private_key_id`** - _str_ <br /> 
* **`client_email`** - _str_ <br /> 
* **`type`** - _str_ <br /> 

### LanceDBCredentials
None

* **`uri`** - _str_ <br /> 
* **`api_key`** - _str_ <br /> API key for the remote connections (LanceDB cloud).
* **`embedding_model_provider_api_key`** - _str_ <br /> API key for the embedding model provider.

### MotherDuckCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`read_only`** - _bool_ <br /> 
* **`custom_user_agent`** - _str_ <br /> 

### MsSqlCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`connect_timeout`** - _int_ <br /> 
* **`driver`** - _str_ <br /> 

### OAuth2Credentials
None

* **`client_id`** - _str_ <br /> 
* **`client_secret`** - _str_ <br /> 
* **`refresh_token`** - _str_ <br /> 
* **`scopes`** - _typing.List[str]_ <br /> 
* **`token`** - _str_ <br /> Access token

### PostgresCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`connect_timeout`** - _int_ <br /> 
* **`client_encoding`** - _str_ <br /> 

### QdrantCredentials
None

* **`location`** - _str_ <br /> 
* **`api_key`** - _str_ <br /> # API key for authentication in Qdrant Cloud. Default: `None`
* **`path`** - _str_ <br /> 

### RedshiftCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`connect_timeout`** - _int_ <br /> 
* **`client_encoding`** - _str_ <br /> 

### SFTPCredentials
Credentials for SFTP filesystem, compatible with fsspec SFTP protocol.

    Authentication is attempted in the following order of priority:

        - `key_filename` may contain OpenSSH public certificate paths
          as well as regular private-key paths; when files ending in `-cert.pub` are found, they are assumed to match
          a private key, and both components will be loaded.

        - Any key found through an SSH agent: any “id_rsa”, “id_dsa”, or “id_ecdsa” key discoverable in ~/.ssh/.

        - Plain username/password authentication, if a password was provided.

        - If a private key requires a password to unlock it, and a password is provided, that password will be used to
          attempt to unlock the key.

    For more information about parameters:
    https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect
    

* **`sftp_port`** - _int_ <br /> 
* **`sftp_username`** - _str_ <br /> 
* **`sftp_password`** - _str_ <br /> 
* **`sftp_key_filename`** - _str_ <br /> 
* **`sftp_key_passphrase`** - _str_ <br /> 
* **`sftp_timeout`** - _float_ <br /> 
* **`sftp_banner_timeout`** - _float_ <br /> 
* **`sftp_auth_timeout`** - _float_ <br /> 
* **`sftp_channel_timeout`** - _float_ <br /> 
* **`sftp_allow_agent`** - _bool_ <br /> 
* **`sftp_look_for_keys`** - _bool_ <br /> 
* **`sftp_compress`** - _bool_ <br /> 
* **`sftp_gss_auth`** - _bool_ <br /> 
* **`sftp_gss_kex`** - _bool_ <br /> 
* **`sftp_gss_deleg_creds`** - _bool_ <br /> 
* **`sftp_gss_host`** - _str_ <br /> 
* **`sftp_gss_trust_dns`** - _bool_ <br /> 

### SnowflakeCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`warehouse`** - _str_ <br /> 
* **`role`** - _str_ <br /> 
* **`authenticator`** - _str_ <br /> 
* **`token`** - _str_ <br /> 
* **`private_key`** - _str_ <br /> 
* **`private_key_path`** - _str_ <br /> 
* **`private_key_passphrase`** - _str_ <br /> 
* **`application`** - _str_ <br /> 

### SqlalchemyCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`engine_args`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to `sqlalchemy.create_engine`

### SynapseCredentials
None

* **`drivername`** - _str_ <br /> 
* **`database`** - _str_ <br /> 
* **`password`** - _str_ <br /> 
* **`username`** - _str_ <br /> 
* **`host`** - _str_ <br /> 
* **`port`** - _int_ <br /> 
* **`query`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`connect_timeout`** - _int_ <br /> 
* **`driver`** - _str_ <br /> 

### WeaviateCredentials
None

* **`url`** - _str_ <br /> 
* **`api_key`** - _str_ <br /> 
* **`additional_headers`** - _typing.Dict[str, str]_ <br /> 
