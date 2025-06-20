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

## All other Configurations
### BaseConfiguration
None


### ConfigProvidersConfiguration
None

* **`enable_airflow_secrets`** - _bool_ <br /> 
* **`enable_google_secrets`** - _bool_ <br /> 
* **`airflow_secrets`** - _[VaultProviderConfiguration](#vaultproviderconfiguration)_ <br /> 
* **`google_secrets`** - _[VaultProviderConfiguration](#vaultproviderconfiguration)_ <br /> 

### ConfigSectionContext
None

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context
* **`pipeline_name`** - _str_ <br /> 
* **`sections`** - _typing.Tuple[str, ...]_ <br /> 
* **`merge_style`** - _typing.Callable[[dlt.common.configuration.specs.config_section_context.ConfigSectionContext, dlt.common.configuration.specs.config_section_context.ConfigSectionContext], NoneType]_ <br /> 
* **`source_state_key`** - _str_ <br /> 

### ContainerInjectableContext
Base class for all configurations that may be injected from a Container. Injectable configuration is called a context

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context

### CsvFormatConfiguration
None

* **`delimiter`** - _str_ <br /> 
* **`include_header`** - _bool_ <br /> 
* **`quoting`** - _quote_all | quote_needed_ <br /> 
* **`on_error_continue`** - _bool_ <br /> 
* **`encoding`** - _str_ <br /> 

### DBTRunnerConfiguration
None

* **`package_location`** - _str_ <br /> 
* **`package_repository_branch`** - _str_ <br /> 
* **`package_repository_ssh_key`** - _str_ <br /> 
* **`package_profiles_dir`** - _str_ <br /> 
* **`package_profile_name`** - _str_ <br /> 
* **`auto_full_refresh_when_out_of_sync`** - _bool_ <br /> 
* **`package_additional_vars`** - _typing.Mapping[str, typing.Any]_ <br /> 
* **`runtime`** - _[RuntimeConfiguration](#runtimeconfiguration)_ <br /> 

### DestinationCapabilitiesContext
Injectable destination capabilities required for many Pipeline stages ie. normalize

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context
* **`preferred_loader_file_format`** - _jsonl | typed-jsonl | insert_values | parquet | csv | reference | model_ <br /> 
* **`supported_loader_file_formats`** - _typing.Sequence[typing.Literal['jsonl', 'typed-jsonl', 'insert_values', 'parquet', 'csv', 'reference', 'model']]_ <br /> 
* **`loader_file_format_selector`** - _class 'dlt.common.destination.capabilities.LoaderFileFormatSelector'_ <br /> Callable that adapts `preferred_loader_file_format` and `supported_loader_file_formats` at runtime.
* **`preferred_table_format`** - _iceberg | delta | hive | native_ <br /> 
* **`supported_table_formats`** - _typing.Sequence[typing.Literal['iceberg', 'delta', 'hive', 'native']]_ <br /> 
* **`type_mapper`** - _typing.Type[dlt.common.destination.capabilities.DataTypeMapper]_ <br /> 
* **`recommended_file_size`** - _int_ <br /> Recommended file size in bytes when writing extract/load files
* **`preferred_staging_file_format`** - _jsonl | typed-jsonl | insert_values | parquet | csv | reference | model_ <br /> 
* **`supported_staging_file_formats`** - _typing.Sequence[typing.Literal['jsonl', 'typed-jsonl', 'insert_values', 'parquet', 'csv', 'reference', 'model']]_ <br /> 
* **`format_datetime_literal`** - _typing.Callable[..., str]_ <br /> 
* **`escape_identifier`** - _typing.Callable[[str], str]_ <br /> 
* **`escape_literal`** - _typing.Callable[[typing.Any], typing.Any]_ <br /> 
* **`casefold_identifier`** - _typing.Callable[[str], str]_ <br /> Casing function applied by destination to represent case insensitive identifiers.
* **`has_case_sensitive_identifiers`** - _bool_ <br /> Tells if destination supports case sensitive identifiers
* **`decimal_precision`** - _typing.Tuple[int, int]_ <br /> 
* **`wei_precision`** - _typing.Tuple[int, int]_ <br /> 
* **`max_identifier_length`** - _int_ <br /> 
* **`max_column_identifier_length`** - _int_ <br /> 
* **`max_query_length`** - _int_ <br /> 
* **`is_max_query_length_in_bytes`** - _bool_ <br /> 
* **`max_text_data_type_length`** - _int_ <br /> 
* **`is_max_text_data_type_length_in_bytes`** - _bool_ <br /> 
* **`supports_transactions`** - _bool_ <br /> 
* **`supports_ddl_transactions`** - _bool_ <br /> 
* **`naming_convention`** - _str | typing.Type[dlt.common.normalizers.naming.naming.NamingConvention] | class 'module'_ <br /> 
* **`alter_add_multi_column`** - _bool_ <br /> 
* **`supports_create_table_if_not_exists`** - _bool_ <br /> 
* **`supports_truncate_command`** - _bool_ <br /> 
* **`schema_supports_numeric_precision`** - _bool_ <br /> 
* **`timestamp_precision`** - _int_ <br /> 
* **`max_rows_per_insert`** - _int_ <br /> 
* **`insert_values_writer_type`** - _str_ <br /> 
* **`supports_multiple_statements`** - _bool_ <br /> 
* **`supports_clone_table`** - _bool_ <br /> Destination supports CREATE TABLE ... CLONE ... statements
* **`max_table_nesting`** - _int_ <br /> Allows a destination to overwrite max_table_nesting from source
* **`supported_merge_strategies`** - _typing.Sequence[typing.Literal['delete-insert', 'scd2', 'upsert']]_ <br /> 
* **`merge_strategies_selector`** - _class 'dlt.common.destination.capabilities.MergeStrategySelector'_ <br /> 
* **`supported_replace_strategies`** - _typing.Sequence[typing.Literal['truncate-and-insert', 'insert-from-staging', 'staging-optimized']]_ <br /> 
* **`replace_strategies_selector`** - _class 'dlt.common.destination.capabilities.ReplaceStrategySelector'_ <br /> 
* **`max_parallel_load_jobs`** - _int_ <br /> The destination can set the maximum amount of parallel load jobs being executed
* **`loader_parallelism_strategy`** - _parallel | table-sequential | sequential_ <br /> The destination can override the parallelism strategy
* **`max_query_parameters`** - _int_ <br /> The maximum number of parameters that can be supplied in a single parametrized query
* **`supports_native_boolean`** - _bool_ <br /> The destination supports a native boolean type, otherwise bool columns are usually stored as integers
* **`supports_nested_types`** - _bool_ <br /> Tells if destination can write nested types, currently only destinations storing parquet are supported
* **`enforces_nulls_on_alter`** - _bool_ <br /> Tells if destination enforces null constraints when adding NOT NULL columns to existing tables
* **`sqlglot_dialect`** - _str_ <br /> The SQL dialect used by sqlglot to transpile a query to match the destination syntax.

### FilesystemConfiguration
A configuration defining filesystem location and access credentials.

    When configuration is resolved, `bucket_url` is used to extract a protocol and request corresponding credentials class.
    * s3
    * gs, gcs
    * az, abfs, adl, abfss, azure
    * file, memory
    * gdrive
    * sftp
    

* **`bucket_url`** - _str_ <br /> 
* **`credentials`** - _[AwsCredentials](#awscredentials) | [GcpServiceAccountCredentials](#gcpserviceaccountcredentials) | [AzureCredentialsWithoutDefaults](#azurecredentialswithoutdefaults) | [AzureServicePrincipalCredentialsWithoutDefaults](#azureserviceprincipalcredentialswithoutdefaults) | [AzureCredentials](#azurecredentials) | [AzureServicePrincipalCredentials](#azureserviceprincipalcredentials) | [GcpOAuthCredentials](#gcpoauthcredentials) | [SFTPCredentials](#sftpcredentials)_ <br /> 
* **`read_only`** - _bool_ <br /> Indicates read only filesystem access. Will enable caching
* **`kwargs`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to fsspec constructor ie. dict(use_ssl=True) for s3fs
* **`client_kwargs`** - _typing.Dict[str, typing.Any]_ <br /> Additional arguments passed to underlying fsspec native client ie. dict(verify="public.crt) for botocore
* **`deltalake_storage_options`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`deltalake_configuration`** - _typing.Dict[str, typing.Optional[str]]_ <br /> 

### Incremental
Adds incremental extraction for a resource by storing a cursor value in persistent state.

    The cursor could for example be a timestamp for when the record was created and you can use this to load only
    new records created since the last run of the pipeline.

    To use this the resource function should have an argument either type annotated with `Incremental` or a default `Incremental` instance.
    For example:

    >>> @dlt.resource(primary_key='id')
    >>> def some_data(created_at=dlt.sources.incremental('created_at', '2023-01-01T00:00:00Z'):
    >>>    yield from request_data(created_after=created_at.last_value)

    When the resource has a `primary_key` specified this is used to deduplicate overlapping items with the same cursor value.

    Alternatively you can use this class as transform step and add it to any resource. For example:
    >>> @dlt.resource
    >>> def some_data():
    >>>     last_value = dlt.sources.incremental.from_existing_state("some_data", "item.ts")
    >>>     ...
    >>>
    >>> r = some_data().add_step(dlt.sources.incremental("item.ts", initial_value=now, primary_key="delta"))
    >>> info = p.run(r, destination="duckdb")

    Args:
        cursor_path: The name or a JSON path to a cursor field. Uses the same names of fields as in your JSON document, before they are normalized to store in the database.
        initial_value: Optional value used for `last_value` when no state is available, e.g. on the first run of the pipeline. If not provided `last_value` will be `None` on the first run.
        last_value_func: Callable used to determine which cursor value to save in state. It is called with a list of the stored state value and all cursor vals from currently processing items. Default is `max`
        primary_key: Optional primary key used to deduplicate data. If not provided, a primary key defined by the resource will be used. Pass a tuple to define a compound key. Pass empty tuple to disable unique checks
        end_value: Optional value used to load a limited range of records between `initial_value` and `end_value`.
            Use in conjunction with `initial_value`, e.g. load records from given month `incremental(initial_value="2022-01-01T00:00:00Z", end_value="2022-02-01T00:00:00Z")`
            Note, when this is set the incremental filtering is stateless and `initial_value` always supersedes any previous incremental value in state.
        row_order: Declares that data source returns rows in descending (desc) or ascending (asc) order as defined by `last_value_func`. If row order is know, Incremental class
                    is able to stop requesting new rows by closing pipe generator. This prevents getting more data from the source. Defaults to None, which means that
                    row order is not known.
        allow_external_schedulers: If set to True, allows dlt to look for external schedulers from which it will take "initial_value" and "end_value" resulting in loading only
            specified range of data. Currently Airflow scheduler is detected: "data_interval_start" and "data_interval_end" are taken from the context and passed Incremental class.
            The values passed explicitly to Incremental will be ignored.
            Note that if logical "end date" is present then also "end_value" will be set which means that resource state is not used and exactly this range of date will be loaded
        on_cursor_value_missing: Specify what happens when the cursor_path does not exist in a record or a record has `None` at the cursor_path: raise, include, exclude
        lag: Optional value used to define a lag or attribution window. For datetime cursors, this is interpreted as seconds. For other types, it uses the + or - operator depending on the last_value_func.
        range_start: Decide whether the incremental filtering range is `open` or `closed` on the start value side. Default is `closed`.
            Setting this to `open` means that items with the same cursor value as the last value from the previous run (or `initial_value`) are excluded from the result.
            The `open` range disables deduplication logic so it can serve as an optimization when you know cursors don't overlap between pipeline runs.
        range_end: Decide whether the incremental filtering range is `open` or `closed` on the end value side. Default is `open` (exact `end_value` is excluded).
            Setting this to `closed` means that items with the exact same cursor value as the `end_value` are included in the result.
    

* **`cursor_path`** - _str_ <br /> 
* **`initial_value`** - _typing.Any_ <br /> 
* **`end_value`** - _typing.Any_ <br /> 
* **`row_order`** - _asc | desc_ <br /> 
* **`allow_external_schedulers`** - _bool_ <br /> 
* **`on_cursor_value_missing`** - _raise | include | exclude_ <br /> 
* **`lag`** - _float_ <br /> 
* **`range_start`** - _open | closed_ <br /> 
* **`range_end`** - _open | closed_ <br /> 

### ItemsNormalizerConfiguration
None

* **`add_dlt_id`** - _bool_ <br /> When true, items to be normalized will have `_dlt_id` column added with a unique ID for each row.
* **`add_dlt_load_id`** - _bool_ <br /> When true, items to be normalized will have `_dlt_load_id` column added with the current load ID.

### LanceDBClientOptions
None

* **`max_retries`** - _int_ <br /> `EmbeddingFunction` class wraps the calls for source and query embedding

### LoadPackageStateInjectableContext
None

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context
* **`storage`** - _class 'dlt.common.storages.load_package.PackageStorage'_ <br /> 
* **`load_id`** - _str_ <br /> 

### LoadStorageConfiguration
None

* **`load_volume_path`** - _str_ <br /> 
* **`delete_completed_jobs`** - _bool_ <br /> 

### LoaderConfiguration
None

* **`pool_type`** - _process | thread | none_ <br /> type of pool to run, must be set in derived configs
* **`start_method`** - _str_ <br /> start method for the pool (typically process). None is system default
* **`workers`** - _int_ <br /> how many parallel loads can be executed
* **`run_sleep`** - _float_ <br /> how long to sleep between runs with workload, seconds
* **`parallelism_strategy`** - _parallel | table-sequential | sequential_ <br /> Which parallelism strategy to use at load time
* **`raise_on_failed_jobs`** - _bool_ <br /> when True, raises on terminally failed jobs immediately
* **`raise_on_max_retries`** - _int_ <br /> When gt 0 will raise when job reaches raise_on_max_retries
* **`truncate_staging_dataset`** - _bool_ <br /> 

### NormalizeConfiguration
None

* **`pool_type`** - _process | thread | none_ <br /> type of pool to run, must be set in derived configs
* **`start_method`** - _str_ <br /> start method for the pool (typically process). None is system default
* **`workers`** - _int_ <br /> # how many threads/processes in the pool
* **`run_sleep`** - _float_ <br /> how long to sleep between runs with workload, seconds
* **`destination_capabilities`** - _[DestinationCapabilitiesContext](#destinationcapabilitiescontext)_ <br /> 
* **`json_normalizer`** - _[ItemsNormalizerConfiguration](#itemsnormalizerconfiguration)_ <br /> 
* **`parquet_normalizer`** - _[ItemsNormalizerConfiguration](#itemsnormalizerconfiguration)_ <br /> 
* **`model_normalizer`** - _[ItemsNormalizerConfiguration](#itemsnormalizerconfiguration)_ <br /> 

### NormalizeStorageConfiguration
None

* **`normalize_volume_path`** - _str_ <br /> 

### ParquetFormatConfiguration
None

* **`flavor`** - _str_ <br /> 
* **`version`** - _str_ <br /> 
* **`data_page_size`** - _int_ <br /> 
* **`timestamp_timezone`** - _str_ <br /> 
* **`row_group_size`** - _int_ <br /> 
* **`coerce_timestamps`** - _s | ms | us | ns_ <br /> 
* **`allow_truncated_timestamps`** - _bool_ <br /> 

### PipelineContext
None

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context

### PoolRunnerConfiguration
None

* **`pool_type`** - _process | thread | none_ <br /> type of pool to run, must be set in derived configs
* **`start_method`** - _str_ <br /> start method for the pool (typically process). None is system default
* **`workers`** - _int_ <br /> # how many threads/processes in the pool
* **`run_sleep`** - _float_ <br /> how long to sleep between runs with workload, seconds

### QdrantClientOptions
None

* **`port`** - _int_ <br /> 
* **`grpc_port`** - _int_ <br /> 
* **`prefer_grpc`** - _bool_ <br /> 
* **`https`** - _bool_ <br /> 
* **`prefix`** - _str_ <br /> 
* **`timeout`** - _int_ <br /> 
* **`host`** - _str_ <br /> 

### RuntimeConfiguration
None

* **`pipeline_name`** - _str_ <br /> 
* **`sentry_dsn`** - _str_ <br /> 
* **`slack_incoming_hook`** - _str_ <br /> 
* **`dlthub_telemetry`** - _bool_ <br /> 
* **`dlthub_telemetry_endpoint`** - _str_ <br /> 
* **`dlthub_telemetry_segment_write_key`** - _str_ <br /> 
* **`log_format`** - _str_ <br /> 
* **`log_level`** - _str_ <br /> 
* **`request_timeout`** - _float_ <br /> Timeout for http requests
* **`request_max_attempts`** - _int_ <br /> Max retry attempts for http clients
* **`request_backoff_factor`** - _float_ <br /> Multiplier applied to exponential retry delay for http requests
* **`request_max_retry_delay`** - _float_ <br /> Maximum delay between http request retries
* **`config_files_storage_path`** - _str_ <br /> Platform connection
* **`dlthub_dsn`** - _str_ <br /> 

### SchemaConfiguration
None

* **`naming`** - _str | typing.Type[dlt.common.normalizers.naming.naming.NamingConvention] | class 'module'_ <br /> 
* **`json_normalizer`** - _typing.Dict[str, typing.Any]_ <br /> 
* **`allow_identifier_change_on_table_with_data`** - _bool_ <br /> 
* **`use_break_path_on_normalize`** - _bool_ <br /> Post 1.4.0 to allow table and column names that contain table separators

### SchemaStorageConfiguration
None

* **`schema_volume_path`** - _str_ <br /> 
* **`import_schema_path`** - _str_ <br /> 
* **`export_schema_path`** - _str_ <br /> 
* **`external_schema_format`** - _json | yaml_ <br /> 
* **`external_schema_format_remove_defaults`** - _bool_ <br /> 

### SourceInjectableContext
A context containing the source schema, present when dlt.resource decorated function is executed

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context
* **`source`** - _class 'dlt.extract.source.DltSource'_ <br /> 

### SourceSchemaInjectableContext
A context containing the source schema, present when dlt.source/resource decorated function is executed

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context
* **`schema`** - _class 'dlt.common.schema.schema.Schema'_ <br /> 

### StateInjectableContext
None

* **`in_container`** - _bool_ <br /> Current container, if None then not injected
* **`extras_added`** - _bool_ <br /> Tells if extras were already added to this context
* **`state`** - _class 'dlt.common.pipeline.TPipelineState'_ <br /> 

### TransformationConfiguration
Configuration for a transformation

* **`buffer_max_items`** - _int_ <br /> 

### VaultProviderConfiguration
None

* **`only_secrets`** - _bool_ <br /> 
* **`only_toml_fragments`** - _bool_ <br /> 
* **`list_secrets`** - _bool_ <br /> 
