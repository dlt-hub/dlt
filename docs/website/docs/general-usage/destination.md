---
title: Destination
description: Declare and configure destinations to which to load data
keywords: [destination, load data, configure destination, name destination]
---

# Destination

[Destination](glossary.md#destination) is a location in which `dlt` creates and maintains the current version of the schema and loads your data. Destinations come in various forms: databases, datalakes, vector stores, or files. `dlt` deals with this variety via destination type modules which you declare when creating a pipeline.

We maintain a set of [built-in destinations](../dlt-ecosystem/destinations/) that you can use right away.

## Declare the destination type
We recommend that you declare the destination type when creating a pipeline instance with `dlt.pipeline`. This allows the `run` method to synchronize your local pipeline state with the destination and `extract` and `normalize` to create compatible load packages and schemas. You can also pass the destination to the `run` and `load` methods.

* Use destination **shorthand type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::shorthand-->

Above, we want to use the **filesystem** built-in destination. You can use shorthand types only for built-ins.

* Use a [**named destination**](#use-named-destinations) with a configured type
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::custom_destination_name-->

Above, we use a custom destination name and configure the destination type to **filesystem** using an environment variable. This approach is especially useful when switching between destinations without modifying the actual pipeline code. See details in the [section on using named destinations](#use-named-destinations-to-switch-destinations-without-changing-code).

* Use full **destination factory type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class_type-->

Above, we use the built-in **filesystem** destination by providing a factory type `filesystem` from the module `dlt.destinations`. You can implement [your own destination](../walkthroughs/create-new-destination.md) and pass this external module as well.

* Import **destination factory**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class-->

Above, we import the destination factory for **filesystem** and pass it to the pipeline.

All examples above will create the same destination class with default parameters and pull required config and secret values from [configuration](credentials/index.md) - they are equivalent.

### Pass explicit parameters and a name to a destination factory
You can instantiate the **destination factory** yourself to configure it explicitly. When doing this, you work with destinations the same way you work with [sources](source.md)
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::instance-->

Above, we import and instantiate the `filesystem` destination factory. We pass the explicit URL of the bucket and name the destination `production_az_bucket`.

If a destination is not named, its shorthand type (the Python factory name) serves as the destination name. Name your destination explicitly if you need several separate configurations for destinations of the same type (i.e., when you wish to maintain credentials for development, staging, and production storage buckets in the same config file). The destination name is also stored in the [load info](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) and pipeline traces, so use explicit names when you need more descriptive identifiers (rather than generic names like `filesystem`).


## Configure a destination
We recommend passing the credentials and other required parameters to configuration via TOML files, environment variables, or other [config providers](credentials/setup). This allows you, for example, to easily switch to production destinations after deployment.

Use the [default config section layout](credentials/advanced#organize-configuration-and-secrets-with-sections) as shown below:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::default_layout-->

Alternatively, you can use environment variables:
```sh
DESTINATION__FILESYSTEM__BUCKET_URL=az://dlt-azure-bucket
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=dltdata
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY="storage key"
```

When using destination factories, use the destination name in the config section:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::name_layout-->

For custom destination names passed to your pipeline (e.g., `destination="my_destination"`), dlt resolves the destination type from configuration. Add `destination_type` to specify which destination type to use:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::custom_name_layout-->


Note that when you use the [`dlt init` command](../walkthroughs/add-a-verified-source.md) to create or add a data source, `dlt` creates a sample configuration for the selected destination.



### Pass explicit credentials
You can pass credentials explicitly when creating a destination factory instance. This replaces the `credentials` argument in `dlt.pipeline` and `pipeline.load` methods, which is now deprecated. You can pass the required credentials object, its dictionary representation, or the supported native form like below:
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_explicit-->


:::tip
You can create and pass partial credentials, and `dlt` will fill in the missing data. Below, we pass a PostgreSQL connection string but without a password and expect that it will be present in environment variables (or any other [config provider](credentials/setup))
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_partial-->


<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_partial_spec-->


Please read how to use [various built-in credentials types](credentials/complex_types).
:::

### Inspect destination capabilities
[Destination capabilities](../walkthroughs/create-new-destination.md#3-set-the-destination-capabilities) tell `dlt` what a given destination can and cannot do. For example, it tells which file formats it can load, what the maximum query or identifier length is. Inspect destination capabilities as follows:
```py
import dlt
pipeline = dlt.pipeline("snowflake_test", destination="snowflake")
print(dict(pipeline.destination.capabilities()))
```

### Pass additional parameters and change destination capabilities
The destination factory accepts additional parameters that will be used to pre-configure it and change destination capabilities.
```py
import dlt
duck_ = dlt.destinations.duckdb(naming_convention="duck_case", recommended_file_size=120000)
print(dict(duck_.capabilities()))
```
The example above is overriding the `naming_convention` and `recommended_file_size` in the destination capabilities.


## Use named destinations

Named destinations are destinations with a custom name, the type of which can be explicitly provided or be resolved from the toml files or equivalent environment variables. There are multiple ways to use named destinations:

- Use a named destination string reference with type configured via an environment variable

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::custom_destination_name-->

- Use `dlt.destination()` with the type configured via an environment variable

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::named_destination_dlt_destination-->

- Use `dlt.destination()` with the type explicitly configured

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::named_destination_dlt_destination_explicit_type-->

For all of the above, the destination type can alternatively be configured in the `secrets.toml` file as follows:

```toml
[destination.my_destination]
destination_type = "filesystem"
```

:::note
When resolving non-module destination string references (e.g., `"bigquery"` or `"my_destination"`, not `"dlt.destinations.bigquery"`), dlt first attempts to resolve the reference as a named destination with a valid destination type configured, then falls back to shorthand type resolution.

This means that, in the examples above, if the destination type was not properly configured or was not a valid destination type, dlt would have attempted to resolve `"my_destination"` as a shorthand for a built-in type and would have eventually failed.

As another example, the following:
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::avoid_example-->
will be resolved as a DuckDB destination that is named `"bigquery"`, because a valid destination type `"duckdb"` is configured and dlt does not attempt to resolve the name `"bigquery"` as a shorthand for a built-in type!

**Exception:** If `dlt.destination()` is used and the `destination_type` is explicitly provided as an argument, dlt will skip the shorthand fallback and only attempt named destination resolution.

:::


### Configure multiple destinations of the same type

One of the benefits of named destinations is the ability to configure multiple destinations of the same type for use across different pipelines in a single script. For example, if you have two BigQuery destinations, you could define the following in the `secrets.toml` file:

```toml
[destination.my_destination]
location = "US"
[destination.my_destination.credentials]
project_id = "please set me up!"
private_key = "please set me up!"
client_email = "please set me up!"

[destination.my_other_destination]
location = "EU"
[destination.my_other_destination.credentials]
project_id = "please set me up!"
private_key = "please set me up!"
client_email = "please set me up!"
```

And use it in the pipeline code as follows:
```py
import dlt

# Configure the pipeline to use the "my_destination" BigQuery destination
my_pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination=dlt.destination("my_destination", destination_type="bigquery"),
    dataset_name='dataset_name'
)

# Configure the pipeline to use the "my_other_destination" BigQuery destination
my_other_pipeline = dlt.pipeline(
    pipeline_name='my_other_pipeline',
    destination=dlt.destination("my_other_destination", destination_type="bigquery"),
    dataset_name='dataset_name'
)
```

### Use named destinations to switch destinations without changing code

Another advantage of named destinations is environment-based switching. When you need to use different destinations across development, staging, and production environments, named destinations allow you to switch destinations by modifying only the toml configuration files without changing the pipeline code. For example, if you are developing with DuckDB, you would first have your `secrets.toml` file configured as follows:

```toml
[destination.my_destination]
destination_type = "duckdb"
```

With the pipeline code being:

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination='my_destination',
    dataset_name='dataset_name'
)
```

Then when you deploy the script, you would simply adjust your toml file with:

```toml
[destination.my_destination]
destination_type = "bigquery"
[destination.my_destination.credentials]
project_id = "please set me up!"
private_key = "please set me up!"
client_email = "please set me up!"
```

And keep the pipeline code intact.


## Access a destination
When loading data, `dlt` will access the destination in two cases:
1. At the beginning of the `run` method to sync the pipeline state with the destination (or if you call `pipeline.sync_destination` explicitly).
2. In the `pipeline.load` method - to migrate the schema and load the load package.

`dlt` will also access the destination when you instantiate [sql_client](../dlt-ecosystem/transformations/sql.md).

:::note
`dlt` will not import the destination dependencies or access destination configuration if access is not needed. You can build multi-stage pipelines where steps are executed in separate processes or containers - the `extract` and `normalize` step do not need destination dependencies, configuration, and actual connection.

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::late_destination_access-->

:::

## Control how `dlt` creates table, column, and other identifiers
`dlt` maps identifiers found in the source data into destination identifiers (i.e., table and column names) using [naming conventions](naming-convention.md) which ensure that
character set, identifier length, and other properties fit into what the given destination can handle. For example, our [default naming convention (**snake case**)](./naming-convention.md#use-default-naming-convention-snake_case) converts all names in the source (i.e., JSON document fields) into snake case, case-insensitive identifiers.

Each destination declares its preferred naming convention, support for case-sensitive identifiers, and case folding function that case-insensitive identifiers follow. For example:
1. Redshift - by default, does not support case-sensitive identifiers and converts all of them to lower case.
2. Snowflake - supports case-sensitive identifiers and considers upper-cased identifiers as case-insensitive (which is the default case folding).
3. DuckDb - does not support case-sensitive identifiers but does not case fold them, so it preserves the original casing in the information schema.
4. Athena - does not support case-sensitive identifiers and converts all of them to lower case.
5. BigQuery - all identifiers are case-sensitive; there's no case-insensitive mode available via case folding (but it can be enabled at the dataset level).

You can change the naming convention used in [many different ways](naming-convention.md#configure-naming-convention). Below, we set the preferred naming convention on the Snowflake destination to `sql_cs` to switch Snowflake to case-sensitive mode:
```py
import dlt
snow_ = dlt.destinations.snowflake(naming_convention="sql_cs_v1")
```
Setting the naming convention will impact all new schemas being created (i.e., on the first pipeline run) and will re-normalize all existing identifiers.

:::warning
`dlt` prevents re-normalization of identifiers in tables that were already created at the destination. Use [refresh](pipeline.md#refresh-pipeline-data-and-state) mode to drop the data. You can also disable this behavior via [configuration](naming-convention.md#avoid-identifier-collisions).
:::

:::note
Destinations that support case-sensitive identifiers but use a case folding convention to enable case-insensitive identifiers are configured in case-insensitive mode by default. Examples: Postgres, Snowflake, Oracle.
:::

:::warning
If you use a case-sensitive naming convention with a case-insensitive destination, `dlt` will:
1. Fail the load if it detects an identifier collision due to case folding.
2. Warn if any case folding is applied by the destination.
:::

### Enable case-sensitive identifiers support
Selected destinations may be configured so they start accepting case-sensitive identifiers. For example, it is possible to set case-sensitive collation on an **mssql** database and then tell `dlt` about it.
```py
from dlt.destinations import mssql
dest_ = mssql(has_case_sensitive_identifiers=True, naming_convention="sql_cs_v1")
```
Above, we can safely use a case-sensitive naming convention without worrying about name collisions.

You can configure the case sensitivity, **but configuring destination capabilities is not currently supported**.
```toml
[destination.mssql]
has_case_sensitive_identifiers=true
```

:::note
In most cases, setting the flag above just indicates to `dlt` that you switched the case-sensitive option on a destination. `dlt` will not do that for you. Refer to the destination documentation for details.
:::

## Create a new destination
You have two ways to implement a new destination:
1. You can use the `@dlt.destination` decorator and [implement a sink function](../dlt-ecosystem/destinations/destination.md). This is a perfect way to implement reverse ETL destinations that push data back to REST APIs.
2. You can implement [a full destination](../walkthroughs/create-new-destination.md) where you have full control over load jobs and schema migration.

