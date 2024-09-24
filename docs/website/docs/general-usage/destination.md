---
title: Destination
description: Declare and configure destinations to which to load data
keywords: [destination, load data, configure destination, name destination]
---

# Destination

[Destination](glossary.md#destination) is a location in which `dlt` creates and maintains the current version of the schema and loads your data. Destinations come in various forms: databases, datalakes, vector stores, or files. `dlt` deals with this variety via modules which you declare when creating a pipeline.

We maintain a set of [built-in destinations](../dlt-ecosystem/destinations/) that you can use right away.

## Declare the destination type
We recommend that you declare the destination type when creating a pipeline instance with `dlt.pipeline`. This allows the `run` method to synchronize your local pipeline state with the destination and `extract` and `normalize` to create compatible load packages and schemas. You can also pass the destination to the `run` and `load` methods.

* Use destination **shorthand type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::shorthand-->

Above, we want to use the **filesystem** built-in destination. You can use shorthand types only for built-ins.

* Use full **destination factory type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class_type-->

Above, we use the built-in **filesystem** destination by providing a factory type `filesystem` from the module `dlt.destinations`. You can pass [destinations from external modules](#declare-external-destination) as well.

* Import **destination factory**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class-->

Above, we import the destination factory for **filesystem** and pass it to the pipeline.

All examples above will create the same destination class with default parameters and pull required config and secret values from [configuration](credentials/index.md) - they are equivalent.


### Pass explicit parameters and a name to a destination
You can instantiate the **destination factory** yourself to configure it explicitly. When doing this, you work with destinations the same way you work with [sources](source.md)
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::instance-->

Above, we import and instantiate the `filesystem` destination factory. We pass the explicit URL of the bucket and name the destination `production_az_bucket`.

If a destination is not named, its shorthand type (the Python factory name) serves as a destination name. Name your destination explicitly if you need several separate configurations of destinations of the same type (i.e., you wish to maintain credentials for development, staging, and production storage buckets in the same config file). The destination name is also stored in the [load info](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) and pipeline traces, so use them also when you need more descriptive names (other than, for example, `filesystem`).


## Configure a destination
We recommend passing the credentials and other required parameters to configuration via TOML files, environment variables, or other [config providers](credentials/setup). This allows you, for example, to easily switch to production destinations after deployment.

We recommend using the [default config section layout](credentials/setup#structure-of-secrets.toml-and-config.toml) as below:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::default_layout-->

or via environment variables:
```sh
DESTINATION__FILESYSTEM__BUCKET_URL=az://dlt-azure-bucket
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=dltdata
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY="storage key"
```

For named destinations, you use their names in the config section
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::name_layout-->


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

### Configure multiple destinations in a pipeline
To configure multiple destinations within a pipeline, you need to provide the credentials for each destination in the "secrets.toml" file. This example demonstrates how to configure a BigQuery destination named `destination_one`:

```toml
[destination.destination_one]
location = "US"
[destination.destination_one.credentials]
project_id = "please set me up!"
private_key = "please set me up!"
client_email = "please set me up!"
```

You can then use this destination in your pipeline as follows:
```py
import dlt
from dlt.common.destination import Destination

# Configure the pipeline to use the "destination_one" BigQuery destination
pipeline = dlt.pipeline(
    pipeline_name='pipeline',
    destination=Destination.from_reference(
        "bigquery",
        destination_name="destination_one"
    ),
    dataset_name='dataset_name'
)
```
Similarly, you can assign multiple destinations to the same or different drivers.

## Access a destination
When loading data, `dlt` will access the destination in two cases:
1. At the beginning of the `run` method to sync the pipeline state with the destination (or if you call `pipeline.sync_destination` explicitly).
2. In the `pipeline.load` method - to migrate the schema and load the load package.

Obviously, `dlt` will access the destination when you instantiate [sql_client](../dlt-ecosystem/transformations/sql.md).

:::note
`dlt` will not import the destination dependencies or access destination configuration if access is not needed. You can build multi-stage pipelines where steps are executed in separate processes or containers - the `extract` and `normalize` step do not need destination dependencies, configuration, and actual connection.

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::late_destination_access-->

:::

## Control how `dlt` creates table, column, and other identifiers
`dlt` maps identifiers found in the source data into destination identifiers (i.e., table and column names) using [naming conventions](naming-convention.md) which ensure that
character set, identifier length, and other properties fit into what the given destination can handle. For example, our [default naming convention (**snake case**)](naming-convention.md#default-naming-convention-snake_case) converts all names in the source (i.e., JSON document fields) into snake case, case-insensitive identifiers.

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

:::caution
`dlt` prevents re-normalization of identifiers in tables that were already created at the destination. Use [refresh](pipeline.md#refresh-pipeline-data-and-state) mode to drop the data. You can also disable this behavior via [configuration](naming-convention.md#avoid-identifier-collisions).
:::

:::note
Destinations that support case-sensitive identifiers but use a case folding convention to enable case-insensitive identifiers are configured in case-insensitive mode by default. Examples: Postgres, Snowflake, Oracle.
:::

:::caution
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

