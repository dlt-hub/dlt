---
title: Destination
description: Declare and configure destinations to which to load data
keywords: [destination, load data, configure destination, name destination]
---

# Destination

[Destination](glossary.md#destination) is a location in which `dlt` creates and maintains the current version of the schema and loads your data. Destinations come in various forms: databases, datalakes, vector stores or files. `dlt` deals with this variety via modules which you declare when creating a pipeline.

We maintain a set of [built-in destinations](../dlt-ecosystem/destinations/) that you can use right away.

## Declare the destination type
We recommend that you declare the destination type when creating a pipeline instance with `dlt.pipeline`. This allows the `run` method to synchronize your local pipeline state with destination and `extract` and `normalize` to create compatible load packages and schemas. You can also pass the destination to `run` and `load` methods.

* Use destination **shorthand type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::shorthand-->

Above we want to use **filesystem** built-in destination. You can use shorthand types only for built-ins.

* Use full **destination class type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class_type-->

Above we use built in **filesystem** destination by providing a class type `filesystem` from module `dlt.destinations`. You can pass [destinations from external modules](#declare-external-destination) as well.

* Import **destination class**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class-->

Above we import destination class for **filesystem** and pass it to the pipeline.

All examples above will create the same destination class with default parameters and pull required config and secret values from [configuration](credentials/configuration.md) - they are equivalent.


### Pass explicit parameters and a name to a destination
You can instantiate **destination class** yourself to configure it explicitly. When doing this you work with destinations the same way you work with [sources](source.md)
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::instance-->

Above we import and instantiate the `filesystem` destination class. We pass explicit url of the bucket and name the destination to `production_az_bucket`.

If destination is not named, its shorthand type (the Python class name) serves as a destination name. Name your destination explicitly if you need several separate configurations of destinations of the same type (i.e. you wish to maintain credentials for development, staging and production storage buckets in the same config file). Destination name is also stored in the [load info](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) and pipeline traces so use them also when you need more descriptive names (other than, for example, `filesystem`).

## Configure a destination
We recommend to pass the credentials and other required parameters to configuration via TOML files, environment variables or other [config providers](credentials/config_providers.md). This allows you, for example, to  easily switch to production destinations after deployment.

We recommend to use the [default config section layout](credentials/configuration.md#default-layout-and-default-key-lookup-during-injection) as below:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::default_layout-->

or via environment variables:
```sh
DESTINATION__FILESYSTEM__BUCKET_URL=az://dlt-azure-bucket
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=dltdata
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY="storage key"
```

For named destinations you use their names in the config section
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::name_layout-->


Note that when you use [`dlt init` command](../walkthroughs/add-a-verified-source.md) to create or add a data source, `dlt` creates a sample configuration for selected destination.

### Pass explicit credentials
You can pass credentials explicitly when creating destination class instance. This replaces the `credentials` argument in `dlt.pipeline` and `pipeline.load` methods - which is now deprecated. You can pass the required credentials object, its dictionary representation or the supported native form like below:
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_explicit-->


:::tip
You can create and pass partial credentials and `dlt` will fill the missing data. Below we pass postgres connection string but without password and expect that it will be present in environment variables (or any other [config provider](credentials/config_providers.md))
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_partial-->


<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_partial_spec-->


Please read how to use [various built in credentials types](credentials/config_specs.md).
:::

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
2. In the `pipeline.load` method - to migrate schema and load the load package.

Obviously, dlt will access the destination when you instantiate [sql_client](../dlt-ecosystem/transformations/sql.md).

:::note
`dlt` will not import the destination dependencies or access destination configuration if access is not needed. You can build multi-stage pipelines where steps are executed in separate processes or containers - the `extract` and `normalize` step do not need destination dependencies, configuration and actual connection.

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::late_destination_access-->

:::

## Create new destination
You have two ways to implement a new destination:
1. You can use `@dlt.destination` decorator and [implement a sink function](../dlt-ecosystem/destinations/destination.md). This is perfect way to implement reverse ETL destinations that push data back to REST APIs.
2. You can implement [a full destination](../walkthroughs/create-new-destination.md) where you have a full control over load jobs and schema migration.
