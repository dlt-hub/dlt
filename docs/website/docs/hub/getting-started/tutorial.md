---
title: Project tutorial
description: Using the dltHub cli commands to create and manage dltHub Project
keywords: [command line interface, cli, dlt init, dltHub, project]
---

This tutorial introduces you to dltHub Project and the essential cli commands needed to create and manage it. You will learn how to:

* initialize a new dltHub Project
* navigate the `dlt.yml` file
* add sources, destinations, and pipelines
* run pipelines using cli commands
* inspect datasets
* work with dltHub Profiles for enabling different configurations

## Prerequisites

To follow this tutorial, make sure:

- dltHub is set up according to the [installation guide](./installation.md)
- you're familiar with the [core concepts of dlt](../../reference/explainers/how-dlt-works.md)

:::tip
You can find the full list of available cli commands under cli reference
:::

## Creating a new dltHub Project

Start by creating a new folder for your project. Then, navigate to the folder in your terminal.

```sh
mkdir tutorial && cd tutorial
```

Run the following command to initialize a new dltHub Project:

```sh
# Initialize a dltHub Project named "tutorial", the name is derived from the folder name
dlt project init arrow duckdb
```

This command generates a project named `tutorial` with:
- one [pipeline](../../general-usage/pipeline)
- one Arrow source defined in `sources/arrow.py`
- one DuckDB destination
- one dataset on the DuckDB destination

:::warning
Currently, `dlt project init` only supports a limited number of sources (for example, [REST API](../../dlt-ecosystem/verified-sources/rest_api/index.md), [SQL database](../../dlt-ecosystem/verified-sources/sql_database/index.md), [filesystem](../../dlt-ecosystem/verified-sources/filesystem/index.md), etc.). To list all available sources, please use the cli command:

```sh
dlt source list-available
```
The support for other verified sources is coming soon!
:::

### The generated folder structure
After running the command, the following folder structure is created:

```sh
.
├── .dlt/                 # your dlt settings including profile settings
│   ├── dev.secrets.toml
│   └── secrets.toml
├── _data/                # local storage for your project, excluded from git
├── sources/              # your sources, contains the code for the arrow source
│   └── arrow.py
├── .gitignore
├── requirements.txt
└── dlt.yml               # the main project manifest
```

### Understanding `dlt.yml`

The `dlt.yml` file is the central configuration for your dltHub Project. It defines the pipelines, sources, and destinations. In the generated project, the file looks like this:

```yaml
profiles:
  # profiles allow you to configure different settings for different environments
  dev: {}

# your sources are the data sources you want to load from
sources:
  arrow:
    type: sources.arrow.source

# your destinations are the databases where your data will be saved
destinations:
  duckdb:
    type: duckdb

# your datasets are the datasets on your destinations where your data will go
datasets: {}

# your pipelines orchestrate data loading actions
pipelines:
  my_pipeline:
    source: arrow
    destination: duckdb
    dataset_name: my_pipeline_dataset
```

:::tip
If you do not want to start with a source, destination, and pipeline, you can simply run `dlt project init --project-name tutorial`. This will generate a project with empty sources, destinations, and pipelines.
:::

Some details about the project structure above:

* The `runtime` section is analogous to the config.toml [runtime] section and could also be omitted in this case.
* The `profiles` section is not doing much in this case. There are two implicit profiles: `dev` and `tests` that are present in any project; we will learn about profiles in more detail later.

You can reference environment variables in the `dlt.yml` file using the `{env.ENV_VARIABLE_NAME}` syntax. Additionally, dlt+ provides several [predefined project variables](../features/project/overview.md#project-settings-and-variable-substitution) that are automatically substituted during loading.

:::tip
You can find more information about the `dlt.yml` structure in the [dltHub Project section](../core-concepts/project.md).
:::

## Running the pipeline

Once the project is initialized, you can run the pipeline using:

```sh
dlt pipeline my_pipeline run
```

This command:
- Locates the pipeline named `my_pipeline` in `dlt.yml`.
- Executes it, populating the duckdb destination that [is defined to be stored](../features/project/overview.md#local-and-temporary-files-data_dir) in `_data/dev/local/duckdb.duckdb`.

:::tip
Take a look at the [Projects context](../features/project/overview.md#project-context) to learn more about how to work with nested projects and how dlt searches for the pipelines based on its name.
:::

### Inspecting the results

Use the `dlt dataset` command to interact with the dataset stored in the DuckDB destination. For example:

### Counting the loaded rows
To count rows in the dataset, run:

```sh
dlt dataset my_pipeline_dataset row-counts
```

This will show the number of rows in the items table as specified by the arrow source. Additionally, the internal dlt tables are shown.

```sh
            table_name  row_count
0                items        100
1         _dlt_version          1
2           _dlt_loads          1
3  _dlt_pipeline_state          1
```

### View data
To view the first five rows of the `items` table:

```sh
dlt dataset my_pipeline_dataset head items
```

This displays the top entries in the `items` table, enabling quick validation of the pipeline's output. The output will be something like this:

```sh
Loading first 5 rows of table items.

   id   name  age
0   0  jerry   49
1   1    jim   25
2   2   jane   46
3   3   john   48
4   4  jenny   49
```

To show more rows, use the `--limit` flag.

```sh
dlt dataset duckdb_dataset head items --limit 50
```

## Adding sources, destinations, and pipelines to your project

Adding a new entity to an existing dltHub Project is easy. You can add a new entity to your project by running the command:

```sh
dlt <entity_type> <entity_name> add
```

Depending on the entity you are adding, different options are available.
To explore all commands, refer to the cli command reference. You can also use the `--help` option to see available settings for a specific entity. For example: `dlt destination add --help`. Let's individually add a source, destination, and pipeline to a new project, replicating the default project we created in the previous chapter.

### Create an empty project

Delete all the files in the `tutorial` folder and run the following command to create an empty project:

```sh
dlt project init
```

This will create a project without any sources, destinations, datasets, or pipelines; the project will be named after the folder.

### Add all entities

Now we can add all of our entities individually. This way, we can also give them their own names, which will be useful when having multiple destinations of the same type, for example.

Add a source with:

```sh
# add a new arrow source called "my_arrow_source"
dlt source my_arrow_source add arrow
```

Add a destination:

```sh
# add a new duckdb destination called "my_duckdb_destination"
# this will also create a new dataset called "my_duckdb_destination_dataset"
dlt destination my_duckdb_destination add duckdb
```

Now we can add a pipeline that uses the source and destination we just added:

```sh
# add a new pipeline called "my_pipeline" which loads from my_arrow_source and saves to my_duckdb_destination
# we select the my_duckdb_destination_dataset with the optional flag
dlt pipeline my_pipeline add my_arrow_source my_duckdb_destination
```

### Adding the core source

You can add multiple entities using CLI commands. Let's add another source - this time, a core source such as a
([REST API](../../dlt-ecosystem/verified-sources/rest_api/index.md), [SQL database](../../dlt-ecosystem/verified-sources/sql_database/index.md), [filesystem](../../dlt-ecosystem/verified-sources/filesystem/index.md)).

Run the following command to add an SQL database source named `sql_db_1`:
```sh
# add a new sql_database source called "sql_db_1"
dlt source sql_db_1 add sql_database
```

This will add the new source to your `dlt.yml` file:

```yaml
sources:
  arrow:
    type: sources.arrow.source

  sql_db_1:
    type: sql_database
```

The corresponding credential placeholders will be added to `.dlt/secrets.toml`, but you can also define them in `dlt.yml`.
```toml
[sources.sql_db_1]
table_names = ["family", "clan"]

[sources.sql_db_1.credentials]
drivername = "mysql+pymysql"
database = "Rfam"
username = "rfamro"
host = "mysql-rfam-public.ebi.ac.uk"
port = 4497
```

## Configuration and profiles

dltHub introduces a new core concept - [Profiles](../core-concepts/profiles.md), which provides a way to manage different configurations for different environments. Let's have a look at our example project. The profiles section currently looks like this:

```yaml
profiles:
  dev: {}
```

Which means the `dev` profile is empty and by default, all the settings are inherited from the project configuration. We can inspect the current state of the project configuration by running

```sh
dlt project --profile dev config show
```

This will show the current state of the project configuration with the `dev` profile loaded. If you don't specify the `--profile` option, the `dev` profile is used by default.

### Adding a new profile

We can now create a new profile called `prod` that changes the location of the duckdb file we are loading to, as well as the log level of the project and the number of rows we are loading. Please run:

```sh
dlt profile prod add
```

And change the prod profile to the following:

```yaml
  prod:
    sources:
      my_arrow_source:
        row_count: 200
    runtime:
      log_level: INFO
    destinations:
      my_duckdb_destination:
        credentials: my_data_prod.duckdb
```

We can now inspect the prod profile. You will see that the new settings are merged with the project configuration and the `dev` profile settings.

```sh
dlt project --profile prod config show
```

### Run a pipeline with the new profile and inspect the results

Now, let's run the pipeline with the `prod` profile.

```sh
dlt pipeline --profile prod my_pipeline run
```

You can now see more output in the console due to the more verbose log level, and the number of rows loaded is now 200 instead of 100. Let's inspect our datasets for each profile (assuming you still have the duckdb database file from the previous chapter).

```sh
dlt dataset --profile dev my_duckdb_destination_dataset row-counts
dlt dataset --profile prod my_duckdb_destination_dataset row-counts
```

You will see that the number of rows loaded is now 200 instead of 100 in the prod profile.

:::tip
Profiles can also be inherited from other profiles; you can find more information in [Profiles](../core-concepts/profiles.md).
:::

### Using config files with profiles

You can also use the same configuration and secrets toml files and environment variables. You have probably noticed that your project contains more than one secrets file with the profile name prepended. These secrets files are only loaded if a given profile is active. Let's move the duckdb credentials, runtime settings, and source settings to the toml files instead of the `dlt.yml` file to demonstrate this:

First, remove all the content of the `prod` section in the `dlt.yml` file, but keep the key and the empty secrets file. We can also remove the `runtime` section from the `dlt.yml` file as well as the `credentials` key from the destination and the `row_count` key from the `sources.my_arrow_source` section. If you try to run the pipeline now, dlt will complain about missing configuration values:

```sh
dlt pipeline my_pipeline run
```

Now let's add the following to the `dev.secrets.toml` file:

```toml
[runtime]
log_level = "WARNING"

[destination.my_duckdb_destination]
credentials = "my_data.duckdb"

[sources.my_arrow_source]
row_count = 100
```

And the following to the `prod.secrets.toml` file:

```toml
[runtime]
log_level = "INFO"

[destination.my_duckdb_destination]
credentials = "my_data_prod.duckdb"

[sources.my_arrow_source]
row_count = 200
```

We can now clear the `_data` directory and repeat the steps above where you run both pipelines and inspect both datasets; you will see that the settings from the toml files are applied:

Load some data:

```sh
dlt pipeline --profile dev my_pipeline run
dlt pipeline --profile prod my_pipeline run
```

Inspect the datasets:

```sh
dlt dataset --profile dev my_duckdb_destination_dataset row-counts
dlt dataset --profile prod my_duckdb_destination_dataset row-counts
```

To locate your [loaded data](../features/project/overview.md#local-and-temporary-files-data_dir), check the `_data\{profile name}\local` directory.

