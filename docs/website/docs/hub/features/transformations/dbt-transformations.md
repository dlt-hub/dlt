---
title: dbt model generator
description: Generate dbt models automatically
---

import { DltHubFeatureAdmonition } from '@theme/DltHubFeatureAdmonition';

<DltHubFeatureAdmonition />

The **dbt generator** creates scaffolding for dbt projects using data ingested by dlt. It analyzes the pipeline schema and automatically generates staging and fact dbt models. By integrating with dlt-configured destinations, it automates code creation and supports incremental loading, ensuring that only new records are processed in both the ingestion and transformation layers.

The dbt generator can be used as part of the local transformations feature as well as a standalone tool, enabling you to generate dbt models for any dlt pipeline.
In this context, the dbt generator will be discussed as a standalone feature, though all the information provided is also applicable when using it with local transformations.

The dbt generator works as follows:

- It automatically inspects the pipeline schema and generates a baseline dbt project, complete with staging and marts layers. The generator is able to create staging, dimensional, and fact models.

- Additionally, the dlt dbt generator lets you define relationships between the schema tables, which can be used to automatically create fact tables.

- The resulting project can be executed using the credentials already provided to the pipeline and is capable of processing incoming data incrementally.

## Adding relationship hints for fact tables

To generate fact tables, you will first need to add additional relationship hints to your pipeline. This requires ensuring that each table has a primary key defined, as relationships are based on these keys:

```py
import dlt


@dlt.resource(name="customers", primary_key="id")
def customers():
    ...

```

To add relationship hints, use the relationship adapter:

```py
import dlt
from dlthub.dbt_generator.utils import table_reference_adapter


# Example countries table
@dlt.resource(name="countries", primary_key="id", write_disposition="merge")
def countries():
    yield from [
        {"id": 1, "name": "USA"},
        {"id": 2, "name": "Germany"},
    ]


# Example companies table
@dlt.resource(name="companies", primary_key="id", write_disposition="merge")
def companies():
    yield from [
        {"id": 1, "name": "GiggleTech", "country_id": 2},
        {"id": 2, "name": "HappyHacks", "country_id": 1},
    ]


# Example customers table which references company
@dlt.resource(name="customers", primary_key="id", write_disposition="merge")
def customers():
    yield from [
        {"id": 1, "name": "Andrea", "company_id": 1},
        {"id": 2, "name": "Violetta", "company_id": 2},
        {"id": 3, "name": "Marcin", "company_id": 1},
    ]


# Example orders table which references customer
@dlt.resource(name="orders", primary_key="id", write_disposition="merge")
def orders():
    yield from [
        {"id": 1, "date": "1-2-2020", "customer_id": 1},
        {"id": 2, "date": "14-2-2020", "customer_id": 2},
        {"id": 3, "date": "18-2-2020", "customer_id": 1},
        {"id": 4, "date": "1-3-2020", "customer_id": 3},
        {"id": 5, "date": "2-3-2020", "customer_id": 3},
    ]

# Run your pipeline
p = dlt.pipeline(pipeline_name="example_shop", destination="duckdb")
p.run([customers(), companies(), orders(), countries()])

# Define relationships in your schema
table_reference_adapter(
    p,
    "companies",
    references=[
        {
            "referenced_table": "countries",
            "columns": ["country_id"],
            "referenced_columns": ["id"],
        }
    ],
)

table_reference_adapter(
    p,
    "customers",
    references=[
        {
            "referenced_table": "companies",
            "columns": ["company_id"],
            "referenced_columns": ["id"],
        }
    ],
)

table_reference_adapter(
    p,
    "orders",
    references=[
        {
            "referenced_table": "customers",
            "columns": ["customer_id"],
            "referenced_columns": ["id"],
        }
    ],
)

```

:::note
Only the relationships that the pipeline is not aware of need to be explicitly passed to the adapter, meaning you don't need to define the parent-child relationships created by dlt during the normalization stage, as it will already know about them.
:::

## Generating your baseline project

Ensure that your dlt pipeline has been run at least once locally or restored from the destination. Then, navigate to the directory where your pipeline is located and, using its name, execute the following command to create a baseline dbt project with dimensional tables for all existing pipeline tables:

```sh
dlt dbt generate <pipeline-name>
```

This command generates a new folder named `dbt_<pipeline-name>`, which contains the project with the following structure:

```sh
dbt_<pipeline-name>/
├── analysis/
├── macros/
├── models/
│   ├── marts/
│   │   ├── dim_<pipeline-name>__<table1>.sql
│   │   ├── dim_<pipeline-name>__<table2>.sql
│   │   └── dim_<pipeline-name>__<tableN>.sql
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_<pipeline-name>__<table1>.sql
│   │   ├── stg_<pipeline-name>__<table2>.sql
│   │   └── stg_<pipeline-name>__<tableN>.sql
│   ├── <pipeline-name>_dlt_active_load_ids.sql # Used for incremental processing of data
│   └── <pipeline-name>_dlt_processed_load.sql # Used for incremental processing of data
├── tests/
├── dbt_project.yml
└── requirements.txt
```

Additionally, in the directory where you ran the generator, you will find a new Python file named `run_<pipeline-name>_dbt.py`, which you can execute to run the project.


## Generating fact tables

After creating the base project with dimensional tables, you can create fact tables that will use the previously added relationship hints by running:

```sh
dlt dbt generate <pipeline-name> --fact <fact_table_name>
```

The `<fact_table_name>` you provide should be the name of the base table in which the relationships are to be found. This fact table will automatically join all related tables IDs discovered through dlt defined parent-child relationships, as well as any relationship IDs manually added through the adapter. You can then select and add additional fields in the generated model.

For the example above, we can run this for the `orders` table:

```sh
dlt dbt generate example_shop --fact orders
```

This will generate the `fact_<pipeline-name>__orders.sql` model in the `marts` folder of the dbt project:

```sh
dbt_<pipeline-name>/
├── analysis/
├── macros/
├── models/
│   ├── marts/
│   │   ├── dim_<pipeline-name>__<table1>.sql
│   │   ├── dim_<pipeline-name>__<table2>.sql
│   │   └── dim_<pipeline-name>__<tableN>.sql
│   │   └── fact_<pipeline-name>__orders.sql # <-- This is the fact table model
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_<pipeline-name>__<table1>.sql
│   │   ├── stg_<pipeline-name>__<table2>.sql
│   │   └── stg_<pipeline-name>__<tableN>.sql
│   ├── <pipeline-name>_dlt_active_load_ids.sql  # Used for incremental processing of data
│   └── <pipeline-name>_dlt_processed_load.sql  # Used for incremental processing of data
├── tests/
├── dbt_project.yml
└── requirements.txt
```

## Running your dbt project

You can run your dbt project with the previously mentioned script that was generated by `dlt dbt generate <pipeline-name>`:

```sh
python run_<pipeline_name>_dbt.py
```
This script executes your dbt transformations, loads the results into a new dataset named `<original-dataset>_transformed`, and runs the dbt tests. If needed, you can adjust the dataset name directly in the script.

If you want to see the `dbt run` command output, increase the logging level. For example:

```sh
RUNTIME__LOG_LEVEL=INFO python run_<pipeline_name>_dbt.py
```

or by setting `config.toml`:
```toml
[runtime]
log_level="INFO"
```

## Running dbt package directly

If you'd like to run your dbt package without a pipeline instance, please refer to our [dbt runner docs](../../../dlt-ecosystem/transformations/dbt/dbt.md).

## Understanding incremental processing

dlt generates unique IDs for load packages, which are stored in the `_dlt_load_id` column of all tables in the dataset. This column indicates the specific load package to which each row belongs.

The generated dbt project uses these load IDs to process data incrementally. To manage this process, the project includes two key tables that track the status of load packages:

- `<pipeline_name>_dlt_active_load_ids`: At the start of each dbt run, this table is populated with all load IDs that were successful and have not yet been processed in previous dbt runs, referred to as active load IDs. The staging tables are then populated only with rows associated with these active load IDs.
- `<pipeline_name>_dlt_processed_load_ids`: At the end of each dbt run, the active load IDs are recorded in this table, along with a timestamp. This allows you to track when each load ID was processed.

