---
title: Deploy with Modal
description: How to deploy a pipeline with Modal
keywords: [how to, deploy a pipeline, Modal]
canonical: https://modal.com/blog/analytics-stack
---

# Deploy with Modal

## Introduction to Modal

[Modal](https://modal.com/) is a serverless platform designed for developers. It allows you to run and deploy code in the cloud without managing infrastructure.

With Modal, you can perform tasks like running generative models, large-scale batch jobs, and job queues, all while easily scaling compute resources.

### Modal features

- Serverless Compute: No infrastructure management; scales automatically from zero to thousands of CPUs/GPUs.
- Cloud Functions: Run Python code in the cloud instantly and scale horizontally.
- GPU/CPU Scaling: Easily attach GPUs for heavy tasks like AI model training with a single line of code.
- Web Endpoints: Expose any function as an HTTPS API endpoint quickly.
- Scheduled Jobs: Convert Python functions into scheduled tasks effortlessly.

To know more, please refer to [Modals's documentation.](https://modal.com/docs)

## Building data pipelines with dlt

dlt is an open-source Python library that allows you to declaratively load data sources into well-structured tables or datasets. It does this through automatic schema inference and evolution. The library simplifies building data pipelines by providing functionality to support the entire extract and load process.

### How does dlt integrate with Modal for pipeline orchestration?

As an example of how to set up a pipeline in Modal, we'll use the [building a cost-effective analytics stack with Modal, dlt, and dbt.](https://modal.com/blog/analytics-stack) case study.

The example demonstrates automating a workflow to load data from Postgres to Snowflake using dlt.

## How to run dlt on Modal

Here’s our dlt setup copying data from our Postgres read replica into Snowflake:

1. Run the `dlt init` CLI command to initialize the SQL database source and set up the `sql_database_pipeline.py` template.
   ```sh
   dlt init sql_database snowflake
   ```
2. Open the file and define the Modal Image you want to run `dlt` in:
   ```py
   import dlt
   import pendulum

   from sql_database import sql_database, ConnectionStringCredentials, sql_table

   import modal
   import os

   image = (
       modal.Image.debian_slim()
       .apt_install(["libpq-dev"]) # system requirement for postgres driver
       .pip_install(
           "sqlalchemy>=1.4", # how `dlt` establishes connections
           "dlt[snowflake]>=0.4.11",
           "psycopg2-binary", # postgres driver
           "dlt[parquet]",
           "psutil==6.0.0", # for `dlt` logging
           "connectorx", # creates arrow tables from database for fast data extraction
       )
   )

   app = modal.App("dlt-postgres-pipeline", image=image)
   ```

3. Wrap the provided `load_select_tables_from_database` with the Modal Function decorator, Modal Secrets containing your database credentials, and a daily cron schedule, as shown below. The function is in `sql_pipeline.py` and will be customized for your specific use case.
   ```py
   # Function to load the table from the database, scheduled to run daily
   @app.function(
       secrets=[
           modal.Secret.from_name("snowflake-secret"),
           modal.Secret.from_name("postgres-read-replica-prod"),
       ],
       # run this pipeline daily at 6:24 AM
       schedule=modal.Cron("24 6 * * *"),
       timeout=3000,
   )
   def load_select_tables_from_database(
       table: str,
       incremental_col: str,
       dev: bool = False,
   ) -> None:
       # Placeholder for future logic
       pass
   ```
   :::NOTE
   You can also configure credentials using the environment variables method supported by dlt, which automatically pulls credentials from environment variables.
   For more details on this approach, refer to the documentation here: [Docs](https://dlthub.com/docs/general-usage/credentials/setup#environment-variables).
   :::

4. Write your `dlt` pipeline:
   ```py
   # Modal Secrets are loaded as environment variables which are used here to create the SQLALchemy connection string
   pg_url = f'postgresql://{os.environ["PGUSER"]}:{os.environ["PGPASSWORD"]}@localhost:{os.environ["PGPORT"]}/{os.environ["PGDATABASE"]}'
   snowflake_url = f'snowflake://{os.environ["SNOWFLAKE_USER"]}:{os.environ["SNOWFLAKE_PASSWORD"]}@{os.environ["SNOWFLAKE_ACCOUNT"]}/{os.environ["SNOWFLAKE_DATABASE"]}'
   # Create a pipeline
   schema = "POSTGRES_DLT_DEV" if dev else "POSTGRES_DLT"
   pipeline = dlt.pipeline(
       pipeline_name="task",
       destination=dlt.destinations.snowflake(snowflake_url),
       dataset_name=schema,
       progress="log",
   )
   credentials = ConnectionStringCredentials(pg_url)
   # defines the postgres table to sync (in this case, the "task" table)
   source_1 = sql_database(credentials, backend="connectorx").with_resources("task")
   # defines which column to reference for incremental loading (i.e. only load newer rows)
   source_1.task.apply_hints(
       incremental=dlt.sources.incremental(
           "enqueued_at",
           initial_value=pendulum.datetime(2024, 7, 24, 0, 0, 0, tz="UTC"),
       )
   )

    # if there are duplicates, merge the latest values
   info = pipeline.run(source_1, write_disposition="merge")
   print(info)
   ```
> It's recommended to clean up any unused functions in sql_pipeline.py if they are not needed.

5. Run the pipeline on Modal as:
   ```sh
   modal run sql_pipeline.py
   ```

## Advanced configuration
### Modal Proxy

If your database is in a private VPN, you can use [Modal Proxy](https://modal.com/docs/reference/modal.Proxy) as a bastion server (only available to Enterprise customers). We use Modal Proxy to connect to our production read replica by attaching it to the Function definition and changing the hostname to localhost:
```py
@app.function(
    secrets=[
        modal.Secret.from_name("snowflake-secret"),
        modal.Secret.from_name("postgres-read-replica-prod"),
    ],
    schedule=modal.Cron("24 6 * * *"),
    proxy=modal.Proxy.from_name("prod-postgres-proxy", environment_name="main"),
    timeout=3000,
)
def task_pipeline(dev: bool = False) -> None:
    pg_url = f'postgresql://{os.environ["PGUSER"]}:{os.environ["PGPASSWORD"]}@localhost:{os.environ["PGPORT"]}/{os.environ["PGDATABASE"]}'
```

### Capturing deletes

One limitation of our simple approach above is that it does not capture updates or deletions of data. This isn’t a hard requirement yet for our use cases, but it appears that `dlt` does have a [Postgres CDC replication feature](../../dlt-ecosystem/verified-sources/pg_replication) that we are considering.

### Scaling out

The example above syncs one table from our Postgres data source. In practice, we are syncing multiple tables and mapping each table copy job to a single container using [Modal.starmap](https://modal.com/docs/reference/modal.Function#starmap):
```py
@app.function(timeout=3000, schedule=modal.Cron("29 11 * * *"))
def main(dev: bool = False):
    tables = [
        ("task", "enqueued_at", dev),
        ("worker", "launched_at", dev),
        ...
    ]
    list(load_table_from_database.starmap(tables))
```