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


### How does dlt integrate with Modal for pipeline orchestration?

As an example of how to set up a pipeline in Modal, we'll use the [building a cost-effective analytics stack with Modal, dlt, and dbt.](https://modal.com/blog/analytics-stack) case study.

The article above demonstrates automating a workflow to load data from Postgres to Snowflake using dlt.

## How to run dlt on Modal

Here’s a dlt project setup to copy data from our MySQL into DuckDB:

### Step 1: Initialize source
Run the `dlt init` CLI command to initialize the SQL database source and set up the `sql_database_pipeline.py` template.
```sh
dlt init sql_database duckdb
```

### Step 2: Define Modal Image
Open the file and define the Modal Image you want to run `dlt` in:
<!--@@@DLT_SNIPPET ./deploy_snippets/deploy-with-modal-snippets.py::modal_image-->

### Step 3: Define Modal Function
A Modal Function is a containerized environment that runs tasks.
It can be scheduled (e.g., daily or on a Cron schedule), request more CPU/memory, and scale across
multiple containers.

Here’s how to include your SQL pipeline in the Modal Function:

<!--@@@DLT_SNIPPET ./deploy_snippets/deploy-with-modal-snippets.py::modal_function-->

### Step 4: Set up credentials
You can securely store your credentials using Modal secrets. When you reference secrets within a Modal script,
the defined secret is automatically set as an environment variable. dlt natively supports environment variables,
enabling seamless integration of your credentials. For example, to declare a connection string, you can define it as follows:
```text
SOURCES__SQL_DATABASE__CREDENTIALS=mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam
```
In the script above, the credentials specified are automatically utilized by dlt.
For more details, please refer to the [documentation.](../../general-usage/credentials/setup#environment-variables)

### Step 5: Run pipeline
Execute the pipeline once.
To run your pipeline a single time, use the following command:
```sh
modal run sql_pipeline.py
```

### Step 6: Deploy
If you want to deploy your pipeline on Modal for continuous execution or scheduling, use this command:
```sh
modal deploy sql_pipeline.py
```

## Advanced configuration
### Modal Proxy

If your database is in a private VPN, you can use [Modal Proxy](https://modal.com/docs/reference/modal.Proxy) as a bastion server (available for Enterprise customers).
To connect to a production read replica, attach the proxy to the function definition and change the hostname to localhost:
```py
@app.function(
    secrets=[
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
To capture updates or deleted rows from your Postgres database, consider using dlt's [Postgres CDC replication feature](../../dlt-ecosystem/verified-sources/pg_replication), which is
useful for tracking changes and deletions in the data.

### Sync Multiple Tables in Parallel
To sync multiple tables in parallel, map each table copy job to a separate container using [Modal.starmap](https://modal.com/docs/reference/modal.Function#starmap):

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