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

To learn more, please refer to [Modal's documentation.](https://modal.com/docs)


## How to run dlt on Modal

Here’s a dlt project setup to copy data from public MySQL database into DuckDB as a destination:

### Step 1: Initialize source
Run the `dlt init` CLI command to initialize the SQL database source and set up the `sql_database_pipeline.py` template.
```sh
dlt init sql_database duckdb
```

### Step 2: Define Modal Image
Open the file and define the Modal Image you want to run `dlt` in:
```py
import modal

# Define the Modal Image
image = modal.Image.debian_slim().pip_install(
    "dlt>=1.1.0",
    "dlt[duckdb]",  # destination
    "dlt[sql_database]",  # source (MySQL)
    "dlt[parquet]",  # file format dependency
    "pymysql",  # database driver for MySQL source
)

app = modal.App("example-dlt", image=image)

# Modal Volume used to store the duckdb database file
vol = modal.Volume.from_name("duckdb-vol", create_if_missing=True)
```

### Step 3: Define Modal Function
A Modal Function is a containerized environment that runs tasks.
It can be scheduled (e.g., daily or on a Cron schedule), request more CPU/memory, and scale across
multiple containers.

Here’s how to include your SQL pipeline in the Modal Function:

```py
@app.function(volumes={"/data/": vol}, schedule=modal.Period(days=1), serialized=True)
def load_tables() -> None:
    import dlt
    import os
    from dlt.sources.sql_database import sql_database

    # Define the source database credentials; in production, you would save this as a Modal Secret which can be referenced here as an environment variable
    os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"] = (
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )
    # Load tables "family" and "genome" with minimal reflection to avoid column constraint error
    source = sql_database(reflection_level="minimal").with_resources("family", "genome")

    # Create dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="sql_to_duckdb_pipeline",
        destination=dlt.destinations.duckdb(
            "/data/rfam.duckdb"
        ),  # write the duckdb database file to this file location, which will get mounted to the Modal Volume
        dataset_name="sql_to_duckdb_pipeline_data",
        progress="log",  # output progress of the pipeline
    )

    # Run the pipeline
    load_info = pipeline.run(source, write_disposition="replace")

    # Print run statistics
    print(load_info)
```

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
* Use [Proxy IPs](https://modal.com/docs/guide/proxy-ips) to connect to resources in your private network
* Sync tables in parallel using [map()](https://modal.com/docs/guide/scale)


## More examples

For a practical, real-world example, check out the article ["Building a Cost-Effective Analytics Stack with Modal, dlt, and dbt"](https://modal.com/blog/analytics-stack).

This article illustrates how to automate a workflow for loading data from Postgres into Snowflake using dlt, providing valuable insights into building an efficient analytics pipeline.
