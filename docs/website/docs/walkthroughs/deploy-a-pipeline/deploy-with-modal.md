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
* Use [Proxy IPs](https://modal.com/docs/guide/proxy-ips) to connect to resources in your private network
* Sync tables in parallel using [map()](https://modal.com/docs/guide/scale)


## More examples

For a practical, real-world example, check out the article ["Building a Cost-Effective Analytics Stack with Modal, dlt, and dbt"](https://modal.com/blog/analytics-stack).

This article illustrates how to automate a workflow for loading data from Postgres into Snowflake using dlt, providing valuable insights into building an efficient analytics pipeline.
