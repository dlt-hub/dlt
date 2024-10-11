---
title: Deploy with Kestra
description: How to deploy a pipeline with Kestra
keywords: [how to, deploy a pipeline, Kestra]
---

# Deploy with Kestra

## Introduction to Kestra

[Kestra](https://kestra.io/docs) is an open-source, scalable orchestration platform that enables
engineers to manage business-critical workflows declaratively in code. By applying
infrastructure as code best practices to data, process, and microservice orchestration, you
can build and manage reliable workflows.

Kestra facilitates reliable workflow management, offering advanced settings for resiliency,
triggers, real-time monitoring, and integration capabilities, making it a valuable tool for data
engineers and developers.

### Kestra features

Kestra provides a robust orchestration engine with features including:

- Workflows accessible through a user interface, event-driven
  automation, and an embedded visual studio code editor.
- It also offers embedded documentation, a live-updating topology view, and access to over 400
  plugins, enhancing its versatility.
- Kestra supports Git & CI/CD integrations, basic authentication, and benefits from community
  support.

To know more, please refer to [Kestra's documentation.](https://kestra.io/docs)

## Building data pipelines with `dlt`

**`dlt`** is an open-source Python library that allows you to declaratively load data sources
into well-structured tables or datasets. It does this through automatic schema inference and evolution.
The library simplifies building data pipelines by providing functionality to support the entire extract 
and load process.

### How does `dlt` integrate with Kestra for pipeline orchestration?

To illustrate setting up a pipeline in Kestra, we’ll be using the following example: 
[From Inbox to Insights: AI-Enhanced Email Analysis with dlt and Kestra.](https://kestra.io/blogs/2023-12-04-dlt-kestra-usage)

The example demonstrates automating a workflow to load data from Gmail to BigQuery using the `dlt`,
complemented by AI-driven summarization and sentiment analysis. You can refer to the project's
GitHub repo by clicking [here.](https://github.com/dlt-hub/dlt-kestra-demo)

:::info 
For the detailed guide, please take a look at the project's [README](https://github.com/dlt-hub/dlt-kestra-demo/blob/main/README.md) section. 
:::

Here is the summary of the steps:

1. Start by creating a virtual environment.

2. Generate an `.env` file: Inside your project repository, create an `.env` file to store
   credentials in "base64" format, prefixed with 'SECRET\_' for compatibility with Kestra's `secret()`
   function.

3. As per Kestra’s recommendation, install Docker Desktop on your machine.

4. Ensure Docker is running, then download the Docker Compose file with:

   ```sh
    curl -o docker-compose.yml \
    https://raw.githubusercontent.com/kestra-io/kestra/develop/docker-compose.yml
   ```

5. Configure Docker Compose file: 
   Edit the downloaded Docker Compose file to link the `.env` file for environment 
   variables.

   ```yaml
   kestra:
       image: kestra/kestra:develop-full
       env_file:
           - .env
   ```

6. Enable auto-restart: In your `docker-compose.yml`, set `restart: always` for both PostgreSQL and
   Kestra services to ensure they reboot automatically after a system restart.

7. Launch Kestra server: Execute `docker compose up -d` to start the server.

8. Access Kestra UI: Navigate to `http://localhost:8080/` to use the Kestra user interface.

9. Create and configure flows:

   - Go to 'Flows', then 'Create'.
   - Configure the flow files in the editor.
   - Save your flows.

10. **Understand flow components**:

    - Each flow must have an `id`, `namespace`, and a list of `tasks` with their respective `id` and
      `type`.
    - The main flow orchestrates tasks like loading data from a source to a destination.

By following these steps, you establish a structured workflow within Kestra, leveraging its powerful
features for efficient data pipeline orchestration.

:::info
For detailed information on these steps, please consult the `README.md` in the 
[dlt-kestra-demo](https://github.com/dlt-hub/dlt-kestra-demo/blob/main/README.md) repo.
:::

### Additional resources

- Ingest Zendesk data into Weaviate using `dlt` with Kestra:
  [here](https://kestra.io/blueprints/148-ingest-zendesk-data-into-weaviate-using-dlt).
- Ingest Zendesk data into DuckDb using dlt with Kestra:
  [here.](https://kestra.io/blueprints/147-ingest-zendesk-data-into-duckdb-using-dlt)
- Ingest Pipedrive CRM data to BigQuery using `dlt` and schedule it to run every hour:
  [here.](https://kestra.io/blueprints/146-ingest-pipedrive-crm-data-to-bigquery-using-dlt-and-schedule-it-to-run-every-hour)

