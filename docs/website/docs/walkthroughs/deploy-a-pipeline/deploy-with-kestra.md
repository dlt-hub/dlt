---
title: Deploy with Kestra
description: How to deploy a pipeline with Kestra
keywords: [how to, deploy a pipeline, Kestra]
---

# Deploy with Kestra

## Introduction to Kestra

[Kestra](https://kestra.io/docs) is an open-source, **scalable orchestration platform** that enables
all engineers to manage **business-critical workflows** declaratively in code. By
applying Infrastructure as Code best practices to data, process, and microservice orchestration, you
can build reliable workflows and manage them.

Kestra facilitates reliable workflow management, offering advanced settings for resiliency,
triggers, real-time monitoring, and integration capabilities, making it a valuable tool for data
engineers and developers.

### Kestra features

Kestra, as an open-source platform, provides a robust orchestration engine with features including:

- Declarative workflows are accessible as code and through a user interface, event-driven
  automation, and an embedded Visual Studio code editor.
- It also offers embedded documentation, a live-updating topology view, and access to over 400
  plugins, enhancing its versatility.
- Kestra supports Git & CI/CD integrations, basic authentication, and benefits from community
  support.

To know more, please refer to [Kestra's documentation.](https://kestra.io/pricing)

## Building Data Pipelines with `dlt`

**`dlt`** is an open-source Python library that allows you to declaratively **load** data sources
into well-structured tables or datasets through automatic schema inference and evolution. It
simplifies building data pipelines by providing functionality to support the entire extract and load
process.

### How does `dlt` integrate with Kestra for pipeline orchestration?

To illustrate setting up a pipeline in Kestra, we’ll be using
[this example.](https://kestra.io/blogs/2023-12-04-dlt-kestra-usage)

It demonstrates automating a workflow to load data from Gmail to BigQuery using the `dlt`,
complemented by AI-driven summarization and sentiment analysis. You can refer to the project's
github repo here: [Github repo.](https://github.com/dlt-hub/dlt-kestra-demo)

:::info 
For the detailed guide, refer to the project's README section for project setup. 
:::

Here is the summary of the steps:

1. Start by creating a virtual environment.

1. Generate an `.env` File\*\*: Inside your project repository, create an `.env` file to store
   credentials in base64 format, prefixed with 'SECRET\_' for compatibility with Kestra's `secret()`
   function.

1. As per Kestra’s recommendation, install the docker desktop on your machine.

1. Download Docker Compose File: Ensure Docker is running, then download the Docker Compose file
   with:

   ```python
    curl -o docker-compose.yml \
    https://raw.githubusercontent.com/kestra-io/kestra/develop/docker-compose.yml
   ```

1. Configure Docker Compose File: Edit the downloaded Docker Compose file to link the `.env` file
   for environment variables.

   ```python
   kestra:
       image: kestra/kestra:develop-full
       env_file:
           - .env
   ```

1. Enable Auto-Restart: In your `docker-compose.yml`, set `restart: always` for both postgres and
   kestra services to ensure they reboot automatically after a system restart.

1. Launch Kestra Server: Execute `docker compose up -d` to start the server.

1. Access Kestra UI: Navigate to `http://localhost:8080/` to use the Kestra user interface.

1. Create and Configure Flows:

   - Go to 'Flows', then 'Create'.
   - Configure the flow files in the editor.
   - Save your flows.

1. **Understand Flow Components**:

   - Each flow must have an `id`, `namespace`, and a list of `tasks` with their respective `id` and
     `type`.
   - The main flow orchestrates tasks like loading data from a source to a destination.

By following these steps, you establish a structured workflow within Kestra, leveraging its powerful
features for efficient data pipeline orchestration.

### Additional Resources

- Ingest Zendesk data into Weaviate using dlt with Kestra:
  [here](https://kestra.io/blueprints/148-ingest-zendesk-data-into-weaviate-using-dlt).
- Ingest Zendesk data into DuckDb using dlt with Kestra:
  [here.](https://kestra.io/blueprints/147-ingest-zendesk-data-into-duckdb-using-dlt)
- Ingest Pipedrive CRM data to BigQuery using dlt and schedule it to run every hour:
  [here.](https://kestra.io/blueprints/146-ingest-pipedrive-crm-data-to-bigquery-using-dlt-and-schedule-it-to-run-every-hour)

## Conclusion

Deploying `dlt` on Kestra streamlines data workflow management by automating and simplifying data
loading processes. This integration offers developers and data engineers a robust framework for
scalable, resilient, and manageable data pipelines. By following the outlined steps, users can use
the orchestration capabilities of Kestra and the intuitive data pipeline construction offered by
`dlt`.

We encourage data engineers and developers to explore the capabilities of `dlt` within the Kestra
platform. In embracing Kestra and `dlt`, you gain access to a community-driven ecosystem that
encourages innovation and collaboration. Using `dlt` on Kestra streamlines the pipeline development
process and unlocks the potential making better data ingestion pipelines.
