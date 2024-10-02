---
title: Deploy with Dagster
description: How to deploy a pipeline with Dagster
keywords: [how to, deploy a pipeline, Dagster]
---

# Deploy with Dagster

## Introduction to Dagster

Dagster is an orchestrator designed for developing and maintaining data assets, such as
tables, datasets, machine learning models, and reports. Dagster ensures these processes are
reliable and focuses on using software-defined assets (SDAs) to simplify complex data management,
enhance the ability to reuse code, and provide a better understanding of data.

To read more, please refer to Dagster’s
[documentation.](https://docs.dagster.io/getting-started?_gl=1*19ikq9*_ga*NTMwNTUxNDAzLjE3MDg5Mjc4OTk.*_ga_84VRQZG7TV*MTcwOTkwNDY3MS4zLjEuMTcwOTkwNTYzNi41Ny4wLjA.*_gcl_au*OTM3OTU1ODMwLjE3MDg5Mjc5MDA.)

### Dagster Cloud features

Dagster Cloud offers an enterprise-level orchestration service with serverless or hybrid deployment
options. It incorporates native branching and built-in CI/CD to prioritize the developer experience.
It enables scalable, cost-effective operations without the hassle of infrastructure management.

### Dagster deployment options: **Serverless** versus **Hybrid**

The *serverless* option fully hosts the orchestration engine, while the *hybrid* model offers
flexibility to use your computing resources, with Dagster managing the control plane, reducing
operational overhead and ensuring security.

For more info, please refer to the Dagster Cloud [docs.](https://dagster.io/cloud)

### Using Dagster for free

Dagster offers a 30-day free trial during which you can explore its features, such as pipeline
orchestration, data quality checks, and embedded ELTs. You can try Dagster using its open source or
by signing up for the trial.

## Building data pipelines with `dlt`

**How does `dlt` integrate with Dagster for pipeline orchestration?**

`dlt` integrates with Dagster for pipeline orchestration, providing a streamlined process for
building, enhancing, and managing data pipelines. This enables developers to leverage `dlt`'s
capabilities for handling data extraction and load, and Dagster's orchestration features to efficiently manage and monitor data pipelines.

Dagster supports [native integration with dlt](https://docs.dagster.io/integrations/embedded-elt/dlt),
here is a guide on how this integration works.

### Orchestrating `dlt` pipeline on Dagster

Here's a concise guide to orchestrating a `dlt` pipeline with Dagster, creating a pipeline that ingests GitHub issues data from a repository and loads it into DuckDB.

You can find the full example code in [this repository](https://github.com/dlt-hub/dlthub-education/blob/main/workshops/workshop_august_2024/part2/deployment/deploy_dagster/README.md).

**The steps are as follows:**

1. Install Dagster and the embedded ELT package using pip:
    ```sh
    pip install dagster dagster-embedded-elt
    ```

1. Set up a Dagster project:
      ```sh
      mkdir dagster_github_issues
      cd dagster_github_issues
      dagster project scaffold --name github-issues
      ```
      ![image](https://github.com/user-attachments/assets/f9002de1-bcdf-49f4-941b-abd59ea7968d)

1. In your Dagster project, define the dlt pipeline in the `github_source` folder.

   **Note**: The dlt Dagster helper works only with dlt sources. Your resources should always be grouped in a source.
     ```py
     import dlt
     ...
     @dlt.resource(
         table_name="issues",
         write_disposition="merge",
         primary_key="id",
     )
     def get_issues(
             updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
     ):
         url = (
             f"{BASE_URL}?since={updated_at.last_value}&per_page=100&sort=updated"
             "&direction=desc&state=open"
         )
         yield pagination(url)

     @dlt.source
     def github_source():
         return get_issues()
     ```
 1. Create a `dlt_assets` definition.

    The `@dlt_assets` decorator takes a `dlt_source` and `dlt_pipeline` parameter.
    In this example, we used the `github_source` source and created a `dlt_pipeline` to ingest data from GitHub to DuckDB.

    Here’s an example of how to define assets (`github_source/assets.py`):

      ```py
      import dlt
      from dagster import AssetExecutionContext
      from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
      from .github_pipeline import github_source

      @dlt_assets(
          dlt_source=github_source(),
          dlt_pipeline=dlt.pipeline(
              pipeline_name="github_issues",
              dataset_name="github",
              destination="duckdb",
              progress="log",
          ),
          name="github",
          group_name="github",
      )
      def dagster_github_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
          yield from dlt.run(context=context)
      ```

    For more information, please refer to
    [Dagster’s documentation.](https://docs.dagster.io/_apidocs/libraries/dagster-embedded-elt#dagster_embedded_elt.dlt.dlt_assets)

 1. Create the Definitions object.

    The last step is to include the assets and resource in a [Definitions](https://docs.dagster.io/_apidocs/definitions#dagster.Definitions) object (`github_source/definitions.py`). This enables Dagster tools to load everything we have defined:

     ```py
     import assets
     from dagster import Definitions, load_assets_from_modules
     from dagster_embedded_elt.dlt import DagsterDltResource

     dlt_resource = DagsterDltResource()
     all_assets = load_assets_from_modules([assets])

     defs = Definitions(
         assets=all_assets,
         resources={
             "dlt": dlt_resource,
         },
     )
     ```

1. Run the web server locally:
    1. Install the necessary dependencies using the following command:

       ```sh
       pip install -e ".[dev]"
       ```

       We use -e to install dependencies in [editable mode](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs). This allows changes to be automatically applied when we modify the code.

    2. Run the project:

       ```sh
       dagster dev
       ```

    3. Navigate to localhost:3000 in your web browser to access the Dagster UI.

       ![image](https://github.com/user-attachments/assets/97b74b86-df94-47e5-8ae2-de7cc47f56d8)

1. Run the pipeline.

   Now that you have a running instance of Dagster, you can run your data pipeline.

   To run the pipeline, go to **Assets** and click the **Materialize** button in the top right. In Dagster, materialization refers to executing the code associated with an asset to produce an output.

   ![image](https://github.com/user-attachments/assets/79416fb7-8362-4640-b205-e59aa7ac785c)

   You will see the following logs in your command line:

   ![image](https://github.com/user-attachments/assets/f0e3bec8-f702-46a6-b69f-194a1dacf625)

   Want to see real-world examples of dlt in production? Check out how dlt is used internally at Dagster in the [Dagster Open Platform](https://github.com/dagster-io/dagster-open-platform) project.


:::info
For a complete picture of Dagster's integration with dlt, please refer to their [documentation](https://docs.dagster.io/integrations/embedded-elt/dlt). This documentation offers a detailed overview and steps for ingesting GitHub data and storing it in Snowflake. You can use a similar approach to build your pipelines.
:::

### Frequently Asked Questions
- **Can I remove the generated `.dlt` folder with `secrets.toml` and `config.toml` files?**

  Yes. Since dlt is compatible with environment variables, you can use this for secrets required by both Dagster and dlt.

- **I'm working with several sources – how can I best group these assets?**

  To effectively group assets in Dagster when working with multiple sources, use the `group_name` parameter in your `@dlt_assets` decorator. This helps organize and visualize assets related to a particular source or theme in the Dagster UI. Here’s a simplified example:

  ```py
  import dlt
  from dagster_embedded_elt.dlt import dlt_assets
  from dlt_sources.google_analytics import google_analytics

  # Define assets for the first Google Analytics source
  @dlt_assets(
      dlt_source=google_analytics(),
      dlt_pipeline=dlt.pipeline(
        pipeline_name="google_analytics_pipeline_1",
        destination="bigquery",
        dataset_name="google_analytics_data_1"
      ),
      group_name='Google_Analytics'
  )
  def google_analytics_assets_1(context, dlt):
      yield from dlt.run(context=context)

  # Define assets for the second Google Analytics source
  @dlt_assets(
      dlt_source=google_analytics(),
      dlt_pipeline=dlt.pipeline(
        pipeline_name="google_analytics_pipeline_2",
        destination="bigquery",
        dataset_name="google_analytics_data_2"
      ),
      group_name='Google_Analytics'
  )
  def google_analytics_assets_2(context, dlt):
      yield from dlt.run(context=context)
  ```



- **How can I use `bigquery_adapter` with `@dlt_assets` in Dagster for partitioned tables?**

  To use `bigquery_adapter` with `@dlt_assets` in Dagster for partitioned tables, modify your resource setup to include `bigquery_adapter` with the partition parameter. Here's a quick example:

  ```py
  import dlt
  from google.analytics import BetaAnalyticsDataClient
  from dlt.destinations.adapters import bigquery_adapter
  from dagster import dlt_asset

  @dlt_asset
  def google_analytics_asset(context):
      # Configuration (replace with your actual values or parameters)
      queries = [
          {"dimensions": ["dimension1"], "metrics": ["metric1"], "resource_name": "resource1"}
      ]
      property_id = "your_property_id"
      start_date = "2024-01-01"
      rows_per_page = 1000
      credentials = your_credentials

      # Initialize Google Analytics client
      client = BetaAnalyticsDataClient(credentials=credentials.to_native_credentials())

      # Fetch metadata
      metadata = get_metadata(client=client, property_id=property_id)
      resource_list = [metadata | metrics_table, metadata | dimensions_table]

      # Configure and add resources to the list
      for query in queries:
          dimensions = query["dimensions"]
          if "date" not in dimensions:
              dimensions.append("date")

          resource_name = query["resource_name"]
          resource_list.append(
              bigquery_adapter(
                  dlt.resource(basic_report, name=resource_name, write_disposition="append")(
                      client=client,
                      rows_per_page=rows_per_page,
                      property_id=property_id,
                      dimensions=dimensions,
                      metrics=query["metrics"],
                      resource_name=resource_name,
                      start_date=start_date,
                      last_date=dlt.sources.incremental("date"),
                  ),
                  partition="date"
              )
          )

      return resource_list
  ```

### Additional resources

- Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud/) to learn more about deploying on Dagster Cloud.

- Learn more about Dagster's integration with dlt:
  [dlt & Dagster](https://docs.dagster.io/integrations/embedded-elt/dlt)
  [Embedded ELT Documentation](https://docs.dagster.io/_apidocs/libraries/dagster-embedded-elt#dagster_embedded_elt.dlt.dlt_assets).

- A general configurable `dlt` resource orchestrated on Dagster:
  [dlt resource](https://github.com/dagster-io/dagster-open-platform/blob/5030ff6828e2b001a557c6864f279c3b476b0ca0/dagster_open_platform/resources/dlt_resource.py#L29).

- Configure `dlt` pipelines for Dagster:
  [dlt pipelines](https://github.com/dagster-io/dagster-open-platform/tree/5030ff6828e2b001a557c6864f279c3b476b0ca0/dagster_open_platform/assets/dlt_pipelines).

- Configure MongoDB source as an Asset factory:

   Dagster provides the feature of
   [@multi_asset](https://github.com/dlt-hub/dlt-dagster-demo/blob/21a8d18b6f0424f40f2eed5030989306af8b8edb/mongodb_dlt/mongodb_dlt/assets/__init__.py#L18)
   declaration that will allow us to convert each collection under a database into a separate
   asset. This will make our pipeline easy to debug in case of failure and the collections
   independent of each other.

:::note
Some of these are external repositories and are subject to change.
:::

