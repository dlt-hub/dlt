---
title: Deploy with Dagster
description: How to deploy a pipeline with Dagster
keywords: [how to, deploy a pipeline, Dagster]
---

# Deploy with Dagster

## Introduction to Dagster

Dagster is an orchestrator designed for developing and maintaining data assets, such as
tables, data sets, machine learning models, and reports. Dagster ensures these processes are
reliable and focuses on using software-defined assets (SDAs) to simplify complex data management,
enhance the ability to reuse code, and provide a better understanding of data.

To read more, please refer to Dagster’s
[documentation.](https://docs.dagster.io/getting-started?_gl=1*19ikq9*_ga*NTMwNTUxNDAzLjE3MDg5Mjc4OTk.*_ga_84VRQZG7TV*MTcwOTkwNDY3MS4zLjEuMTcwOTkwNTYzNi41Ny4wLjA.*_gcl_au*OTM3OTU1ODMwLjE3MDg5Mjc5MDA.)

### Dagster Cloud Features

Dagster Cloud offers enterprise-level orchestration service with serverless or hybrid deployment
options. It incorporates native branching and built-in CI/CD to prioritize the developer experience.
It enables scalable, cost-effective operations without the hassle of infrastructure management.

### Dagster deployment options: **Serverless** versus **Hybrid**:

The *serverless* option fully hosts the orchestration engine, while the *hybrid* model offers
flexibility to use your computing resources, with Dagster managing the control plane. Reducing
operational overhead and ensuring security.

For more info, please refer to the Dagster Cloud [docs.](https://dagster.io/cloud)

### Using Dagster for Free

Dagster offers a 30-day free trial during which you can explore its features, such as pipeline
orchestration, data quality checks, and embedded ELTs. You can try Dagster using its open source or
by signing up for the trial.

## Building Data Pipelines with `dlt`

`dlt` is an open-source Python library that allows you to declaratively load data sources into
well-structured tables or datasets through automatic schema inference and evolution. It simplifies 
building data pipelines with support for extract and load processes.

**How does `dlt` integrate with Dagster for pipeline orchestration?**

`dlt` integrates with Dagster for pipeline orchestration, providing a streamlined process for
building, enhancing, and managing data pipelines. This enables developers to leverage `dlt`'s
capabilities for handling data extraction and load and Dagster's orchestration features to efficiently manage and monitor data pipelines.

### Orchestrating `dlt` pipeline on Dagster

Here's a concise guide to orchestrating a `dlt` pipeline with Dagster, using the project "Ingesting
GitHub issues data from a repository and storing it in BigQuery" as an example. 

More details can be found in the article
[“Orchestrating unstructured data pipelines with dagster and dlt."](https://dagster.io/blog/dagster-dlt)

**The steps are as follows:**
1. Create a `dlt` pipeline. For more, please refer to the documentation:
[Creating a pipeline.](https://dlthub.com/docs/walkthroughs/create-a-pipeline)

1. Set up a Dagster project, configure resources, and define the asset as follows:

   1. To create a Dagster project:
      ```sh
      mkdir dagster_github_issues  
      cd dagster_github_issues  
      dagster project scaffold --name github-issues  
      ```

   1. Define `dlt` as a Dagster resource:
      ```py
      from dagster import ConfigurableResource  
      from dagster import ConfigurableResource  
      import dlt  

      class DltPipeline(ConfigurableResource):  
          pipeline_name: str  
          dataset_name: str  
          destination: str  

          def create_pipeline(self, resource_data, table_name):  
       
              # configure the pipeline with your destination details  
              pipeline = dlt.pipeline(  
                  pipeline_name=self.pipeline_name, 
                                destination=self.destination, 
                                dataset_name=self.dataset_name  
              )  

              # run the pipeline with your parameters  
              load_info = pipeline.run(resource_data, table_name=table_name)  

              return load_info  
      ```
   1. Define the asset as:
      ```py
      @asset  
      def issues_pipeline(pipeline: DltPipeline):  
          
          logger = get_dagster_logger()  
          results = pipeline.create_pipeline(github_issues_resource, table_name='github_issues')  
          logger.info(results)  
      ```
      > For more information, please refer to
      > [Dagster’s documentation.](https://docs.dagster.io/getting-started/quickstart)

1. Next, define Dagster definitions as follows:
   ```py
   all_assets = load_assets_from_modules([assets])  
   simple_pipeline = define_asset_job(name="simple_pipeline", selection= ['issues_pipeline'])  

   defs = Definitions(  
       assets=all_assets,  
       jobs=[simple_pipeline],  
       resources={  
           "pipeline": DltPipeline(
               pipeline_name = "github_issues",
               dataset_name = "dagster_github_issues",
               destination = "bigquery",
           ),  
       }  
   ) 
   ```

1. Finally, start the web server as:

   ```sh
   dagster dev
   ```

:::info 
For the complete hands-on project on “Orchestrating unstructured data pipelines with dagster and
`dlt`", please refer to [article](https://dagster.io/blog/dagster-dlt). The author offers a
detailed overview and steps for ingesting GitHub issue data from a repository and storing it in
BigQuery. You can use a similar approach to build your pipelines.
:::

### Additional Resources

- A general configurable `dlt` resource orchestrated on Dagster:
  [dlt resource](https://github.com/dagster-io/dagster-open-platform/blob/5030ff6828e2b001a557c6864f279c3b476b0ca0/dagster_open_platform/resources/dlt_resource.py#L29).

- Configure `dlt` pipelines for Dagster:
  [dlt pipelines](https://github.com/dagster-io/dagster-open-platform/tree/5030ff6828e2b001a557c6864f279c3b476b0ca0/dagster_open_platform/assets/dlt_pipelines).

- Configure MongoDB source as an Asset factory:
  > Dagster provides the feature of
  > [@multi_asset](https://github.com/dlt-hub/dlt-dagster-demo/blob/21a8d18b6f0424f40f2eed5030989306af8b8edb/mongodb_dlt/mongodb_dlt/assets/__init__.py#L18)
  > declaration that will allow us to convert each collection under a database into a separate
  > asset. This will make our pipeline easy to debug in case of failure and the collections
  > independent of each other.

:::note 
These are external repositories and are subject to change. 
:::
