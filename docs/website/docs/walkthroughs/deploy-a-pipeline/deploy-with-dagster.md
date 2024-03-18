---
title: Deploy with Dagster
description: How to deploy a pipeline with Dagster
keywords: [how to, deploy a pipeline, Dagster]
---

# Deploy with Dagster


## Introduction to Dagster

Dagster is an orchestrator that's designed for developing and maintaining data assets, such as tables, data sets, machine learning models, and reports. Dagster ensures these processes are reliable and focuses on using software-defined assets (SDAs) to simplify complex data management, enhance the ability to reuse code and provide a better understanding of data.

To read more, please refer to Dagster’s [documentation.](https://docs.dagster.io/getting-started?_gl=1*19ikq9*_ga*NTMwNTUxNDAzLjE3MDg5Mjc4OTk.*_ga_84VRQZG7TV*MTcwOTkwNDY3MS4zLjEuMTcwOTkwNTYzNi41Ny4wLjA.*_gcl_au*OTM3OTU1ODMwLjE3MDg5Mjc5MDA.)

### Dagster Cloud Features

Dagster Cloud further enhances these features by providing an enterprise-level orchestration service with serverless or hybrid deployment options. It incorporates native branching and built-in CI/CD to prioritize the developer experience. It enables scalable, cost-effective operations without the hassle of infrastructure management. 

### Dagster deployment options: **Serverless** versus **Hybrid**:

The *serverless* option fully hosts the orchestration engine, while the *hybrid* model offers flexibility to use your computing resources, with Dagster managing the control plane. Reducing operational overhead and ensuring security.

For more info, please [refer.](https://dagster.io/cloud)

### Using Dagster for Free

Dagster offers a 30-day free trial during which you can explore its features, such as pipeline orchestration, data quality checks, and embedded ELTs. You can try Dagster using its open source or by signing up for the trial. 

## Building Data Pipelines with `dlt`

`dlt` is an open-source Python library that allows you to declaratively load data sources into well-structured tables or datasets through automatic schema inference and evolution. It simplifies building data pipelines by providing functionality to support the entire extract and load process.

How does `dlt` integrate with Dagster for pipeline orchestration?

`dlt` integrates with Dagster for pipeline orchestration, providing a streamlined process for building, enhancing, and managing data pipelines. This enables developers to leverage `dlt`'s capabilities for handling data extraction and load and Dagster's orchestration features to efficiently manage and monitor data pipelines.

Here’s a brief summary of how to orchestrate `dlt` pipeline on Dagster:

1. Create a `dlt` pipeline. For detailed instructions on creating a pipeline, please refer to the 
[documentation](https://dlthub.com/docs/walkthroughs/create-a-pipeline).

1. Set up a Dagster project, configure resources, and define the asset. For more information, please refer to [Dagster’s documentation.](https://docs.dagster.io/getting-started/quickstart)

1. Next, define Dagster definitions, start the web server, and materialize the asset.
1. View the populated metadata and data in the configured destination.

:::info
For a hands-on project on “Orchestrating unstructured data pipelines with dagster and dlt", read the [article](https://dagster.io/blog/dagster-dlt) provided. The author offers a detailed overview and steps for ingesting GitHub issue data from a repository and storing it in BigQuery. You can use a similar approach to build your pipelines.
:::

### Additional Resources

- A general configurable `dlt` resource orchestrated on Dagster: [dlt resource](https://github.com/dagster-io/dagster-open-platform/blob/5030ff6828e2b001a557c6864f279c3b476b0ca0/dagster_open_platform/resources/dlt_resource.py#L29).
- `dlt` pipelines configured for Dagster: [dlt pipelines](https://github.com/dagster-io/dagster-open-platform/tree/5030ff6828e2b001a557c6864f279c3b476b0ca0/dagster_open_platform/assets/dlt_pipelines).

:::note
These are external repositories and are subject to change.
:::

## Conclusion

In conclusion, integrating `dlt` into the data pipeline ecosystem markedly improves data operations' efficiency and manageability. The combination of `dlt` and Dagster eases the development of data pipelines, making data assets more maintainable and scalable over time. With a wealth of [verified sources](https://dlthub.com/docs/dlt-ecosystem/verified-sources/) available, `dlt` enables streamlined orchestration on Dagster, offering easy management, customization, and maintenance.

We encourage data engineers and developers to explore the capabilities of `dlt` within the Dagster platform. By levraging `dlt` on Dagster, you can simplify the pipeline development process and gain greater insights and value from your data assets.