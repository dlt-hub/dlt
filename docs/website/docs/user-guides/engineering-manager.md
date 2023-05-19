---
title: Engineering Manager
description: A guide to using dlt for Engineering Managers
keywords: [engineering manager, EM, data platform engineer]
---

# Engineering Manager

## What are the responsibilities of a data engineering manager? and how can dlt, a python library, support you?

### Managing team resources to deliver on team goals: Dlt massively reduces human hours cost and delays.

  - dlt massively lowers pipeline maintenance cost by inferring and evolving schemas from unstructured data. With configurable schema evolution strategies, you are able to choose how much and what kind of pipeline maintenance you will perform.
  - dlt runs anywhere, lowering infra costs. dlt can buffer big data on small machines, or make use of parallelism to speed up operations.
  - dlt runs anywhere, lowering infra overheads and requirements. Micro airflow workers, large containers, colab notebooks or cloud functions are all fine for dlt python library.
  - dlt is cheap and efficient. Born out of "sexy but poor" Berlin's startup scene, dlt does not waste resources. Cost efficient loading, efficient maintenance, efficient development though schema inference, pipeline building blocks and declarative loading are things dlt does to make you more efficient.
  - dlt plays well with your other tools. For example, it runs well on airflow and can [run dbt packges on the fly](../using-loaded-data/transforming-the-data#transforming-the-data-using-dbt). There is no vendor or tech lock with dlt.

### Managing data resources via data governance. Dlt has lineage and data contracts built-in.

  - dlt provides [lineage out of the box](../using-loaded-data/understanding-the-tables). Every piece of data is loaded together with the name of the pipeline and the timestamp. Schemas are versioned. Evolution events are notified. dlt cli [command `dlt show` can be used to describe the data](../using-loaded-data/understanding-the-tables#show-tables-and-data-in-the-destination).
  - dlt provides a configurable schema evolution engine that enables you to [notify schema changes](../running-in-production/running#inspect-save-and-alert-on-schema-changes) and automate schema migrations.
  - dlt offers building blocks that enable you to customise how data is loaded. [Anonymisers, renamers](../customizations/customizing-pipelines/pseudonymizing_columns), transformers, are all either provided or easy to build on the fly.

### Data loading democratised: Dlt supports interfaces your team already uses.

Declarative configuration your analytics engineers are already familiar with enables everyone to build professional pipelines:
 - For example, thanks to dlt's [schema inference](../general-usage/schema#data-normalizer), an analyst can build an incremental scalable pipeline with chatgpt and dlt in minutes. (for example our jira pipeline)
 - A data engineer can customise the above pipeline with [schema](../general-usage/schema) hints, or advanced features to do exactly what they want

Support for your tools:
 - a dbt runner that lets you [run dbt repositories or folders](../using-loaded-data/transforming-the-data) without additional setup.
 - built in streamlit app for [exploring the loaded data](../using-loaded-data/understanding-the-tables#show-tables-and-data-in-the-destination).
 - airflow support, such as credentials, using the mounted data folder, or dag templates. [example airflow setup](../running-in-production/orchestrators/airflow-gcp-cloud-composer)
 - built in slack notification. Together with schema evolution, you can [notify maintenance events](../running-in-production/running#inspect-save-and-alert-on-schema-changes) to the data team and schema changes to both the data team and the producer.
