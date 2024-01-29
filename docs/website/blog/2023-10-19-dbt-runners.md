---
slug: dbt-runners-usage
title: "Running dbt Cloud or core from python - use cases and simple solutions"
image: /img/purple-python-spoderweb.png
authors:
  name: Adrian Brudaru
  title: Open source data engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [dbt runner, dbt cloud runner, dbt core runner]
---

> tl;dr: You can kick off dbt jobs from Python - either by wrapping dbt Core, or by wrapping the Cloud API.
> But why should you use one over the other, and how to best do it to keep things simple?


# Outline:

1. **What is dbt, and what’s the use case for Core and Cloud?**
   - **The Problem dbt Solves**
   - **What is dbt Core?**
   - **What is dbt Cloud?**
   - **When to Use One or the Other**
   - **Use Cases of dbt Cloud Over Core**

2. **What are the use cases for running dbt core or Cloud from Python?**
   - **Case 1: Analytics Engineering and Data Engineering Teams**
   - **Case 2: Real-time Data Processing and Analytics**
   - **Case 3: Avoiding Library Conflicts**

3. **Introducing dlt’s dbt runners - how the Extract and Load steps can trigger the Transform.**
   - **The Cloud runner**
   - **The Core runner**

4. **A short demo on how to do that with dlt’s dbt runner.**
   - **dbt Cloud Runner Demo**
   - **dbt Core Runner Demo**

---

### 1. **What is dbt, and what’s the use case for Core and Cloud?**

**dbt (data build tool)** is an open-source software that plays a crucial role in the data transformation process.
It empowers data analysts and engineers to create, manage, and document data transformation workflows using SQL (Structured Query Language).
dbt primarily focuses on solving the transformation aspect in ELT (Extract, Load, Transform) data processing.

### **The Problem dbt Solves**

dbt addresses the challenge of efficient data transformation, streamlining the 'Transform' stage in ELT workflows.
Traditionally, transforming raw data into a structured, analyzable format has been complex and laborious.
dbt simplifies and automates this process, allowing users to define data transformations through SQL queries.

### **What is dbt Core?**

dbt Core is the fundamental open-source version of dbt. It provides the essential features and functionalities
for developing and running data transformation workflows using SQL scripts.
dbt Core offers local execution capabilities, making it suitable for small to medium-scale projects run within a user's environment.

### **What is dbt Cloud?**

dbt Cloud is a cloud-based platform provided by Fishtown Analytics, the company behind dbt.
dbt Cloud offers a managed environment for running dbt, providing additional features and capabilities beyond what dbt Core offers.
It is hosted on the cloud, providing a centralized, collaborative, and scalable solution for data transformation needs.

### **When to Use One or the Other?**

The choice between dbt Core and dbt Cloud depends on various factors, including the scale of your data transformation needs, collaboration requirements, and resource constraints.

- **Use dbt Core:**
    - For small to medium-sized projects.
    - When you prefer to manage and execute dbt locally within your environment.
    - If you have specific security or compliance requirements that necessitate an on-premises solution.
- **Use dbt Cloud:**
    - For larger, enterprise-scale projects with significant data transformation demands.
    - When you require a managed, cloud-hosted solution to reduce operational overhead.
    - If you value collaborative features, centralized project management, and simplified access control.

But, dbt Core is free and open source, where dbt Cloud is paid. So let’s look into why we would use the paid service:

### **Use Cases of dbt Cloud Over Core**

We could summarize this as: Cloud is the best solution if your Analytics engineer team wants analytics engineer specific
tooling and does not want to concern itself with data-engineer specific tooling.

1. **Scalability and Performance:** dbt Cloud provides seamless scalability to handle large-scale data transformation workloads efficiently.
2. **Collaboration and Team Management:** dbt Cloud offers centralized project management and collaboration features, enhancing team productivity and coordination.
3. **Automated Task Scheduling:** dbt Cloud allows for automated scheduling of dbt jobs, streamlining data transformation processes.
4. **Easy Integration with Cloud Data Warehouses:** dbt Cloud integrates seamlessly with various cloud data warehouses, facilitating simplified setup and configuration.

So dbt Cloud is kind of like a standalone orchestrator, IDE and more.

## 2. What are the use cases for running dbt Core or Cloud from Python?

### Case 1: You have an Analytics engineering team and a data engineering team that work with different tools.

This is a normal case to have in an enterprise teams, where we have a clear separation of responsibilities and tooling based on team preferences and competencies.

In this case, the Analyics Engineering team will use dbt Cloud for its convenient features, making them more effective.

However, the Data Engineers will want to ensure that the dbt models only run after new data has been loaded - not before,
not after, and not at all in case the data did not load.
So how to coordinate this?

To avoid race conditions, or dbt starting despite a broken loading pipeline, the data engineer needs to be able to trigger the dbt run and wait for it.

Of course, this is a case for the dbt **Cloud** runner.

### **Case 2: Real-time Data Processing and Analytics**

In scenarios where you require real-time or near real-time data processing and analytics,
integrating dbt with Python allows for dynamic and immediate transformations based on incoming data.

If you only refresh data once a day, you do not need the runners - you can set the loads to start at midnight, and the transforms to start at 7 AM.
The hours in between are typically more than enough for loading to happen, and so you will have time to deliver the transformed data by 9 AM.

However, if you want to refresh data every 5, 15, 60 minutes or something similar,
you will want to have fine grained control over calling the transform after loading the new increment.

Such, we have to be able to kick off the dbt job and wait for it, before starting the next refresh cycle.

Here, both the dbt **Cloud** and **Core** runners would fit.

### Case 3. Avoiding Library conflicts between dbt Core and run environment.

If you are running dbt from some orchestrators, such as Airflow, you might find that you cannot, because installing dbt causes library conflicts with the base environment.

In such cases, you would want to create a venv or run the job off the orchestrator.

Such, both the **Cloud** runner and the **Core** runner with virtual env would fit well here.

## 3. Introducing the dbt runners we have created in open source

Here at dlt we solve the EL in the ELT - so naturally we want to kick off dbt to solve the T.

dlt is an open source library made for easily building data pipelines for Python first people.

The dlt library auto cleans data and generates database-agnostic schemas before loading - so regardless of which database we use, our schema is the same.
This provides a unique opportunity to standardise dbt packages on top using cross db macros.

So let’s look at the 2 runners we offer:

### The Cloud runner

Docs link: [dbt Cloud runner docs.](https://dlthub.com/docs/dlt-ecosystem/transformations/dbt/dbt_cloud)

The Cloud runner we support can do the following:

- Start a dbt job in your dbt Cloud account, optionally wait for it to finish.
- Check the status of a dbt job in your account.

Code example:
```python
from dlt.helpers.dbt_cloud import run_dbt_cloud_job

# Trigger a job run with additional data
additional_data = {
    "git_sha": "abcd1234",
    "schema_override": "custom_schema",
    # ... other parameters
}
status = run_dbt_cloud_job(job_id=1234, data=additional_data, wait_for_outcome=True)
print(f"Job run status: {status['status_humanized']}")
```

Read more about the additional data dbt accepts [in their docs.](https://docs.getdbt.com/dbt-cloud/api-v2#/operations/Trigger%20Job%20Run)

### The core runner

Docs link: [dbt Core runner docs.](https://dlthub.com/docs/dlt-ecosystem/transformations/dbt)

The core runner does the following:

- Run dbt core from a local or repository package path.
- Set up the running:
    - Optionally install a venv.
    - Install dbt if not exists.
    - Copy over the remote package.
    - Inject credentials from dlt (which can be passed via env, vaults, or directly).
    - Execute the package and report the outcome.

Code example:
```python
# Create a transformation on a new dataset called 'pipedrive_dbt'
# we created a local dbt package
# and added pipedrive_raw to its sources.yml
# the destination for the transformation is passed in the pipeline
pipeline = dlt.pipeline(
    pipeline_name='pipedrive',
    destination='bigquery',
    dataset_name='pipedrive_dbt'
)

# make or restore venv for dbt, using latest dbt version
venv = dlt.dbt.get_venv(pipeline)

# get runner, optionally pass the venv
dbt = dlt.dbt.package(
    pipeline,
    "pipedrive/dbt_pipedrive/pipedrive",
    venv=venv
)

# run the models and collect any info
# If running fails, the error will be raised with full stack trace
models = dbt.run_all()

# on success print outcome
for m in models:
    print(
        f"Model {m.model_name} materialized" +
        f"in {m.time}" +
        f"with status {m.status}" +
        f"and message {m.message}"
```

## 4. A short demo on how to do that with dlt’s dbt runner.

### dbt Cloud runner

In this example, we start from the Pokemon API, load some data with dlt, and then kick off the dbt run in our dbt Cloud account.

GitHub repo: [dbt Cloud runner example.](https://github.com/dlt-hub/dlt_dbt_cloud)


### dbt Core runner

In this example, we copy GA4 events data from BigQuery into DuckDB, and run a dbt package to calculate metrics.

Article: [BQ-dlt-dbt_core-MotherDuck.](https://dlthub.com/docs/blog/dlt-motherduck-demo)

Accompanying GitHub repo: [dbt Core runner example.](https://github.com/dlt-hub/bigquery-motherduck)


## In conclusion

Running dbt from Python is an obvious necessity for a data team that also uses Python for ingestion, orchestration, or analysis.
Having the 2 options to run Cloud or Core versions of dbt enables better integration between the Transform component and the rest of the data stack.

Want more?

- [Join the ⭐Slack Community⭐ for discussion and help!](https://dlthub.com/community)
- Dive into our [Getting Started.](https://dlthub.com/docs/getting-started)
- Star us on [GitHub](https://github.com/dlt-hub/dlt)!
