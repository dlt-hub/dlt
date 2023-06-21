---
title: Choosing an orchestrator
description: How to choose an orchestractor to a deploy dlt pipeline
keywords: [orchestrator, airflow, github actions]
---

# Choosing an orchestrator

Orchestrators enable developers to quickly and easily deploy and manage applications in the cloud.

## What is an orchestrator?

An orchestrator is a software system that automates the deployment, scaling, and management of
applications and services.

It provides a single platform for managing and coordinating the components of distributed
applications, including containers, microservices, and other cloud-native resources.

## Do I need an orchestrator?

No, but if you do not use one, you will need to consider how to solve the problems one would:

- Monitoring, Alerting;
- deployment and execution;
- scheduling complex workflows;
- task triggers, dependencies, retries;
- UI for visualising workflows;
- secret vaults/storage;

So in short, unless you need something very lightweight, you can benefit from an orchestrator.

## So which one?

### **Airflow**

Airflow is a market standard that’s hard to beat. Any shortcomings it might have is more than
compensated for by being an open source mature product with a large community.

`dlt` supports **airflow deployments**, meaning it’s particularly easy to deploy `dlt` pipelines to
Airflow via simple commands.

Your Airflow options are:

1. Broadly used managed Airflow vendors:

   - [GCP Cloud Composer](../../../walkthroughs/deploy-a-pipeline/airflow-gcp-cloud-composer.md) is
     recommended due to GCP being easy to use for non engineers and Google BigQuery being a popular
     solution.
   - [Astronomer.io](http://Astronomer.io) (recommended for non GCP users). CI/CD out of the box
   - AWS has a managed Airflow too, though it is the hardest to use.

2. Self-managed Airflow:

   - You can self-host and run your own Airflow. This is not recommended unless the team plans to have
     the skills to work with this in house.

Limitations:

- Airflow manages large scale setups and small alike.

To deploy a pipeline on Airflow with Google Composer, read our [step-by-step tutorial](../../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer)
about using the `dlt deploy` command.

### **GitHub Actions**

GitHub Actions is not a full-blown orchestrator, but it works. It supports simple workflows and
scheduling, allowing for a visual, lightweight deployment with web-based monitoring (i.e. you can
see runs in GitHub Actions). It has a free tier, but its pricing is not convenient for large jobs.

To deploy a pipeline on GitHub Actions, read
[here](../../../walkthroughs/deploy-a-pipeline/deploy-with-github-actions) about using the `dlt deploy`
command and
[here](https://docs.github.com/en/actions/learn-github-actions/usage-limits-billing-and-administration)
about the limitations of GitHub Actions and how their billing works.

### **Other orchestrators**

Other orchestrators can also do the job, so if you are limited in choice or prefer something else,
choose differently. If your team prefers a different tool which affects their work positively,
consider that as well.

What do you need to consider when using other orchestrators?

## Source - Resource decomposition:

You can decompose a pipeline into strongly connected components with
`source().decompose(strategy="scc")`. The method returns a list of dlt sources each containing a
single component. Method makes sure that no resource is executed twice.

Serial decomposition:

You can load such sources as tasks serially in order present of the list.
Such DAG is safe for pipelines that use the state internally.
[It is used internally by our Airflow mapper to construct DAGs.](https://github.com/dlt-hub/dlt/blob/devel/dlt/helpers/airflow_helper.py)

Custom decomposition:

- When decomposing pipelines into tasks, be mindful of shared state.
- Dependent resources pass data to each other via hard disk - so they need to run on the same
  worker. Group them in a task that runs them together.
- State is per-pipeline. The pipeline identifier is the pipeline name. A single pipeline state
  should be accesed serially to avoid losing details on parallel runs.

Parallel decomposition:

If you are using only the resource state (which most of the pipelines
really should!) you can run your tasks in parallel.

- Perform the `scc` decomposition.
- Run each component in a pipeline with different but deterministic `pipeline_name` (same component
  \- same pipeline, you can use names of selected resources in source to construct unique id).

Each pipeline will have its private state in the destination and there won't be any clashes. As all
the components write to the same schema you may observe a that loader stage is attempting to migrate
the schema, that should be a problem though as long as your data does not create variant columns.

## Credentials

[See credentials section for passing credentials to or from dlt](../../../general-usage/credentials.md)
