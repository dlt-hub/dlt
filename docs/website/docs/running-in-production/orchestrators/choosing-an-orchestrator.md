---
title: Choosing an orchestrator
description: How to choose an orchestractor to a deploy dlt pipeline
keywords: [orchestrator, airflow, github actions]
---

# Choosing an orchestrator

Orchestrators enable developers to quickly and easily deploy and manage applications in the cloud.

### What is an orchestrator?

An orchestrator is a software system that automates the deployment, scaling, and management of applications and services.

It provides a single platform for managing and coordinating the components of distributed applications, including containers, microservices, and other cloud-native resources.

### Do I need an orchestrator?

No, but if you do not use one, you will need to consider how to solve the problems one would

- Monitoring, Alerting
- deployment and execution
- scheduling complex workflows
- task triggers, dependencies, retries
- UI for visualising workflows
- secret vaults/storage

So in short, unless you need something very lightweight, you can benefit from an orchestrator

### So which one?

### **Airflow**

Airflow  is a market standard that’s hard to beat. Any shortcomings it might have are more than compensated for by being an open source mature product with a large community.

**dlt** supports **airflow** **deployments**, meaning it’s particularly easy to deploy dlt pipelines to airflow via simple commands.

Your airflow options are:

- Broadly used managed airflow vendors:
    - GCP’s cloud composer (recommended due to GCP being easy to use for non engineers and BQ being a popular solution). Simple to set up with ci-cd
    - [Astronomer.io](http://Astronomer.io) (recommended for non gcp users). Ci-cd out of the box
    - Aws’s managed airflow (hardest to use, AWS in general is harder to use than GCP)
- Self managed airflow:
    - You can self host and run your own airflow. This is not recommended unless the team plans to have the skills to work with this in house.

Limitations:

Airflow can manage large scale setups and small alike.

### **GitHub Actions**

GitHub Actions is not a full blown orchestrator, but it works. It supports simple workflows and scheduling, allowing for a visual, lightweight deployment with web-based monitoring (can see runs in git actions).
It has a free tier, but its pricing is not convenient for large jobs.

To deploy to git actions, [read here about using the dlt deploy command](./walkthroughs/deploy-a-pipeline)

Read here about the limitations and billing of git actions.
[https://docs.github.com/en/actions/learn-github-actions/usage-limits-billing-and-administration](https://docs.github.com/en/actions/learn-github-actions/usage-limits-billing-and-administration)

### Other orchestrators

Other orchestrators can also do the job, so if you are limited in choice or prefer something else, choose differently.
If your team prefers a different tool which affects their work positively, consider that as well.

What do you need to consider when using other orchestrators?

**Source - Resource decomposition:**
* you can decompose a pipeline into tasks by using the `source().with_resource(resource1, resource2)`selector to pass resource names you want to run.
* you can find the resources and run them by themselves. `source().resources` produces a dict of resources with the names as keys and generators as values.
* when decomposing pipelines into tasks, be mindful of shared state
  * dependent resources pass data to each other via hard disk - so they need to run on the same worker. Group them in a task that runs them together.
  * state is per-pipeline. The pipeline identifier is the pipeline name. A single pipeline state should be accesed serially to avoid losing details on parallel runs.


**Credentials:**

[See credentials section for passing credentials to or from dlt](../../general-usage/credentials.md)