---
title: Deploy with Prefect
description: How to deploy a pipeline with Prefect
keywords: [how to, deploy a pipeline, Prefect]
---

# Deploy with Prefect

## Introduction to Prefect

Prefect is a workflow management system that automates and orchestrates data pipelines. As an open-source platform, it offers a framework for defining, scheduling, and executing tasks with dependencies. It enables users to scale and maintain their data workflows efficiently.

### Prefect features

- **Flows**:  are defined as Python functions.
- **Tasks**: You can create Prefect workflows with flows using Python, encapsulating logic in reusable tasks for flows and subflows.
- **Deployments and Scheduling**:  Deployments transform workflows from manually called functions into API-managed entities that you can trigger remotely. Prefect allows you to use schedules to automatically create new flow runs for deployments.
- **Automation:** Prefect Cloud enables you to configure [actions](https://docs.prefect.io/latest/concepts/automations/#actions) that Prefect executes automatically based on [trigger](https://docs.prefect.io/latest/concepts/automations/#triggers) conditions.
- **Caching:** refers to the ability of a task to reflect a finished state without actually running the code that defines the task.
- **Oberservality**: enables users to monitor workflows and tasks, providing insights into data pipeline performance and behavior. It includes logging, metrics, and notifications,

## Building Data Pipelines with `dlt`

`dlt` is an open-source Python library that enables the declarative loading of data sources into well-structured tables or datasets by automatically inferring and evolving schemas. It simplifies the construction of data pipelines by offering functionality to support the complete extract and load process.

### How does **`dlt`** integrate with Prefect for pipeline orchestration?

Here's a concise guide to orchestrating a `dlt` pipeline with Prefect. Let's take the example of the pipeline "Moving Slack data into BigQuery".

You can find a comprehensive, step-by-step guide in the article [“Building resilient data pipelines in minutes with dlt + Prefect”.](https://www.prefect.io/blog/building-resilient-data-pipelines-in-minutes-with-dlt-prefect)

You can take a closer look at its GitHub repository [here.](https://github.com/dylanbhughes/dlt_slack_pipeline/blob/main/slack_pipeline_with_prefect.py)

### Here’s a summary of the steps followed:

1. Create a `dlt` pipeline. For detailed instructions on creating a pipeline, please refer to the [documentation](https://dlthub.com/docs/walkthroughs/create-a-pipeline).

1. Add `@task` decorator to the individual functions.
    1. Here we use `@task` decorator for `get_users` function: 
        
        ```py
        @task
        def get_users() -> None:
            """Execute a pipeline that will load Slack users list."""
        ```
        
    1. Use `@flow` function on the `slack_pipeline` function as:
        
        ```py
        @flow
        def slack_pipeline(
            channels=None, 
            start_date=pendulum.now().subtract(days=1).date()
        ) -> None:
            get_users()
        
        ```
        
2. Lastly, append `.serve` to the `if __name__ == '__main__'` block to automatically create and schedule a Prefect deployment for daily execution as:
    
    ```py
    if __name__ == "__main__":
        slack_pipeline.serve("slack_pipeline", cron="0 0 * * *")
    ```
    
3. You can view deployment details and scheduled runs, including successes and failures, using [PrefectUI](https://app.prefect.cloud/auth/login).