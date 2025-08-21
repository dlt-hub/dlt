---
title: Runners
description: Run pipelines in production
keywords: [runners, lambda, airflow, prefect]
---

# Runners

With dlt+ you can now run pipelines directly from the command line, allowing you to go to production faster:

```sh
dlt pipeline my_pipeline run
```

This will use the dlt+ [pipeline runner](pipeline-runner.md) to run the pipeline.

## Profiles
The `run` command can be used with [profiles](../core-concepts/profiles.md) to run the pipeline in different environments
with different configurations:

```sh
dlt project --profile prod my_pipeline run
```

## Running on Orchestrators
We are working on specialized runners and integrations for environments like Airflow, Dagster, Prefect, and more. If you're interested, feel free to [join our early access program](https://info.dlthub.com/waiting-list).

