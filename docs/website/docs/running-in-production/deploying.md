# Deploying

Once your pipeline is working as expected, you will want to deploy it on a schedule, so that it periodically loads the latest data from the [source](../general-usage/glossary.md#source) to the [destination](../general-usage/glossary.md#destination).

At the moment, we have two recommended deployment approaches:
1. Airflow
2. GitHub Actions

## 1. Airflow

Airflow is the current market standard for orchestrators. You can read more about it [here](./orchestrators/choosing-an-orchestrator.md#airflow) and learn how to deploy it on Google Cloud Platform (GCP) [here](./orchestrators/airflow-gcp-cloud-composer.md).

## 2. GitHub Actions

GitHub Actions supports simple workflows and scheduling, allowing for a visual, lightweight deployment with web-based monitoring (i.e. you can see runs in GitHub Actions). It has a free tier but its pricing is not convenient for large jobs. You can read more about it [here](./orchestrators/choosing-an-orchestrator.md#github-actions) and learn how to deploy it using the `dlt deploy` command [here](../walkthroughs/deploy-a-pipeline/deploy-with-github-actions.md).

## Alternatives

### Other orchestrators

If neither of the recommended deployment approaches work for you, then you could also consider other orchestrators. You will find some considerations to keep in mind [here](./orchestrators/choosing-an-orchestrator.md).

### Crontab

If neither of the recommended deployment approaches work for you, then you could alternatively schedule cron jobs to run your pipeline using [Crontab](https://crontab.guru/).