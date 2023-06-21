---
title: Airflow with Cloud Composer
description: How to run dlt pipeline with Airflow
keywords: [dlt, webhook, serverless, airflow, gcp, cloud composer]
---

# Deployment with Airflow and Google Cloud Composer

Airflow is like your personal assistant for managing data workflows. It's a cool open-source
platform that lets you create and schedule complex data pipelines. You can break down your tasks
into smaller chunks, set dependencies between them, and keep an eye on how everything's running.

Google cloud composer is a Google Cloud managed Airflow, which allows you to use Airflow without
having to deploy it. It costs the same as you would run your own, except all the kinks and
inefficiencies have mostly been ironed out. The latest version they offer features autoscaling which
helps reduce cost further by shutting down unused workers.

Combining Airflow, `dlt`, and Google Cloud Composer is a game-changer. You can supercharge your data
pipelines by leveraging Airflow's workflow management features, enhancing them with `dlt`'s
specialized templates, and enjoying the scalability and reliability of Google Cloud Composer's
managed environment. It's the ultimate combo for handling data integration, transformation, and
loading tasks like a pro.

`dlt` makes it super convenient to deploy your data load script and integrate it seamlessly with
your Airflow workflow in Google Cloud Composer. It's all about simplicity and getting things done
with just a few keystrokes.

For this easy style of deployment, `dlt` supports
[the cli command](../../../reference/command-line-interface.md#airflow-composer):

```bash
dlt deploy {pipeline_script}.py airflow-composer
```

which generates the necessary code and instructions.

Read our
[Walkthroughs: Deploy a pipeline with Airflow and Google Composer](../../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md)
to find out more.
