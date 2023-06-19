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
[the cli command](../../reference/command-line-interface.md#airflow-composer):

```bash
dlt deploy {pipeline_script}.py airflow-composer
```

which generates the necessary code and instructions.

Read our
[Walkthroughs: Deploy a pipeline with Airflow and Google Composer](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md)
to find out more.

## How does the cli deployment work?

In the case of airflow-composer, running the deploy command will do the following:

1. Instructions will get printed to your CLI. You will need to follow them.
1. Two folders are created: `build` and `dags`:
   1. The `build` folder contains a file called `cloudbuild.yaml` with a simple configuration for
      cloud deployment.
   1. The `dags` folder contains a Python script called `dag_{pipeline_name}.py`, it contains
      instructions of when and how to run your pipeline.
1. You need to modify the `dags/dag_{pipeline_name}.py` file in the `dags` directory. You'll find
   the structure of the file in
   [the guide](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#2-modify-dag-file),
   and you can customize it by changing the name, runtime settings, and other parameters to fit your
   needs.
1. You need to configure the `build/cloudbuild.yaml` file to run the deployment on the cloud
   platform you're using.
   [The guide](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#5-configure-buildcloudbuildyaml)
   provides the necessary steps for this configuration.
1. Add the necessary credentials and packages to the Airflow environment using Google Composer UI.
   These credentials and packages are required for the pipeline to access the necessary resources.

Here's what the instructions look like - by following them step by step you will have a running
deployment:

```toml
Your airflow-composer deployment for pipeline workable is ready!
* The airflow cloudbuild.yaml file was created in build.
* The dag_workable.py script was created in dags.

You must prepare your repository first:
1. Import your sources in dag_workable.py, change default_args if necessary.
2. Run airflow pipeline locally.
See Airflow getting started: https://airflow.apache.org/docs/apache-airflow/stable/start.html

If you are planning run the pipeline with Google Cloud Composer, follow the next instructions:

1. Read this doc and set up the Environment: https://dlthub.com/docs/running-in-production/orchestrators/airflow-gcp-cloud-composer
2. Set _BUCKET_NAME up in build/cloudbuild.yaml file.
3. Add the following secret values (typically stored in ./.dlt/secrets.toml):
SOURCES__WORKABLE__ACCESS_TOKEN

in ENVIRONMENT VARIABLES using Google Composer UI

Name:
SOURCES__WORKABLE__ACCESS_TOKEN
Secret:
JoI...

4. Add dlt package below using Google Composer UI.
dlt[bigquery]>=0.3.0
NOTE: You may need to add more packages ie. when your source requires additional dependencies
5. Commit and push the pipeline files to github:
a. Add stage deployment files to commit. Use your Git UI or the following command
git add dags/dag_workable.py build/cloudbuild.yaml
b. Commit the files above. Use your Git UI or the following command
git commit -m 'initiate workable pipeline with Airflow'
c. Push changes to github. Use your Git UI or the following command
git push origin
6. You should see your pipeline in Airflow.
```