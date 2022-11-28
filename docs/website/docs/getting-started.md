---
sidebar_position: 3
---

# Getting started

Follow the steps below to have a working `dlt` [pipeline](./glossary.md/#pipeline) in 5 minutes. 

Please make sure you have [installed `dlt`](./installation.mdx) before getting started here.

## 1. Initialize project

Create a `dlt` project with a pipeline that loads data from the chess.com API to Google BigQuery by running:

```
dlt init chess bigquery
```

Install the dependencies necessary for Google BigQuery:
```
pip install -r requirements.txt
```

## 2. Set up Google BigQuery

**a. Log in to or create a Google Cloud account**

 Sign up for or log in to the [Google Cloud Platform](https://console.cloud.google.com/) in your web browser.

**b. Create a new Google Cloud project**

After arriving at the [Google Cloud console welcome page](https://console.cloud.google.com/welcome), click the
project selector in the top left, then click the `New Project` button, and finally click the `Create` button
after naming the project whatever you would like.

**c. Create a service account and grant BigQuery permissions**

You will then need to [create a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating). After clicking the `Go to Create service account` button
on the linked docs page, select the project you just created and name the service account whatever you would like.

Click the `Continue` button and grant the following roles, so that `dlt` can create schemas and load data:
- *BigQuery Data Editor*
- *BigQuery Job User*
- *BigQuery Read Session User*

You don't need to grant users access to this service account at this time, so click the `Done` button.

**d. Download the service account JSON**

In the service accounts table page that you are redirected to after clicking `Done` as instructed above,
select the three dots under the `Actions` column for the service account you just created and 
select `Manage keys`. 

This will take you to page where you can click the `Add key` button, then the `Create new key` button, 
and finally the `Create` button, keeping the preselected `JSON` option.

A `JSON` file that includes your service account private key will then be downloaded.

**e. Update your `dlt` credentials file with your service account info**

Open your `dlt` credentials file:
```
open .dlt/secrets.toml
```

Replace the `project_id`, `private_key`, and `client_email` with the values from the downloaded `JSON` file:
```
[destination.bigquery.credentials]

location = "US"
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```

## 3. Run pipeline

Run the pipeline to load data from the chess.com API to Google BigQuery by running:
```
python3 chess.py
```

Go to the [Google BigQuery](https://console.cloud.google.com/bigquery) console and view the tables 
that have been loaded.

## 4. Next steps

Now that you have a working pipeline, you have options for what to learn next:
- Try loading data to a [different destination](./destinations.md) like Amazon Redshift or Postgres
- [Deploy this pipeline](./walkthroughs/deploy-a-pipeline.md), so that the data is automatically 
loaded on a schedule
- [Create a pipeline](./walkthroughs/deploy-a-pipeline.md) for an API that has data you want to load and use
- Transform the [loaded data](./using-loaded-data/) with dbt or in Pandas DataFrames
- Set up a [pipeline in production](./running-in-production/) with scheduling, 
monitoring, and alerting