---
title: Deploy with Google Cloud Functions
description: How to deploy a pipeline with Google Cloud Functions
keywords: [how to, deploy a pipeline, Cloud Function]
---

# Deploy a pipeline with Google Cloud Functions

This guide shows you how to deploy a pipeline using the gcloud shell and dlt CLI commands. To deploy a pipeline using this method, you must have a working knowledge of GCP and its associated services, such as Cloud Functions, IAM and permissions, and GCP service accounts.

To deploy a pipeline with GCP Cloud Functions, navigate to the directory on your local machine or cloud repository (e.g., GitHub, Bitbucket) from where the function code is to be deployed.

## 1. Setup pipeline

1. In this guide, we'll be setting up the dlt
   [Notion verified source](../../dlt-ecosystem/verified-sources/notion). However, you can use any verified source or create a custom one to suit your needs.
1. In the terminal:
   - Run the following command to initialize the verified source with Notion and create a pipeline example with BigQuery as the target.

     ```sh
     dlt init notion bigquery
     ```

   - After the command executes, new files and folders with the necessary configurations are created in the main directory where the command was executed.

   - Detailed information about initializing a verified source and a pipeline example can be found in the dlthub [documentation](../../dlt-ecosystem/verified-sources/notion).
1. Create a new Python file called "main.py" in the main directory. The file can be configured as follows:
   ```py
   from notion_pipeline import load_databases

   def pipeline_notion(request):
     load_databases()
     return "Pipeline run successfully!"
   ```
   By default, Google Cloud Functions looks for the "main.py" file in the directory.
   
1. If you need any additional dependencies, add them to the "requirements.txt" that was created.

## 2. Deploying GCP Cloud Function

In the terminal, navigate to the directory where the "main.py" file is located and run the following command in the terminal:

```sh
gcloud functions deploy pipeline_notion --runtime python310 \
  --trigger-http --allow-unauthenticated --source . --timeout 300
```

- This command uses a function called `pipeline_notion` with Python 3.10 as the runtime environment, an HTTP trigger, and allows unauthenticated access. The source "." refers to all files in the directory. The timeout is set to 5 minutes (300 seconds). To learn more about deploying the cloud function, read the [documentation here.](https://cloud.google.com/functions/docs/deploy)
- If you are uploading a large number of files to the destination, you can increase this to 60 minutes for HTTP functions and 10 minutes for event-driven functions. To learn more about the function timeout, see the [documentation here](https://cloud.google.com/functions/docs/configuring/timeout).

> Your project has a default service account associated with the project ID. Please assign the `Cloud Functions Developer` role to the associated service account.

## 3. Setting up environmental variables in the Cloud Function

Environmental variables can be declared in the Cloud Function in two ways:

#### 3a. Directly in the function:

- Go to the Google Cloud Function and select the deployed function. Click "EDIT".
- Navigate to the "BUILD" tab and click "ADD VARIABLE" under "BUILD ENVIRONMENTAL VARIABLE".
- Enter a name for the variable that corresponds to the argument required by the pipeline. Make sure
  to capitalize the variable name if it is specified in "secrets.toml". For example, if the variable
  name is `api_key`, set the variable name to `API_KEY`.
- Enter the value for the Notion API key.
- Click Next and deploy the function.

#### 3b. Use GCP Secret Manager:

- Go to the Google Cloud function and select the function you deployed. Click "EDIT".
- In the "Runtime, Build, Connections and Security Settings" section, select "Security and Images
  Repo".
- Click "Add a secret reference" and select the secret you created, for example, "notion_secret".
- Set the "Reference method" to "Mounted as environment variable".
- In the "Environment Variable" field, enter the environment variable's name that corresponds
  to the argument required by the pipeline. Remember to capitalize the variable name if it is
  required by the pipeline and specified in secrets.toml. For example, if the variable name is
  `api_key`, you would declare the environment variable as `API_KEY`.
- Finally, click "DEPLOY" to deploy the function. The HTTP trigger will now successfully execute the
  pipeline each time the URL is triggered.
- Assign the `Secret Manager Secret Accessor` role to the service account used to deploy the cloud
  function. Typically, this is the default service account associated with the Google Project in
  which the function is being created.

## 4. Monitor (and manually trigger) the cloud function

To manually trigger the created function, you can open the trigger URL created by the Cloud Function
in the address bar. The message "Pipeline run successfully!" confirms that the pipeline was
successfully run and the data was successfully loaded into the destination.

That's it! Have fun using dlt in Google Cloud Functions!

## Deploy as a webhook

A webhook is a way for one application to send automated messages or data to another application in real time. Unlike traditional APIs, which require constant polling for updates, webhooks allow applications to push information instantly as soon as an event occurs. This event-driven architecture enables faster and more responsive interactions between systems, saving valuable resources and improving overall system performance.

With this `dlt` Google Cloud event ingestion webhook, you can ingest the data and load it to the destination in real time as soon as a post request is triggered by the webhook. You can use this cloud function as an event ingestion webhook on various platforms such as Slack, Discord, Stripe, PayPal, or any service that supports webhooks.

You can set up a GCP cloud function webhook using `dlt` as follows:

### 1. Initialize deployment

1. Sign in to your GCP account and enable the Cloud Functions API.
2. Go to the Cloud Functions section and click Create Function. Set up the environment and select the region.
3. Configure the trigger type; you can use any trigger, but for this example, we will use HTTP and select "Allow unauthenticated invocations".
4. Click "Save" and then "Next".
5. Select "Python 3.10" as the environment.
6. Use the code provided to set up the cloud function for event ingestion:

    ```py
    import dlt
    import time
    from google.cloud import bigquery
    from dlt.common import json

    def your_webhook(request):
        # Extract relevant data from the request payload
        data = request.get_json()

        Event = [data]

        pipeline = dlt.pipeline(
            pipeline_name='platform_to_bigquery',
            destination='bigquery',
            dataset_name='webhooks',
        )

        pipeline.run(Event, table_name='webhook') #table_name can be customized
        return 'Event received and processed successfully.'
    ```

7. Set the function name as "your_webhook" in the Entry point field.
8. In the requirements.txt file, specify the necessary packages:

    ```text
    # Function dependencies, for example:
    # package>=version
    dlt
    dlt[bigquery]
    ```

9. Click on "Deploy" to complete the setup.

> You can now use this cloud function as a webhook for event ingestion on various platforms such as Slack, Discord, Stripe, PayPal, and any other as per your requirement. Just remember to use the "Trigger URL" created by the cloud function when setting up the webhook. The Trigger URL can be found in the Trigger tab.


### 2. Monitor (and manually trigger) the webhook

To manually test the function you have created, you can send a manual POST request as a webhook using the following code:

```py
import requests

webhook_url = 'please set me up!' # Your cloud function Trigger URL
message = {
    'text': 'Hello, Slack!',
    'user': 'dlthub',
    'channel': 'dlthub'
}

response = requests.post(webhook_url, json=message)
if response.status_code == 200:
  print('Message sent successfully.')
else:
  print('Failed to send message. Error:', response.text)
```

> Replace the `webhook_url with the Trigger URL for the cloud function created.
Now, after setting up the webhook using cloud functions, every time an event occurs, the data will be ingested into your specified destination.
