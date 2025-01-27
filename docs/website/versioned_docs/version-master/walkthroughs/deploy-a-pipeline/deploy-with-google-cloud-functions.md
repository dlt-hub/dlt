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

