---
title: Deploy with Google Cloud Functions
description: How to deploy a pipeline with Google Cloud Functions
keywords: [how to, deploy a pipeline, Cloud Function]
---

# Deploy a pipeline with Google Could Fuctions

Before you can deploy a pipeline, you need to have a working knowledge of GCP and its associated services such as Cloud Functions, Cloud Source Repositories, Shell Editor, IAM and Permissions, and GCP service accounts. 

To deploy a pipeline using the GCP Cloud functions, you'll first need to set up a blank repo in Cloud Source Repositories, a service provided by GCP for hosting repositories, or you can clone it to your local machine and then install the Google Cloud SDK on your local machine.

## Setup Pipeline in Google Cloud Repositories
To deploy the pipeline, we'll use the Google Cloud Source Repositories(first method):

1. Sign in to your GCP account and enable the Cloud Functions API.
2. To set up the environment, you can follow these steps:
    - Create an empty repo in Cloud Source Repositories.
      (Cloud Source repository is a service in GCP for hosting repositories from different platforms).
        
    - After making the repo, click Edit repo to open it in a "Shell Editor".
3. In this guide, we'll demonstrate using the `dlt` Notion verified source. However, you can use any verified source or create a custom one to suit your needs.
4. In the shell editor
    - Run the following command to initialise the verified source with Notion and create a pipeline with BigQuery as the target.
    
      ```bash
      dlt init notion bigquery
      ```
    
    - After running the command, a new directory will be created with the necessary files and configurations.
    - Detailed information about initialising a verified source and a pipeline example can be found in the `dlthub` [documentation](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion).
5. Rename the file "notion_pipeline.py" to "[main.py](http://main.py/)" and modify the code as follows:
    
    ```python
    import os
    import dlt
    from .notion import notion_databases
    
    def load_databases() -> str:
        pipeline = dlt.pipeline(
            pipeline_name="notion",
            destination='bigquery',
            dataset_name="notion_data_aman_atul",
        )
        data = notion_databases()
    
        info = pipeline.run(data)
        print(info)
        return "Data loaded successfully". # returns the value
    
    # Created a new function to call the load_database function 
    def pipeline_notion(request):
        return load_databases()
    ```
    
- In this code, we define a function named "pipeline_notion" that will be invoked by the cloud function. By default, Google Cloud Functions searches for the function in "main.py".
6. Further, you need to modify,  the `__init__.py` file in the notion folder, and modify the `notion_databases` function as follows:
    
    ```python
    @dlt.source
    def notion_databases(
        database_ids: Optional[List[Dict[str, str]]] = None,
        api_key = notion_apikey_env, # Refers to the notion secret
    ) -> Iterator[DltResource]:
    ```
    
    Note how we have referenced api_key to the variable named "notion_apikey_env*"* instead of dlt.secrets.value.
 ## Managing secrets with Secret Manager
Next, go to the Google Secrets Manager. In Google Secrets Manager, create the secret "Notion API Key", name the secret "notion_secret" and add "Notion API key" to the secret values. To learn more about Google Secrets Manager, read the full **[documentation](https://cloud.google.com/secret-manager/docs/create-secret-quickstart) here**.
    
   - Assign the "Secret Manager Secret Accessor" role to the service account that was used to create the function (this is usually the default service account associated with the Google Project where the function will be created).
   - There would be a *default* *service account* associated with the project ID you are using, please assign the *Cloud Functions Developer* role to the associated service account. To learn more about the service account, read the [documentation here.](https://cloud.google.com/iam/docs/service-account-overview)

## Deploying GCP Cloud Function
In a shell editor, navigate to the directory where the [main.py](http://main.py/) file is located and run the following command in the terminal
```bash
  gcloud functions deploy pipeline_notion --runtime python310 --trigger-http --allow-unauthenticated --source .
```
        
- This command deploys a function named "pipeline_notion" with Python 3.10 as the runtime environment, an HTTP trigger, and allows unauthenticated access. The source "." refers to all files in the directory.
- See [the documentation](https://cloud.google.com/functions/docs) for more details on Google Cloud Functions.
### Setting up environmental variables in the Cloud feature using Secret Manager
After deploying the function, you need to create an environment variable in Cloud Function using the Notion Secret you created earlier.
Go to the Google Cloud function, click on the function you deployed earlier using the shell editor and select "EDIT".
 1. In the 'Runtime, Build, Connections and Security Settings' section, select 'Security and Images Repo'.
 2. Click on 'Add a secret reference' and select the secret you created, e.g. 'notion_secret'.
 3. Set the 'Reference method' to 'Mounted as environment variable'.
 4. In the 'Environment Variable' field, enter the name of the environment variable assigned in the source method, in this case, 'notion_apikey_env', and set the version to 'latest'.
 5. Finally, click 'DEPLOY' to deploy the function. The HTTP trigger will now successfully execute the pipeline each time the URL is triggered.


## Monitor (and manually trigger) the cloud function
To manually trigger the function created, you can send a manual POST request to the trigger URL created by the cloud function as follows
```python
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
    
Replace the webhook_url with the Trigger URL for the cloud function created. 
    
That’s it! Enjoy using `dlt` in google cloud functions!
