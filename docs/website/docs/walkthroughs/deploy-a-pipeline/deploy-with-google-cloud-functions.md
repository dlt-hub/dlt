---
title: Deploy with Google Cloud Functions
description: How to deploy a pipeline with Google Cloud Functions
keywords: [how to, deploy a pipeline, Cloud Function]
---

# Deploy a pipeline with Google Could Fuctions

This guide shows you how to deploy a pipeline using the gcloud shell and `dlt` CLI commands. To deploy a pipeline using this method, you must have a working knowledge of GCP and its associated services, such as cloud functions, cloud source repositories, shell editor, IAM and permissions, and GCP service accounts.  

To deploy a pipeline using the GCP cloud functions, you'll first need to set up an empty repo in Cloud Source Repositories, a service provided by GCP for hosting repositories, or you can clone it to your local machine and then install the Google Cloud SDK on your local machine.

## 1. Setup Pipeline in Google Cloud Repositories
To deploy the pipeline, we'll use the Google Cloud Source Repositories(first method):

1. Sign in to your GCP account and enable the Cloud Functions API.
2. To set up the environment, you can follow these steps:
    - Create an empty repo in Cloud Source Repositories.
      (Cloud Source repository is a service in GCP for hosting repositories from different platforms).
        
    - After making the repo, click Edit repo to open it in a "Shell Editor".
3. In this guide, we'll demonstrate using the `dlt` [Notion verified source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion). However, you can use any verified source or create a custom one to suit your needs.
4. In the shell editor
    - Run the following command to initialise the verified source with Notion and create a pipeline with BigQuery as the target.
    
      ```bash
      dlt init notion bigquery
      ```
    
    - After running the command, a new directory will be created with the necessary files and configurations.
    - Detailed information about initialising a verified source and a pipeline example can be found in the `dlthub` [documentation](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion).
5. In the notion_pipeline.py method, delete the `if __name__ == "__main__":` method and create a new Python file called "main.py", which can be configured as follows:
    
    ```python
    from notion_pipeline import load_databases

    def pipeline_notion(request):
      load_databases()
      return "Pipeline run successfull"
    ```
    By default, Google Cloud Functions looks for the main.py file in the main directory, and we called the `load_databases()` function from notion_pipeline.py as shown above.

    ### Managing secrets with Secret Manager
     Next, go to the Google Secrets Manager. In Google Secrets Manager, create the secret "Notion API Key", name the secret "notion_secret" and add "Notion API key" to the secret values. To learn more about Google Secrets Manager, read the full **[documentation](https://cloud.google.com/secret-manager/docs/create-secret-quickstart) here**.
    
      - Assign the "Secret Manager Secret Accessor" role to the service account that was used to create the function (*this is usually the default service account associated with the Google Project where the function is being created*).
      
      - There would be a *default* *service account* associated with the project ID you are using, please assign the *Cloud Functions Developer* role to the associated service account. To learn more about the service account, see the [documentation](https://cloud.google.com/iam/docs/service-account-overview) here.

## 2. Deploying GCP Cloud Function
In a shell editor, navigate to the directory where the "main.py" file is located and run the following command in the terminal
```bash
 gcloud functions deploy pipeline_notion --runtime python310 --trigger-http --allow-unauthenticated --source . --timeout 300

```
        
- This command deploys a function called "pipeline_notion" with Python 3.10 as the runtime environment, an HTTP trigger, and allows unauthenticated access. The source "." refers to all files in the directory. The timeout is set to 5 mins ( 300 secs ), if you are loading a large number of files to the destination you can increase this to 60 minutes for HTTP functions. 10 minutes for event driven functions. To learn more about the function timeout, you can read the [documentation here.](https://cloud.google.com/functions/docs/configuring/timeout)
- See [the documentation](https://cloud.google.com/functions/docs) for more details on Google Cloud Functions.
  
  ### Setting up environmental variables in the Cloud function using Secret Manager
  
  After deploying the function, you need to create an environment variable in Cloud Function using the Notion Secret you created earlier.
  Go to the Google Cloud function, click on the function you deployed earlier using the shell editor and select "EDIT".
   1. In the 'Runtime, Build, Connections and Security Settings' section, select 'Security and Images Repo'.
   2. Click on 'Add a secret reference' and select the secret you created, e.g. 'notion_secret'.
   3. Set the 'Reference method' to 'Mounted as environment variable'.
   4. In the 'Environment Variable' field, enter the name of the environment variable that corresponds to the argument required by the pipeline.  (*If the variable name is specified in secrets.toml, store the corresponding value in the secrets manager. For example, in this example the variable name is api_key. We have defined the value of api_key in the secrets manager secret called "notion_secret" and then declared this secret as an environment variable called "API_KEY", remember to capitalise the variable name required by the pipeline.*)
   7. Finally, click 'DEPLOY' to deploy the function. The HTTP trigger will now successfully execute the pipeline each time the URL is  triggered.


## 3. Monitor (and manually trigger) the cloud function
To manually trigger the created function, you can open the trigger URL in the address bar that was created by the Cloud function. The message "Pipeline run successful" would mean that the pipeline was successfully run and the data was successfully loaded into the destination.

    
That’s it! Enjoy using `dlt` in google cloud functions!
