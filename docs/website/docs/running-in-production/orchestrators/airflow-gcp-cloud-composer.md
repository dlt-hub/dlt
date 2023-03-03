# Cloud composer setup

### CI-CD setup

- Create a github repository
- in google cloud web interface, go to Source repositories and create a repository that mirrors your github repo. This will simplify the authentication by doing it through this mirroring service.
- in this github repository, add the following folders:
    - build
        - with a file called `cloudbuild.yaml`, we will use this below
    - dags
        - with another folder called `pipedrive`that will contain a dlt pipeline

   ![folder-structure](/img/orchestrator_gcp/folder-structure.png)
- In cloud build, add a trigger on commit to main
- point it to your cloud build file. In our example, we place our file at `build/cloudbuild.yaml`

    ![trigger-config](/img/orchestrator_gcp/trigger-config.png)


- Now, create this file in your repository. To fill it, we need to get the name of the dags bucket from Cloud composer, so we know where to deploy.
- Go to cloud composer, click on the dags folder, and get the bucket name

![test-composer](/img/orchestrator_gcp/test-composer.png)

- in your `cloudbuild.yaml`, paste the below code and replace the bucket name with the correct bucket name. This code copies the dags folder from the repo into the bucket’s dags folder.

```yaml
steps:
  - name: gcr.io/cloud-builders/gsutil
    args: ["-m", "rsync", "-r", "-c", "-d", "./dags/", "gs://us-central1-test-f3c5800e-bucket/dags"]
```

- make sure your repository code is pushed to main
- Run the trigger you build (in cloud build)
- Wait a minute, and check if your files arrived in the bucket. In our case, we added a `pipedrive`folder, and we can see it appeared.

![bucket-details](/img/walkthroughs/bucket-details.png)

### Airflow setup

### Adding the libraries needed

Assuming you already spun up a cloud composer

- Make sure the user you added has rights to change the base image (add libraries). I already had these added, you may get away with less (not clear in docs)
    - Artifact Registry Administrator
    - Artifact Registry Repository Administrator
    - Remote Build Execution Artifact Admin
- Navigate to your composer environment and add the needed libraries. In the case of this example pipedrive pipeline, we only need dlt, so add `python-dlt` library


![add-package](/img/orchestrator_gcp/add-package.png)

### Adding credentials

You have multiple ways to manage credentials in airflow and several outside

Airflow:

- credentials basehook - this is OK but dependent on the deployment
- env variables - simple alternative, not encrypted, can be added through web ui

External:

- Google secrets - this is a SOC2 compliant store, to implement it, you need to add a function to call the credentials and give your airflow user permissions to query secrets.

In this example, we will add our credential to env for simplicity

- Get the credential name from your source. In our case, it’s `pipedrive_api_key`

```yaml
@dlt.source(name="pipedrive")
def pipedrive_source(pipedrive_api_key=dlt.secrets.value, fix_custom_fields=True):
```

- Capitalize it and add it into airflow’s env variables, save it. Now, dlt can pick it up.

![add-credential](/img/orchestrator_gcp/add-credential.png)

### Destination credentials

If you are on google cloud and using bigquery, you don’t need to pass credentials - just ensure your ETL service account has permission to load to bigquery.

If you are using a different destination, set up those credentials in the same way as you did Pipedrive. You can find the required variable names by attempting to run the pipeline and looking out for the CLI error about which credentials are missing.

### Setting up the first pipeline

You can follow the Pipedrive example for a different source, by doing the step by step instructions below

1. Copy the pipeline code into a folder of its own by doing `dlt init pipeline_name`
2. Set up credentials so you can test. Fill them in the secrets file for now.
3. Add a dag that calls your source such as the one here: [https://github.com/dlt-hub/airflow_example/blob/main/dags/dag_pipedrive.py](https://github.com/dlt-hub/airflow_example/blob/main/dags/dag_pipedrive.py)
4. git add, commit, push, and you should see your pipeline in Airflow

![pipeline](/img/orchestrator_gcp/pipeline.png)


5. If you wish to use dlt, please refer to the dlt transformation guide.