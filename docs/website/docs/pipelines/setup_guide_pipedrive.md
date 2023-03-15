# Pipedrive pipeline setup guide

# **Pipedrive setup**

1. Set up a pipedrive account.
2. Pipedrive provides a unique domain name. The domain name is generally [yourbusiness].pipedrive.com
3. For eg. If a company name is dlthub, then the domain name shall be dlthub.pipedrive.com or something similar.

## API token for authentication

1. In Pipedrive, go to your name(in the top right corner) and company settings.
2. Go to personal preferences
3. Select the API tab and copy your API token(to be used in the dlt configuration)
4. To learn more about pipedrive API token please read the [docs](https://pipedrive.readme.io/docs/how-to-find-the-api-token).

## Initing the pipeline

1. Initialize the pipeline by using the following command

```bash
dlt init pipedrive destination
```

2. To know more about destinations, please read the [docs](https://dlthub.com/docs/destinations).

3. The file structure for the inited repository shall look like below:

```bash
pipedrive_pipeline
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── pipedrive
│   └── pipedrive_docs_images
│   └── __init__.py
│   └── custom_fields_munger.py
│   └── ReadMe.md
├── .gitignore
├── pipedrive_pipeline.py
└── requirements.txt
```

4. To learn more about initing please read the docs [“adding a pipeline”](https://dlthub.com/docs/walkthroughs/add-a-pipeline).

## Configuring credentials

1. In .dlt folder is  secrets.toml. It should look like the code below

```bash
# put your secret values and credentials here. do not share this file and do not push it to github
pipedrive_api_key = "Pipedrive API Token" # please set me up!

[destination.bigquery.credentials]
project_id = "set me up" # GCP project ID!
private_key = "set me up" # Unique private key !(Must be copied fully including BEGIN and END PRIVATE KEY)
client_email = "set me up" # Email for service account
location = "set me up" #Project Location For ex. “US”
```

2. For the pipedrive API token go to your name(in the top right corner) > company settings> personal preferences > API token *(Copy that and paste above in pipedrive_api_key)* 
3. Google cloud credentials may be copied from secrets manager in GCP.
4. To learn more on bigquery authentication please read the [docs](https://cloud.google.com/bigquery/docs/authentication).

## Running the pipeline

1. To install requirements for the pipeline, run the following command

```bash
pip install -r requirements.txt
```

2. To run the pipeline, run the following command.

```bash
python3 pipedrive_pipeline.py
```

3. After a successful pipeline run, the load job shall be completed and data shall be loaded to the destination.