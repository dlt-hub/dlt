---
title: Pipedrive
description: dlt verified source for Pipedrive API
keywords: [pipedrive api, pipedrive verified source, pipedrive]
---

# Pipedrive

:::info
Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::


Here you will find a setup guide for the [Pipedrive](https://developers.pipedrive.com/docs/api/v1) verified source.

## Set up account

**To get started:**
1. Set up a Pipedrive account
2. Grab your Pipedrive subdomain

Pipedrive provides a unique domain name that is generally `[company].pipedrive.com`. For example, if your company name is `dltHub`, then the subdomain name is `dlthub.pipedrive.com`.

## Initialize the pipeline from Pipedrive verified source

**Initialize the pipeline by using the following command with your [destination](../../dlt-ecosystem/destinations) of choice:**
```bash
dlt init pipedrive [destination]
```

This will create a directory that includes the following file structure:
```
pipedrive_pipeline
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── pipedrive
│   └── pipedrive_docs_images
│   └── __init__.py
│   └── custom_fields_munger.py
│   └── README.md
├── .gitignore
├── pipedrive_pipeline.py
└── requirements.txt
```

## Grab API auth token

**On Pipedrive:**
1. Go to your name (in the top right corner)
2. Select company settings
3. Go to personal preferences
4. Select the API tab
5. Copy your API token (to be used in the dlt configuration)

You can learn more about Pipedrive API token authentication in the docs [here](https://pipedrive.readme.io/docs/how-to-find-the-api-token).

## Configure `dlt` credentials

1. In the `.dlt` folder, you will find `secrets.toml`, which looks like this:
```toml
# Put your secret values and credentials here
# Note: Do not share this file and do not push it to GitHub!
pipedrive_api_key = "PIPEDRIVE_API_TOKEN" # please set me up :)
```

2. Replace `PIPEDRIVE_API_TOKEN` with the API token you [copied above](#grab-api-auth-token)

3. Add the credentials required by your destination (e.g. [Google BigQuery](../destinations/bigquery.md)

## Run the pipeline

1. Install requirements for the pipeline by running the following command:
```bash
pip install -r requirements.txt
```

2. Run the pipeline with the following command:
```bash
python3 pipedrive_pipeline.py
```

3. Use `dlt pipeline pipedrive_pipeline show` to make sure that everything loaded as expected.