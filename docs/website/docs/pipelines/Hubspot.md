---
title: Hubspot
description: dlt pipeline for Hubspot API
keywords: [hubspot api, hubspot pipeline, hubspot]
---

# HubSpot

HubSpot is a customer relationship management (CRM) software and inbound marketing platform that helps businesses to attract visitors, engage customers, and close leads. 

The `dlt` HubSpot pipeline allows you to automatically load data from HubSpot into a [destination](https://dlthub.com/docs/destinations) of your choice. It loads data from the following endpoints:

|API|Data|
| --- | --- |
| Contacts | visitors, potential customers, leads |
| Companies | information about organizations  |
| Deals | deal records, deal tracking |
| Products | goods, services |
| Tickets | request for help from customers or users |
| Quotes | pricing information of a product |
| Web analytics  | events |

## Get API credentials

:::note
As of November 30, 2022, HubSpot API Keys are being deprecated and are no longer supported. Instead, it would be best to authenticate using a private app access token or OAuth.
:::

Before running the pipeline, you will need to get API credentials. HubSpot no longer supports direct access tokens, but you can get an authentication token by creating a private app as follows:

1. Log in to your HubSpot account and go to ⚙️ settings in the main navigation bar.
2. In the left sidebar menu under **Account Setup** select "Integrations" and then click on "Private Apps".
4. Click on “Create a private app”.
5. Fill in the “Basic Info” tab, by specifying a name and description.
6. Next go to the “Scopes” tab and select the following permissions:
    1. All the read scopes under the CMS, CRM, and Settings options.
    2. The following under the Standard options:
    
    ```
    
    business-intelligence, actions, crm.export, e-commerce, oauth, tickets
    ```
    
7. Click on Create app and choose Continue creating.
8. Click on Show token, and copy the displayed token. This will need to be added to the pipeline.
        

## Initialize the pipeline

Initialize the pipeline with the following command:

`dlt init hubspot bigquery`

Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations](https://dlthub.com/docs/destinations/duckdb).

Running this command will create a directory with the following structure:

```bash
hubspot_pipeline
├── .dlt
│   ├── .pipelines
│   ├── config.toml
│   └── secrets.toml
├── hubspot
│   └── __init__.py
│   └── client.py
│   └── endpoints.py
├── .gitignore
├── hubspot_pipeline.py
└── requirements.txt
```

## Specify the data loading function

1. There are two data loading functions inside the script `hubspot_pipeline.py`:
    1. `load_without_events()`: This function loads data from HubSpot to the destination without enabling company events.
    2. `load_with_company_events()`: This function loads data from HubSpot to the destination with company and contacts events selected.
    
2. To specify a loading function, simply include it in the `__main__` block:
```python
if __name__ == "__main__":

    #load_without_events() # Comment out the functions that you don't want to use
    load_with_company_events()
```

## Run the pipeline[](https://dlthub.com/docs/pipelines/pipedrive#run-the-pipeline)

1. Install requirements for the pipeline by running the following command:

```
pip install -r requirements.txt

```

2. Run the pipeline with the following command:

```
python3 hubspot_pipeline.py

```

3. To make sure that everything is loaded as expected, use the command:
```
dlt pipeline hubspot_pipeline show 
```
