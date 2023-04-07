# HubSpot

HubSpot is a customer relationship management (CRM) software and inbound marketing platform that helps businesses to attract visitors, engage customers, and close leads. 

The endpoints that could be loaded from HubSpot using this pipeline are as follows:

| Endpoint | Description |
| --- | --- |
| Contacts | visitors, potential customers, leads |
| Companies | information about organization  |
| Deals | deal records, deal tracking |
| Products | goods, services |
| Tickets | request for help from customers or users |
| Quotes | pricing information of a product |
| Web analytics  | events |

## Initialize the pipeline

Initialize the pipeline with the following command:

`dlt init hubspot bigquery`

Here, we chose BigQuery as the destination. To choose a different destination, replace `bigquery` with your choice of [destination](https://dlthub.com/docs/destinations).

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

## Grab API token

```
**Please note:**
As of November 30, 2022, HubSpot API Keys are being deprecated and are no longer supported. Instead, it would be best to authenticate using a private app access token or OAuth.
```

HubSpot no longer supports direct Access tokens, so API token can be grabbed using the private app as follows:

1. Log in to your HubSpot account.
2. On your account, go to ⚙️ settings in the main navigation bar
3. In the left sidebar menu, go to “Private Apps”
4. Click on “Create a private app”
5. In the “Basic info” tab, give the name and description of the app
6. In the “Scopes” tab give the following permissions:
    1. Check out all the read scopes in the CMS, CRM, and Settings options
    2. In the Standard options, check out the following :
    
    ```
    
    Business Intelligence, actions, crm.export, e-commerce, oauth, tickets
    ```
    
7. Click on Create app
8. Click on show token, and copy it (safely)

## Customize the pipeline

1. You can customize the pipeline by modifying `hubspot_pipeline.py`
2. It has two functions named
   1. `load_without_events()`
   
        As the name suggests, it loads the data from HubSpot to the destination without the events
   2. `load_with_company_events()` 
   
        This function loads the data from HubSpot into the destination with the company and
    
     contact events.
    
3. You can configure the main function in `huspot_pipeline.py` to run one of the above functions or to run both.

For example, if you want to run the pipeline for loading data with events, you may comment out the function `load_without_events()` as 

```python
if __name__ == "__main__":

    #load_without_events() #This way comment out a function to skip a it
    load_with_company_events()
```

4. After making appropriate changes in the pipeline file, save it (ctrl + s or cmd + s)

## Run the pipeline[](https://dlthub.com/docs/pipelines/pipedrive#run-the-pipeline)

1. Install requirements for the pipeline by running the following command:

```
pip install -r requirements.txt

```

2. Run the pipeline with the following command:

```
python3 hubspot_pipeline.py

```

3. Use `dlt pipeline hubspot show` to make sure that everything loaded as expected.