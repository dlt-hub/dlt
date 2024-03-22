---
slug: reverse-etl-dlt
title: "dlt adds Reverse ETL - build a custom destination in minutes"
image:  https://storage.googleapis.com/dlt-blog-images/reverse-etl.png
authors:
  name: Adrian Brudaru
  title: Open source Data Engineer
  url: https://github.com/adrianbr
  image_url: https://avatars.githubusercontent.com/u/5762770?v=4
tags: [reverse etl, pythonic]
---

# Pythonic reverse ETL is here

## Why python is the right approach for doing Reverse ETL

Reverse ETL is generally about putting data into a business app. This data would often come from a SQL database used as a middle layer for data integrations and calculations.

That’s fine - but nowadays most data people speak python, and the types of things we want to put into an operational app don’t always come from a DB, they often come from other business apps, or from things like a dataframe on which we did some scoring, etc.

![reverse etl](https://storage.googleapis.com/dlt-blog-images/reverse-etl.png)

### The full potential of Reverse ETL is in the flexibility of sources

Sql databases are a good start, but in reality very often our data source is something else. More often than not, it’s a python analyst’s implementation of some scoring, or some business calculation.

Other times, it’s a business app - for example, we might have a form that sends the response data to a webhook, from where it could end up in Salesforce, DWH and Slack as a notification. And of course, if this is done by a data person it will be done in Python.

Such, it follows that if we want to cater to the data crowd, we need to be pythonic.

## There’s synergy with ETL

Reverse ETL is ultimately ETL. Data is extracted from a source, it’s transformer, and then loaded to a destination. The challenges are similar, the most notable difference being in the fact that pulling data from a strongly typed environment like a db and converting it to weakly typed JSON is MUCH easier than the other way around. So in fact you can argue that Reverse ETL is simpler than ETL.

### Flavors of Reverse ETL

Just like we have ETL and ELT, we also have flavors of Reverse ETL

- Reverse ETL, or TEL: Transform the data to a specification, read it from db and send it to the app. An example could be.
- Tool Reverse ETL or ETL: Extract from db, map fields to destination in the tool, load to destination.
- Pythonic Freestyle Reverse ETL: You extract data from wherever you want and put it anywhere except storage/db. Transformations optional. :

Examples of Python reverse ETL

- Read data from Mongo, do anomaly detection, notify anomalies to slack.
- Read membership data from Stripe, calculate chance to churn, upload to CRM for account managers.
- Capture a form response with a webhook and send the information to CRM, DWH and slack.

## Add python? - new skills unlocked!

So why is it much better to do reverse ETL in python?

- **Live Streaming and Flexibility**: Python's ability to handle live data streams and integrate with various APIs and services surpasses the capabilities of SQL-based data warehouses designed for batch processing.
- **End-to-End Workflow**: Employing Python from data extraction to operational integration facilitates a streamlined workflow, enabling data teams to maintain consistency and efficiency across the pipeline.
- **Customization and Scalability**: Python's versatility allows for tailored solutions that can scale with minimal overhead, reducing the reliance on additional tools and simplifying maintenance.
- **Collaboration and Governance**: By keeping the entire data workflow within Python, teams can ensure better governance, compliance, and collaboration, leveraging common tools and repositories.

## **Example: Building a Custom Destination and a pipeline in under 1h.**

Documentation used:
Building a destination: [docs](https://dlthub.com/devel/dlt-ecosystem/destinations/destination)
SQL source: [docs](https://dlthub.com/devel/dlt-ecosystem/verified-sources/sql_database)
In this example, you will see why it’s faster to build a custom destination than set up a separate tool.

DLT allows you to define custom destination functions. You'll write a function that extracts the relevant data from your dataframe and formats it for the Notion API.

This example assumes you have set up Google Sheets API access and obtained the necessary credentials to authenticate.T

### **Step 1: Setting Up Google Sheets API (10min)**

1. Enable the Google Sheets API in the Google Developers Console.
2. Download the credentials JSON file.
3. Share the target Google Sheet with the email address found in your credentials JSON file.

### **Step 2: Define the Destination method in its own file `sheets_destination.py` (20min)**

Install the required package for Google API client:

```bash
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

```

Here’s how to define a destination function to update a Google Sheet:

```python
import dlt
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

@dlt.destination(name="update_google_sheet", batch_size=10)
def append_to_google_sheets(items, table_schema, sheets_id: str = dlt.config.value("SHEETS_ID"), credentials_json: dict = dlt.secrets.value("GOOGLE_SHEETS_CREDENTIALS"), range_name: str = 'Sheet1!A1'):
    """
    Send data to a Google Sheet.
    :param items: Batch of items to send.
    :param table_schema: Schema of the table (unused in this example but required by dlt).
    :param sheets_id: ID of the Google Sheet, retrieved from config.
    :param credentials_json: Google Service Account credentials, retrieved from secrets.
    :param range_name: The specific range within the Sheet where data should be appended.
    """
    credentials = Credentials.from_service_account_info(credentials_json)
    service = build('sheets', 'v4', credentials=credentials)

    values = [
        [item['id'], item['name'], item['status']] for item in items  # Adjust based on your data structure
    ]
    body = {'values': values}
    result = service.spreadsheets().values().append(
        spreadsheetId=sheets_id, range=range_name,
        valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body
    ).execute()
    print(f"{result.get('updates').get('updatedRows')} rows have been added to the sheet.")


```

### **Step 3: Configure secrets (5min)**

For the custom destination, you can follow this example. Configure the source as instructed in the source [documentation](https://dlthub.com/devel/dlt-ecosystem/verified-sources/shopify).

 **`secrets.toml`**

```toml
[google_sheets]
credentials_json = '''
{
  "type": "service_account",
  "project_id": "your_project_id",
  "private_key_id": "your_private_key_id",
  ...
}
'''
```

**`config.toml`**

```toml
[SHEETS]
id = "your_google_sheet_id_here"

```

### **Step 4: Running the pieline in `my_pipeline.py`(10min)**

Now, assuming you have a source function **`sql_database()`** from the verified sources, you can set up and run your pipeline as follows:

```python

import dlt
from dlt.common.destination import Destination
from your_source_module import sql_database  # Adjust the import based on your actual source module

# Assuming your source and destination setup as previously defined

# Initialize the dlt pipeline with a unique name
pipeline = dlt.pipeline(
    name="my_google_sheets_pipeline",
    destination=Destination.from_reference(
        destination_callable="sheets_destination.append_to_google_sheets"
    )
)

# Use the source function and specify the resource "people_report"
source = sql_database().with_resources("people_report")

# Now, run the pipeline with the specified source
# The write_disposition parameter might be unused depending on your destination function's implementation
info = pipeline.run(source=source)

```

In this setup, **`send_to_google_sheets`** acts as a custom destination within your dlt pipeline, pushing the fetched data to the specified Google Sheet. This method enables streamlined and secure data operations, fully utilizing Python's capabilities for Reverse ETL processes into Google Sheets.

## What does dlt do for me here?

Using dlt for reverse ETL instead of plain Python, especially with its **`@dlt.destination`** decorator, provides you with a structured framework that streamlines the process of integrating data into various destinations. Here’s how the dlt decorator specifically aids you compared to crafting everything from scratch in plain Python:

### Faster time to Production grade pipelines.

The **`@dlt.destination`** decorator significantly reduces the need for custom boilerplate code. It provides a structured approach to manage batch processing, error handling, and retries, which would otherwise require complex custom implementations in plain Python. This built-in functionality ensures reliability and resilience in your data pipelines.

### **Focus on custom business logic and adding value**

The flexibility of creating custom destinations with dlt shifts the focus from the possibilities to the necessities of your specific use case. This empowers you to concentrate on implementing the best solutions for your unique business requirements.

### **Scalability and efficient resource use**

dlt facilitates efficient handling of large data loads through chunking and batching, allowing for optimal use of computing resources. This means even small worker machines can stream data effectively into your chosen endpoint instead of wasting a large machine waiting for network. The library design supports easy scaling and adjustments. Making changes to batch sizes or configurations is straightforward, ensuring your data pipelines can grow and evolve with minimal effort. This approach not only makes maintenance simpler but ensures that once a solution is implemented, it's broadly applicable across your projects.

### **In Conclusion**

Reverse ETL is just a piece of the ETL puzzle. It could be done cleaner and better when done in python end to end.

Tools will always appeal to the non technical folks. However, anyone with ability to do python pipelines can do Reverse ETL pipelines too, bringing typical benefits of code vs tool to a dev team - customisation, collaboration, best practices etc.

So read more about ~~how to built a dlt destination~~ and consider giving it a try in your next reverse ETL pipeline.