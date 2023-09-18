---
title: 'Importing Ticket Data from Zendesk API to Weaviate'
description: Importing Ticket Data from Zendesk API to Weaviate
keywords: [how to, zendesk, weaviate, vector database, vector search]
---

# Importing Ticket Data from Zendesk API to Weaviate

Zendesk is a cloud-based customer service and support platform. Zendesk Support API, which is also known as the Ticketing API lets’s you access support tickets data. By analyzing this data, businesses can gain insights into customer needs, behavior, trends, and make data-driven decisions. The newest type of databases, vector databases, can help in advanced analysis of tickets data such as identifying common issues and sentiment analysis.

In this walkthrough, we’ll show you how to import Zendesk ticket data to one of the vector databases, Weaviate. We’ll use dlt to connect to the Zendesk API, extract the data, and load it into Weaviate.

## Prerequisites

We're going to use some ready-made components from the [dlt ecosystem](https://dlthub.com/docs/dlt-ecosystem) to make this process easier:

1. A [Zendesk verified source](../dlt-ecosystem/verified-sources/zendesk.md) to extract the tickets from the API.
2. A [Weaviate destination](../dlt-ecosystem/destinations/weaviate.md) to load the data into a Weaviate instance.

## Setup

1. Create a new folder for your project, navigate to it, and create a virtual environment:

    ```bash
    mkdir zendesk-weaviate
    cd zendesk-weaviate
    python -m venv venv
    source venv/bin/activate
    ```
2. Install dlt with Weaviate support

    ```bash
    pip install "dlt[weaviate]"
    ```

3. Install dlt Zendesk verified source

    ```bash
    dlt init zendesk weaviate
    ```

The last command [dlt init](../reference/command-line-interface#dlt-init) initializes dlt project: it downloads the verified source and installs it in your project folder.

## Configuration

### Source

Before we configure the source and destination, you need to make sure you have access to API in both Zendesk and Weaviate.

Head to the [Zendesk](../dlt-ecosystem/verified-sources/zendesk.md) docs to see how to fetch credentials for Zendesk API. In our walkthrough, we're using the email address and password authentication method. Once you have fetched the credentials, you can configure the source. Add the following lines to the dlt secrets file `~/.dlt/secrets.toml`:

```toml
[sources.zendesk.zendesk_support.credentials]
password = "..."
subdomain = "..."
email = "..."
```

### Destination

For the destination we're using [Weaviate Cloud Services](https://console.weaviate.cloud/). You would need to create an account and get a URL and an API key for your Weaviate instance. We're also be using [OpenAI API](https://platform.openai.com/) to generate embeddings for the text data needed to perform vector search. If you haven't already, you would need to create an account and get an API key for OpenAI API.

When you have the credentials, add more lines to the dlt secrets file `~/.dlt/secrets.toml`:

```toml
[destination.weaviate.credentials]
url = "https://weaviate_url"
api_key = "api_key"

[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "sk-..
```

### Configuring the pipeline

When you run `dlt init zendesk weaviate`, dlt creates a file called `zendesk_pipeline.py` in the current directory. This file contains an example pipeline that you can use to load data from Zendesk source. Let's edit this file to make it work for our use case:

```python
import dlt
from dlt.destinations.weaviate import weaviate_adapter

from zendesk import zendesk_support

def main():
    # 1. Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="weaviate_zendesk_pipeline",
        destination="weaviate",
        dataset_name="zendesk_data",
    )

    # 2. Initialize Zendesk source to get the ticket data
    zendesk_source = zendesk_support(load_all=False)
    tickets = zendesk_source.tickets

    info = pipeline.run(
        # 3. Here we use a special function to tell Weaviate to
        # tell Weaviate which fields to vectorize
        weaviate_adapter(
            tickets,
            vectorize=["subject", "description"],
        ),
    )

    return info

if __name__ == "__main__":
    load_info = main()
    print(load_info)
```

### Running the pipeline

Now that we have the pipeline configured, we can run it:

```bash
python zendesk_pipeline.py
```

We have successfully loaded the data from Zendesk to Weaviate. Let's check it out.

## Query the data

Let's query the Weaviate to select all tickets similar to the ticket with the ID `56b9449e-65db-5df4-887b-0a4773f52aa7` with the subject of the ticket is “How do I change the password for my account?”

```python
    import weaviate
    client = weaviate.Client(
        url='YOUR_WEAVIATE_URL',
        auth_client_secret=weaviate.AuthApiKey(
            api_key='YOUR_WEAVIATE_API_KEY'
        ),
        additional_headers={
            "X-OpenAI-Api-Key": 'YOUR_OPENAI_API_KEY'
        }
    )

    # Select all tickets similar to the ticket with the ID.
    response = (
        client.query
        .get("ZendeskData_Tickets", ["subject", "description"])
        .with_near_object({
            "id": "56b9449e-65db-5df4-887b-0a4773f52aa7"
        })
        .with_additional(["distance"])
        .do()
    )

	  print(response)
```

The output should be similar to:

```json
{
 "data": {
   "Get": {
     "ZendeskData_Tickets": [
       {
          "subject": "How to update my account information?",
           "description": "I also want to change my email and password.",
       },
       {
          "subject": "What are the steps to reset the password for my account?",
          "description": "I can't access my account because I've forgotten my password.",
       },
			 ...
     ]
   }
}
```

## What's next?

If you interested in learning more about Weaviate support in dlt, check out the [Weaviate destination](../dlt-ecosystem/destinations/weaviate.md) docs. We have also have demos of different sources of data for Weaviate in our Jyputer notebooks:

- [Loading PDF data into Weaviate](https://github.com/dlt-hub/dlt_demos/blob/main/pdf_to_weaviate.ipynb)
- [Loading data from SQL database into Weaviate](https://github.com/dlt-hub/dlt_demos/blob/main/sql_to_weaviate.ipynb)

