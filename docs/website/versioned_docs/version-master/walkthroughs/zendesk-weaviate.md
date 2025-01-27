---
title: 'Import ticket data from Zendesk API to Weaviate'
description: How to Import Ticket Data from Zendesk API to Weaviate
keywords: [how to, zendesk, weaviate, vector database, vector search]
---

# How to import ticket data from Zendesk API to Weaviate

Zendesk is a cloud-based customer service and support platform. Zendesk Support API, also known as the Ticketing API, lets you access support tickets data. By analyzing this data, businesses can gain insights into customer needs, behavior, trends, and make data-driven decisions. The newest type of databases, vector databases, can help in advanced analysis of tickets data such as identifying common issues and sentiment analysis.

In this guide, we’ll show you how to import Zendesk ticket data to one of the vector databases, Weaviate. We’ll use dlt to connect to the Zendesk API, extract the ticket data, and load it into Weaviate for querying.

For our example, we will use the "subject" and "description" fields from a ticket as text content to perform vector search on.

## Prerequisites

We're going to use some ready-made components from the [sources](../dlt-ecosystem/verified-sources) and [destinations](../dlt-ecosystem/destinations) to make this process easier:

1. A [Zendesk verified source](../dlt-ecosystem/verified-sources/zendesk.md) to extract the tickets from the API.
2. A [Weaviate destination](../dlt-ecosystem/destinations/weaviate.md) to load the data into a Weaviate instance.

## Setup

1. Create a new folder for your project, navigate to it, and create a virtual environment:

    ```sh
    mkdir zendesk-weaviate
    cd zendesk-weaviate
    python -m venv venv
    source venv/bin/activate
    ```
2. Install dlt with Weaviate support

    ```sh
    pip install "dlt[weaviate]"
    ```

3. Install dlt Zendesk verified source

    ```sh
    dlt init zendesk weaviate
    ```

The last command [dlt init](../reference/command-line-interface#dlt-init) initializes a dlt project: it downloads the verified source and installs it in your project folder.

## Configuration

### Source

Before we configure the source and destination, you need to make sure you have access to the API in both Zendesk and Weaviate.

Head to the [Zendesk](../dlt-ecosystem/verified-sources/zendesk.md) docs to see how to fetch credentials for the Zendesk API. In this guide, we're using the email address and password authentication method. Once you have fetched the credentials, you can configure the source. Add the following lines to the dlt secrets file `~/.dlt/secrets.toml`:

```toml
[sources.zendesk.zendesk_support.credentials]
password = "..."
subdomain = "..."
email = "..."
```

### Destination

For the destination, we're using [Weaviate Cloud Services](https://console.weaviate.cloud/). You will need to create an account and get a URL and an API key for your Weaviate instance. We're also using the [OpenAI API](https://platform.openai.com/) to generate embeddings for the text data needed to perform vector search. If you haven't already, you will need to create an account and get an API key for the OpenAI API.

When you have the credentials, add more lines to the dlt secrets file `~/.dlt/secrets.toml`:

```toml
[destination.weaviate.credentials]
url = "https://weaviate_url"
api_key = "api_key"

[destination.weaviate.credentials.additional_headers]
X-OpenAI-Api-Key = "sk-..."
```

### Customizing the pipeline

When you run `dlt init zendesk weaviate`, dlt creates a file called `zendesk_pipeline.py` in the current directory. This file contains an example pipeline that you can use to load data from a Zendesk source. Let's edit this file to make it work for our use case:

```py
import dlt
from dlt.destinations.adapters import weaviate_adapter

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
        # 3. Here we use a special function to tell Weaviate
        # which fields to vectorize
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

Let's go through the code above step by step:

1. We create a pipeline with the name `weaviate_zendesk_pipeline` and the destination `weaviate`.
2. Then, we initialize the Zendesk verified source. We only need to load the tickets data, so we get the `tickets` resource from the source by accessing the `tickets` attribute.
3. Weaviate is a special kind of destination that requires vectorizing (or [embedding](https://en.wikipedia.org/wiki/Word_embedding)) the data before loading it. Here, we use the `weaviate_adapter()` function to tell dlt which fields Weaviate should vectorize. In our case, we vectorize the `subject` and `description` fields from each ticket. That means that Weaviate will be able to perform vector search (or similarity search) on the content of these fields.
4. `pipeline.run()` runs the pipeline and returns information about the load process.

### Running the pipeline

Now that we have the pipeline configured, we can run the Python script:

```sh
python zendesk_pipeline.py
```

We have successfully loaded the data from Zendesk to Weaviate. Let's check it out.

## Query the data

We can now run a vector search query on the data we loaded into Weaviate. Create a new Python file called `query.py` and add the following code:

```py
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

response = (
    client.query
    .get("ZendeskData_Tickets", ["subject", "description"])
    .with_near_text({
        "concepts": ["problems with password"],
    })
    .with_additional(["distance"])
    .do()
)

print(response)
```

The above code instantiates a Weaviate client and performs a similarity search on the data we loaded. The query searches for tickets that are similar to the text “problems with password”. The output should be similar to:

```json
{
   "data": {
      "Get": {
         "ZendeskData_Tickets": [
            {
               "subject": "How do I change the password for my account?",
               "description": "I forgot my password and I can't log in.",
                "_additional": {
                   "distance": 0.235
                }
            },
            {
               "subject": "I can't log in to my account.",
               "description": "The credentials I use to log in don't work.",
               "_additional": {
                   "distance": 0.247
               }
            }
         ]
      }
   }
}
```

## Incremental loading

During our first load, we loaded all tickets from the Zendesk API. But what if users create new tickets or update existing ones? dlt solves this case by supporting [incremental loading](../general-usage/incremental-loading.md). You don't need to change anything in your pipeline to enable it: the Zendesk source supports incremental loading out of the box based on the `updated_at` field. That means dlt will only load tickets that were created or updated after the last load.

## What's next?

If you are interested in learning more about Weaviate support in dlt, check out the [Weaviate destination](../dlt-ecosystem/destinations/weaviate.md) docs. We also have demos of different sources of data for Weaviate in our Jupyter notebooks:

- [Loading PDF data into Weaviate](https://github.com/dlt-hub/dlt_demos/blob/main/pdf_to_weaviate.ipynb)
- [Loading data from a SQL database into Weaviate](https://github.com/dlt-hub/dlt_demos/blob/main/sql_to_weaviate.ipynb)

