---
title:  URL-parser data enrichment
description: Enriching the url with various parameters.
keywords: [data enrichment, url parser, referer data enrichment]
---

# Data enrichment part three: URL parser data enrichment

URL parser data enrichment involves extracting various URL components to gain additional insights and
context about the URL. This extracted information can be used for data analysis, marketing, SEO, and
more.

## URL parsing process

Here is a step-by-step process for URL parser data enrichment:

1. Get the URL data that needs to be parsed from a source or create one.
2. Send the URL data to an API like [URL Parser API](https://urlparse.com/).
3. Receive the parsed URL data.
4. Include metadata like conversion rate, date, and time.
5. Save the updated dataset in a data warehouse or lake using a data pipeline.

We use **[URL Parse API](https://urlparse.com/)** to extract information about the URL. However,
you can use any API you prefer.

:::tip
`URL Parse API` is free, with a 1000 requests/hour limit, which can be increased upon request.
:::

By default, the URL Parse API will return a JSON response like:

```json
{
    "authority": "urlparse.com",
    "domain": "urlparse.com",
    "domain_label": "urlparse",
    "file": "/",
    "fragment": null,
    "host": "urlparse.com",
    "href": "https://urlparse.com/",
    "is_valid": true,
    "origin": "https://urlparse.com",
    "params": null,
    "path": "/",
    "port": null,
    "query": null,
    "request_url": "https://urlparse.com",
    "scheme": "https",
    "subdomains": null,
    "tld": "com"
}
```

## Creating a data enrichment pipeline

You can either follow the example in the linked Colab notebook or follow this documentation to
create the URL-parser data enrichment pipeline.

### A. Colab notebook

This Colab notebook outlines a three-part data enrichment process for a sample dataset:

- User-agent device data enrichment
- Currency conversion data enrichment
- URL-parser data enrichment

This document focuses on the URL-parser data enrichment (Part Three). For a comprehensive
understanding, you may explore all three enrichments sequentially in the notebook:
[Colab Notebook](https://colab.research.google.com/drive/1ZKEkf1LRSld7CWQFS36fUXjhJKPAon7P?usp=sharing).

### B. Create a pipeline

Alternatively, to create a data enrichment pipeline, you can start by creating the following
directory structure:

```text
url_parser_enrichment/
├── .dlt/
│   └── secrets.toml
└── url_enrichment_pipeline.py
```

### 1. Creating resource

`dlt` works on the principle of [sources](../../general-usage/source.md) and
[resources.](../../general-usage/resource.md)

This data resource yields data typical of what many web analytics and tracking tools can collect.
However, the specifics of what data is collected and how it's used can vary significantly among
different tracking services.

Let's examine a synthetic dataset created for this article. It includes:

- `user_id`: Web trackers typically assign a unique ID to users for tracking their journeys and
  interactions over time.

- `device_name`: User device information helps in understanding the user base's device preferences.

- `page_refer`: The referer URL is tracked to analyze traffic sources and user navigation behavior.

Here's the resource that yields the sample data as discussed above:

```py
    import dlt

    @dlt.resource(write_disposition="append")
    def tracked_data():
        """
        A generator function that yields a series of dictionaries, each representing
        user tracking data.

        This function is decorated with `dlt.resource` to integrate into the DLT (Data
        Loading Tool) pipeline. The `write_disposition` parameter is set to "append" to
        ensure that data from this generator is appended to the existing data in the
        destination table.

        Yields:
            dict: A dictionary with keys 'user_id', 'device_name', and 'page_referer',
            representing the user's tracking data including their device and the page
            they were referred from.
        """

        # Sample data representing tracked user data
        sample_data = [
        {
                "user_id": 1,
                "device_name": "Sony Experia XZ",
                "page_referer": "https://b2venture.lightning.force.com/"
        },
            """
            Data for other users
            """
        ]

        # Yielding each user's data as a dictionary
        for user_data in sample_data:
            yield user_data
```

### 2. Create `url_parser` function

We use a free service called [URL Parse API](https://urlparse.com/), to parse the URLs. You don’t
need to register to use this service nor get an API key.

1. Create a `url_parser` function as follows:
   ```py
   def url_parser(record):
       """
       Send a URL to a parsing service and return the parsed data.

       This function sends a URL to a specified API endpoint for URL parsing.

       Parameters:
       url (str): The URL to be parsed.

       Returns:
       dict: Parsed URL data in JSON format if the request is successful.
       None: If the request fails (e.g., an invalid URL or server error).
       """
       # Define the API endpoint URL for the URL parsing service
       api_url = "https://api.urlparse.com/v1/query"
       url = record['page_referer']
       # Send a POST request to the API with the URL to be parsed
       response = requests.post(api_url, json={"url": url})

       # Check if the response from the API is successful (HTTP status code 200)
       if response.status_code == 200:
           # If successful, return the parsed data in JSON format
           return response.json()
       else:
           # If the request failed, print an error message with the status code and return None
           print(f"Request for {url} failed with status code: {response.status_code}")
           return None
   ```

### 3. Create your pipeline

1. In creating the pipeline, the `url_parser` can be used in the following ways:

   - Add map function
   - Transformer function

   The `dlt` library's `transformer` and `add_map` functions serve distinct purposes in data
   processing.

   `Transformers` are a form of `dlt resource` that takes input from other resources
   via the `data_from` argument to enrich or transform the data.
   [Click here.](../../general-usage/resource.md#process-resources-with-dlttransformer)

   Conversely, `add_map` is used to customize a resource and applies transformations at an item level
   within a resource. It's useful for tasks like anonymizing individual data records. More on this
   can be found under [Customize resources](../../general-usage/resource.md#customize-resources) in
   the documentation.

1. Here, we create the pipeline and use the `add_map` functionality:

   ```py
   # Create the pipeline
   pipeline = dlt.pipeline(
       pipeline_name="data_enrichment_three",
       destination="duckdb",
       dataset_name="user_device_enrichment",
   )

   # Run the pipeline with the transformed source
   load_info = pipeline.run(tracked_data.add_map(url_parser))

   print(load_info)
   ```

   :::info
   Please note that the same outcome can be achieved by using the transformer function. To
   do so, you need to add the transformer decorator at the top of the `url_parser` function. For
   `pipeline.run`, you can use the following code:

   ```py
   # using fetch_average_price as a transformer function
   load_info = pipeline.run(
       tracked_data | url_parser,
       table_name="url_parser"
   )
   ```

   This will execute the `url_parser` function with the tracked data and return the parsed URL.
   :::

### Run the pipeline

1. Install necessary dependencies for the preferred
   [destination](../../dlt-ecosystem/destinations/), for example, duckdb:

   ```sh
   pip install "dlt[duckdb]"
   ```

1. Run the pipeline with the following command:

   ```sh
   python url_enrichment_pipeline.py
   ```

1. To ensure that everything loads as expected, use the command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the "pipeline_name" for the above pipeline example is `data_enrichment_three`; you
   can use any custom name instead.

