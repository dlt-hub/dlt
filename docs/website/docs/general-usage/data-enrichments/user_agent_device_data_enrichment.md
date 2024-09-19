---
title:  User-agent device data enrichment
description: Enriching the user-agent device data with average device price.
keywords: [data enrichment, user-agent data, device enrichment]
---

# Data enrichment part one: User-agent device data enrichment

Data enrichment enhances raw data with valuable information from multiple sources, increasing its analytical and decision-making value.

This part covers enriching sample data with device price. Understanding the price segment of the device that the user used to access your service can be helpful in personalized marketing, customer segmentation, and more.

This documentation will discuss how to enrich user device information with the average market price.

## Setup guide

We use SerpAPI to retrieve device prices using Google Shopping, but alternative services or APIs are viable.

:::note
SerpAPI's free tier offers 100 free calls monthly. For production, consider upgrading to a higher plan.
:::


## Creating a data enrichment pipeline
You can either follow the example in the linked Colab notebook or follow this documentation to create the user-agent device data enrichment pipeline.

### A. Colab notebook
The Colab notebook combines three data enrichment processes for a sample dataset, starting with "Data enrichment part one: User-agent device data".

Here's the link to the notebook:
**[Colab Notebook](https://colab.research.google.com/drive/1ZKEkf1LRSld7CWQFS36fUXjhJKPAon7P?usp=sharing).**

### B. Create a pipeline
Alternatively, to create a data enrichment pipeline, you can start by creating the following directory structure:

```text
user_device_enrichment/
├── .dlt/
│   └── secrets.toml
└── device_enrichment_pipeline.py
```

### 1. Creating resource

`dlt` works on the principle of [sources](../source) and [resources.](../resource)

This data resource yields data typical of what many web analytics and tracking tools can collect. However, the specifics of what data is collected and how it's used can vary significantly among different tracking services.

Let's examine a synthetic dataset created for this article. It includes:

`user_id`: Web trackers typically assign a unique ID to users for tracking their journeys and interactions over time.

`device_name`: User device information helps in understanding the user base's device preferences.

`page_refer`: The referer URL is tracked to analyze traffic sources and user navigation behavior.

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
        {"user_id": 1, "device_name": "Sony Experia XZ", "page_referer":
        "https://b2venture.lightning.force.com/"},
        {"user_id": 2, "device_name": "Samsung Galaxy S23 Ultra 5G",
        "page_referer": "https://techcrunch.com/2023/07/20/can-dlthub-solve-the-python-library-problem-for-ai-dig-ventures-thinks-so/"},
        {"user_id": 3, "device_name": "Apple iPhone 14 Pro Max",
        "page_referer": "https://dlthub.com/success-stories/freelancers-perspective/"},
        {"user_id": 4, "device_name": "OnePlus 11R",
        "page_referer": "https://www.reddit.com/r/dataengineering/comments/173kp9o/ideas_for_data_validation_on_data_ingestion/"},
        {"user_id": 5, "device_name": "Google Pixel 7 Pro", "page_referer": "https://pypi.org/"},
    ]

    # Yielding each user's data as a dictionary
    for user_data in sample_data:
        yield user_data
```

### 2. Create `fetch_average_price` function

This particular function retrieves the average price of a device by utilizing SerpAPI and Google shopping listings. To filter the data, the function uses dlt state, and only fetches prices from SerpAPI for devices that have not been updated in the most recent run or for those that were loaded more than 180 days in the past.

The first step is to register on [SerpAPI](https://serpapi.com/) and obtain the API token key.

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive information securely, like access tokens. Keep this file safe. Here's its format for service account authentication:

   ```py
   [sources]
   api_key= "Please set me up!"  # Serp Api key.
   ```

1. Replace the value of the `api_key`.

1. Create the `fetch_average_price()` function as follows:
   ```py
   from datetime import datetime, timedelta
   import requests

   # Uncomment transformer function if it is to be used as a transformer,
   # otherwise, it is being used with the `add_map` functionality.

   # @dlt.transformer(data_from=tracked_data)
   def fetch_average_price(user_tracked_data):
       """
       Fetches the average price of a device from an external API and
       updates the user_data dictionary.

       This function retrieves the average price of a device specified in the
       user_data dictionary by making an API request. The price data is cached
       in the device_info state to reduce API calls. If the data for the device
       is older than 180 days, a new API request is made.

       Args:
           user_tracked_data (dict): A dictionary containing user data, including
           the device name.

       Returns:
           dict: The updated user_data dictionary with added device price and
           updated timestamp.
       """

       # Retrieve the API key from dlt secrets
       api_key = dlt.secrets.get("sources.api_key")

       # Get the current resource state for device information
       device_info = dlt.current.resource_state().setdefault("devices", {})

       # Current timestamp for checking the last update
       current_timestamp = datetime.now()

       # Print the current device information
       # print(device_info) # if you need to check state

       # Extract the device name from user data
       device = user_tracked_data['device_name']
       device_data = device_info.get(device, {})

       # Calculate the time since the last update
       last_updated = (
           current_timestamp -
           device_data.get('timestamp', datetime.min)
       )
       # Check if the device is not in state or data is older than 180 days
       if device not in device_info or last_updated > timedelta(days=180):
           try:
               # Make an API request to fetch device prices
               response = requests.get("https://serpapi.com/search", params={
                   "engine": "google_shopping", "q": device,
                   "api_key": api_key, "num": 10
               })
           except requests.RequestException as e:
               print(f"Request failed: {e}")
               return None

           if response.status_code != 200:
               print(f"Failed to retrieve data: {response.status_code}")
               return None

           # Process the response to extract prices
           results = response.json().get("shopping_results", [])
           prices = []
           for r in results:
               if r.get("price"):
                   # Split the price string and convert each part to float
                   price = r.get("price")
                   price_parts = price.replace('$', '').replace(',', '').split()
                   for part in price_parts:
                       try:
                           prices.append(float(part))
                       except ValueError:
                           pass  # Ignore parts that can't be converted to float

           # Calculate the average price and update the device_info
           device_price = round(sum(prices) / len(prices), 2) if prices else None
           device_info[device] = {
               'timestamp': current_timestamp,
               'price': device_price
           }

           # Add the device price and timestamp to the user data
           user_tracked_data['device_price_USD'] = device_price
           user_tracked_data['price_updated_at'] = current_timestamp

       else:
           # Use cached price data if available and not outdated
           user_tracked_data['device_price_USD'] = device_data.get('price')
           user_tracked_data['price_updated_at'] = device_data.get('timestamp')

       return user_tracked_data
   ```

### 3. Create your pipeline

1. In creating the pipeline, the `fetch_average_price` can be used in the following ways:
   - Add map function
   - Transformer function

   The `dlt` library's `transformer` and `add_map` functions serve distinct purposes in data
   processing.

   `Transformers` are used to process a resource and are ideal for post-load data transformations in a
   pipeline, compatible with tools like `dbt`, the `dlt SQL client`, or Pandas for intricate data
   manipulation. To read more:
   [Click here.](../../general-usage/resource#process-resources-with-dlttransformer)

   Conversely, `add_map` is used to customize a resource and applies transformations at an item level
   within a resource. It's useful for tasks like anonymizing individual data records. More on this
   can be found under
   [Customize resources](../../general-usage/resource#customize-resources) in the
   documentation.


1. Here, we create the pipeline and use the `add_map` functionality:

   ```py
   # Create the pipeline
   pipeline = dlt.pipeline(
       pipeline_name="data_enrichment_one",
       destination="duckdb",
       dataset_name="user_device_enrichment",
   )

   # Run the pipeline with the transformed source
   load_info = pipeline.run(tracked_data.add_map(fetch_average_price))

   print(load_info)
   ```

   :::info
   Please note that the same outcome can be achieved by using the transformer function. To
   do so, you need to add the transformer decorator at the top of the `fetch_average_price` function.
   For `pipeline.run`, you can use the following code:

   ```py
   # using fetch_average_price as a transformer function
   load_info = pipeline.run(
       tracked_data | fetch_average_price,
       table_name="tracked_data"
   )
   ```

   This will execute the `fetch_average_price` function with the tracked data and return the average
   price.
   :::

### Run the pipeline

1. Install necessary dependencies for the preferred
   [destination](../../dlt-ecosystem/destinations/), for example, duckdb:

   ```sh
   pip install "dlt[duckdb]"
   ```

1. Run the pipeline with the following command:

   ```sh
   python device_enrichment_pipeline.py
   ```

1. To ensure that everything loads as expected, use the command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the "pipeline_name" for the above pipeline example is `data_enrichment_one`; you can use
   any custom name instead.

