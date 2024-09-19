---
title:  Currency-conversion data enrichment
description: Converting the monetary value in one currency to another using the latest market rates.
keywords: [data enrichment, currency conversion, latest market rates]
---

# Data enrichment part two: Currency conversion data enrichment

Currency conversion data enrichment means adding additional information to currency-related data.
Often, you have a dataset of monetary value in one currency. For various reasons such as reporting,
analysis, or global operations, it may be necessary to convert these amounts into different currencies.

## Currency conversion process

Here is a step-by-step process for currency conversion data enrichment:

1. Define base and target currencies, e.g., USD (base) to EUR (target).
1. Obtain current exchange rates from a reliable source like a financial data API.
1. Convert the monetary values at obtained exchange rates.
1. Include metadata like conversion rate, date, and time.
1. Save the updated dataset in a data warehouse or lake using a data pipeline.

We use the [ExchangeRate-API](https://app.exchangerate-api.com/) to fetch the latest currency
conversion rates. However, you can use any service you prefer.

:::note
ExchangeRate-API free tier offers 1500 free calls monthly. For production, consider
upgrading to a higher plan.
:::

## Creating data enrichment pipeline

You can either follow the example in the linked Colab notebook or follow this documentation to
create the currency conversion data enrichment pipeline.

### A. Colab notebook

The Colab notebook combines three data enrichment processes for a sample dataset; its second part
contains "Data enrichment part two: Currency conversion data enrichment".

Here's the link to the notebook:
**[Colab Notebook](https://colab.research.google.com/drive/1ZKEkf1LRSld7CWQFS36fUXjhJKPAon7P?usp=sharing).**

### B. Create a pipeline

Alternatively, to create a data enrichment pipeline, you can start by creating the following
directory structure:

```text
currency_conversion_enrichment/
├── .dlt/
│   └── secrets.toml
└── currency_enrichment_pipeline.py
```

### 1. Creating resource

`dlt` works on the principle of [sources](../../general-usage/source.md) and
[resources.](../../general-usage/resource.md)

1. The last part of our data enrichment ([part one](../../general-usage/data-enrichments/user_agent_device_data_enrichment.md))
   involved enriching the data with user-agent device data. This included adding two new columns to the dataset as follows:

   - `device_price_usd`: average price of the device in USD.

   - `price_updated_at`: time at which the price was updated.

1. The columns initially present prior to the data enrichment were:

   - `user_id`: Web trackers typically assign a unique ID to users for tracking their journeys and
     interactions over time.

   - `device_name`: User device information helps in understanding the user base's device.

   - `page_referer`: The referer URL is tracked to analyze traffic sources and user navigation
     behavior.

1. Here's the resource that yields the sample data as discussed above:

   ```py
   @dlt.resource()
   def enriched_data_part_two():
       data_enrichment_part_one = [
           {
                "user_id": 1,
                "device_name": "Sony Experia XZ",
                "page_referer": "https://b2venture.lightning.force.com/",
                "device_price_usd": 313.01,
                "price_updated_at": "2024-01-15 04:08:45.088499+00:00"
           },
       ]
       """
       Similar data for the other users.
       """
       for user_data in data_enrichment_part_one:
           yield user_data
   ```

   > `data_enrichment_part_one` holds the enriched data from part one. It can also be directly used
   > in part two as demonstrated in
   > **[Colab Notebook](https://colab.research.google.com/drive/1ZKEkf1LRSld7CWQFS36fUXjhJKPAon7P?usp=sharing).**

### 2. Create `converted_amount` function

This function retrieves conversion rates for currency pairs that either haven't been fetched before
or were last updated more than 24 hours ago from the ExchangeRate-API, using information stored in
the `dlt` [state](../../general-usage/state.md).

The first step is to register on [ExchangeRate-API](https://app.exchangerate-api.com/) and obtain the
API token.

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```py
   [sources]
   api_key= "Please set me up!"  # ExchangeRate-API key
   ```

1. Create the `converted_amount` function as follows:

   ```py
   # @transformer(data_from=enriched_data_part_two)
   def converted_amount(record):
        """
        Converts an amount from base currency to target currency using the latest exchange rate.

        This function retrieves the current exchange rate from an external API and
        applies it to the specified amount in the record. It handles updates to the exchange rate
        if the current rate is over 12 hours old.

        Args:
            record (dict): A dictionary containing the 'amount' key with the value to be converted.

        Yields:
            dict: A dictionary containing the original amount in USD, converted amount in EUR,
                  the exchange rate, and the last update time of the rate.

        Note:
            The base currency (USD) and target currency (EUR) are hard coded in this function,
            but that can be changed.
            The API key is retrieved from the `dlt` secrets.
        """
        # Hardcoded base and target currencies
        base_currency = "USD"
        target_currency = "EUR"

        # Retrieve the API key from DLT secrets
        api_key = dlt.secrets.get("sources.api_key")

        # Initialize or retrieve the state for currency rates
        rates_state = dlt.current.resource_state().setdefault("rates", {})
        currency_pair_key = f"{base_currency}-{target_currency}"
        currency_pair_state = rates_state.setdefault(currency_pair_key, {
            "last_update": datetime.min,
            "rate": None
        })

        # Update the exchange rate if it's older than 12 hours
        if (currency_pair_state.get("rate") is None or
            (datetime.utcnow() - currency_pair_state["last_update"] >= timedelta(hours=12))):
            url = f"https://v6.exchangerate-api.com/v6/{api_key}/pair/{base_currency}/{target_currency}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                currency_pair_state.update({
                    "rate": data.get("conversion_rate"),
                    "last_update": datetime.fromtimestamp(data.get("time_last_update_unix"))
                })
                print(f"The latest rate of {data.get('conversion_rate')} for the currency pair {currency_pair_key} is fetched and updated.")
            else:
                raise Exception(f"Error fetching the exchange rate: HTTP {response.status_code}")

        # Convert the amount using the retrieved or stored exchange rate
        amount = record['device_price_usd']  # Assuming the key is 'amount' as per the function documentation
        rate = currency_pair_state["rate"]
        yield {
            "actual_amount": amount,
            "base_currency": base_currency,
            "converted_amount": round(amount * rate, 2),
            "target_currency": target_currency,
            "rate": rate,
            "rate_last_updated": currency_pair_state["last_update"],
        }
    ```
1. Next, follow the instructions in
   [Destinations](../../dlt-ecosystem/destinations/duckdb.md) to add credentials for
   your chosen destination. This will ensure that your data is properly routed to its final
   destination.

### 3. Create your pipeline

1. In creating the pipeline, the `converted_amount` can be used in the following ways:

   - Add map function
   - Transformer function

   The `dlt` library's `transformer` and `add_map` functions serve distinct purposes in data
   processing.

   `Transformers` are a form of `dlt resource` that takes input from other resources
   via the `data_from` argument to enrich or transform the data.
   [Click here.](../../general-usage/resource.md#process-resources-with-dlttransformer)

   Conversely, `add_map` used to customize a resource applies transformations at an item level
   within a resource. It's useful for tasks like anonymizing individual data records. More on this
   can be found under [Customize resources](../../general-usage/resource.md#customize-resources) in the
   documentation.

1. Here, we create the pipeline and use the `add_map` functionality:

   ```py
   # Create the pipeline
   pipeline = dlt.pipeline(
       pipeline_name="data_enrichment_two",
       destination="duckdb",
       dataset_name="currency_conversion_enrichment",
   )

   # Run the pipeline with the transformed source
   load_info = pipeline.run(enriched_data_part_two.add_map(converted_amount))

   print(load_info)
   ```

   :::info
   Please note that the same outcome can be achieved by using the `@dlt.transformer` decorator function.
   To do so, you need to add the transformer decorator at the top of the `converted_amount` function.
   For `pipeline.run`, you can use the following code:

   ```py
   # using fetch_average_price as a transformer function
   load_info = pipeline.run(
       enriched_data_part_two | converted_amount,
       table_name="data_enrichment_part_two"
   )
   ```

   This will execute the `converted_amount` function with the data enriched in part one and return
   the converted currencies.
   :::

### Run the pipeline

1. Install necessary dependencies for the preferred
   [destination](../../dlt-ecosystem/destinations/), for example, duckdb:

   ```sh
   pip install "dlt[duckdb]"
   ```

1. Run the pipeline with the following command:

   ```sh
   python currency_enrichment_pipeline.py
   ```

1. To ensure that everything loads as expected, use the command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the "pipeline_name" for the above pipeline example is `data_enrichment_two`; you can
   use any custom name instead.

