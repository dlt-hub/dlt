---
title: Stripe
description: dlt verified source for Stripe API
keywords: [stripe api, stripe verified source, stripe]
---
import Header from './_source-info-header.md';

# Stripe

<Header/>

[Stripe](https://stripe.com) is an online payment platform that allows businesses to securely process and manage customer transactions over the Internet.

This Stripe `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/stripe_pipeline.py)
loads data using the Stripe API to the destination of your choice.

This verified source loads data from the following endpoints:

| Name               | Description                                |
|--------------------|--------------------------------------------|
| Subscription       | Recurring payment on Stripe                |
| Account            | User profile on Stripe                     |
| Coupon             | Discount codes offered by businesses       |
| Customer           | Buyers using Stripe                        |
| Product            | Items or services for sale                 |
| Price              | Cost details for products or plans         |
| Event              | Significant activities in a Stripe account |
| Invoice            | Payment request document                   |
| BalanceTransaction | Funds movement record in Stripe            |

Please note that endpoints in the verified source can be customized as per the Stripe API [reference documentation.](https://stripe.com/docs/api)

## Setup guide

### Grab credentials

1. Log in to your Stripe account.
1. Click ⚙️ Settings in the top-right.
1. Go to Developers from the top menu.
1. Choose "API Keys".
1. In "Standard Keys", click "Reveal test key" beside the Secret Key.
1. Note down the API_secret_key for configuring secrets.toml.

> Note: The Stripe UI, which is described here, might change.
The full guide is available at [this link.](https://stripe.com/docs/keys)


### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init stripe_analytics duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/stripe_analytics_pipeline.py)
   with Stripe as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).


### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   # put your secret values and credentials here. do not share this file and do not push it to github
   [sources.stripe_analytics]
   stripe_secret_key = "stripe_secret_key"# please set me up!
   ```
1. Substitute "stripe_secret_key" with the value [you copied above](#grab-credentials) for secure access to your Stripe resources.

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:

   ```sh
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```sh
   python stripe_analytics_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `stripe_analytics`. You may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Default endpoints
You can write your own pipelines to load data to a destination using this verified source. However, it is important to note how the `ENDPOINTS` and `INCREMENTAL_ENDPOINTS` tuples are defined in `stripe_analytics/settings.py`.

```py
# The most popular Stripe API's endpoints
ENDPOINTS = ("Subscription", "Account", "Coupon", "Customer", "Product", "Price")
# Possible incremental endpoints
# The incremental endpoints default to Stripe API endpoints with uneditable data.
INCREMENTAL_ENDPOINTS = ("Event", "Invoice", "BalanceTransaction")
```
>Stripe's default API endpoints miss the "updated" key, triggering 'replace' mode. Use incremental endpoints for incremental loading.

### Source `stripe_source`

This function retrieves data from the Stripe API for the specified endpoint:

```py
@dlt.source
def stripe_source(
    endpoints: Tuple[str, ...] = ENDPOINTS,
    stripe_secret_key: str = dlt.secrets.value,
    start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Iterable[DltResource]:
   ...
```

- `endpoints`: Tuple containing endpoint names.
- `start_date`: Start datetime for data loading (default: None).
- `end_date`: End datetime for data loading (default: None).
>This source loads all provided endpoints in 'replace' mode. For incremental endpoints, use incremental_stripe_source.

### Source `incremental_stripe_source`

This source loads data in 'append' mode from incremental endpoints.

```py
@dlt.source
def incremental_stripe_source(
    endpoints: Tuple[str, ...] = INCREMENTAL_ENDPOINTS,
    stripe_secret_key: str = dlt.secrets.value,
    initial_start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Iterable[DltResource]:
   ...
```
`endpoints`: Tuple containing incremental endpoint names.

`initial_start_date`: Parameter for incremental loading; data after the initial_start_date is loaded on the first run (default: None).

`end_date`: End datetime for data loading (default: None).

After each run, 'initial_start_date' updates to the last loaded date. Subsequent runs then retrieve only new data using append mode, streamlining the process and preventing redundant data downloads.

For more information, read the [Incremental loading](../../general-usage/incremental-loading).

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="stripe_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="stripe_dataset"  # Use a custom name if desired
   )
   ```

1. To load endpoints like "Plan" and "Charge" in replace mode, retrieve all data for the year 2022:

   ```py
   source_single = stripe_source(
       endpoints=("Plan", "Charge"),
       start_date=datetime(2022, 1, 1),
       end_date=datetime(2022, 12, 31),
   )
   load_info = pipeline.run(source_single)
   print(load_info)
   ```

1. To load data from the "Invoice" endpoint, which has static data, using incremental loading:

    ```py
    # Load all data on the first run that was created after start_date and before end_date
    source_incremental = incremental_stripe_source(
        endpoints=("Invoice", ),
        initial_start_date=datetime(2022, 1, 1),
        end_date=datetime(2022, 12, 31),
    )
    load_info = pipeline.run(source_incremental)
    print(load_info)
    ```
    > For subsequent runs, the dlt module sets the previous "end_date" as "initial_start_date", ensuring incremental data retrieval.

1. To load data created after December 31, 2022, adjust the data range for stripe_source to prevent redundant loading. For `incremental_stripe_source`, the `initial_start_date` will auto-update to the last loaded date from the previous run.

    ```py
    source_single = stripe_source(
        endpoints=("Plan", "Charge"),
        start_date=datetime(2022, 12, 31),
    )
    source_incremental = incremental_stripe_source(
        endpoints=("Invoice", ),
    )
    load_info = pipeline.run(data=[source_single, source_incremental])
    print(load_info)
    ```
    > To load data, maintain the pipeline name and destination dataset name. The pipeline name is vital for accessing the last run's [state](../../general-usage/state), which determines the incremental data load's end date. Altering these names can trigger a [“dev_mode”](../../general-usage/pipeline#do-experiments-with-dev-mode), disrupting the metadata (state) tracking for [incremental data loading](../../general-usage/incremental-loading).

<!--@@@DLT_TUBA stripe_analytics-->

