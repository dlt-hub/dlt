---
title: Stripe
description: dlt verified source for Stripe API
keywords: [stripe api, stripe verified source, stripe]
---

# Stripe

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::


[Stripe](https://stripe.com) is an online payment platform that allows businesses to securely process and manage customer transactions over the Internet.

This Stripe `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/stripe_pipeline.py)
loads data using Stripe API to the destination of your choice.

This verified source loads data from the following endpoints:

| Name	            | Description                                |
|--------------------|--------------------------------------------|
| Subscription       | Recurring payment on Stripe                |
| Account	         | User profile on Stripe                     |
| Coupon	            | Discount codes offered by businesses       |
| Customer	         | Buyers using Stripe                        |
| Product	         | Items or services for sale                 |
| Price	            | Cost details for products or plans         |
| Event	            | Significant activities in a Stripe account |
| Invoice	         | Payment request document                   |
| BalanceTransaction | Funds movement record in Stripe            |

Please note that endpoints in the verified source can be customized as per the Stripe API [reference documentation.](https://stripe.com/docs/api)

## Setup Guide

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

   ```bash
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

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```bash
   pip install -r requirements.txt
   ```

1. You're now ready to run the pipeline! To get started, run the following command:

   ```bash
   python stripe_analytics_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `stripe_analytics`, you
   may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Default endpoints
You can write your own pipelines to load data to a destination using this verified source.
However, it is important to note is how the `ENDPOINTS` and `INCREMENTAL_ENDPOINTS` tuples are defined in `stripe_analytics/settings.py`.

```python
# The most popular Stripe API's endpoints
ENDPOINTS = ("Subscription", "Account", "Coupon", "Customer", "Product", "Price")
# Possible incremental endpoints
# The incremental endpoints default to Stripe API endpoints with uneditable data.
INCREMENTAL_ENDPOINTS = ("Event", "Invoice", "BalanceTransaction")
```
>Stripe's default API endpoints miss the "updated" key, triggering 'replace' mode. Use incremental endpoints for incremental loading.

### Source `stripe_source`

This function retrieves data from the Stripe API for the specified endpoint:

```python
@dlt.source
def stripe_source(
    endpoints: Tuple[str, ...] = ENDPOINTS,
    stripe_secret_key: str = dlt.secrets.value,
    start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Iterable[DltResource]:
```

- `endpoints`: Tuple containing endpoint names.
- `start_date`: Start datetime for data loading (default: None).
- `end_date`: End datetime for data loading (default: None).
>This source loads all provided endpoints in 'replace' mode. For incremental endpoints, use incremental_stripe_source.

### Source `incremental_stripe_source`

This source loads data in 'append' mode from incremental endpoints.

```python
@dlt.source
def incremental_stripe_source(
    endpoints: Tuple[str, ...] = INCREMENTAL_ENDPOINTS,
    stripe_secret_key: str = dlt.secrets.value,
    initial_start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Iterable[DltResource]:
```
`endpoints`: Tuple containing incremental endpoint names.

`initial_start_date`: Parameter for incremental loading; data after initial_start_date is loaded on the first run (default: None).

`end_date`: End datetime for data loading (default: None).


After each run, 'initial_start_date' updates to the last loaded date. Subsequent runs then retrieve only new data using append mode, streamlining the process and preventing redundant data downloads.

For more information, read the [General Usage: Incremental loading](../../general-usage/incremental-loading).

### Resource `metrics_resource`

This function loads a dictionary with calculated metrics, including MRR and Churn rate, along with the current timestamp.

```python
@dlt.resource(name="Metrics", write_disposition="append", primary_key="created")
def metrics_resource() -> Iterable[TDataItem]:
```

Abrevations MRR and Churn rate are as follows:
- Monthly Recurring Revenue (MRR):
  - Measures the predictable monthly revenue from all active subscriptions. It's the sum of the monthly-normalized subscription amounts.
- Churn rate:
  - Indicates the rate subscribers leave a service over a specific period. Calculated by dividing the number of recent cancellations by the total subscribers from 30 days ago, adjusted for new subscribers.


## Customization
### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
       pipeline_name="stripe_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="stripe_dataset"  # Use a custom name if desired
   )
   ```

1. To load endpoints like "Plan" and "Charge" in replace mode, retrieve all data for the year 2022:

   ```python
   source_single = stripe_source(
       endpoints=("Plan", "Charge"),
       start_date=datetime(2022, 1, 1),
       end_date=datetime(2022, 12, 31),
   )
   load_info = pipeline.run(source_single)
   print(load_info)
   ```

1. To load data from the "Invoice" endpoint, which has static data, using incremental loading:

    ```python
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

1. To load data created after December 31, 2022, adjust the data range for stripe_source to prevent redundant loading. For incremental_stripe_source, the initial_start_date will auto-update to the last loaded date from the previous run.

    ```python
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
    > To load data, maintain the pipeline name and destination dataset name. The pipeline name is vital for accessing the last run's [state](https://dlthub.com/docs/general-usage/state), which determines the incremental data load's end date. Altering these names can trigger a [“full_refresh”](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh), disrupting the metadata (state) tracking for [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading).

1. To load important metrics and store them in database:

   ```python
   # Event is an endpoint with uneditable data, so we can use 'incremental_stripe_source'.
   source_event = incremental_stripe_source(endpoints=("Event",))
   # Subscription is an endpoint with editable data, use stripe_source.
   source_subs = stripe_source(endpoints=("Subscription",))
   load_info = pipeline.run(data=[source_subs, source_event])
   print(load_info)
   resource = metrics_resource()
   print(list(resource))
   load_info = pipeline.run(resource)
   print(load_info)
   ```


