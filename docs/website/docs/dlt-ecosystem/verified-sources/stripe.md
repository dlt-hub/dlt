# Stripe

Stripe is an online payment company that offers a platform for businesses to process payments from customers over the Internet. It's a convenient way for businesses to accept payments and manage their financial transactions securely.

This verified source utilizes Stripe's API and `dlt` to extract key data such as customer information, subscription details, event records, etc, and then load them into a database. Additionally, the pipeline example shows how to calculate some important metrics such as MRR (monthly recurring revenue) and churn rate.

This verified source loads data from the following default endpoints:

| Endpoint | Description |
| --- | --- |
| Subscription | recurring payment model offered by the Stripe payment platform |
| Account | the entities that represent businesses or individuals using the Stripe platform to accept payments |
| Coupon | promotional codes that businesses can create and offer to customers to provide discounts or other special offers |
| Customer | individuals or businesses who make purchases or transactions with a business using the Stripe platform |
| Product | specific item or service that a business offers for sale |
| Price | represents the specific cost or pricing information associated with a product or subscription plan |
| Event | a record or notification of a significant occurrence or activity that takes place within a Stripe account |
| Invoice | a document that represents a request for payment from a customer |
| BalanceTransaction | represents a record of funds movement within a Stripe account |

> Please note that the endpoints within the verified source can be tailored to meet your specific requirements, as outlined in the Stripe API reference documentation Detailed instructions on customizing these endpoints can be found in the customization section here.
>

## Grab API credentials

1. Log in to your Stripe account.
2. Click on the `⚙️ Settings` option in the top-right menu.
3. Navigate to the Developers section by clicking on the `Developers` button in the top-right menu bar.
4. Select the "API Keys" option from the Developers section.
5. Locate the "Standard Keys" section and click on the "Reveal test key" button next to the Secret Key.
6. Make a note of the API_secret_key that you will use to configure `secrets.toml` further.

## Initialize the Stripe verified source and the pipeline example

To get started with this verified source, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:
    ```properties
    dlt init stripe_analytics bigquery
    ```
    This command will initialize your verified source with Stripe and creates pipeline example with BigQuery as the destination. If you'd like to use a different destination, simply replace **`bigquery`** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](../destinations/)

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

```
stripe_source
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── stripe_analytics
│   └── __init__.py
│   └── helpers.py
│   └── metrics.py
│   └── settings.py
├── .gitignore
├── requirements.txt
└── stripe_analytics_pipeline.py
```

## **Add credential**

1. Inside the **`.dlt`** folder, you'll find a file called **`secrets.toml`**, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

    Here's what the file looks like:

    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.stripe_analytics]
    stripe_secret_key = "stripe_secret_key"# please set me up!

    [destination.bigquery.credentials]
    project_id = "project_id" # GCP project ID
    private_key = "private_key" # Unique private key (including `BEGIN and END PRIVATE KEY`)
    client_email = "client_email" # Service account email
    location = "US" # Project location (e.g. “US”)
    ```

2. Replace the value of **stripe_secret_key** with the one that [you copied above](stripe.md#grab-api-credentials). This will ensure that this source can access your Stripe resources securely.
3. Finally, follow the instructions in **[Destinations](../destinations/)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    ```properties
    pip install -r requirements.txt
    ```

2. Now the verified source can be run by using the command:
    ```properties
    python3 stripe_analytics_pipeline.py
    ```

3. To make sure that everything is loaded as expected, use the command:
    ```properties
    dlt pipeline <pipeline_name> show
    ```
    (For example, the pipeline_name for the above pipeline example is `stripe_analytics`, you may also use any custom name instead)

## Customization

To load data to the destination using this verified source, you have the option to write your own methods. However, it is important to note is how the `ENDPOINTS` and `INCREMENTAL_ENDPOINTS` tuples are defined by default (see `stripe_analytics/settings.py`).

```python
# The most popular Stripe API's endpoints
ENDPOINTS = ("Subscription", "Account", "Coupon","Customer","Product","Price")
# Possible incremental endpoints
# The incremental endpoints default to Stripe API endpoints with uneditable data.
INCREMENTAL_ENDPOINTS = ("Event", "Invoice", "BalanceTransaction")

```

### **Source and resource methods**

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source) and [resources](https://dlthub.com/docs/general-usage/resource) that for this verified source are found in the `__init__.py` file within the *stripe_analytics* directory. This Stripe verified source has three default methods that form the basis of loading. The methods are:

#### 1. Source stripe_source:

```python
@dlt.source
def stripe_source(
    endpoints: Tuple[str, ...] = ENDPOINTS,
    stripe_secret_key: str = dlt.secrets.value,
    start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Iterable[DltResource]:
```

- **`endpoints`**: A tuple of endpoint names used to retrieve data from.
- **`start_date`**: An optional start date to limit the data retrieved. The default value is None.
- **`end_date`**: An optional end date that limits the data retrieved. The default value is None.

By default, this method utilizes the *replace* mode, which means that all the data will be loaded fresh into the table. In other words, the existing data in the destination is completely replaced with the new data being loaded on every run.

#### 2. Source incremental_stripe_source:

```python
@dlt.source
def incremental_stripe_source(
    endpoints: Tuple[str, ...] = INCREMENTAL_ENDPOINTS,
    stripe_secret_key: str = dlt.secrets.value,
    initial_start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Iterable[DltResource]:

```

- **`endpoints`**: A tuple of incremental endpoint names used to retrieve data.
- **`initial_start_date`**: An optional parameter that specifies the initial value for *dlt.sources.incremental*. If the parameter is not set to “None”, only data created after *initial_start_date* will be loaded during the first run. The default value is None.
- **`end_date`**: An optional end date that limits the data retrieved. The default value is None.

After each run, the value of *initial_start_date* will be automatically updated to the date for which the pipeline last loaded data in the previous run. This ensures that in subsequent runs, only new data created after the last loaded date will be retrieved by the pipeline by using *append* mode. With this method, a massive amount of data is not downloaded each time, making the loading process more efficient and less time-consuming.

#### 3. Resource metrics_resource**

```python
@dlt.resource(name="Metrics", write_disposition="append", primary_key="created")
def metrics_resource() -> Iterable[TDataItem]:
```

This method is used to calculate and get metrics such as monthly recurring revenue (MRR) and churn rate from endpoints `Subscriptions` and `Events`.

- **Monthly Recurring Revenue (MRR)**:
    - is a valuable metric used to estimate the total amount of monthly revenue that a business can expect to receive on a recurring basis. It's calculated by adding up the monthly-normalized amounts of all subscriptions from which payment is being collected at the current time.
- **Churn rate**:
    - is a metric used to measure the rate at which subscribers are leaving a service or product over a given period of time. It's calculated by adding up the number of subscribers who have cancelled or ended their subscription in the past 30 days and dividing that number by the total number of active subscribers 30 days ago, plus any new subscribers that have joined during that same 30-day period.

### **Create Your Data Loading Pipeline using Stripe verified source**

If you wish to create your own pipelines you can leverage these functions.

To create your data pipeline using single loading and [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading), follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our [documentation here](https://dlthub.com/docs/general-usage/pipeline).

    ```python
    pipeline = dlt.pipeline(
        pipeline_name="stripe_pipeline",# Use a custom name if desired
        destination="bigquery",# Choose the appropriate destination (e.g., duckdb etc.)
        dataset_name="stripe_dataset"# Use a custom name if desired
    )
    ```

2. First load only endpoints you want to be loaded in *replace* mode, for example, "Plan" and "Charge". Load all data only for the year 2022.

    ```python
    source_single = stripe_source(
      endpoints=("Plan", "Charge"),
      start_date=datetime(2022, 1, 1),
      end_date=datetime(2022, 12, 31),
    )
    ```

3. Then load data from the endpoint “Invoice”. This endpoint has uneditable data, so we can load it incrementally. For future runs, the **`dlt`** module will store the "end_date" for this pipeline run as the "initial_start_date" and load the data incrementally.

    ```python
    # Load all data on the first run that was created after start_date and before end_date
    source_incremental = incremental_stripe_source(
      endpoints=("Invoice", ),
      initial_start_date=datetime(2022, 1, 1),
      end_date=datetime(2022, 12, 31),
    )

    ```

4. Use the method **`pipeline.run()`** to execute the pipeline.

    ```python
    load_info = pipeline.run(data=[source_single, source_incremental])
    print(load_info)
    ```

5. If you need to load the new data that was created after 31, December 2022, change the data range for *stripe_source,* do this to avoid loading already loaded data again. You don’t have to provide the new data range for *incremental_stripe_source,* the value of *initial_start_date* will be automatically updated to the date for which the pipeline last loaded data in the previous run.

    ```python
    pipeline = dlt.pipeline(
        pipeline_name="stripe_pipeline",
        destination="bigquery",
        dataset_name="stripe_dataset"
    )
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

6. It's important to keep the pipeline name and destination dataset name unchanged. The pipeline name is crucial for retrieving the [state](https://dlthub.com/docs/general-usage/state) of the last pipeline run, which includes the end date needed for loading data incrementally. Modifying these names can lead to [“full_refresh”](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh) which will disrupt the tracking of relevant metadata(state) for [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading).

That's it! Enjoy running your Stripe DLT pipeline!
