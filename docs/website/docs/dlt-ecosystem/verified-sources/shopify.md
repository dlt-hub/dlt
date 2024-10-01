---
title: Shopify
description: dlt pipeline for Shopify API
keywords: [shopify api, shopify pipeline, shopify]
---
import Header from './_source-info-header.md';

# Shopify

<Header/>

[Shopify](https://www.shopify.com/) is a user-friendly e-commerce solution that enables anyone to easily create and manage their
own online store. Whereas a [Shopify partner](https://partners.shopify.com/) is an individual or company that
develops e-commerce stores, applications, or themes for Shopify merchants, often earning revenue through services, app sales, or
referrals.

This Shopify `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/shopify_dlt_pipeline.py)
loads data using the 'Shopify API' or 'Shopify Partner API' to the destination of your choice.

The resources that this verified source supports are:

| Name                  | Description                                                                         |
|-----------------------|-------------------------------------------------------------------------------------|
| customers             | Individuals or entities who have created accounts on a Shopify-powered online store |
| orders                | Transactions made by customers on an online store                                   |
| products              | The individual items or goods that are available for sale                           |
| shopify_partner_query | To query data using GraphQL queries from the Shopify partner API                    |

## Setup guide

### Grab credentials

#### Grab Admin API access token
To load data using the Shopify API, you need an Admin API access token. This token can be obtained by following
these steps:

1. Log in to Shopify.
1. Click the settings icon⚙️ at the bottom left.
1. Choose “Apps and sales channels.”
1. Select the “Develop apps” tab.
1. Click “Create an app” and enter app details.
1. Go to “Configuration” and choose “Configure” under “Admin API integration."
1. Grant read access in “Admin API access scopes.”
1. Save the configuration.
1. Hit “Install app” and confirm.
1. Reveal and copy the Admin API token. Store it safely; it's shown only once.

#### Grab Partner API access token
To load data using the Shopify Partner API, you need a Partner API access token. This token can be obtained by following
these steps:

1. Log in to Shopify Partners and click the settings icon⚙️ at the bottom left.
1. Scroll to 'Partner API client' in partner account settings and select 'Manage Partner API clients'.
1. Create an API client with a suitable name and assign necessary permissions.
1. Save and create the API client, then click to show and copy the access token securely.

> Note: The Shopify and Shopify Partner UI, described here, might change.
The full guide is available at [this link.](https://www.shopify.com/partners/blog/17056443-how-to-generate-a-shopify-api-token)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init shopify_dlt duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/shopify_dlt_pipeline.py)
   with Shopify as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md)
   as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

   Here's what the file looks like:

      ```toml
      #shopify
      [sources.shopify_dlt]
      private_app_password="Please set me up!" #Admin API access token
      access_token="Please set me up!" #Partner API access token
      ```

1. Update `private_app_password` with the "Admin API access token".
1. Similarly, update the `access_token` with the "Partner API access token".

   >To load data using the Shopify API, update the `private_app_password`.
   >To load data using the Shopify partner API, update the `access_token`.

1. Next, store your pipeline configuration details in the `.dlt/config.toml`.

   Here's what the `config.toml` looks like:

   ```toml
   [sources.shopify_dlt]
   shop_url = "Please set me up!"
   organization_id = "Please set me up!"
   ```

1. Update `shop_url` with the URL of your Shopify store. For example, "https://shop-123.myshopify.com/".

1. Update `organization_id` with a code from your Shopify partner URL. For example, in "https://partners.shopify.com/1234567", the code '1234567' is the organization ID.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to add credentials for your chosen destination, ensuring proper routing of your data to the final destination.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python shopify_dlt_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `shopify_data`, you may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Source `shopify_source`:

This function returns a list of resources to load products, orders, and customers data from the Shopify API.

```py
def shopify_source(
    private_app_password: str = dlt.secrets.value,
    api_version: str = DEFAULT_API_VERSION,
    shop_url: str = dlt.config.value,
    start_date: TAnyDateTime = FIRST_DAY_OF_MILLENNIUM,
    end_date: Optional[TAnyDateTime] = None,
    created_at_min: TAnyDateTime = FIRST_DAY_OF_MILLENNIUM,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
    order_status: TOrderStatus = "any",
) -> Iterable[DltResource]:
   ...
```

`private_app_password`: App's password for your shop.

`api_version`: API version (e.g., 2023-01).

`shop_url`: Your shop's URL (e.g., https://my-shop.myshopify.com).

`items_per_page`: Max items fetched per page (Default: 250).

`start_date`: Imports items updated since this date (Default: 2000-01-01). Used for incremental loading if end_time isn't specified. Accepts ISO 8601 date/datetime formats.

`end_time`: Data load range end time. Paired with start_date for specified time range. Enables incremental loading if unspecified.

`created_at_min`: Load items created since this date (Default: 2000-01-01).

`order_status`: Filter for order status: 'open', 'closed', 'cancelled', 'any' (Default: 'any').

### Resource `products`:

This resource loads products from your Shopify shop into the destination. It supports incremental loading and pagination.

```py
@dlt.resource(primary_key="id", write_disposition="merge")
def products(
    updated_at: dlt.sources.incremental[
        pendulum.DateTime
    ] = dlt.sources.incremental(
        "updated_at",
        initial_value=start_date_obj,
        end_value=end_date_obj,
        allow_external_schedulers=True,
    ),
    created_at_min: pendulum.DateTime = created_at_min_obj,
    items_per_page: int = items_per_page,
) -> Iterable[TDataItem]:
   ...
```

`updated_at`: The saved [state](../../general-usage/state) of the last 'updated_at' value.

Similar to the mentioned resource, there are two more resources "orders" and "customers", both support incremental loading and pagination.

### Resource `shopify_partner_query`:
This resource can be used to run custom GraphQL queries to load paginated data.

```py
@dlt.resource
def shopify_partner_query(
    query: str,
    data_items_path: jp.TJsonPath,
    pagination_cursor_path: jp.TJsonPath,
    pagination_variable_name: str = "after",
    variables: Optional[Dict[str, Any]] = None,
    access_token: str = dlt.secrets.value,
    organization_id: str = dlt.config.value,
    api_version: str = DEFAULT_PARTNER_API_VERSION,
) -> Iterable[TDataItem]:
   ...
```

`query`: The GraphQL query for execution.

`data_items_path`: JSONPath to array items in query results.

`pagination_cursor_path`: The JSONPath to the pagination cursor in the query result, which will be piped to the next query via variables.

`pagination_variable_name`: The name of the variable to pass the pagination cursor to.

`variables`: Mapping of additional variables used in the query.

`access_token`: The Partner API Client access token, created in the Partner Dashboard.

`organization_id`: Your Organization ID, found in the Partner Dashboard.

`api_version`: The API version to use (e.g., 2024-01). Use `unstable` for the latest version.

## Customization



### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="shopify",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="shopify_data"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our [documentation](../../general-usage/pipeline).

1. To load data from "products", "orders", and "customers" from January 1, 2023:

   ```py
   # Add your desired resources to the list...
   resources = ["products", "orders", "customers"]
   start_date="2023-01-01"

   load_data = shopify_source(start_date=start_date).with_resources(*resources)
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

1. To load past Shopify orders in weekly chunks using start_date and end_date parameters. This minimizes potential failure during large data loads. Running chunks and incremental loads in parallel accelerates the initial load.

   ```py
   # Load all orders from 2023-01-01 to now
   min_start_date = current_start_date = pendulum.datetime(2023, 1, 1)
   max_end_date = pendulum.now()
   # Create a list of time ranges of 1 week each, we'll use this to load the data in chunks
   ranges: List[Tuple[pendulum.DateTime, pendulum.DateTime]] = []
   while current_start_date < max_end_date:
        end_date = min(current_start_date.add(weeks=1), max_end_date)
        ranges.append((current_start_date, end_date))
        current_start_date = end_date

   for start_date, end_date in ranges:
        print(f"Load orders between {start_date} and {end_date}")
        # Create the source with start and end date set according to the current time range to filter
        # created_at_min lets us set a cutoff to exclude orders created before the initial date of (2023-01-01)
        # even if they were updated after that date
        load_data = shopify_source(
            start_date=start_date, end_date=end_date, created_at_min=min_start_date
        ).with_resources("orders")

        load_info = pipeline.run(load_data)
        print(load_info)
   # Continue loading new data incrementally starting at the end of the last range
   # created_at_min still filters out items created before 2023-01-01
   load_info = pipeline.run(
        shopify_source(
            start_date=max_end_date, created_at_min=min_start_date
        ).with_resources("orders")
   )
   print(load_info)
   ```
1. To load the first 10 transactions via a GraphQL query from the Shopify Partner API.
   ```py
    # Construct query to load transactions 100 per page, the `$after` variable is used to paginate
    query = """query Transactions($after: String) {
    transactions(after: $after, first: 10) {
        edges {
            cursor
            node {
                id
            }
        }
    }
    }
    """

    # Configure the resource with the query and JSON paths to extract the data and pagination cursor
    resource = shopify_partner_query(
        query,
        # JSON path pointing to the data item in the results
        data_items_path="data.transactions.edges[*].node",
        # JSON path pointing to the highest page cursor in the results
        pagination_cursor_path="data.transactions.edges[-1].cursor",
        # The variable name used for pagination
        pagination_variable_name="after",
    )

    load_info = pipeline.run(resource)
    print(load_info)
   ```

<!--@@@DLT_TUBA shopify_dlt-->

