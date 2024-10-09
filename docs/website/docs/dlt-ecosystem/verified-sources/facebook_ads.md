---
title: Facebook Ads
description: dlt verified source for Facebook Ads
keywords: [facebook ads api, verified source, facebook ads]
---
import Header from './_source-info-header.md';

# Facebook ads

<Header/>

Facebook Ads is the advertising platform that lets businesses and individuals create targeted ads on
Facebook and its affiliated apps like Instagram and Messenger.

This Facebook `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads_pipeline.py)
loads data using the [Facebook Marketing API](https://developers.facebook.com/products/marketing-api/) to the destination of your choice.

The endpoints that this verified source supports are:

| Name              | Description                                                                    |
| ----------------- | ------------------------------------------------------------------------------ |
| campaigns         | A structured marketing initiative that focuses on a specific objective or goal |
| ad_sets           | A subset or group of ads within a campaign                                     |
| ads               | An individual advertisement that is created and displayed within an ad set      |
| creatives         | Visual and textual elements that make up an advertisement                      |
| ad_leads          | Information collected from users who have interacted with lead generation ads  |
| facebook_insights | Data on audience demographics, post reach, and engagement metrics              |

To get a complete list of sub-endpoints that can be loaded, see
[facebook_ads/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads/settings.py)

## Setup guide

### Grab credentials

#### Grab `Account ID`

1. Ensure that you have Ads Manager active for your Facebook account.
1. Find your account ID, which is a long number. You can locate it by clicking on the Account
   Overview dropdown in Ads Manager or by checking the link address. For example,
   https://adsmanager.facebook.com/adsmanager/manage/accounts?act=10150974068878324.
1. Note this account ID as it will further be used in configuring dlt.

#### Grab `Access_Token`

1. Sign up for a developer account on
   [developers.facebook.com](https://developers.facebook.com/).
1. Log in to your developer account and click on "My Apps" in the top right corner.
1. Create an app, select "Other" as the type, choose "Business" as the category, and click "Next".
1. Enter the name of your app and select the associated business manager account.
1. Go to the "Basic" settings in the left-hand side menu.
1. Copy the "App ID" and "App secret" and paste them as "client_id" and "client_secret" in the
   secrets.toml file in the .dlt folder.
1. Next, obtain a short-lived access token at https://developers.facebook.com/tools/explorer/.
1. Select the created app, add "ads_read" and "lead_retrieval" permissions, and generate a
   short-lived access token.
1. Copy the access token and update it in the `.dlt/secrets.toml` file.

#### Exchange short-lived token for a long-lived token

By default, Facebook access tokens have a short lifespan of one hour. To exchange a short-lived Facebook access token for a long-lived token, update the `.dlt/secrets.toml` with client_id and client_secret, and execute the provided Python code.

```py
from facebook_ads import get_long_lived_token
print(get_long_lived_token("your short-lived token"))
```

Replace the `access_token` in the `.dlt/secrets.toml` file with the long-lived token obtained from the above code snippet.

To retrieve the expiry date and the associated scopes of the token, you can use the following command:

```py
from facebook_ads import debug_access_token
debug_access_token()
```

We highly recommend you add the token expiration timestamp to get notified a week before token expiration that you need to rotate it. Right now, the notifications are sent to the logger with error level. In `config.toml` / `secrets.toml`:

```toml
[sources.facebook_ads]
access_token_expires_at=1688821881
```

> Note: The Facebook UI, which is described here, might change.
The full guide is available at [this link.](https://developers.facebook.com/docs/marketing-apis/overview/authentication)

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init facebook_ads duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads_pipeline.py) with Facebook Ads as the [source](../../general-usage/source) and [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credential

1. Inside the `.dlt` folder, you'll find a file called `secrets.toml`, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe. Here's what the file looks like:

   ```toml
   # put your secret values and credentials here
   # do not share this file and do not push it to github
   [sources.facebook_ads]
   access_token="set me up!"
   ```

1. Replace the access_token value with the [previously copied one](facebook_ads.md#grab-credentials) to ensure secure access to your Facebook Ads resources.

1. Next, follow the [destination documentation](../../dlt-ecosystem/destinations) instructions to add credentials for your chosen destination, ensuring proper routing of your data to the final destination.

1. It is strongly recommended to add the token expiration timestamp to your `config.toml` or `secrets.toml` file.

1. Next, store your pipeline configuration details in the `.dlt/config.toml`.

   Here's what the `config.toml` looks like:

   ```toml
   [sources.facebook_ads]
   account_id = "Please set me up!"
   ```

1. Replace the value of the "account id" with the one [copied above](#grab-account-id).

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```sh
   pip install -r requirements.txt
   ```
2. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python facebook_ads_pipeline.py
   ```
3. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `facebook_ads`. You may also
   use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Default endpoints

You can write your own pipelines to load data to a destination using this verified source. However,
it is important to note the complete list of the default endpoints given in
[facebook_ads/settings.py.](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads/settings.py)

### Source `facebook_ads_source`

This function returns a list of resources to load campaigns, ad sets, ads, creatives, and ad leads
data from the Facebook Marketing API.

```py
@dlt.source(name="facebook_ads")
def facebook_ads_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    chunk_size: int = 50,
    request_timeout: float = 300.0,
    app_api_version: str = None,
) -> Sequence[DltResource]:
   ...
```

`account_id`: Account ID associated with the ad manager, configured in "config.toml".

`access_token`: Access token associated with the Business Facebook App, configured in
"secrets.toml".

`chunk_size`: The size of the page and batch request. You may need to decrease it if you request a lot
of fields. Defaults to 50.

`request_timeout`: Connection timeout. Defaults to 300.0.

`app_api_version`: A version of the Facebook API required by the app for which the access tokens
were issued, e.g., 'v17.0'. Defaults to the _facebook_business_ library default version.

### Resource `ads`

The ads function fetches ad data. It retrieves ads from a specified account with specific fields and
states.

```py
@dlt.resource(primary_key="id", write_disposition="replace")
def ads(
    fields: Sequence[str] = DEFAULT_AD_FIELDS,
    states: Sequence[str] = None,
) -> Iterator[TDataItems]:

  yield get_data_chunked(account.get_ads, fields, states, chunk_size)
```

`fields`: Retrieves fields for each ad. For example, “id”, “name”, “adset_id”, etc.

`states`: The possible states include "Active," "Paused," "Pending Review," "Disapproved,"
"Completed," and "Archived."

### Resources for `facebook_ads_source`

Similar to resource `ads`, the following resources have been defined in the `__init__.py` for source
`facebook_ads_source`:

| Resource     | Description                                                          |
| ------------ | -------------------------------------------------------------------- |
| campaigns    | Fetches all `DEFAULT_CAMPAIGN_FIELDS`                                |
| ad_sets      | Fetches all `DEFAULT_ADSET_FIELDS`                                   |
| leads        | Fetches all `DEFAULT_LEAD_FIELDS`, uses `@dlt.transformer` decorator |
| ad_creatives | Fetches all `DEFAULT_ADCREATIVE_FIELDS`                              |

The default fields are defined in
[facebook_ads/settings.py](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads/settings.py)

### Source `facebook_insights_source`

This function returns a list of resources to load facebook_insights.

```py
@dlt.source(name="facebook_ads")
def facebook_insights_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    initial_load_past_days: int = 30,
    fields: Sequence[str] = DEFAULT_INSIGHT_FIELDS,
    attribution_window_days_lag: int = 7,
    time_increment_days: int = 1,
    breakdowns: TInsightsBreakdownOptions = "ads_insights_age_and_gender",
    action_breakdowns: Sequence[str] = ALL_ACTION_BREAKDOWNS,
    level: TInsightsLevels = "ad",
    action_attribution_windows: Sequence[str] = ALL_ACTION_ATTRIBUTION_WINDOWS,
    batch_size: int = 50,
    request_timeout: int = 300,
    app_api_version: str = None,
) -> DltResource:
   ...
```

`account_id`: Account ID associated with ads manager, configured in _config.toml_.

`access_token`: Access token associated with the Business Facebook App, configured in _secrets.toml_.

`initial_load_past_days`: How many past days (starting from today) to initially load. Defaults to 30.

`fields`: A list of fields to include in each report. Note that the “breakdowns” option adds fields automatically. Defaults to DEFAULT_INSIGHT_FIELDS.

`attribution_window_days_lag`: Attribution window in days. The reports in the attribution window are refreshed on each run. Defaults to 7.

`time_increment_days`: The report aggregation window in days. Use 7 for weekly aggregation. Defaults to 1.

`breakdowns`: Presents with common aggregations. See [settings.py](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads/settings.py) for details. Defaults to "ads_insights_age_and_gender".

`action_breakdowns`: Action aggregation types. See [settings.py](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads/settings.py) for details. Defaults to ALL_ACTION_BREAKDOWNS.

`level`: The granularity level. Defaults to "ad".

`action_attribution_windows`: Attribution windows for actions. Defaults to ALL_ACTION_ATTRIBUTION_WINDOWS.

`batch_size`: Page size when reading data from a particular report. Defaults to 50.

`request_timeout`: Connection timeout. Defaults to 300.

`app_api_version`: A version of the Facebook API required by the app for which the access tokens were issued, e.g., 'v17.0'. Defaults to the facebook_business library default version.

### Resource `facebook_insights`

This function fetches Facebook insights data incrementally from a specified start date until the current date, in day steps.

```py
@dlt.resource(primary_key=INSIGHTS_PRIMARY_KEY, write_disposition="merge")
def facebook_insights(
    date_start: dlt.sources.incremental[str] = dlt.sources.incremental(
        "date_start", initial_value=initial_load_start_date_str
    )
) -> Iterator[TDataItems]:
   ...
```

`date_start`: Parameter sets the initial value for the "date_start" parameter in dlt.sources.incremental. It is based on the last pipeline run or defaults to today's date minus the specified number of days in the "initial_load_past_days" parameter.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="facebook_ads",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="facebook_ads_data"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our [documentation](../../general-usage/pipeline).

1. To load all the data from campaigns, ad sets, ads, ad creatives, and leads:

   ```py
   load_data = facebook_ads_source()
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

1. To merge the Facebook Ads with the state "DISAPPROVED" and with ads state "PAUSED", you can do the following:

   ```py
   load_data = facebook_ads_source()
   # It is recommended to enable root key propagation on a source that is not a merge one by default. This is not required if you always use merge but below we start with replace
   load_data.root_key = True

   # Load only disapproved ads
   load_data.ads.bind(states=("DISAPPROVED",))
   load_info = pipeline.run(load_data.with_resources("ads"), write_disposition="replace")
   print(load_info)

   # Here we merge the paused ads but the disapproved ads stay there!
   load_data = facebook_ads_source()
   load_data.ads.bind(states=("PAUSED",))
   load_info = pipeline.run(load_data.with_resources("ads"), write_disposition="merge")
   print(load_info)
   ```

   In the above steps, we first load the "ads" data with the "DISAPPROVED" state in _replace_ mode and then merge the ads data with the "PAUSED" state on that.

1. To load data with a custom field, for example, to load only "id" from Facebook ads, you can do the following:

   ```py
   load_data = facebook_ads_source()
   # Only loads ad ids, works the same for campaigns, leads, etc.
   load_data.ads.bind(fields=("id",))
   load_info = pipeline.run(load_data.with_resources("ads"))
   print(load_info)
   ```

1. This pipeline includes an enrichment transformation called `enrich_ad_objects` that you can apply to any resource to obtain additional data per object using `object.get_api`. The following code demonstrates how to enrich objects by adding an enrichment transformation that includes additional fields.

   ```py
   # You can reduce the chunk size for smaller requests
   load_data = facebook_ads_source(chunk_size=2)

   # Request only the "id" field for ad_creatives
   load_data.ad_creatives.bind(fields=("id",))

   # Add a transformation to the ad_creatives resource
   load_data.ad_creatives.add_step(
       # Specify the AdCreative object type and request the desired fields
       enrich_ad_objects(AdCreative, DEFAULT_ADCREATIVE_FIELDS)
   )

   # Run the pipeline with the ad_creatives resource
   load_info = pipeline.run(load_data.with_resources("ad_creatives"))

   print(load_info)
   ```

   In the above code, the "load_data" object represents the Facebook Ads source, and we specify the desired chunk size for the requests. We then bind the "id" field for the "ad_creatives" resource using the "bind()" method.

   To enrich the ad_creatives objects, we add a transformation using the "add_step()" method. The "enrich_ad_objects" function is used to specify the AdCreative object type and request the fields defined in _DEFAULT_ADCREATIVE_FIELDS_.

   Finally, we run the pipeline with the ad_creatives resource and store the load information in the `load_info`.

1. You can also load insights reports incrementally with defined granularity levels, fields, breakdowns, etc., as defined in the `facebook_insights_source`. This function generates daily reports for a specified number of past days.

   ```py
   load_data = facebook_insights_source(
       initial_load_past_days=30,
       attribution_window_days_lag=7,
       time_increment_days=1
   )
   load_info = pipeline.run(load_data)
   print(load_info)
   ```

> By default, daily reports are generated from `initial_load_past_days` ago to today. On subsequent runs, only new reports are loaded, with the past `attribution_window_days_lag` days (default is 7) being refreshed to accommodate any changes. You can adjust `time_increment_days` to change report frequency (default set to one).

<!--@@@DLT_TUBA facebook_ads-->

