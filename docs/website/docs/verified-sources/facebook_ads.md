# Facebook Ads

Facebook is a social media platform that allows users to create profiles, connect with friends, share photos and videos, and engage in various forms of online communication.

Facebook Ads refer to the advertising platform and features provided by Facebook for businesses and individuals to create and run targeted advertisements on the Facebook platform and its affiliated apps, such as Instagram, Messenger, and Audience Network.

This Facebook `dlt` verified source and pipeline example loads data using “Facebook Marketing API” to the destination of your choice.

The endpoints that this verified source supports are:

| Endpoint | Description |
| --- | --- |
| campaigns | a structured marketing initiative that focuses on a specific objective or goal |
| ad_sets | a subset or group of ads within a campaign |
| ads | individual advertisement that is created and displayed within an ad set |
| creatives | visual and textual elements that make up an advertisement. |
| ad_leads | information collected from users who have interacted with lead generation ads |

## Grab credentials

### Grab `Account ID`

1. Ensure that you have Ads Manager active for your Facebook account.
2. Find your account ID, which is a long number. You can locate it by clicking on the Account Overview dropdown in Ads Manager or by checking the link address. For example **https://adsmanager.facebook.com/adsmanager/manage/accounts?act=10150974068878324**
3. Note this account ID as it will further be used in configuring dlt.

### Grab `Access_Token`

1. Sign up for a developer account on **[developers.facebook.com](https://developers.facebook.com/)**.
2. Log in to your developer account and click on "My Apps" at the top right corner.
3. Click on "Create App" and select "Other" as the app type. Choose "Business" as the category, and click on "Next."
4. Enter the name of your app and select the associated business manager account.
5. In the left-hand side menu bar, go to the "Basic" settings section.
6. Copy the "App ID" and "App secret" and paste them as "client_id" and "client_secret" respectively in the `secrets.toml` file located in the `.dlt` folder.
7. Next, obtain a short-lived access token by using the Facebook Graph API Explorer. Visit **https://developers.facebook.com/tools/explorer/**.
8. Select the app you just created in the API Explorer and add the permissions "ads_read" and "lead_retrieval". Click on "Generate Access Token". Note that this token is short-lived and expires after one hour.
9. Copy the generated access token and update the corresponding value in the `secrets.toml` file mentioned above.

### Exchange _short-lived token_ for a _long-lived token_:

By default, Facebook access tokens have a short lifespan of one hour. However, you can exchange a short-lived token for a long-lived token. To do this, follow the steps mentioned earlier to add the short-lived access token, client_id, and client_secret to the **`secrets.toml`** file as mentioned above. Then, using the Python interactive command prompt, execute the following code:

```python
from facebook_ads import get_long_lived_token
print(get_long_lived_token("your short-lived token")
```

Replace the **`access_token`** in the **`secrets.toml`** file with the long-lived token obtained from the above code snippet.

To retrieve the expiry date and the associated scopes of the token, you can use the following command:

```python
from facebook_ads.fb import debug_access_token
debug_access_token()
```

```toml
[sources.facebook_ads]
access_token_expires_at=1688821881
```

## Initialize the Facebook Ads verified source and pipeline example

To get started with your data verified source, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:
    ```properties
    dlt init facebook_ads bigquery
    ```
    This command will initialize your verified source with Facebook Marketing API and creates an example pipeline with BigQuery as the destination. If you'd like to use a different destination, simply replace **`bigquery`** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](https://dlthub.com/docs/destinations/duckdb)

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

    ```
    facebook_ads_source
    ├── .dlt
    │   ├── config.toml
    │   └── secrets.toml
    ├── facebook_ads
    │   └── __init__.py
    │   └── exceptions.py
    │   └── fb.py
    │   └── README.py
    │   └── settings.py
    ├── .gitignore
    ├── requirements.txt
    └── facebook_ads_pipeline.py
    ```

## **Add credential**

1. Inside the **`.dlt`** folder, you'll find a file called **`secrets.toml`**, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

    Here's what the file looks like:
    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.workable]
    access_token = "access_token" # Your Workable token copied above
    client_id = "enter your App ID here" # Enter the App ID copied above
    client_secret = "enter your App secret here" # Enter the App secret copied above
    access_token_expires_at="enter your access token expiry time stamp"

    [destination.bigquery.credentials]
    project_id = "project_id" # GCP project ID
    private_key = "private_key" # Unique private key (including `BEGIN and END PRIVATE KEY`)
    client_email = "client_email" # Service account email
    location = "US" # Project location (e.g. “US”)
    ```

2. Replace the value of **`access_token`** with the one that [you copied above](facebook_ads.md#grab-credentials). This will ensure that your verified source can access your Workable resources securely.
3. Next, follow the instructions in **[Destinations](https://dlthub.com/docs/destinations/duckdb)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.
4. It is strongly recommended to add the token expiration timestamp to your **`config.toml`** or **`secrets.toml`** file.
5. Inside the **`.dlt`** folder, you'll find a file called **`config.toml`**, where you can securely store your pipeline configuration details.

    Here's what the config.toml looks like:
    ```toml
    [sources.facebook_ads]
    account_id = "1430280281077689" # please set me up!
    ```

6. Replace the value of the account id with the one copied above.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    ```properties
    pip install -r requirements.txt
    ```

2. Now the pipeline can be run by using the command:
    ```properties
    python3 facebook_ads_pipeline.py`
    ```

3. To make sure that everything is loaded as expected, use the command:
    ```properties
    dlt pipeline <pipeline_name> show
    ```
    (For example, the pipeline_name for the above pipeline  example is `facebook_ads`, you may also use any custom name instead)

## Customizations

To load data to the destination using  this verified source, you have the option to write your own methods. However, it is important to note the endpoints that can be loaded using this method:

| campaigns | ad_sets | ads | creatives | ad_leads | facebook_insights |
| --- | --- | --- | --- | --- | --- |

### Source and resource methods

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source) and [resources](https://dlthub.com/docs/general-usage/resource) that for this verified source are found in the `__init__.py` file within the *facebook_ads* directory. This verified source has two source methods and several resource methods:

#### 1. Source _facebook_ads_source_:
```python
@dlt.source(name="facebook_ads")
def facebook_ads_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    chunk_size: int = 50,
    request_timeout: float = 300.0,
    app_api_version: str = None,
) -> Sequence[DltResource]:
```

**`account_id`**: Account id associated with add manager, configured in _config.toml_.

**`access_token`**: Access token associated with the Business Facebook App, configured in _secrets.toml_.

**`chunk_size`**: A size of the page and batch request. You may need to decrease it if you request a lot of fields. Defaults to 50.

**`request_timeout`**:  Connection timeout. Defaults to 300.0.

**`app_api_version`**: A version of the facebook api required by the app for which the access tokens were issued i.e. 'v17.0'. Defaults to the _facebook_business_ library default version.

The above function returns a list of resources to load campaigns, ad sets, ads, creatives and ad leads data from Facebook Marketing API. Each resource has a different function as discussed below:

#### a) Resource _ads_:

```python
@dlt.resource(primary_key="id", write_disposition="replace")
def ads(
  fields: Sequence[str] = DEFAULT_AD_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
  yield get_data_chunked(account.get_ads, fields, states, chunk_size)
```

**`fields`**: Represents the fields that you want to retrieve for each ad. For example, “id”, “name”, “adset_id” etc.

**`states`**: Refer to the different statuses that an ad can be in. For example, in ads endpoint states can be “Active”, “Paused”, “Pending Review”, “Disapproved”, “Completed” and “Archived”.

By default, this method utilizes the “*replace”* mode, which means that all the data will be loaded fresh into the table. In other words, the existing data in the destination is completely replaced with the new data being loaded on every run. However, the write deposition can also be changed to “*merge*” so that only new data is loaded into the destination.

Similar to the resource “campaigns” the following resources have been defined in the `__init__.py` file.

| campaigns() | ad_sets() | leads() | ad_creatives() |
| --- | --- | --- | --- |

#### 2. Source _facebook_insights_source_:

```python
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
```

**`account_id`**: Account id associated with add manager, configured in _config.toml_.

**`access_token`**: Access token associated with the Business Facebook App, configured in _secrets.toml_.

**`initial_load_past_days`**: How many past days (starting from today) to initially load. Defaults to 30.

**`fields`**: A list of fields to include in each report. Note that the “breakdowns” option adds fields automatically. Defaults to DEFAULT_INSIGHT_FIELDS.

**`attribution_window_days_lag`**: Attribution window in days. The reports in the attribution window are refreshed on each run. Defaults to 7.

**`time_increment_days`**: The report aggregation window in days. use 7 for weekly aggregation. Defaults to 1.

**`breakdowns`**: A presents with common aggregations. See [settings.py](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads/settings.py) for details. Defaults to "ads_insights_age_and_gender".

**`action_breakdowns`**: Action aggregation types. See [settings.py](https://github.com/dlt-hub/verified-sources/blob/master/sources/facebook_ads/settings.py) for details. Defaults to ALL_ACTION_BREAKDOWNS.

**`level`**: The granularity level. Defaults to "ad".

**`action_attribution_windows`**: Attribution windows for actions. Defaults to ALL_ACTION_ATTRIBUTION_WINDOWS.

**`batch_size`**: Page size when reading data from a particular report. Defaults to 50.

**`request_timeout`**: Connection timeout. Defaults to 300.

**`app_api_version`**: A version of the Facebook API required by the app for which the access tokens were issued i.e. 'v17.0'. Defaults to the facebook_business library default version.

The above function returns a list of resources to load facebook_insights. The facebook_insights are loaded using the following resource:

#### a) Resource _facebook_insights_:
```python
@dlt.resource(primary_key=INSIGHTS_PRIMARY_KEY, write_disposition="merge")
def facebook_insights(
        date_start: dlt.sources.incremental[str] = dlt.sources.incremental(
            "date_start", initial_value=initial_load_start_date_str
        )
    ) -> Iterator[TDataItems]:
```

**`date_start`**: parameter is used to set the initial value for the “date_start” parameter in *dlt.sources.incremental*. This parameter is equal to the “date_start” in the last pipeline run or defaults to today's date minus the number of days set in the “initial_load_past_days” parameter in the source method.

### **Create Your Data Loading Pipeline**

If you wish to create your own pipelines you can leverage these functions.

To create your data-loading pipeline follow the following steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our documentation [here](https://dlthub.com/docs/general-usage/pipeline).

    ```python

    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",  # Use a custom name if desired
        destination="bigquery",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="facebook_ads_data"  # Use a custom name if desired
    )
    ```

2. To load all the data from, campaigns, ad sets, ads, ad creatives and leads.

    ```python
    load_data = facebook_ads_source()
    load_info = pipeline.run(load_data)
    print(load_info)
    ```

3. To merge the Facebook ads with the state “DISAPPROVED” with the ads with the state “PAUSED” you can do the following:

    ```python
    load_data = facebook_ads_source()
    # It is recommended to enable root key propagation on a source that is not a merge one by default. this is not required if you always use merge but below we start with replace
    load_data.root_key = True

    # load only disapproved ads
    load_data.ads.bind(states=("DISAPPROVED",))
    load_info = pipeline.run(load_data.with_resources("ads"), write_disposition="replace")
    print(load_info)

    # Here we merge the paused ads but the disapproved ads stay there!
    load_data = facebook_ads_source()
    load_data.ads.bind(states=("PAUSED",))
    load_info = pipeline.run(load_data.with_resources("ads"), write_disposition="merge")
    print(load_info)
    ```
    In the above steps, we first load the “ads” data with the “DISAPPROVED” state in _replace_ mode and then merge the ads data with the “PAUSED” state on that.

4. To load data with a custom field, for example, to load only “id” from Facebook ads, you can do the following:

    ```python
    load_data = facebook_ads_source()
    # Only loads add ids, works the same for campaigns, leads etc.
    load_data.ads.bind(fields=("id",))
    load_info = pipeline.run(load_data.with_resources("ads"))
    print(load_info)
    ```

5. This pipeline includes an enrichment transformation called **`enrich_ad_objects`** that you can apply to any resource to obtain additional data per object using **`object.get_api`**. The following code demonstrates how to enrich objects by adding an enrichment transformation that includes additional fields.

    ```python
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

6. You can load the insights reports incrementally with defined granularity levels, fields, breakdowns etc. As defined in the facebook_insights_source. To load the data from the past 7 days in time increments of 1 day you can do as follows:
    ```python
    load_data = facebook_insights_source(initial_load_past_days=7, time_increment_days=7)
    load_info = pipeline.run(load_data)
    print(load_info)
    ```

    The above pipeline loads the data from the last 7 days in single-day increments.

> By default, the reports are generated individually for each day, starting from today minus the number of days specified in the "attribution_window_days_lag". During subsequent runs, only the reports from the last report date until today are loaded, resulting in an **[incremental load](https://dlthub.com/docs/general-usage/incremental-loading)**. The reports from the past 7 days (as defined by the "attribution_window_days_lag") are refreshed during each load to include any changes that occurred within the attribution window.
>

That’s it!  Enjoy running your DLT Facebook_ads pipeline!
