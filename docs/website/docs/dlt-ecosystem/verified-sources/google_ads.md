---
title: Google Ads
description: dlt verified source for Google Ads API
keywords: [google ads api, google ads verified source, google ads]
---
import Header from './_source-info-header.md';

# Google ads

[Google Ads](https://ads.google.com/home/) is a digital advertising service by Google that allows advertisers to display ads across Google's search results, websites, and other platforms.

:::warning Alert!
Please note that we are unable to conduct regular testing on the specified source due to difficulties in obtaining the necessary credentials. We confirmed this source works at creation, and it is being used by the community. We anticipate that the source should operate smoothly over time given Google's best practices in versioning APIs.
:::

This Google Ads `dlt` verified source and [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_ads_pipeline.py) loads data using the "Google Ads API" to the destination of your choice.

Resources that can be loaded using this verified source are:

| Name             | Description                                                             |
|------------------|-------------------------------------------------------------------------|
| customers        | Businesses or individuals who pay to advertise their products           |
| campaigns        | Structured sets of ad groups and advertisements                         |
| change_events    | Modifications made to an account's ads, campaigns, and related settings |
| customer_clients | Accounts that are managed by a given account                            |

## Setup guide

### Grab credentials

To access Google Ads verified sources, you'll need a developer token. For instructions on obtaining one, you can search online or ask GPT.

Next, there are two methods to get authenticated for using this verified source:

- OAuth credentials
- Service account credentials

Let's go over how to set up both OAuth tokens and service account credentials. In general, OAuth tokens are preferred when user consent is required, while service account credentials are better suited for server-to-server interactions. You can choose the method of authentication as per your requirement.

### Grab Google service account credentials

You need to create a GCP service account to get API credentials if you don't have one. To create one, follow these steps:

1. Sign in to [console.cloud.google.com](http://console.cloud.google.com/).

1. [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create#creating) if needed.

1. Enable the "Google Ads API". Refer to the [Google documentation](https://support.google.com/googleapi/answer/6158841?hl=en) for comprehensive instructions on this process.

1. Generate credentials:

   1. Navigate to IAM & Admin in the console's left panel, and then select Service Accounts.
   1. Identify the service account you intend to use, and click on the three-dot menu under the "Actions" column next to it.
   1. Create a new JSON key by selecting "Manage Keys" > "ADD KEY" > "CREATE".
   1. You can download the ".json" file containing the necessary credentials for future use.

### Grab Google OAuth credentials

You need to create a GCP account to get OAuth credentials if you don't have one. To create one,
follow these steps:

1. Ensure your email used for the GCP account has access to the GA4 property.

1. Open a GCP project in your GCP account.

1. Enable the Google Ads API in the project.

1. Search for credentials in the search bar and go to Credentials.

1. Go to Credentials -> OAuth client ID -> Select Desktop App from the Application type and give an
   appropriate name.

1. Download the credentials and fill in "client_id", "client_secret", and "project_id" in
   "secrets.toml".

1. Go back to credentials and select the OAuth consent screen on the left.

1. Fill in the App name, user support email (your email), authorized domain (localhost.com), and dev
   contact info (your email again).

1. Add the following scope:

   ```text
   "https://www.googleapis.com/auth/adwords"
   ```

1. Add your email as a test user.

After configuring "client_id", "client_secret", and "project_id" in "secrets.toml", to generate the
refresh token, run the following script from the root folder:

```sh
python google_ads/setup_script_gcp_oauth.py
```

Once you have executed the script and completed the authentication, you will receive a "refresh
token" that can be used to set up the "secrets.toml".

### Share the Google Ads account with the API:

:::note
For service account authentication, use the client_email. For OAuth authentication, use the
email associated with the app creation and refresh token generation.
:::

1. Log into your Google Ads account.

1. Select the Google Ads account you want to access.

1. Click on the "Tools & Settings" icon in the upper right corner of the screen.

1. Under ‘Setup’, choose 'Account access' from the menu.

1. Click the blue “+” icon to add a new user.

1. Enter the email address associated with either the service account (for service account authentication)
   or the email used during app creation and refresh token generation (for OAuth authentication).

1. Assign the appropriate access level; for API purposes, 'Read-only' access might suffice if you only need data
   retrieval capabilities. However, if using a service account, you might need to give 'Admin' access since service
   accounts usually perform tasks requiring higher privileges.

1. Conclude the process by clicking the “Send invitation” button.

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init google_ads duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_ads_pipeline.py)
   with Google Ads as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. In this file, set up the "developer
   token", "customer ID", and "impersonated_email" as follows:
   ```toml
   [sources.google_ads]
   dev_token = "please set me up!"
   customer_id = "please set me up!"
   impersonated_email = "please set me up"
   ```
   - `dev_token` is the developer token that lets you connect to the Google Ads API.
   - `customer_id` in Google Ads is a unique three-part number (formatted as XXX-XXX-XXXX) that identifies
   and helps manage individual Google Ads accounts. It is used for API access and account operations, and
   is visible in the top right corner of your Google Ads dashboard.
   - `impersonated_email` enables secure access to Google Ads accounts through the API using a service account,
   while leveraging the permissions of a specific user within the Ads platform.

1. Next, for service account authentication:

   ```toml
   [sources.google_ads.credentials]
   project_id = "project_id" # please set me up!
   client_email = "client_email" # please set me up!
   private_key = "private_key" # please set me up!
   ```

1. From the ".json" that you
   [downloaded earlier](google_ads.md#grab-google-service-account-credentials),
   copy `project_id`, `private_key`,
   and `client_email` under `[sources.google_ads.credentials]`.

1. Alternatively, if you're using OAuth credentials, replace the fields and values with those
   you [grabbed for OAuth credentials](google_ads.md#grab-google-oauth-credentials).

1. The secrets.toml for OAuth authentication looks like:

   ```toml
   [sources.google_ads.credentials]
   client_id = "client_id" # please set me up!
   client_secret = "client_secret" # please set me up!
   refresh_token = "refresh_token" # please set me up!
   project_id = "project_id" # please set me up!
   ```

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:
   ```sh
   pip install -r requirements.txt
   ```
1. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python google_ads_pipeline.py
   ```
1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is
   `dlt_google_ads_pipeline`, you may also use any custom name instead.

For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `google_ads`

This function returns a list of resources including metadata, fields, and metrics data from
the Google Ads API.

```py
def google_ads(
    credentials: Union[
        GcpOAuthCredentials, GcpServiceAccountCredentials
    ] = dlt.secrets.value,
    impersonated_email: str = dlt.secrets.value,
    dev_token: str = dlt.secrets.value,
)  -> List[DltResource]:
   """
   Initializes a client with the provided credentials and development token to
   load default tables from Google Ads into the database. This function returns
   various resources such as customers, campaigns, change events, and customer
   clients.
   """
```

`credentials`: GCP OAuth or service account credentials.

`impersonated_email`: enables secure access to Google Ads accounts through the API using a service account,
while leveraging the permissions of a specific user within the Ads platform.

`dev_token`: A developer token, which is required to access the Google Ads API.

### Resource `customers`

This function retrieves all dimensions for a report from a Google Ads project.

```py
@dlt.resource(write_disposition="replace")
def customers(
    client: Resource, customer_id: str = dlt.secrets.value
) -> Iterator[TDataItem]:
    """
    Fetches customer data from the Google Ads service and
    yields each customer as a dictionary.
    """
```

`client`: Refers to a Google API Resource object used to interact with Google services.

`customer_id`: Individual identifier for a Google Ads account.

Similarly, there are resource functions called `campaigns`, `change_events`, and `customer_clients` that populate
respective dimensions.

## Customization
### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
       pipeline_name="dlt_google_ads_pipeline",  # Use a custom name if desired
       destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
       dataset_name="full_load_google_ads"  # Use a custom name if desired
   )
   ```

   To read more about pipeline configuration, please refer to our
   [documentation](../../general-usage/pipeline).

1. To load all the dimensions from Google Ads:

   ```py
   data_default = google_ads()
   info = pipeline.run(data=[data_default])
   print(info)
   ```

1. To load the data from `customers` and `campaigns`:

   ```py
   data_selected = google_ads().with_resources("customers", "campaigns")
   info = pipeline.run(data=[data_default])
   print(info)
   ```

<!--@@@DLT_TUBA hubspot-->

