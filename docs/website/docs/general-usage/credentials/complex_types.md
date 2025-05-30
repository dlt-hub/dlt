---
title: Built-in credentials
description: Configure access to AWS, Azure, Google Cloud and other systems
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, specs]
---

## Overview

`dlt` provides built-in credential (**specs**) for seamless integration with common external systems. These specs can be configured using the methods described in the [overview](setup.md) documentation. For major cloud providers like AWS, Azure, and Google Cloud, `dlt` also calls client-specific code to authenticate users and can automatically retrieve default credentials from the running environment. Additionally, `dlt` understands common string representations for credentials such as connection strings or service json, making it easier to work with different credential formats beyond the typical dictionary representation.

:::tip
Learn about the authentication methods supported by the `dlt` RestAPI Client in detail in the [RESTClient section](../http/rest-client.md#authentication).
:::


## Example with ConnectionStringCredentials

`ConnectionStringCredentials` handles database connection strings:

```py
from dlt.sources.credentials import ConnectionStringCredentials

@dlt.source
def query(sql: str, dsn: ConnectionStringCredentials = dlt.secrets.value):
  ...
```

The source above executes the `sql` against the database defined in `dsn`. `ConnectionStringCredentials` ensures you get the correct values with the correct types and understands the relevant native form of the credentials.

Below are examples of how you can set credentials in `secrets.toml` and `config.toml` files.

### Dictionary form

```toml
[dsn]
database="dlt_data"
password="loader"
username="loader"
host="localhost"
```

### Native form

```toml
dsn="postgres://loader:loader@localhost:5432/dlt_data"
```

### Mixed form

If all credentials, except the password, are provided explicitly in the code, `dlt` will look for the password in `secrets.toml`.

```toml
dsn.password="loader"
```

You can explicitly provide credentials in various forms:

```py
query("SELECT * FROM customers", "postgres://loader@localhost:5432/dlt_data") # type: ignore[arg-type]
# or
query("SELECT * FROM customers", {"database": "dlt_data", "username": "loader"}) # type: ignore[arg-type]
```

## Built-in credentials

`dlt` offers some ready-made credentials you can reuse:

```py
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.credentials import OAuth2Credentials
from dlt.sources.credentials import GcpServiceAccountCredentials, GcpOAuthCredentials
from dlt.sources.credentials import AwsCredentials
from dlt.sources.credentials import AzureCredentials
```

### ConnectionStringCredentials

The `ConnectionStringCredentials` class handles connection string credentials for SQL database connections. It includes attributes for the driver name, database name, username, password, host, port, and additional query parameters. This class provides methods for parsing and generating connection strings.

#### Usage
```py
credentials = ConnectionStringCredentials()

# Set the necessary attributes
credentials.drivername = "postgresql"
credentials.database = "my_database"
credentials.username = "my_user"
credentials.password = "my_password"  # type: ignore
credentials.host = "localhost"
credentials.port = 5432

# Convert credentials to a connection string
connection_string = credentials.to_native_representation()

# Parse a connection string and update credentials
native_value = "postgresql://my_user:my_password@localhost:5432/my_database"
credentials.parse_native_representation(native_value)

# Get a URL representation of the connection
url_representation = credentials.to_url()
```
Above, you can find an example of how to use this spec with sources and TOML files.

### OAuth2Credentials

The `OAuth2Credentials` class handles OAuth 2.0 credentials, including client ID, client secret, refresh token, and access token. It also allows for the addition of scopes and provides methods for client authentication.

Usage:
```py
oauth_credentials = OAuth2Credentials(
    client_id="CLIENT_ID",
    client_secret="CLIENT_SECRET",  # type: ignore
    refresh_token="REFRESH_TOKEN",  # type: ignore
    scopes=["scope1", "scope2"]
)

# Authorize the client
oauth_credentials.auth()

# Add additional scopes
oauth_credentials.add_scopes(["scope3", "scope4"])
```

`OAuth2Credentials` is a base class to implement actual OAuth; for example, it is a base class for [GcpOAuthCredentials](#gcpoauthcredentials).

### GCP credentials

#### Examples
* [Google Analytics verified source](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_analytics/__init__.py): an example of how to use GCP Credentials.
* [Google Analytics example](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_analytics/setup_script_gcp_oauth.py): how you can get the refresh token using `dlt.secrets.value`.

#### Types

* [GcpServiceAccountCredentials](#gcpserviceaccountcredentials).
* [GcpOAuthCredentials](#gcpoauthcredentials).

#### GcpServiceAccountCredentials

The `GcpServiceAccountCredentials` class manages GCP Service Account credentials. This class provides methods to retrieve native credentials for Google clients.

##### Usage

- You may just pass the `service.json` as a string or dictionary (in code and via config providers).
- Or default credentials will be used.

```py
gcp_credentials = GcpServiceAccountCredentials()
# Parse a native value (ServiceAccountCredentials)
# Accepts a native value, which can be either an instance of ServiceAccountCredentials
# or a serialized services.json.
# Parses the native value and updates the credentials.
gcp_native_value = {"private_key": ".."} # or "path/to/services.json"
gcp_credentials.parse_native_representation(gcp_native_value)
```
or more preferred use:
```py
import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials
from google.analytics import BetaAnalyticsDataClient

@dlt.source
def google_analytics(
    property_id: str = dlt.config.value,
    credentials: GcpServiceAccountCredentials = dlt.secrets.value,
):
    # Retrieve native credentials for Google clients
    # For example, build the service object for Google Analytics PI.
    client = BetaAnalyticsDataClient(credentials=credentials.to_native_credentials())

    # Get a string representation of the credentials
    # Returns a string representation of the credentials in the format client_email@project_id.
    credentials_str = str(credentials)
    ...
```
while `secrets.toml` looks as follows:
```toml
[sources.google_analytics.credentials]
client_id = "client_id" # please set me up!
client_secret = "client_secret" # please set me up!
refresh_token = "refresh_token" # please set me up!
project_id = "project_id" # please set me up!
```
and `config.toml`:
```toml
[sources.google_analytics]
property_id = "213025502"
```

#### GcpOAuthCredentials

The `GcpOAuthCredentials` class is responsible for handling OAuth2 credentials for desktop applications in Google Cloud Platform (GCP). It can parse native values either as `GoogleOAuth2Credentials` or as serialized OAuth client secrets JSON. This class provides methods for authentication and obtaining access tokens.

##### Usage

```py
oauth_credentials = GcpOAuthCredentials()

# Accepts a native value, which can be either an instance of GoogleOAuth2Credentials
# or serialized OAuth client secrets JSON.
# Parses the native value and updates the credentials.
native_value_oauth = {"client_secret": ...}
oauth_credentials.parse_native_representation(native_value_oauth)
```
Or more preferred use:
```py
import dlt
from dlt.sources.credentials import GcpOAuthCredentials

@dlt.source
def google_analytics(
    property_id: str = dlt.config.value,
    credentials: GcpOAuthCredentials = dlt.secrets.value,
):
    # Authenticate and get access token
    credentials.auth(scopes=["scope1", "scope2"])

    # Retrieve native credentials for Google clients
    # For example, build the service object for Google Analytics API.
    client = BetaAnalyticsDataClient(credentials=credentials.to_native_credentials())

    # Get a string representation of the credentials
    # Returns a string representation of the credentials in the format client_id@project_id.
    credentials_str = str(credentials)
    ...
```
While `secrets.toml` looks as follows:
```toml
[sources.google_analytics.credentials]
client_id = "client_id" # please set me up!
client_secret = "client_secret" # please set me up!
refresh_token = "refresh_token" # please set me up!
project_id = "project_id" # please set me up!
```
And `config.toml`:
```toml
[sources.google_analytics]
property_id = "213025502"
```

In order for the `auth()` method to succeed:

- You must provide valid `client_id`, `client_secret`, `refresh_token`, and `project_id` to get a current **access token** and authenticate with OAuth. Keep in mind that the `refresh_token` must contain all the scopes that are required for your access.
- If the `refresh_token` is not provided, and you run the pipeline from a console or a notebook, `dlt` will use InstalledAppFlow to run the desktop authentication flow.

#### Defaults

If configuration values are missing, `dlt` will use the default Google credentials (from `default()`) if available. Read more about [Google defaults.](https://googleapis.dev/python/google-auth/latest/user-guide.html#application-default-credentials)

- `dlt` will try to fetch the `project_id` from default credentials. If the project id is missing, it will look for `project_id` in the secrets. So it is normal practice to pass partial credentials (just `project_id`) and take the rest from defaults.

### AwsCredentials

The `AwsCredentials` class is responsible for handling AWS credentials, including access keys, session tokens, profile names, region names, and endpoint URLs. It inherits the ability to manage default credentials and extends it with methods for handling partial credentials and converting credentials to a botocore session.

#### Usage
```py
aws_credentials = AwsCredentials()
# Set the necessary attributes
aws_credentials.aws_access_key_id = "ACCESS_KEY_ID"
aws_credentials.aws_secret_access_key = "SECRET_ACCESS_KEY"
aws_credentials.region_name = "us-east-1"
```
or
```py
# Imports an external botocore session and sets the credentials properties accordingly.
import botocore.session

aws_credentials = AwsCredentials()
session = botocore.session.get_session()
aws_credentials.parse_native_representation(session)
print(aws_credentials.aws_access_key_id)
```
or more preferred use:
```py
@dlt.source
def aws_readers(
    bucket_url: str = dlt.config.value,
    credentials: AwsCredentials = dlt.secrets.value,
):
    ...
    # Convert credentials to s3fs format
    s3fs_credentials = credentials.to_s3fs_credentials()
    print(s3fs_credentials["key"])

    # Get AWS credentials from botocore session
    aws_credentials = credentials.to_native_credentials()
    print(aws_credentials.access_key)
    ...
```
while `secrets.toml` looks as follows:
```toml
[sources.aws_readers.credentials]
aws_access_key_id = "key_id"
aws_secret_access_key = "access_key"
region_name = "region"
```
and `config.toml`:
```toml
[sources.aws_readers]
bucket_url = "bucket_url"
```

#### Defaults

If configuration is not provided, `dlt` uses the default AWS credentials (from `.aws/credentials`) as present on the machine:

- It works by creating an instance of a botocore Session.
- If `profile_name` is specified, the credentials for that profile are used. If not, the default profile is used.

### AzureCredentials

The `AzureCredentials` class is responsible for handling Azure Blob Storage credentials, including account name, account key, Shared Access Signature (SAS) token, and SAS token permissions. It inherits the ability to manage default credentials and extends it with methods for handling partial credentials and converting credentials to a format suitable for interacting with Azure Blob Storage using the adlfs library.

#### Usage
```py
az_credentials = AzureCredentials()
# Set the necessary attributes
az_credentials.azure_storage_account_name = "ACCOUNT_NAME"
az_credentials.azure_storage_account_key = "ACCOUNT_KEY"
```
or more preferred use:
```py
@dlt.source
def azure_readers(
    bucket_url: str = dlt.config.value,
    credentials: AzureCredentials = dlt.secrets.value,
):
    ...
    # Generate a SAS token
    credentials.create_sas_token()
    print(credentials.azure_storage_sas_token)

    # Convert credentials to adlfs format
    adlfs_credentials = credentials.to_adlfs_credentials()
    print(adlfs_credentials["account_name"])

    # to_native_credentials() is not yet implemented
    ...
```
while `secrets.toml` looks as follows:
```toml
[sources.azure_readers.credentials]
azure_storage_account_name = "account_name"
azure_storage_account_key = "account_key"
```
and `config.toml`:
```toml
[sources.azure_readers]
bucket_url = "bucket_url"
```

#### Defaults

If configuration is not provided, `dlt` uses the default credentials using `DefaultAzureCredential`.

## Working with alternatives of credentials (Union types)

If your source/resource allows for many authentication methods, you can support those seamlessly for your user. The user just passes the right credentials, and `dlt` will inject the right type into your decorated function.

Example:

```py
@dlt.source
def zen_source(credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials, str] = dlt.secrets.value, some_option: bool = False):
  # Depending on what the user provides in config, ZenApiKeyCredentials or ZenEmailCredentials will be injected into the `credentials` argument. Both classes implement `auth` so you can always call it.
  credentials.auth() # type: ignore[union-attr]
  return dlt.resource([credentials], name="credentials")

# Pass native value
os.environ["CREDENTIALS"] = "email:mx:pwd"
assert list(zen_source())[0].email == "mx"

# Pass explicit native value
assert list(zen_source("secret:🔑:secret"))[0].api_secret == "secret"
# Pass explicit dict
assert list(zen_source(credentials={"email": "emx", "password": "pass"}))[0].email == "emx"
```

:::info
This applies not only to credentials but to [all specs](advanced.md#write-custom-specs).
:::

:::tip
Check out the [complete example](https://github.com/dlt-hub/dlt/blob/devel/tests/common/configuration/test_spec_union.py), to learn how to create unions of credentials that derive from the common class, so you can handle it seamlessly in your code.
:::
