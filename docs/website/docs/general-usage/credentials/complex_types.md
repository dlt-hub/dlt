---
title: Complex credential types
description: Instructions for credentials like DB connection string.
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, specs]
---

## Overview

Often, credentials do not consist of just one `api_key`, but instead can be quite a complex structure. In this section, you'll learn how `dlt` supports different credential types and authentication options.

:::tip
Learn about the authentication methods supported by the `dlt` RestAPI Client in detail in the [RESTClient section](../http/rest-client.md#authentication).
:::

`dlt` supports different credential types by providing various Python data classes called Configuration Specs. These classes define how complex configuration values, particularly credentials, should be handled. They specify the types, defaults, and parsing methods for these values.

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
query("SELECT * FROM customers", "postgres://loader@localhost:5432/dlt_data")
# or
query("SELECT * FROM customers", {"database": "dlt_data", "username": "loader"})
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
credentials = OAuth2Credentials(
    client_id="CLIENT_ID",
    client_secret="CLIENT_SECRET",  # type: ignore
    refresh_token="REFRESH_TOKEN",  # type: ignore
    scopes=["scope1", "scope2"]
)

# Authorize the client
credentials.auth()

# Add additional scopes
credentials.add_scopes(["scope3", "scope4"])
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
credentials = GcpServiceAccountCredentials()
# Parse a native value (ServiceAccountCredentials)
# Accepts a native value, which can be either an instance of ServiceAccountCredentials
# or a serialized services.json.
# Parses the native value and updates the credentials.
native_value = {"private_key": ".."} # or "path/to/services.json"
credentials.parse_native_representation(native_value)
```
or more preferred use:
```py
import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials

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
credentials = AwsCredentials()
# Set the necessary attributes
credentials.aws_access_key_id = "ACCESS_KEY_ID"
credentials.aws_secret_access_key = "SECRET_ACCESS_KEY"
credentials.region_name = "us-east-1"
```
or
```py
# Imports an external botocore session and sets the credentials properties accordingly.
import botocore.session

credentials = AwsCredentials()
session = botocore.session.get_session()
credentials.parse_native_representation(session)
print(credentials.aws_access_key_id)
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
credentials = AzureCredentials()
# Set the necessary attributes
credentials.azure_storage_account_name = "ACCOUNT_NAME"
credentials.azure_storage_account_key = "ACCOUNT_KEY"
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
  credentials.auth()
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
This applies not only to credentials but to [all specs](#writing-custom-specs).
:::

:::tip
Check out the [complete example](https://github.com/dlt-hub/dlt/blob/devel/tests/common/configuration/test_spec_union.py), to learn how to create unions of credentials that derive from the common class, so you can handle it seamlessly in your code.
:::

## Writing custom specs

**Custom specifications** let you take full control over the function arguments. You can:

- Control which values should be injected, the types, default values.
- Specify optional and final fields.
- Form hierarchical configurations (specs in specs).
- Provide your own handlers for `on_partial` (called before failing on missing config key) or `on_resolved`.
- Provide your own native value parsers.
- Provide your own default credentials logic.
- Utilize Python dataclass functionality.
- Utilize Python `dict` functionality (`specs` instances can be created from dicts and serialized from dicts).

In fact, `dlt` synthesizes a unique spec for each decorated function. For example, in the case of `google_sheets`, the following class is created:

```py
from dlt.sources.config import configspec, with_config

@configspec
class GoogleSheetsConfiguration(BaseConfiguration):
  tab_names: List[str] = None  # mandatory
  credentials: GcpServiceAccountCredentials = None # mandatory secret
  only_strings: Optional[bool] = False
```

### All specs derive from [BaseConfiguration](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/configuration/specs/base_configuration.py#L170)
This class serves as a foundation for creating configuration objects with specific characteristics:

- It provides methods to parse and represent the configuration in native form (`parse_native_representation` and `to_native_representation`).

- It defines methods for accessing and manipulating configuration fields.

- It implements a dictionary-compatible interface on top of the dataclass. This allows instances of this class to be treated like dictionaries.

- It defines helper functions for checking if a certain attribute is present, if a field is valid, and for calling methods in the method resolution order (MRO).

More information about this class can be found in the class docstrings.

### All credentials derive from [CredentialsConfiguration](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/configuration/specs/base_configuration.py#L307)

This class is a subclass of `BaseConfiguration` and is meant to serve as a base class for handling various types of credentials. It defines methods for initializing credentials, converting them to native representations, and generating string representations while ensuring sensitive information is appropriately handled.

More information about this class can be found in the class docstrings.

