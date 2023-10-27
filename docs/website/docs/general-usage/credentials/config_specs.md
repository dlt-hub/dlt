---
title: Configuration Specs
description: Overview configuration specs and how to create custom specs
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, specs]
---

# Configuration Specs

Configuration Specs in `dlt` are Python `dataclasses` that define how complex configuration values,
particularly credentials, should be handled.
They specify the types, defaults, and parsing methods for these values.

## Working with credentials (and other complex configuration values)

For example, a spec like `GcpServiceAccountCredentials` manages Google Cloud Platform
service account credentials, while `ConnectionStringCredentials` handles database connection strings.

### Example

```python
import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials

@dlt.source
def google_sheets(
    spreadsheet_id: str,
    tab_names: List[str] = dlt.config.value,
    credentials: GcpServiceAccountCredentials = dlt.secrets.value,
    only_strings: bool = False
):
  ...
```

Here, `GcpServiceAccountCredentials` is an example of a **spec**: a Python `dataclass` that describes
the configuration fields, their types and default values. It also allows parsing various native
representations of the configuration. Credentials marked with `WithDefaults` mixin are also to
instantiate itself from the machine/user default environment i.e. Googles `default()` or AWS
`.aws/credentials`.

As an example, let's use `ConnectionStringCredentials` which represents a database connection
string.

```python
from dlt.sources.credentials import ConnectionStringCredentials

@dlt.source
def query(sql: str, dsn: ConnectionStringCredentials = dlt.secrets.value):
  ...
```

The source above executes the `sql` against database defined in `dsn`. `ConnectionStringCredentials`
makes sure you get the correct values with correct types and understands the relevant native form of
the credentials.

Example 1. Use the **dictionary** form.

```toml
[dsn]
database="dlt_data"
password="loader"
username="loader"
host="localhost"
```

Example 2. Use the **native** form.

```toml
dsn="postgres://loader:loader@localhost:5432/dlt_data"
```

Example 3. Use the **mixed** form: the password is missing in explicit dsn and will be taken from the
`secrets.toml`.

```toml
dsn.password="loader
```

You can explicitly provide credentials in various forms:

```python
query("SELECT * FROM customers", "postgres://loader@localhost:5432/dlt_data")
# or
query("SELECT * FROM customers", {"database": "dlt_data", "username": "loader"...})
```

## Built in credentials

We have some ready-made credentials you can reuse:

```python
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.credentials import OAuth2Credentials
from dlt.sources.credentials import GcpServiceAccountCredentials, GcpOAuthCredentials
from dlt.sources.credentials import AwsCredentials
from dlt.sources.credentials import AzureCredentials
```

### ConnectionStringCredentials

The ConnectionStringCredentials class handles connection string
credentials for SQL database connections.
It includes attributes for the driver name, database name, username, password, host, port,
and additional query parameters.
This class provides methods for parsing and generating connection strings.

Usage:
```python
credentials = ConnectionStringCredentials()

# Set the necessary attributes
credentials.drivername = "postgresql"
credentials.database = "my_database"
credentials.username = "my_user"
credentials.password = "my_password"
credentials.host = "localhost"
credentials.port = 5432

# Convert credentials to connection string
connection_string = credentials.to_native_representation()

# Parse a connection string and update credentials
native_value = "postgresql://my_user:my_password@localhost:5432/my_database"
credentials.parse_native_representation(native_value)

# Get a URL representation of the connection
url_representation = credentials.to_url()
```

### OAuth2Credentials

The OAuth2Credentials class handles OAuth 2.0 credentials, including client ID,
client secret, refresh token, and access token.
It also allows for the addition of scopes and provides methods for client authentication.

Usage:
```python
credentials = OAuth2Credentials(
    client_id="CLIENT_ID",
    client_secret="CLIENT_SECRET",
    refresh_token="REFRESH_TOKEN",
    scopes=["scope1", "scope2"]
)

# Authorize the client
credentials.auth()

# Add additional scopes
credentials.add_scopes(["scope3", "scope4"])
```

### GCP Credentials

#### GcpServiceAccountCredentials

The GcpServiceAccountCredentials class manages GCP Service Account credentials.
It can parse native values either as ServiceAccountCredentials or as serialized `services.json`.
This class provides methods to retrieve native credentials for Google clients.

Usage:
```python
credentials = GcpServiceAccountCredentials()

# Parse a native value (ServiceAccountCredentials)
# Accepts a native value, which can be either an instance of ServiceAccountCredentials
# or a serialized services.json.
# Parses the native value and updates the credentials.
native_value = ...
credentials.parse_native_representation(native_value)

# Retrieve native credentials for Google clients
# For example, build the service object for Google Analytics PI.
client = BetaAnalyticsDataClient(credentials=credentials.to_native_credentials())

# Get a string representation of the credentials
# Returns a string representation of the credentials in the format client_email@project_id.
credentials_str = str(credentials)
```

#### GcpOAuthCredentials

The GcpOAuthCredentials class is responsible for handling OAuth2 credentials for
desktop applications in Google Cloud Platform (GCP).
It can parse native values either as GoogleOAuth2Credentials or as
serialized OAuth client secrets JSON.
This class provides methods for authentication and obtaining access tokens.

Usage:
```python
oauth_credentials = GcpOAuthCredentials()

# Accepts a native value, which can be either an instance of GoogleOAuth2Credentials
# or serialized OAuth client secrets JSON.
# Parses the native value and updates the credentials.
native_value_oauth = ...
oauth_credentials.parse_native_representation(native_value_oauth)

# Authenticate and get access token
oauth_credentials.auth(scopes=["scope1", "scope2"])

# Retrieve native credentials for Google clients
# For example, build the service object for Google Analytics PI.
client = BetaAnalyticsDataClient(credentials=oauth_credentials.to_native_credentials())

# Get a string representation of the credentials
# Returns a string representation of the credentials in the format client_id@project_id.
credentials_str = str(oauth_credentials)
```

[The example](https://github.com/dlt-hub/verified-sources/blob/master/sources/google_analytics/setup_script_gcp_oauth.py): how you can get the refresh token using `dlt.secrets.value`.

### AwsCredentials

The AwsCredentials class is responsible for handling AWS credentials,
including access keys, session tokens, profile names, region names, and endpoint URLs.
It inherits the ability to manage default credentials and extends it with methods
for handling partial credentials and converting credentials to a botocore session.

Usage:
```python
credentials = AwsCredentials()

# Set the necessary attributes
credentials.aws_access_key_id = "ACCESS_KEY_ID"
credentials.aws_secret_access_key = "SECRET_ACCESS_KEY"
credentials.region_name = "us-east-1"

# Convert credentials to s3fs format
s3fs_credentials = credentials.to_s3fs_credentials()
print(s3fs_credentials["key"])

# Imports an external boto3 session and sets the credentials properties accordingly.
import botocore.session
session = botocore.session.get_session()
credentials.parse_native_representation(session)
print(credentials.aws_access_key_id)

# Get AWS credentials from botocore session
aws_credentials = credentials.to_native_credentials()
print(aws_credentials.access_key)
```

### AzureCredentials

The AzureCredentials class is responsible for handling Azure Blob Storage credentials,
including account name, account key, Shared Access Signature (SAS) token, and SAS token permissions.
It inherits the ability to manage default credentials and extends it with methods for
handling partial credentials and converting credentials to a format suitable
for interacting with Azure Blob Storage using the adlfs library.

Usage:
```python
credentials = AzureCredentials()

# Set the necessary attributes
credentials.azure_storage_account_name = "ACCOUNT_NAME"
credentials.azure_storage_account_key = "ACCOUNT_KEY"

# Generate a SAS token
credentials.create_sas_token()
print(credentials.azure_storage_sas_token)

# Convert credentials to adlfs format
adlfs_credentials = credentials.to_adlfs_credentials()
print(adlfs_credentials["account_name"])
```

## Working with alternatives of credentials (Union types)

If your source/resource allows for many authentication methods, you can support those seamlessly for
your user. The user just passes the right credentials and `dlt` will inject the right type into your
decorated function.

Example:

```python
@dlt.source
def zen_source(credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials, str] = dlt.secrets.value, some_option: bool = False):
  # depending on what the user provides in config, ZenApiKeyCredentials or ZenEmailCredentials will be injected in `credentials` argument
  # both classes implement `auth` so you can always call it
  credentials.auth()
  return dlt.resource([credentials], name="credentials")

# pass native value
os.environ["CREDENTIALS"] = "email:mx:pwd"
assert list(zen_source())[0].email == "mx"

# pass explicit native value
assert list(zen_source("secret:🔑:secret"))[0].api_secret == "secret"

# pass explicit dict
assert list(zen_source(credentials={"email": "emx", "password": "pass"}))[0].email == "emx"

```

> This applies not only to credentials but to all specs (see next chapter).

Read the [whole test](https://github.com/dlt-hub/dlt/blob/devel/tests/common/configuration/test_spec_union.py), it shows how to create unions
of credentials that derive from the common class, so you can handle it seamlessly in your code.

## Writing custom specs

**specs** let you take full control over the function arguments:

- Which values should be injected, the types, default values.
- You can specify optional and final fields.
- Form hierarchical configurations (specs in specs).
- Provide own handlers for `on_partial` (called before failing on missing config key) or `on_resolved`.
- Provide own native value parsers.
- Provide own default credentials logic.
- Adds all Python dataclass goodies to it.
- Adds all Python `dict` goodies to it (`specs` instances can be created from dicts and serialized
  from dicts).

This is used a lot in the `dlt` core and may become useful for complicated sources.

In fact, for each decorated function a spec is synthesized. In case of `google_sheets` following
class is created:

```python
from dlt.sources.config import configspec, with_config

@configspec
class GoogleSheetsConfiguration(BaseConfiguration):
  tab_names: List[str] = None  # manadatory
  credentials: GcpServiceAccountCredentials = None # mandatory secret
  only_strings: Optional[bool] = False
```

- All specs derive from [BaseConfiguration.](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/configuration/specs/base_configuration.py#L170)

- All credentials derive from [CredentialsConfiguration.](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/configuration/specs/base_configuration.py#L307)

> Read the docstrings in the code above.
