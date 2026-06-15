---
title: Built-in credentials
description: Configure access to AWS, Azure, Google Cloud and other systems
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, specs]
---

## Overview

`dlt` provides built-in credential (**specs**) for seamless integration with common external systems. These specs can be configured using the methods described in the [overview](setup.md) documentation. For major cloud providers like AWS, Azure, and Google Cloud, `dlt` also calls client-specific code to authenticate users and can automatically retrieve default credentials from the running environment. Additionally, `dlt` understands common string representations for credentials such as connection strings or service json, making it easier to work with different credential formats beyond the typical dictionary representation.

:::tip
Learn about the authentication methods supported by the `dlt` RestAPI Client in detail in the [RESTClient section](../../dlt-ecosystem/verified-sources/rest_api/advanced.md#authentication).
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

:::note Default credentials, handover, and refresh
When you access cloud storage (the `filesystem` destination, `dlt.dataset`, or `delta`/`iceberg`/`lance` tables), `dlt` resolves your credentials and passes them to the underlying data-access library — `fsspec` (s3fs/adlfs/gcsfs), DuckDB, the `object_store` Rust crate (used by `delta` and `lance`), or `pyarrow`/`pyiceberg`. The same rule applies to all of them:

- **Default credentials**, resolved from the environment, are **handed over**: `dlt` lets the library resolve and **refresh** them through its own provider chain. This matters for long-running reads, where a temporary token would otherwise expire mid-read. `dlt` freezes only the cases a given library cannot resolve itself.
- **Static credentials** (keys, SAS tokens, connection strings, service principals) and **external sessions** (a session or credential object you pass explicitly) are **not** handed over to the library's own chain — `dlt` uses exactly the identity you provided, snapshotting it to keys or a bearer token where needed (in-process `fsspec` may instead receive the live credential object).

The **Credential handover** table in each section below shows what each library receives.
:::

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

If configuration values are missing, `dlt` uses **Application Default Credentials** (ADC, via `google.auth.default()`) if available — read more about [Google defaults](https://googleapis.dev/python/google-auth/latest/user-guide.html#application-default-credentials). ADC resolves from the `GOOGLE_APPLICATION_CREDENTIALS` service-account file, `gcloud` user credentials, or the GCE/GKE metadata server, and the resolved tokens are **refreshed** by the library performing the access.

- `dlt` will try to fetch the `project_id` from default credentials. If the project id is missing, it will look for `project_id` in the secrets. So it is normal practice to pass partial credentials (just `project_id`) and take the rest from defaults.

#### Credential handover

On default (ADC) credentials `dlt` hands over to the consumer so it resolves and refreshes via ADC itself; explicit service-account credentials are passed as-is.

| consumer | default credentials (ADC) | explicit service account |
| --- | --- | --- |
| `fsspec` / gcsfs | gcsfs resolves & refreshes via Google ADC | service account credentials |
| DuckDB | no native GCS credential chain — `dlt` reads via `fsspec`/gcsfs (which refreshes), or via HMAC keys on the S3-compatibility layer | HMAC keys (S3-compatibility layer) |
| `object_store` (delta, lance) | handed over (empty options) → the crate resolves & refreshes via ADC. OAuth user credentials are not supported with `delta` | service-account JSON |
| `pyarrow` / pyiceberg | `project-id` only → pyarrow `GcsFileSystem` resolves & refreshes via ADC | service-account JSON / OAuth token |

#### External sessions

Pass a native `google.auth` credentials object (or a serialized `service.json`) as the credentials value to use an exact identity; it is used as-is, for example:

```py
from google.oauth2.service_account import Credentials
from dlt.sources.credentials import GcpServiceAccountCredentials

# a native google credentials object (or a serialized service.json string) is used as-is
native = Credentials.from_service_account_file("path/to/service.json")
gcp_credentials = GcpServiceAccountCredentials()
gcp_credentials.parse_native_representation(native)
```

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

If configuration is not provided, `dlt` resolves AWS credentials from the **botocore default provider chain** as present on the machine or runtime:

- environment variables, `~/.aws/config` and `~/.aws/credentials` (the `default` profile, or `profile_name` if set), SSO, web identity (EKS IRSA), assume-role, and the EC2/ECS instance metadata service.
- `region_name` is taken from the resolved session.

These default credentials are **refreshable**: temporary tokens (for example an ECS task role) are renewed for as long as the connection lives.

#### Credential handover

On default credentials `dlt` hands over so the consumer resolves and **refreshes** the credentials itself; static and external-session credentials are frozen.

| consumer | default credentials | static / external session |
| --- | --- | --- |
| `fsspec` / s3fs | static key/secret/token omitted → s3fs resolves & refreshes via its own aiobotocore chain | frozen key/secret/token |
| DuckDB | `PROVIDER credential_chain` + `REFRESH auto` (re-runs the full AWS chain on token expiry) | frozen `KEY_ID` / `SECRET` / `SESSION_TOKEN` |
| `object_store` (delta, lance) | handed over **only** for self-refreshing creds (EC2 IMDS / ECS); deferred creds (IRSA / SSO / assume-role) are frozen because the crate cannot resolve them | frozen key/secret/token |
| `pyarrow` / pyiceberg | static keys omitted → pyarrow `S3FileSystem` resolves & refreshes via the AWS chain (region/endpoint preserved) | frozen key/secret/token |

#### External sessions

Pass your own `boto3`/`botocore` session to use an exact identity. `dlt` always **freezes** the session's current credentials (it does not refresh them, just like static keys):

```py
import botocore.session
from dlt.sources.credentials import AwsCredentials

# snapshot the credentials of an explicit boto session (e.g. a specific SSO profile)
aws_credentials = AwsCredentials.from_session(botocore.session.get_session())
```

This is the way to force a specific local or dev identity for consumers that would otherwise resolve their own chain.

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

If configuration is not provided, `dlt` uses `DefaultAzureCredential`, which resolves from (in order) an environment service principal, AKS workload identity, managed identity (IMDS), the shared token cache, and the Azure CLI (`az login`), among others. The resulting tokens are **refreshed** by whichever library performs the access.

#### Credential handover

On default credentials `dlt` hands over so the consumer resolves and **refreshes** the credentials itself; static credentials and external sessions are frozen.

| consumer | default credentials | static (account key / SAS / service principal) | external session |
| --- | --- | --- | --- |
| `fsspec` / adlfs | `anon=False` → adlfs resolves & refreshes via its own `DefaultAzureCredential` | account key / SAS / service principal | the live credential object is passed to adlfs (refreshes in-process) |
| DuckDB | `PROVIDER credential_chain` (env / workload / managed identity / `az` CLI, refreshes) | connection string / `PROVIDER service_principal` | frozen bearer token via `PROVIDER access_token` |
| `object_store` (delta, lance) | handed over — the crate resolves & refreshes env service principal / workload identity / managed identity | account key / SAS / service principal | frozen bearer token |
| `pyarrow` / pyiceberg | handed over → adlfs resolves & refreshes via its own `DefaultAzureCredential`, **but only when `AZURE_STORAGE_ANON=false`** is set (pyiceberg passes no `anon` flag, so adlfs is anonymous by default) | account key / SAS / service principal (service principal auto-refreshes) | **not supported — raises** |

#### External sessions

Pass any azure-identity credential to pin an exact identity. `dlt` **freezes** a bearer token from it for the cross-process consumers (object_store, DuckDB) and passes the live credential to adlfs:

```py
from azure.identity import AzureCliCredential
from dlt.sources.credentials import AzureCredentials

# freeze your local `az login` identity (or pass DefaultAzureCredential(), a ManagedIdentityCredential, etc.)
az_credentials = AzureCredentials.from_credential(AzureCliCredential())
az_credentials.azure_storage_account_name = "myaccount"
```

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
