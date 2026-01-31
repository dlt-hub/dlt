---
title: Access to configuration in code
description: Access configuration via dlt function arguments or explicitly
keywords: [credentials, secrets.toml, secrets, config, configuration, environment variables, provider]
---


## Access to configuration in dlt decorated functions

`dlt` automatically generates configuration **specs** for functions decorated with `@dlt.source`, `@dlt.resource`, and `@dlt.destination`, without additional code needed. You can configure these functions using any of the [standard configuration methods](setup.md) including environment variables and TOML files. You can call them like regular Python functions - dlt injects configuration values for any argument you don't explicitly provide.

### Injection rules

1. Arguments passed explicitly are **never injected**. This makes the injection mechanism optional. Example with the Pipedrive source:
  ```py
  @dlt.source(name="pipedrive")
  def pipedrive_source(
      pipedrive_api_key: str = dlt.secrets.value,
      since_timestamp: Optional[Union[pendulum.DateTime, str]] = "1970-01-01 00:00:00",
  ) -> Iterator[DltResource]:
    ...

  my_key = os.environ["MY_PIPEDRIVE_KEY"]
  my_source = pipedrive_source(pipedrive_api_key=my_key)
  ```
  You can specify `pipedrive_api_key` explicitly if you prefer not to use the [standard options](setup) for credential handling.

2. Required arguments (without default values) **are never injected** and must be specified explicitly when calling. Example:

  ```py
  @dlt.source
  def slack_data(channels_list: List[str], api_key: str = dlt.secrets.value):
    ...
  ```
  The `channels_list` argument won't be injected and will produce an error if not specified explicitly.

3. Arguments with default values are injected if found in config providers. Otherwise, the default values from the function signature are used. Example:

  ```py
  @dlt.source
  def slack_source(
    page_size: int = MAX_PAGE_SIZE,
    access_token: str = dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = START_DATE
  ):
    ...
  ```
  `dlt` first searches for `page_size`, `access_token`, and `start_date` in config providers in a [specific order](setup). If these values aren't found, it falls back to the default values.

4. Arguments with special defaults `dlt.secrets.value` and `dlt.config.value` **must be injected** (or explicitly passed). If not found in config providers, `dlt` raises an exception.

  Additionally, `dlt.secrets.value` indicates to `dlt` that the value is a secret, meaning it will only be injected from secure config providers.

### Add typing to your sources and resources

We recommend adding type annotations to your function signatures. This requires minimal effort and provides several important benefits:

1. You won't receive invalid data types in your code.
2. `dlt` automatically parses and converts types for you, eliminating the need for manual parsing.
3. `dlt` can generate sample config and secret files for your source automatically.
4. You can request [built-in and custom credentials](complex_types) (connection strings, AWS/GCP/Azure credentials).
5. You can specify multiple possible types via `Union`, such as OAuth or API Key authorization.

Example:

```py
@dlt.source
def google_sheets(
    spreadsheet_id: str = dlt.config.value,
    tab_names: List[str] = dlt.config.value,
    credentials: GcpServiceAccountCredentials = dlt.secrets.value,
    only_strings: bool = False
):
    ...
```

Benefits:
1. You'll receive a properly typed list of strings as `tab_names`.
2. You'll receive properly configured Google credentials (see [GCP Credential Configuration](complex_types#gcp-credentials)), which users can provide in different forms:
   * `service.json` as a string or dictionary (in code or via config providers)
   * Connection string (used in SQL Alchemy)
   * Default credentials if nothing is passed (such as those available on Cloud Function runners)

## Organize configuration and secrets with sections

`dlt` organizes configuration and secrets sections in a **configuration layout** that integrates with the [injection mechanism](#injection-rules). This structure applies to all [configuration providers](setup), including TOML files, environment variables, and other sources.

This hierarchical structure efficiently handles simple cases while supporting more complex scenarios, such as multiple sources with different credentials or multiple pipelines sharing configuration in the same project.

```text
pipeline_name
    |
    |-sources
        |-<source 1 module name>
            |-<source function 1 name>
                |- {all source and resource options and secrets}
            |-<source function 2 name>
                |- {all source and resource options and secrets}
        |-<source 2 module>
            |...

        |-extract
            |- extract options for resources i.e., parallelism settings, maybe retries
    |-destination
        |- <destination name>
            |- {destination options}
                |-credentials
                    |-{credentials options}
    |-schema
        |-<schema name>
            |-schema settings: not implemented but I'll let people set nesting level, name convention, normalizer, etc. here
    |-load
    |-normalize
```

When using TOML files, this structure is represented as nested sections with dotted keys. For environment variables and other config providers, the layout is flattened using double underscores (e.g., `PIPELINE_NAME__SOURCES__MODULE_NAME__FUNCTION_NAME__OPTION`).

## Access configs and secrets in code

While `dlt` handles credentials automatically, you can also access them directly in your code. The `dlt.secrets` and `dlt.config` objects provide dictionary-like access to configuration values and secrets, enabling custom preprocessing if required. You can also store custom settings in the same configuration files.

```py
# Use `dlt.secrets` and `dlt.config` to explicitly retrieve values from providers
source_instance = google_sheets(
    dlt.config["sheet_id"],
    dlt.config["my_section.tabs"],
    dlt.secrets["my_section.gcp_credentials"]
)

source_instance.run(destination="bigquery")
```

`dlt.config` and `dlt.secrets` function as dictionaries. `dlt` examines all [config providers](setup) - environment variables, TOML files, etc. - to populate these dictionaries. You can also use `dlt.config.get()` or `dlt.secrets.get()` to retrieve a value and convert it to a specific type:

```py
credentials = dlt.secrets.get("my_section.gcp_credentials", GcpServiceAccountCredentials)
```
This creates a `GcpServiceAccountCredentials` instance from the values stored under the `my_section.gcp_credentials` key.

## Write configs and secrets in code

You can also set values programmatically using `dlt.config` and `dlt.secrets`:
```py
dlt.config["sheet_id"] = "23029402349032049"
dlt.secrets["destination.postgres.credentials"] = BaseHook.get_connection('postgres_dsn').extra
```

This effectively mocks the TOML provider with your specified values.

## Configure destination credentials in code

You can programmatically set destination credentials when needed. This example demonstrates how to use [GcpServiceAccountCredentials](complex_types#gcp-credentials) **spec** with a BigQuery destination:

```py
import os

import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials
from dlt.destinations import bigquery

# Retrieve credentials from environment variable
creds_dict = os.getenv('BIGQUERY_CREDENTIALS')

# Create and initialize credentials instance
gcp_credentials = GcpServiceAccountCredentials()
gcp_credentials.parse_native_representation(creds_dict)

# Pass credentials to the BigQuery destination
pipeline = dlt.pipeline(destination=bigquery(credentials=gcp_credentials))
pipeline.run([{"key1": "value1"}], table_name="temp")
```

### Google Sheets source example

This example demonstrates a `google_sheets` source function that reads selected tabs from Google Sheets:

```py
@dlt.source
def google_sheets(
    spreadsheet_id=dlt.config.value,
    tab_names=dlt.config.value,
    credentials=dlt.secrets.value,
    only_strings=False
):
    # Handle credentials as either dictionary or string
    if isinstance(credentials, str):
        credentials = json.loads(credentials)
    # Handle tabs as either list or comma-separated string
    if isinstance(tab_names, str):
      tab_names = tab_names.split(",")
    sheets = build('sheets', 'v4', credentials=ServiceAccountCredentials.from_service_account_info(credentials))
    tabs = []
    for tab_name in tab_names:
        data = _get_sheet(sheets, spreadsheet_id, tab_name)
        tabs.append(dlt.resource(data, name=tab_name))
    return tabs
```

The `@dlt.source` decorator makes all arguments in the function configurable. The special defaults `dlt.secrets.value` and `dlt.config.value` indicate to `dlt` that these arguments are required and must either be passed explicitly or exist in the configuration. Additionally, `dlt.secrets.value` designates an argument as a secret.

In this example:
- `spreadsheet_id` is a **required config** argument
- `tab_names` is a **required config** argument
- `credentials` is a **required secret** argument (Google Sheets credentials as a dictionary)
- `only_strings` is an **optional config** argument with a default value

:::tip
`dlt.resource` functions in the same way, so [standalone resources](../resource.md#declare-a-standalone-resource) (not defined as inner functions of a **source**) follow the same injection rules
:::

## Write custom specs

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
from dlt.common.configuration import configspec, with_config

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

