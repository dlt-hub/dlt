---
title: Advanced secrets and configs
description: Learn advanced hacks and tricks about credentials.
keywords: [credentials, secrets.toml, secrets, config, configuration, environment variables, provider]
---

`dlt` provides a lot of flexibility for managing credentials and configuration. In this section, you will learn how to correctly manage credentials in your custom sources and destinations, how the `dlt` injection mechanism works, and how to get access to configurations managed by `dlt`.

## Injection mechanism

`dlt` has a special treatment for functions decorated with `@dlt.source`, `@dlt.resource`, and `@dlt.destination`. When such a function is called, `dlt` takes the argument names in the signature and supplies (injects) the required values by looking for them in [various config providers](setup).

### Injection rules

1. The arguments that are passed explicitly are **never injected**. This makes the injection mechanism optional. For example, for the pipedrive source:
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
  `dlt` allows the user to specify the argument `pipedrive_api_key` explicitly if, for some reason, they do not want to use [out-of-the-box options](setup) for credentials management.

2. Required arguments (without default values) **are never injected** and must be specified when calling. For example, for the source:

  ```py
  @dlt.source
  def slack_data(channels_list: List[str], api_key: str = dlt.secrets.value):
    ...
  ```
  The argument `channels_list` would not be injected and will output an error if it is not specified explicitly.

3. Arguments with default values are injected if present in config providers. Otherwise, defaults from the function signature are used. For example, for the source:

  ```py
  @dlt.source
  def slack_source(
    page_size: int = MAX_PAGE_SIZE,
    access_token: str = dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = DEFAULT_START_DATE
  ):
    ...
  ```
  `dlt` firstly searches for all three arguments: `page_size`, `access_token`, and `start_date` in config providers in a [specific order](setup). If it cannot find them, it will use the default values.

4. Arguments with the special default value `dlt.secrets.value` and `dlt.config.value` **must be injected**
   (or explicitly passed). If they are not found by the config providers, the code raises an
   exception. The code in the functions always receives those arguments.

  Additionally, `dlt.secrets.value` tells `dlt` that the supplied value is a secret, and it will be injected only from secure config providers.

### Add typing to your sources and resources

We highly recommend adding types to your function signatures.
The effort is very low, and it gives `dlt` much more
information on what the source or resource expects.

Doing so provides several benefits:

1. You'll never receive invalid data types in your code.
1. `dlt` will automatically parse and coerce types for you, so you don't need to parse them yourself.
1. `dlt` can generate sample config and secret files for your source automatically.
1. You can request [built-in and custom credentials](complex_types) (i.e., connection strings, AWS / GCP / Azure credentials).
1. You can specify a set of possible types via `Union`, i.e., OAuth or API Key authorization.

Let's consider the example:

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

Now,

1. You are sure that you get a list of strings as `tab_names`.
1. You will get actual Google credentials (see [GCP Credential Configuration](complex_types#gcp-credentials)), and users can
   pass them in many different forms:

   * `service.json` as a string or dictionary (in code and via config providers).
   * connection string (used in SQL Alchemy) (in code and via config providers).
   * if nothing is passed, the default credentials are used (i.e., those present on Cloud Function runner)

## TOML files structure

`dlt` arranges the sections of [TOML files](setup/#secretstoml-and-configtoml) into a **default layout** that is expected by the [injection mechanism](#injection-mechanism).
This layout makes it easy to configure simple cases but also provides room for more explicit sections and complex cases, i.e., having several sources with different credentials
or even hosting several pipelines in the same project sharing the same config and credentials.

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


## Read configs and secrets manually

`dlt` handles credentials and configuration automatically, but also offers flexibility for manual processing.
`dlt.secrets` and `dlt.config` provide dictionary-like access to configuration values and secrets, enabling any custom preprocessing if needed.
Additionally, you can store custom settings within the same configuration files.

```py
# use `dlt.secrets` and `dlt.config` to explicitly take
# those values from providers from the explicit keys
data_source = google_sheets(
    dlt.config["sheet_id"],
    dlt.config["my_section.tabs"],
    dlt.secrets["my_section.gcp_credentials"]
)

data_source.run(destination="bigquery")
```

`dlt.config` and `dlt.secrets` behave like dictionaries from which you can request a value with any key name. `dlt` will look in all [config providers](setup) - environment variables, TOML files, etc. to create these dictionaries. You can also use `dlt.config.get()` or `dlt.secrets.get()` to
request a value and cast it to a desired type. For example:

```py
credentials = dlt.secrets.get("my_section.gcp_credentials", GcpServiceAccountCredentials)
```
Creates a `GcpServiceAccountCredentials` instance out of values (typically a dictionary) under the `my_section.gcp_credentials` key.

## Write configs and secrets in code

`dlt.config` and `dlt.secrets` objects can also be used as setters. For example:
```py
dlt.config["sheet_id"] = "23029402349032049"
dlt.secrets["destination.postgres.credentials"] = BaseHook.get_connection('postgres_dsn').extra
```

This will mock the TOML provider to desired values.

## Example

In the example below, the `google_sheets` source function is used to read selected tabs from Google Sheets.
It takes several arguments that specify the spreadsheet, the tab names, and the Google credentials to be used when extracting data.

```py
@dlt.source
def google_sheets(
    spreadsheet_id=dlt.config.value,
    tab_names=dlt.config.value,
    credentials=dlt.secrets.value,
    only_strings=False
):
    # Allow both a dictionary and a string to be passed as credentials
    if isinstance(credentials, str):
        credentials = json.loads(credentials)
    # Allow both a list and a comma-delimited string to be passed as tabs
    if isinstance(tab_names, str):
      tab_names = tab_names.split(",")
    sheets = build('sheets', 'v4', credentials=ServiceAccountCredentials.from_service_account_info(credentials))
    tabs = []
    for tab_name in tab_names:
        data = get_sheet(sheets, spreadsheet_id, tab_name)
        tabs.append(dlt.resource(data, name=tab_name))
    return tabs
```
The `dlt.source` decorator makes all arguments in the `google_sheets` function signature configurable.
`dlt.secrets.value` and `dlt.config.value` are special argument defaults that tell `dlt` that this
argument is required and must be passed explicitly or must exist in the configuration. Additionally,
`dlt.secrets.value` tells `dlt` that an argument is a secret.

In the example above:
- `spreadsheet_id` is a **required config** argument.
- `tab_names` is a **required config** argument.
- `credentials` is a **required secret** argument (Google Sheets credentials as a dictionary ({"private_key": ...})).
- `only_strings` is an **optional config** argument with a default value. It may be specified when calling the `google_sheets` function or included in the configuration settings.

:::tip
`dlt.resource` behaves in the same way, so if you have a [standalone resource](../resource.md#declare-a-standalone-resource) (one that is not an inner function
of a **source**)
:::

