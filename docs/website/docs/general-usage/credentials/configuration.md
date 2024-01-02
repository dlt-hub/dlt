---
title: Secrets and Configs
description: What are secrets and configs and how sources and destinations read them.
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables]
---

# Secrets and Configs

Use secret and config values to pass access credentials and configure or fine-tune your pipelines without the need to modify your code.
When done right you'll be able to run the same pipeline script during development and in production.

**Configs**:

  - Configs refer to non-sensitive configuration data. These are settings, parameters, or options that define the behavior of a data pipeline.
  - They can include things like file paths, database hosts and timeouts, API urls, performance settings, or any other settings that affect the pipeline's behavior.

**Secrets**:

  - Secrets are sensitive information that should be kept confidential, such as passwords, API keys, private keys, and other confidential data.
  - It's crucial to never hard-code secrets directly into the code, as it can pose a security risk.

## Configure dlt sources and resources

In the example below, the `google_sheets` source function is used to read selected tabs from Google Sheets.
It takes several arguments that specify the spreadsheet, the tab names and the Google credentials to be used when extracting data.

```python
@dlt.source
def google_sheets(
    spreadsheet_id=dlt.config.value,
    tab_names=dlt.config.value,
    credentials=dlt.secrets.value,
    only_strings=False
):
    # Allow both dictionary and a string passed as credentials
    if isinstance(credentials, str):
        credentials = json.loads(credentials)
    # Allow both list and comma delimited string to be passed as tabs
    if isinstance(tab_names, str):
      tab_names = tab_names.split(",")
    sheets = build('sheets', 'v4', credentials=ServiceAccountCredentials.from_service_account_info(credentials))
    tabs = []
    for tab_name in tab_names:
        data = get_sheet(sheets, spreadsheet_id, tab_name)
        tabs.append(dlt.resource(data, name=tab_name))
    return tabs
```
`dlt.source` decorator makes all arguments in `google_sheets` function signature configurable.
`dlt.secrets.value` and `dlt.config.value` are special argument defaults that tell `dlt` that this
argument is required and must be passed explicitly or must exist in the configuration. Additionally
`dlt.secrets.value` tells `dlt` that an argument is a secret.

In the example above:
- `spreadsheet_id`: is a **required config** argument.
- `tab_names`: is a **required config** argument.
- `credentials`: is a **required secret** argument (Google Sheets credentials as a dictionary ({"private_key": ...})).
- `only_strings`: is an **optional config** argument with a default value. It may be specified when calling the `google_sheets` function or included in the configuration settings.

:::tip
`dlt.resource` behaves in the same way so if you have a [standalone resource](../resource.md#declare-a-standalone-resource) (one that is not an inner function
of a **source**)
:::

### Allow `dlt` to pass the config and secrets automatically
You are free to call the function above as usual and pass all the arguments in the code. You'll hardcode google credentials and [we do not recommend that](#do-not-pass-hardcoded-secrets).

Instead let `dlt` to do the work and leave it to [injection mechanism](#injection-mechanism) that looks for function arguments in the config files or environment variables and adds them to your explicit arguments during a function call. Below are two most typical examples:

1. Pass spreadsheet id and tab names in the code, inject credentials from the secrets:
    ```python
    data_source = google_sheets("23029402349032049", ["tab1", "tab2"])
    ```
    `credentials` value will be injected by the `@source` decorator (e.g. from `secrets.toml`).
    `spreadsheet_id` and `tab_names` take values from the call arguments.

2. Inject all the arguments from config / secrets
    ```python
    data_source = google_sheets()
    ```
    `credentials` value will be injected by the `@source` decorator (e.g. from **secrets.toml**).

    `spreadsheet_id` and `tab_names` will be also injected by the `@source` decorator (e.g. from **config.toml**).


Where do the configs and secrets come from? By default, `dlt` looks in two **config providers**:

- [TOML files](config_providers#toml-provider):

  Configs are kept in **.dlt/config.toml**. `dlt` will match argument names with
  entries in the file and inject the values:

  ```toml
  spreadsheet_id="1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580"
  tab_names=["tab1", "tab2"]
  ```
  Secrets in **.dlt/secrets.toml**. `dlt` will look for `credentials`,
  ```toml
  [credentials]
  client_email = <client_email from services.json>
  private_key = <private_key from services.json>
  project_id = <project_id from services json>
  ```
  Note that **credentials** will be evaluated as dictionary containing **client_email**, **private_key** and **project_id** as keys. It is standard TOML behavior.
- [Environment Variables](config_providers#environment-provider):
  ```python
  CREDENTIALS=<service.json>
  SPREADSHEET_ID=1HhWHjqouQnnCIZAFa2rL6vT91YRN8aIhts22SUUR580
  TAB_NAMES=tab1,tab2
  ```
  We pass the JSON contents of `service.json` file to `CREDENTIALS` and we specify tab names as comma-delimited values.  Environment variables are always in **upper case**.

:::tip
There are many ways you can organize your configs and secrets. The example above is the simplest default **layout** that `dlt` supports. In more complicated cases (i.e. a single configuration is shared by many pipelines with different sources and destinations) you may use more [explicit layouts](#secret-and-config-values-layout-and-name-lookup).
:::

:::caution
**[TOML provider](config_providers#toml-provider) always loads `secrets.toml` and `config.toml` files from `.dlt` folder** which is looked relative to the
**current [Working Directory](https://en.wikipedia.org/wiki/Working_directory)**. TOML provider also has the capability to read files from `~/.dlt/`
(located in the user's [Home Directory](https://en.wikipedia.org/wiki/Home_directory)).
:::

### Do not hardcode secrets
You should never do that. Sooner or later your private key will leak.

```python
# WRONG!:
# provide all values directly - wrong but possible.
# secret values should never be present in the code!
data_source = google_sheets(
    "23029402349032049",
    ["tab1", "tab2"],
    credentials={"private_key": ""}
)
```

### Pass secrets in code from external providers
You can get the secret values from your own providers. Below we take **credentials** for our `google_sheets` source from Airflow base hook:

```python
from airflow.hooks.base_hook import BaseHook

# get it from airflow connections or other credential store
credentials = BaseHook.get_connection('gcp_credentials').extra
data_source = google_sheets(credentials=credentials)
```

## Configure a destination
We provide detailed guides for [built-in destinations] and [explain how to configure them in code](../destination.md#configure-a-destination) (including credentials)


## Add typing to your sources and resources

We highly recommend adding types to your function signatures.
The effort is very low, and it gives `dlt` much more
information on what source/resource expects.

Doing so provides several benefits:

1. You'll never receive invalid data types in your code.
1. `dlt` will automatically parse and coerce types for you. In our example, you do not need to parse list of tabs or credentials dictionary yourself.
1. We can generate nice sample config and secret files for your source.
1. You can request [built-in and custom credentials](config_specs.md) (i.e. connection strings, AWS / GCP / Azure credentials).
1. You can specify a set of possible types via `Union` i.e. OAuth or API Key authorization.

```python
@dlt.source
def google_sheets(
    spreadsheet_id: str = dlt.config.value,
    tab_names: List[str] = dlt.config.value,
    credentials: GcpServiceAccountCredentials = dlt.secrets.value,
    only_strings: bool = False
):
  ...
```

Now:

1. You are sure that you get a list of strings as `tab_names`.
1. You will get actual Google credentials (see [GCP Credential Configuration](config_specs#gcp-credentials)), and your users can
   pass them in many different forms.

In case of `GcpServiceAccountCredentials`:

- You may just pass the `service.json` as string or dictionary (in code and via config providers).
- You may pass a connection string (used in SQL Alchemy) (in code and via config providers).
- If you do not pass any credentials, the default credentials are used (i.e. those present on Cloud Function runner)

## Read configs and secrets yourself
`dlt.secrets` and `dlt.config` provide dictionary-like access to configuration values and secrets, respectively.

```python
# use `dlt.secrets` and `dlt.config` to explicitly take
# those values from providers from the explicit keys
data_source = google_sheets(
    dlt.config["sheet_id"],
    dlt.config["my_section.tabs"],
    dlt.secrets["my_section.gcp_credentials"]
)

data_source.run(destination="bigquery")
```
`dlt.config` and `dlt.secrets` behave like dictionaries from which you can request a value with any key name. `dlt` will look in all [config providers](#injection-mechanism) - TOML files, env variables etc. just like it does with the standard section layout. You can also use `dlt.config.get()` or `dlt.secrets.get()` to
request value cast to a desired type. For example:
```python
credentials = dlt.secrets.get("my_section.gcp_credentials", GcpServiceAccountCredentials)
```
Creates `GcpServiceAccountCredentials` instance out of values (typically a dictionary) under **my_section.gcp_credentials** key.

### Write configs and secrets in code
**dlt.config** and **dlt.secrets** can be also used as setters. For example:
```python
dlt.config["sheet_id"] = "23029402349032049"
dlt.secrets["destination.postgres.credentials"] = BaseHook.get_connection('postgres_dsn').extra
```
Will mock the **toml** provider to desired values.


## Injection mechanism

Config and secret values are added to the function arguments when a function decorated with `@dlt.source` or `@dlt.resource` is called.

The signature of such function (i.e. `google_sheets` above) is **also a specification of the configuration**.
During runtime `dlt` takes the argument names in the signature and supplies (`inject`) the required values via various config providers.

The injection rules are:

1. If you call the decorated function, the arguments that are passed explicitly are **never injected**,
   this makes the injection mechanism optional.

1. Required arguments (without default values) **are never injected** and must be specified when calling.

1. Arguments with default values are injected if present in config providers, otherwise defaults from function signature is used.

1. Arguments with the special default value `dlt.secrets.value` and `dlt.config.value` **must be injected**
   (or explicitly passed). If they are not found by the config providers, the code raises
   exception. The code in the functions always receives those arguments.

Additionally `dlt.secrets.value` tells `dlt` that supplied value is a secret, and it will be injected
only from secure config providers.

## Secret and config values layout and name lookup

`dlt` uses a layout of hierarchical sections to organize the config and secret values. This makes
configurations and secrets easy to manage, and disambiguate values with the same keys by placing
them in the different sections.

:::note
If you know how TOML files are organized -> this is the same concept!
:::

A lot of config values are dictionaries themselves (i.e. most of the credentials) and you want the
values corresponding to one component to be close together.

You can have a separate credentials for your destinations and each of the sources your pipeline uses,
if you have many pipelines in a single project, you can group them in separate sections.

Here is the simplest default layout for our `google_sheets` example.

### Default layout without sections

**secrets.toml**

```toml
[credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

**config.toml**

```toml
tab_names=["tab1", "tab2"]
```

As you can see the details of GCP credentials are placed under `credentials` which is argument name
to source function.

### Default layout with explicit sections
This makes sure that `google_sheets` source does not share any secrets and configs with any other source or destination.

**secrets.toml**

```toml
[sources.google_sheets.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

**config.toml**

```toml
[sources.google_sheets]
tab_names=["tab1", "tab2"]
```

### Custom layout

Use this if you want to read and pass the config/secrets yourself

**secrets.toml**

```toml
[my_section]

    [my_section.gcp_credentials]
    client_email = <client_email from services.json>
    private_key = <private_key from services.json>
```

**config.toml**

```toml
[my_section]
tabs=["tab1", "tab2"]

    [my_section.gcp_credentials]
    # I prefer to keep my project id in config file and private key in secrets
    project_id = <project_id from services json>
```

### Default layout and default key lookup during injection

`dlt` arranges the sections into **default layout** that is expected by injection mechanism. This layout
makes it easy to configure simple cases but also provides a room for more explicit sections and
complex cases i.e. having several sources with different credentials or even hosting several pipelines
in the same project sharing the same config and credentials.

```
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
            |- extract options for resources ie. parallelism settings, maybe retries
    |-destination
        |- <destination name>
            |- {destination options}
                |-credentials
                    |-{credentials options}
    |-schema
        |-<schema name>
            |-schema settings: not implemented but I'll let people set nesting level, name convention, normalizer etc. here
    |-load
    |-normalize
```

Lookup rules:

**Rule 1:** The lookup starts with the most specific possible path, and if value is not found there,
it removes the right-most section and tries again.

Example: We use the `bigquery` destination and the `google_sheets` source. They both use google credentials and expect them to be configured under `credentials` key.

1. If we create just a single `credentials` section like in [here](#default-layout-without-sections), destination and source will share the same credentials.

2. If we define sections as below, we'll keep the credentials separate

```toml
# google sheet credentials
[sources.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>

# bigquery credentials
[destination.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

Now when `dlt` looks for destination credentials, it will start with `destination.bigquery.credentials`, eliminate `bigquery` and stop at `destination.credentials`.

When looking for `sources` credentials it will start with `sources.google_sheets.google_sheets.credentials`, eliminate `google_sheets` twice and stop at `sources.credentials` (we assume that `google_sheets` source was defined in `google_sheets` python module)

Example: let's be even more explicit and use a full section path possible.

```toml
# google sheet credentials
[sources.google_sheets.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>

# google analytics credentials
[sources.google_analytics.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>

# bigquery credentials
[destination.bigquery.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

Now we can separate credentials for different sources as well.

**Rule 2:** You can use your pipeline name to have separate configurations for each pipeline in your
project.

Pipeline created/obtained with `dlt.pipeline()` creates a global and optional namespace with the
value of `pipeline_name`. All config values will be looked with pipeline name first and then again
without it.

Example: the pipeline is named `ML_sheets`.

```toml
[ML_sheets.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

or maximum path:

```toml
[ML_sheets.sources.google_sheets.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

### The `sources` section

Config and secrets for decorated sources and resources are kept in
`sources.<source module name>.<function_name>` section. **All sections are optional during lookup**. For example,
if source module is named `pipedrive` and the function decorated with `@dlt.source` is
`deals(api_key: str=...)` then `dlt` will look for API key in:

1. `sources.pipedrive.deals.api_key`
1. `sources.pipedrive.api_key`
1. `sources.api_key`
1. `api_key`

Step 2 in a search path allows all the sources/resources in a module to share the same set of
credentials.

Also look at the [following test](https://github.com/dlt-hub/dlt/blob/devel/tests/extract/test_decorators.py#L303) `test_source_sections`.

## Understanding the exceptions

Now we can finally understand the `ConfigFieldMissingException`.

Let's run `chess.py` example without providing the password:

```
$ CREDENTIALS="postgres://loader@localhost:5432/dlt_data" python chess.py
...
dlt.common.configuration.exceptions.ConfigFieldMissingException: Following fields are missing: ['password'] in configuration with spec PostgresCredentials
        for field "password" config providers and keys were tried in following order:
                In Environment Variables key CHESS_GAMES__DESTINATION__POSTGRES__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key CHESS_GAMES__DESTINATION__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key CHESS_GAMES__CREDENTIALS__PASSWORD was not found.
                In secrets.toml key chess_games.destination.postgres.credentials.password was not found.
                In secrets.toml key chess_games.destination.credentials.password was not found.
                In secrets.toml key chess_games.credentials.password was not found.
                In Environment Variables key DESTINATION__POSTGRES__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key DESTINATION__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key CREDENTIALS__PASSWORD was not found.
                In secrets.toml key destination.postgres.credentials.password was not found.
                In secrets.toml key destination.credentials.password was not found.
                In secrets.toml key credentials.password was not found.
Please refer to https://dlthub.com/docs/general-usage/credentials for more information
```

It tells you exactly which paths `dlt` looked at, via which config providers and in which order.

In the example above:

1. First it looked in a big section `chess_games` which is name of the pipeline.
1. In each case it starts with full paths and goes to minimum path `credentials.password`.
1. First it looks into `environ` then in `secrets.toml`. It displays the exact keys tried.
1. Note that `config.toml` was skipped! It may not contain any secrets.

Read more about [Provider Hierarchy](./config_providers).