---
title: Secrets and Configs
description: Overview secrets and configs
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables]
---

# Secrets and Configs

Secrets and configs are two types of sensitive and non-sensitive information used in a data pipeline:

1. **Configs**:
  - Configs refer to non-sensitive configuration data. These are settings, parameters, or options that define the behavior of a data pipeline.
  - They can include things like file paths, database connection strings, API endpoints, or any other settings that affect the pipeline's behavior.
2. **Secrets**:
  - Secrets are sensitive information that should be kept confidential, such as passwords, API keys, private keys, and other confidential data.
  - It's crucial to never hard-code secrets directly into the code, as it can pose a security risk. Instead, they should be stored securely and accessed via a secure mechanism.


**Design Principles**:

1. Adding configuration and secrets to [sources](../source) and [resources](../resource) should be no-effort.
2. You can reconfigure the pipeline for production after it is deployed. Deployed and local code should
   be identical.
3. You can always pass configuration values explicitly and override any default behavior (i.e. naming of the configuration keys).

We invite you to learn how `dlt` helps you adhere to
these principles and easily operate secrets and
configurations using `dlt.secrets.value` and `dlt.config.value` instances.

## General Usage and an Example

In the example below, the `google_sheets` source function is used to read selected tabs from Google Sheets.
It takes several arguments, including `spreadsheet_id`, `tab_names`, and `credentials`.

```python
@dlt.source
def google_sheets(
    spreadsheet_id=dlt.config.value,
    tab_names=dlt.config.value,
    credentials=dlt.secrets.value,
    only_strings=False
):
    sheets = build('sheets', 'v4', credentials=Services.from_json(credentials))
    tabs = []
    for tab_name in tab_names:
        data = sheets.get(spreadsheet_id, tab_name).execute().values()
        tabs.append(dlt.resource(data, name=tab_name))
    return tabs
```

- `spreadsheet_id`: The unique identifier of the Google Sheets document.
- `tab_names`: A list of tab names to read from the spreadsheet.
- `credentials`: Google Sheets credentials as a dictionary ({"private_key": ...}).
- `only_strings`: Flag to specify if only string data should be retrieved.

`spreadsheet_id` and `tab_names` are configuration values that can be provided directly
when calling the function. `credentials` is a sensitive piece of information.

`dlt.secrets.value` and `dlt.config.value` are instances of classes that provide
dictionary-like access to configuration values and secrets, respectively.
These objects allow for convenient retrieval and modification of configuration
values and secrets used by the application.

Below, we will demonstrate the correct and wrong approaches to providing these values.

### Wrong approach
The wrong approach includes providing secret values directly in the code,
which is not recommended for security reasons.

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
:::caution
Be careful not to put your credentials directly in the code.
:::

### Correct approach

The correct approach involves providing config values directly and secrets via
automatic [injection mechanism](#injection-mechanism)
or pass everything via configuration.

1. Option A
    ```python
    # `only_strings` will get the default value False
    data_source = google_sheets("23029402349032049", ["tab1", "tab2"])
    ```
    `credentials` value will be injected by the `@source` decorator (e.g. from `secrets.toml`).

    `spreadsheet_id` and `tab_names` take values from the provided arguments.

2. Option B
    ```python
   # `only_strings` will get the default value False
    data_source = google_sheets()
    ```
    `credentials` value will be injected by the `@source` decorator (e.g. from `secrets.toml`).

    `spreadsheet_id` and `tab_names` will be also injected by the `@source` decorator (e.g. from `config.toml`).

We use `dlt.secrets.value` and `dlt.config.value` to set secrets and configurations via:
- [TOML files](config_providers#toml-provider) (`secrets.toml` & `config.toml`):
  ```toml
  [sources.google_sheets.credentials]
  client_email = <client_email from services.json>
  private_key = <private_key from services.json>
  project_id = <project_id from services json>
  ```
  Read more about [TOML layouts](#secret-and-config-values-layout-and-name-lookup).
- [Environment Variables](config_providers#environment-provider):
  ```python
  SOURCES__GOOGLE_SHEETS__CREDENTIALS__CLIENT_EMAIL
  SOURCES__GOOGLE_SHEETS__CREDENTIALS__PRIVATE_KEY
  SOURCES__GOOGLE_SHEETS__CREDENTIALS__PROJECT_ID
  ```

:::caution
**[TOML provider](config_providers#toml-provider) always loads `secrets.toml` and `config.toml` files from `.dlt` folder** which is looked relative to the
**current [Working Directory](https://en.wikipedia.org/wiki/Working_directory)**. TOML provider also has the capability to read files from `~/.dlt/`
(located in the user's [Home Directory](https://en.wikipedia.org/wiki/Home_directory)).
:::


### Add typing to your sources and resources

We highly recommend adding types to your function signatures.
The effort is very low, and it gives `dlt` much more
information on what source/resource expects.

Doing so provides several benefits:

1. You'll never receive invalid data types in your code.
1. We can generate nice sample config and secret files for your source.
1. You can request dictionaries or special values (i.e. connection strings, service json) to be
   passed.
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
- Or default credentials will be used.

### Pass config values and credentials explicitly
We suggest a [default layout](#default-layout-and-default-key-lookup-during-injection) of secret and config values, but you can fully ignore it and use your own:

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
`dlt.config` and `dlt.secrets` behave like dictionaries from which you can request a value with any key name. `dlt` will look in all [config providers](#injection-mechanism) - TOML files, env variables etc. just like it does with the standard key name layout. You can also use `dlt.config.get()` / `dlt.secrets.get()` to
request value cast to a desired type. For example:
```python
credentials = dlt.secrets.get("my_section.gcp_credentials", GcpServiceAccountCredentials)
```
Creates `GcpServiceAccountCredentials` instance out of values (typically a dictionary) under **my_section.gcp_credentials** key.

See [example](https://github.com/dlt-hub/dlt/blob/devel/docs/examples/archive/credentials/explicit.py).

### Pass credentials as code

You can see that the `google_sheets` source expects a `gs_credentials`. So you could pass it as below.

```python
from airflow.hooks.base_hook import BaseHook

# get it from airflow connections or other credential store
credentials = BaseHook.get_connection('gcp_credentials').extra
data_source = google_sheets(credentials=credentials)
```
:::caution
Be careful not to put your credentials directly in code - use your own credential vault instead.
:::

### Pass explicit destination credentials
You can pass destination credentials and ignore the default lookup:
```python
pipeline = dlt.pipeline(destination="postgres", credentials=dlt.secrets["postgres_dsn"])
```

## Injection mechanism

Config and secret values are injected to the function arguments if the function is decorated with
`@dlt.source` or `@dlt.resource` (also `@with_config` which you can apply to any function - used
heavily in the dlt core).

The signature of the function `google_sheets` is **explicitly accepting all the necessary configuration and secrets in its arguments**.
During runtime, `dlt` tries to supply (`inject`) the required values via various config providers.

The injection rules are:

1. If you call the decorated function, the arguments that are passed explicitly are **never injected**,
   this makes the injection mechanism optional.

1. Required arguments (i.e. `spreadsheet_id` - without default values) are not injected and must be present.

1. Arguments with default values are injected if present in config providers, otherwise default is used.

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

### OPTION A (default layout)

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

### OPTION B (explicit layout)

Here user has full control over the layout.

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

**Rule 1:** All the sections above are optional. You are free to arrange your credentials and config
without any additional sections.

**Rule 2:** The lookup starts with the most specific possible path, and if value is not found there,
it removes the right-most section and tries again.

Example: In case of option A we have just one set of credentials.
But what if `bigquery` credentials are different from `google sheets`? Then we need to
allow some sections to separate them.

```toml
# google sheet credentials
[credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>

# bigquery credentials
[destination.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

Now when `dlt` looks for destination credentials, it will encounter the `destination` section and
stop there. When looking for `sources` credentials it will get directly into `credentials` key
(corresponding to function argument).

> We could also rename the argument in the source function! But then we are **forcing** the user to
> have two copies of credentials.

Example: let's be even more explicit and use a full section path possible.

```toml
# google sheet credentials
[sources.google_sheets.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>

# bigquery credentials
[destination.bigquery.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

Where we add destination and source name to be very explicit.

**Rule 3:** You can use your pipeline name to have separate configurations for each pipeline in your
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