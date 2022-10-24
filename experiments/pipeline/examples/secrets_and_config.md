## Example
How config values and secrets are handled should promote good behavior

1. secret values should never be present in the pipeline code
2. config values can be provided, changed etc. when pipeline is deployed
3. still it must be easy and intuitive

For the source extractor function below (reads selected tab from google sheets) we can pass config values in following ways:

```python

import dlt
from dlt.destinations import bigquery


@dlt.source
def google_sheets(spreadsheet_id, tab_names, credentials, only_strings=False):
    sheets = build('sheets', 'v4', credentials=Services.from_json(credentials))
    tabs = []
    for tab_name in tab_names:
        data = sheets.get(spreadsheet_id, tab_name).execute().values()
        tabs.append(dlt.resource(data, name=tab_name))
    return tabs

# WRONG: provide all values directly - wrong but possible. secret values should never be present in the code!
google_sheets("23029402349032049", ["tab1", "tab2"], credentials={"private_key": ""}).run(destination=bigquery)

# OPTION A: provide config values directly and secrets via automatic injection mechanism (see later)
# `credentials` value will be provided by the `source` decorator
# `spreadsheet_id` and `tab_names` take default values from the arguments below but may be overwritten by the decorator via config providers (see later)
google_sheets("23029402349032049", ["tab1", "tab2"]).run(destination=bigquery)


# OPTION B: all values are injected so there are no defaults and config values must be present in the providers
google_sheets().run(destination=bigquery)


# OPTION C: we use `dlt.secrets` and `dlt.config` to explicitly take those values from providers in the way we control (not recommended but straightforward)
google_sheets(dlt.config["sheet_id"], dlt.config["tabs"], dlt.secrets["gcp_credentials"]).run(destination=bigquery)
```

## Injection mechanism
By the magic of @dlt.source decorator

The signature of the function `google_sheets` is also defining the structure of the configuration and secrets.

When `google_sheets` function is called the decorator takes every input parameter and uses its value as initial.
Then it looks into `providers` if the value is not overwritten there.
It does the same for all arguments that were not in the call but are specified in function signature.
Then it calls the original function with updated input arguments thus passing config and secrets to it.

## Providers
When config or secret values are needed, `dlt` looks for them in providers. In case of `google_sheets()` it will always look for: `spreadsheet_id`, `tab_names`, `credentials` and `strings_only`.

Providers form a hierarchy. At the top are environment variables, then `secrets.toml` and `config.toml` files. Providers like google, aws, azure vaults can be inserted after the environment provider.
For example if `spreadsheet_id` is in environemtn, dlt does not look into other provieers.

The values passed in the code directly are the lowest in provider hierarchy.

## Namespaces
Config and secret values can be grouped in namespaces. Easiest way to visualize it is via `toml` files.

This is valid for OPTION A and OPTION B

**secrets.toml**
```toml
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```
**config.toml**
```toml
spreadsheet_id="302940230490234903294"
tab_names=["tab1", "tab2"]
```

**alternative secrets.toml**
**secrets.toml**
```toml
[credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```

where `credentials` is name of the parameter from `google_sheet`. This parameter is a namespace for keys it contains and namespace are *optional*

For OPTION C user uses its own custom keys to get credentials so:
**secrets.toml**
```toml
[gcp_credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>
```
**config.toml**
```toml
sheet_id="302940230490234903294"
tabs=["tab1", "tab2"]
```

But what about `bigquery` credentials? In the case above it will reuse the credentials from **secrets.toml** (in OPTION A and B) but what if we need different credentials?

Dlt has a nice optional namespace structure to handle all conflicts. It becomes useful in advanced cases like above. The secrets and config files may look as follows (and they will work with OPTION A and B)

**secrets.toml**
```toml
[source.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>

[destination.credentials]
client_email = <client_email from services.json>
private_key = <private_key from services.json>
project_id = <project_id from services json>

```
**config.toml**
```toml
[source]
spreadsheet_id="302940230490234903294"
tab_names=["tab1", "tab2"]
```

How namespaces work in environment variables? they are prefix for the key so to get `spreadsheet_id` `dlt` will look for

`SOURCE__SPREADSHEET_ID` first and `SPREADSHEET_ID` second

## Interesting / Advanced stuff.

The approach above makes configs and secrets explicit and autogenerates required lookups. It lets me for example **generate deployments** and **code templates for pipeline scripts** automatically because I know what are the config parameters and I have total control over users code and final values via the decorator.

There's more cool stuff

Here's how professional source function should look like

```python


@dlt.source
def google_sheets(spreadsheet_id: str, tab_names: List[str], credentials: TCredentials, only_strings=False):
    sheets = build('sheets', 'v4', credentials=Services.from_json(credentials))
    tabs = []
    for tab_name in tab_names:
        data = sheets.get(spreadsheet_id, tab_name).execute().values()
        tabs.append(dlt.resource(data, name=tab_name))
    return tabs
```

Here I provide typing so I can type check injected values so no junk data gets passed to the function.

> I also tell which argument is secret via `TCredentials` that let's me control for the case when user is putting secret values in `config.toml` or some other unsafe provider (and generate even better templates)

We could go even deeper here (ie. configurations `spec` may be explicitly declared via python `dataclasses`, may be embedded in one another etc. -> it comes useful when writing something really complicated)