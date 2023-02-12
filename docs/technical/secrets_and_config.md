# Secrets and Configs
marks features that are:

⛔ not implemented, hard to add

☮️ not implemented, easy to add

## General Usage and an Example
The way config values and secrets are handled should promote correct behavior

1. secret values should never be present in the pipeline code
2. pipeline may be reconfigured for production after it is deployed. deployed and local code should be identical
3. still it must be easy and intuitive

For the source extractor function below (reads selected tab from google sheets) we can pass config values in following ways:

```python

import dlt


@dlt.source
def google_sheets(spreadsheet_id, tab_names=dlt.config.value, credentials=dlt.secrets.value, only_strings=False):
    sheets = build('sheets', 'v4', credentials=Services.from_json(credentials))
    tabs = []
    for tab_name in tab_names:
        data = sheets.get(spreadsheet_id, tab_name).execute().values()
        tabs.append(dlt.resource(data, name=tab_name))
    return tabs

# WRONG: provide all values directly - wrong but possible. secret values should never be present in the code!
google_sheets("23029402349032049", ["tab1", "tab2"], credentials={"private_key": ""}).run(destination="bigquery")

# OPTION A: provide config values directly and secrets via automatic injection mechanism (see later)
# `credentials` value will be injected by the `source` decorator
# `spreadsheet_id` and `tab_names` take values from the arguments below
# `only_strings` will be injected by the source decorator or will get the default value False
google_sheets("23029402349032049", ["tab1", "tab2"]).run(destination="bigquery")


# OPTION B: use `dlt.secrets` and `dlt.config` to explicitly take those values from providers from the explicit keys
google_sheets(dlt.config["sheet_id"], dlt.config["my_section.tabs"], dlt.secrets["my_section.gcp_credentials"]).run(destination="bigquery")
```

> one of the principles is that configuration, credentials and secret values are may be passed explicitly as arguments to the functions. this makes the injection behavior optional.

## Injection mechanism
Config and secret values are injected to the function arguments if the function is decorated with `@dlt.source` or `@dlt resource` (also `@with_config` which you can applu to any function - used havily in the dlt core)

The signature of the function `google_sheets` is **explicitely accepting all the necessary configuration and secrets in its arguments**. During runtime, `dlt` tries to supply (`inject`) the required values via various config providers. The injection rules are:
1. if you call the decorated function, the arguments that are passed explicitly are **never injected**
this makes injection mechanism optional

2. required arguments (ie. `spreadsheet_id`, `tab_names`) are not injected
3. arguments with default values are injected if present in config providers
4. arguments with the special default value `dlt.secrets.value` and `dlt.config.value` **must be injected** (or expicitly passed). If they are not found by the config providers the code raises exception. The code in the functions always receives those arguments.

additionally `dlt.secrets.value` tells `dlt` that supplied value is a secret and it will be injected only from secure config providers

## Passing config values and credentials explicitly

```python
# OPTION B: use `dlt.secrets` and `dlt.config` to explicitly take those values from providers from the explicit keys
google_sheets(dlt.config["sheet_id"], dlt.config["tabs"], dlt.secrets["my_section.gcp_credentials"]).run(destination="bigquery")
```

[See example](/docs/examples/credentials/explicit.py)


## Typing the source and resource signatures

You should type your function signatures! The effort is very low and it gives `dlt` much more information on what source/resource expects.
1. You'll never receive invalid type signatures
2. We can generate nice sample config and secret files for your source
3. You can request dictionaries or special values (ie. connection strings, service json) to be passed
4. ☮️ you can specify a set of possible types via `Union` ie. OAUTH or Api Key authorization

```python
@dlt.source
def google_sheets(spreadsheet_id: str, tab_names: List[str] = dlt.config.value, credentials: GcpClientCredentialsWithDefault = dlt.secrets.value, only_strings: bool = False):
  ...
```
Now:
1. you are sure that you get a list of strings as `tab_names`
2. you will get actual google credentials (see `CredentialsConfiguration` later) and your users can pass them in many different forms.

In case of `GcpClientCredentialsWithDefault`
* you may just pass the `service_json` as string or dictionary (in code and via config providers)
* you may pass a connection string (used in sql alchemy) (in code and via config providers)
* or default credentials will be used


## Providers
If function signature has arguments that may be injected, `dlt` looks for the argument values in providers. **The argument name is a key in the lookup**. In case of `google_sheets()` it will look for: `tab_names`, `credentials` and `strings_only`.

Each provider has its own key naming convention and dlt is able to translate between them.

Providers form a hierarchy. At the top are environment variables, then `secrets.toml` and `config.toml` files. Providers like google, aws, azure vaults can be inserted after the environment provider.

For example if `spreadsheet_id` is in environment, dlt does not look into other providers.

The values passed in the code explitly are the **highest** in provider hierarchy.
The default values of the arguments have the **lowest** priority in the provider hierarchy.

> **Summary of the hierarchy**
> explicit args > env variables > ...vaults, airflow etc > secrets.toml > config.toml > default arg values

Secrets are handled only by the providers supporting them. Some of the providers support only secrets (to reduce the number of requests done by `dlt` when searching sections)
1. `secrets.toml` and environment may hold both config and secret values
2. `config.toml` may hold only config values, no secrets
3. various vaults providers hold only secrets, `dlt` skips them when looking for values that are not secrets.

⛔ Context aware providers will activate in right environments ie. on Airflow or AWS/GCP VMachines

### Provider key formats. toml vs. environment variable

Providers may use diffent formats for the keys. `dlt` will translate the standard format where sections and key names are separated by "." into the provider specific formats.

1. for `toml` names are case sensitive and sections are separated with "."
2. for environment variables all names are capitalized and sections are separated with double underscore "__"

Example:
When `dlt` evaluates the request `dlt.secrets["my_section.gcp_credentials"]` it must find the `private_key` for google credentials. It will look
1. first in env variable `MY_SECTION__GCP_CREDENTIALS__PRIVATE_KEY` and if not found
2. in `secrets.toml` with key `my_section.gcp_credentials.private_key`


### Environment provider
Looks for the values in the environment variables

### Toml provider
Tomls provider uses two `toml` files: `secrets.toml` to store secrets and `config.toml` to store configuration values. The default `.gitignore` file prevents secrets from being added to source control and pushed. The `config.toml` may be freely added.

**Toml provider always loads those files from `.dlt` folder** which is looked **relative to the current working directory**. Example:
if your working dir is `my_dlt_project` and you have:
```
my_dlt_project:
  |
  pipelines/
    |---- .dlt/secrets.toml
    |---- google_sheets.py
```
in it and you run `python pipelines/google_sheets.py` then `dlt` will look for `secrets.toml` in `my_dlt_project/.dlt/secrets.toml` and ignore the existing `my_dlt_project/pipelines/.dlt/secrets.toml`

if you change your working dir to `pipelines` and run `python google_sheets.py` it will look for `my_dlt_project/pipelines/.dlt/secrets.toml` a (probably) expected.

*that was common problem on our workshop - but believe me all other layouts are even worse I've tried*


## Secret and config values layout.
`dlt` uses an layout of hierarchical sections to organize the config and secret values. This makes configurations and secrets easy to manage and disambiguates values with the same keys by placing them in the different sections

> if you know how `toml` files are organized -> this is the same concept!

> a lot of config values are dictionaries themselves (ie. most of the credentials) and you want the values corresponding to one component to be close together.

> you can have a separate credentials for your destinations and each of source your pipeline uses, if you have many pipelines in single project, you can have a separate sections corresponding to them.

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

As you can see the details of gcp credentials are placed under `credentials` which is argument name to source function

### OPTION B (explicit layout)

Here user has full control over the layout

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
  project_id = <project_id from services json>  # I prefer to keep my project id in config file and private key in secrets
```

### Default layout and default key lookup during injection

`dlt` arranges the sections into **default layout** that is used by injection mechanism. This layout makes it easy to configure simple cases but also provides a room for more explicit sections and complex cases ie. having several soures with different credentials or even hosting several pipelines in the same project sharing the same config and credentials.

```
pipeline_name
    |
    |-sources
        |-<source name 1>
        | |- all source and resource options and secrets
        |-<source name 2>
        | |- all source and resource options and secrets
        |-extract
          |- extract options for resources ie. parallelism settings, maybe retries
    |-destination
        |- <destination name>
          |-all destination options and secrets
    |-schema
        |-<schema name>
            |-schema settings: not implemented but I'll let people set nesting level, name convention, normalizer etc. here
    |-load
    |-normalize
    |--extract
        |-all extract options ie.
```

Lookup rules:

**Rule 1** All the sections above are optional. You are free to arrange your credentials and config without any additional sections
Example: OPTION A (default layout)

**Rule 2** The lookup starts with the most specific possible path and if value is not found there, it removes the right-most section and tries again.
Example: In case of option A we have just one credentials. But what if `bigquery` credentials are different from `google sheets`? Then we need to allow some sections to separate them.

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
Now when `dlt` looks for destination credentials, it will encounter the `destination` section and stop there.
When looking for `sources` credentials it will get  directly into `credentials` key (corresponding to function argument)

> we could also rename the argument in the source function! but then we are **forcing** the user to have two copies of credentials.

Example: let's be even more explicit and use full section path possible
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

**Rule 3** You can use your pipeline name to have separate configurations for each pipeline in your project

Pipeline created/obtained with `dlt.pipeline()` creates a global and optional namespace with the value of `pipeline_name`. All config values will be looked with pipeline name first and then again without it.

Example: the pipeline is named `ML_sheets`
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

## Understanding the exceptions
Now we can finally understand the `ConfigFieldMissingException`. Let's run `chess.py` example without providing the password:

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
Please refer to https://dlthub.com/docs/customization/credentials for more information
```

It tells you exactly which paths `dlt` looked at, via which config providers and in which order. In the example above
1. First it looked in a big section `chess_games` which is name of the pipeline
2. In each case it starts with full paths and goes to minimum path `credentials.password`
3. First it looks into `environ` then in `secrets.toml`. It displays the exact keys tried.
4. Note that `config.toml` was skipped! It may not contain any secrets.


## Working with credentials (and other complex configuration values)

`GcpClientCredentialsWithDefault` is an example of a **spec**: a Python `dataclass` that describes the configuration fields, their types and default values. It also allows to parse various native representations of the configuration. Credentials marked with `WithDefaults` mixin are also to instantiate itself from the machine/user default environment ie. googles `default()` or AWS `.aws/credentials`.

As an example, let's use `ConnectionStringCredentials` which represents a database connection string.

```python

@dlt.source
def query(sql: str, dsn: ConnectionStringCredentials = dlt.secrets.value):
  ...
```

The source above executes the `sql` against database defined in `dsn`. `ConnectionStringCredentials` makes sure you get the correct values with correct types and understands the relevant native form of the credentials.


Example 1: use the dictionary form
```toml
[dsn]
database="dlt_data"
password="loader"
username="loader"
host="localhost"
```

Example:2: use the native form
```toml
dsn="postgres://loader:loader@localhost:5432/dlt_data"
```

Example 3: use mixed form: the password is missing in explicit dsn and will be taken from the `secrets.toml`
```toml
dsn.password="loader
```
```python
query("SELECT * FROM customers", "postgres://loader@localhost:5432/dlt_data")
# or
query("SELECT * FROM customers", {"database": "dlt_data", "username": "loader"...})
```

☮️ We will implement more credentials and let people reuse them when writing pipelines:
-  to represent oauth credentials
- api key + api secret
- AWS credentials



## Writing own specs

**specs** let you tak full control over the function arguments:
- which values should be injected, the types, default values.
- you can specify optional and final fields
- form hierarchical configurations (specs in specs).
- provide own handlers for `on_error` or `on_resolved`
- provide own native value parsers
- provide own default credentials logic
- adds all Python dataclass goodies to it
- adds all Python `dict` gooies to it (`specs` instances can be created from dicts and serialized from dicts)

This is used a lot in the `dlt` core and may become useful for complicated sources.

In fact for each decorated function a spec is synthesized. In case of `google_sheets` following class is created.
```python
@configspec
class GoogleSheetsConfiguration:
  tab_names: List[str] = None  # manadatory
  credentials: GcpClientCredentialsWithDefault = None # mandatory secret
  only_strings: Optional[bool] = False
```

## Interesting / Advanced stuff.

The approach above makes configs and secrets explicit and autogenerates required lookups. It lets me for example **generate deployments** and **code templates for pipeline scripts** automatically because I know what are the config parameters and I have total control over users code and final values via the decorator.
