---
title: How to set up credentials
description: Where and in which order dlt looks for config/secrets.
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, provider]
---

`dlt` automatically extracts configuration settings and secrets based on flexible [naming conventions](setup/#naming-convention).
It then [injects](custom_sources/#injection-mechanism) these values where needed in functions decorated with `@dlt.source`, `@dlt.resource`, or `@dlt.destination`.

## Available config providers

**Where configs and credentials are coming from?** `dlt` looks for configuration and secrets values in the config providers in the following order during pipeline execution:

1. [Environment Variables](#env-variables): At the top of the hierarchy are environment variables. If a value for a specific argument is found in an environment variable, dlt will use it and will not proceed to search in lower-priority providers.

1. [Vaults (Airflow/Google/AWS/Azure)](#vaults): Vaults can provide configuration values and secrets. However, they typically focus on handling sensitive information.

1. [secrets.toml and config.toml files](#secrets-toml-and-config-toml): These files are used for storing both configuration values and secrets. `secrets.toml` is dedicated to sensitive information, while `config.toml` contains non-sensitive configuration data.

1. [Default Argument Values](custom_sources#ingestion-mechanism): These are the values specified in the function's signature. They have the lowest priority in the lookup hierarchy.

:::tip
Please make sure your pipeline name contains no whitespace or any other punctuation characters except `"-"` and `"_"`. This way you will ensure your code is working with any configuration option.
:::

## Naming convention

How `dlt` defines which keys to look for? `dlt` uses a specific naming hierarchy to search for the secrets and configs values. This makes configurations and secrets easy to manage and allows to disambiguate values with the same keys by placing them in different sections.

To keep the naming convention flexible, `dlt` looks for a lot of possible combinations of key names, starting from the most specific possible path. Then, if the value is not found, it removes the right-most section and tries again.

* The most specific possible path for **sources** looks like:
```sh
<pipeline_name>.sources.<source_module_name>.<source_function_name>.<argument_name>
```

* The most specific possible path for **destinations** looks like:
```sh
<pipeline_name>.destination.<destination name>.credentials.<credential_option>
```

For example, if the source module is named `pipedrive` and the source is defined as follows:

```py
# pipedrive.py

@dlt.source
def deals(api_key: str = dlt.secrets.value):
    pass
```
`dlt` will search for the following names in this order:

1. `sources.pipedrive.deals.api_key`
1. `sources.pipedrive.api_key`
1. `sources.api_key`
1. `api_key`

:::tip
You can use your pipeline name to have separate configurations for each pipeline in your project. All config values will be looked with the pipeline name first and then again without it.

```toml
[pipeline_name_1.sources.google_sheets.credentials]
client_email = "<client_email_1>"
private_key = "<private_key_1>"
project_id = "<project_id_1>"

[pipeline_name_2.sources.google_sheets.credentials]
client_email = "<client_email_2>"
private_key = "<private_key_2>"
project_id = "<project_id_2>"
```
:::

### Credential types

In most of the cases credentials are just key-value pairs, but in some cases the actual structure of [credentials](prebuilt_types) could be quite complex and support several ways of setting it up.
For example, to connect to `sql_database` source user can either set up a connection string:

```toml
[sources.sql_database]
credentials="snowflake://user:password@service-account/database?warehouse=warehouse_name&role=role"
```
or set up all parameters of connection separately:

```toml
[sources.sql_database.credentials]
drivername="snowflake"
username="user"
password="password"
database = "database"
host = "service-account"
warehouse = "warehouse_name"
role = "role"
```

`dlt` can work with both of ways and convert one to another. To learn more about which credentials types are supported visit [prebuilt credential types](prebuilt_types) page.

## ENV variables

Environment variables could be used both for storing configuration values and for secrets. `dlt` prioritizes security by looking in environment variables before looking into configuration files.

The format of lookup keys is slightly different from secrets files because for ENV variables, all names are capitalized and sections are separated with a double underscore `"__"`. For example, to specify the Facebook Ads access token through environment variables, you would need to set up:

```sh
export SOURCES__FACEBOOK_ADS__ACCESS_TOKEN="<access_token>"
```

Check out the [example](#credentials-via-env-variables) of setting up credentials through environment variables.

:::tip
To organize development and securely manage environment variables for credentials storage, you can use the [python-dotenv](https://pypi.org/project/python-dotenv/) to automatically load variables from an `.env` file.
:::

## Vaults

Vault integration methods vary based on the vault type. For an example involving [GCP credentials](#GCP-Credentials), refer to our documentation.
For other vault integrations you are welcome to [contact sales](https://dlthub.com/contact-sales) to learn about our paid tools designed for data platform teams.
Tools include integration with vaults, ensuring your data remains secure while maintaining user autonomy.

## secrets.toml and config.toml

The TOML config provider in dlt utilizes two TOML files:

`config.toml`:

- Configs refer to non-sensitive configuration data. These are settings, parameters, or options that define the behavior of a data pipeline.
- They can include things like file paths, database hosts and timeouts, API URLs, performance settings, or any other settings that affect the pipeline's behavior.
- Accessible in code through `dlt.config.values`

`secrets.toml`:

- Secrets are sensitive information that should be kept confidential, such as passwords, API keys, private keys, and other confidential data.
- It's crucial to never hard-code secrets directly into the code, as it can pose a security risk.
- Accessible in code through `dlt.secrets.values`

By default, the `.gitignore` file in the project prevents `secrets.toml` from being added to version control and pushed. However, `config.toml` can be freely added to version control.

### Location

:::info
The TOML provider always loads those files from the `.dlt` folder, located **relative** to the current Working Directory.
:::

For example, if your working directory is `my_dlt_project` and your project has the following structure:

```text
my_dlt_project:
  |
  pipelines/
    |---- .dlt/secrets.toml
    |---- google_sheets.py
```

and you run
```sh
python pipelines/google_sheets.py
```
then `dlt` will look for secrets in `my_dlt_project/.dlt/secrets.toml` and ignore the existing `my_dlt_project/pipelines/.dlt/secrets.toml`.

If you change your working directory to `pipelines` and run
```sh
python google_sheets.py
```
`dlt` will look for `my_dlt_project/pipelines/.dlt/secrets.toml` as (probably) expected.

:::caution
The TOML provider also has the capability to read files from `~/.dlt/` (located in the user's home directory) in addition to the local project-specific `.dlt` folder.
:::

### Structure

`dlt` arranges the sections into a **default layout** that is expected by the [injection mechanism](custom_sources/#injection-mechanism). This layout makes it easy to configure simple cases but also provides a room for more explicit sections and complex cases, i.e., having several sources with different credentials or even hosting several pipelines in the same project sharing the same config and credentials.

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

## Examples

### Google credentials for source and destination

Let's assume we use the `bigquery` destination and the `google_sheets` source. They both use Google credentials and expect them to be configured under the `credentials` key.

1. If we create just a single `credentials` section like in [here](#default-layout-without-sections), the destination and source will share the same credentials.

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "Toml config provider", "value": "toml"},
    {"label": "ENV variables", "value": "env"},
]}>

  <TabItem value="toml">

```toml
[credentials]
client_email = "<client_email_both_for_destination_and_source>"
private_key = "<private_key_both_for_destination_and_source>"
project_id = "<project_id_both_for_destination_and_source>"
```

  </TabItem>

  <TabItem value="env">

```sh
export CREDENTIALS__CLIENT_EMAIL="<client_email_both_for_destination_and_source>"
export CREDENTIALS__PRIVATE_KEY="<private_key_both_for_destination_and_source>"
export CREDENTIALS__PROJECT_ID="<project_id_both_for_destination_and_source>"
```

  </TabItem>

</Tabs>

2. If we define sections as below, we'll keep the credentials separate

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "Toml config provider", "value": "toml"},
    {"label": "ENV variables", "value": "env"},
]}>

  <TabItem value="toml">

```toml
# google sheet credentials
[sources.credentials]
client_email = "<client_email from services.json>"
private_key = "<private_key from services.json>"
project_id = "<project_id from services json>"

# bigquery credentials
[destination.credentials]
client_email = "<client_email from services.json>"
private_key = "<private_key from services.json>"
project_id = "<project_id from services json>"
```

  </TabItem>

  <TabItem value="env">

```sh
# google sheet credentials
export SOURCES__CREDENTIALS__CLIENT_EMAIL="<client_email>"
export SOURCES__CREDENTIALS__PRIVATE_KEY="<private_key>"
export SOURCES__CREDENTIALS__PROJECT_ID="<project_id>"

# bigquery credentials
export DESTINATION__CREDENTIALS__CLIENT_EMAIL="<client_email>"
export DESTINATION__CREDENTIALS__PRIVATE_KEY="<private_key>"
export DESTINATION__CREDENTIALS__PROJECT_ID="<project_id>"
```

  </TabItem>

</Tabs>

Now `dlt` looks for destination credentials in the following order:
```sh
destination.bigquery.credentials --> Not found
destination.credentials --> Found
```

When looking for the source credentials:
```sh
sources.google_sheets_module.google_sheets_function.credentials --> Not found
sources.google_sheets_function.credentials --> Not found
sources.credentials --> Found
```

### Credentials for several different sources and destinations

Let's assume we have several different Google sources and destinations. We can use full paths to organize the `secrets.toml` file:

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "Toml config provider", "value": "toml"},
    {"label": "ENV variables", "value": "env"},
]}>

  <TabItem value="toml">

```toml
# google sheet credentials
[sources.google_sheets.credentials]
client_email = "<client_email from services.json>"
private_key = "<private_key from services.json>"
project_id = "<project_id from services json>"

# google analytics credentials
[sources.google_analytics.credentials]
client_email = "<client_email from services.json>"
private_key = "<private_key from services.json>"
project_id = "<project_id from services json>"

# bigquery credentials
[destination.bigquery.credentials]
client_email = "<client_email from services.json>"
private_key = "<private_key from services.json>"
project_id = "<project_id from services json>"
```

  </TabItem>

  <TabItem value="env">

```sh
# google sheet credentials
export SOURCES__GOOGLE_SHEETS__CREDENTIALS__CLIENT_EMAIL="<client_email>"
export SOURCES__GOOGLE_SHEETS__CREDENTIALS__PRIVATE_KEY="<private_key>"
export SOURCES__GOOGLE_SHEETS__CREDENTIALS__PROJECT_ID="<project_id>"

# google analytics credentials
export SOURCES__GOOGLE_ANALYTICS__CREDENTIALS__CLIENT_EMAIL="<client_email>"
export SOURCES__GOOGLE_ANALYTICS__CREDENTIALS__PRIVATE_KEY="<private_key>"
export SOURCES__GOOGLE_ANALYTICS__CREDENTIALS__PROJECT_ID="<project_id>"

# bigquery credentials
export DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL="<client_email>"
export DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY="<private_key>"
export DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID="<project_id>"
```

  </TabItem>

</Tabs>

### Credentials for several sources of the same type

Let's assume we have several sources of the same type, how can we separate them in the `secrets.toml`? The recommended solution is to use different pipeline names for each source:

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "Toml config provider", "value": "toml"},
    {"label": "ENV variables", "value": "env"},
]}>

  <TabItem value="toml">

```toml
[pipeline_name_1.sources.sql_database]
credentials="snowflake://user1:password1@service-account/database1?warehouse=warehouse_name&role=role1"

[pipeline_name_2.sources.sql_database]
credentials="snowflake://user2:password2@service-account/database2?warehouse=warehouse_name&role=role2"
```

  </TabItem>

  <TabItem value="env">

```sh
export PIPELINE_NAME_1_SOURCES__SQL_DATABASE__CREDENTIALS="snowflake://user1:password1@service-account/database1?warehouse=warehouse_name&role=role1"
export PIPELINE_NAME_2_SOURCES__SQL_DATABASE__CREDENTIALS="snowflake://user2:password2@service-account/database2?warehouse=warehouse_name&role=role2"
```

  </TabItem>

</Tabs>

## Understanding the exceptions

If `dlt` expects configuration of secrets value but cannot find it, it will output the `ConfigFieldMissingException`.

Let's run the `chess.py` example without providing the password:

```sh
$ CREDENTIALS="postgres://loader@localhost:5432/dlt_data" python chess.py
...
dlt.common.configuration.exceptions.ConfigFieldMissingException: Following fields are missing: ['password'] in configuration with spec PostgresCredentials
        for field "password" config providers and keys were tried in the following order:
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

1. First, `dlt` looked in a big section `chess_games`, which is the name of the pipeline.
1. In each case, it starts with full paths and goes to the minimum path `credentials.password`.
1. First, it looks into environment variables, then in `secrets.toml`. It displays the exact keys tried.
1. Note that `config.toml` was skipped! It could not contain any secrets.