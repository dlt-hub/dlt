---
title: Configuration providers
description: Where dlt looks for config/secrets and in which order.
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, provider]
---

Configuration Providers in the context of the `dlt` library
refer to different sources from which configuration values
and secrets can be retrieved for a data pipeline.
These providers form a hierarchy, with each having its own
priority in determining the values for function arguments.

## The provider hierarchy

If function signature has arguments that may be injected, `dlt` looks for the argument values in
providers.

### Providers

1. **Environment Variables**: At the top of the hierarchy are environment variables.
   If a value for a specific argument is found in an environment variable,
   dlt will use it and will not proceed to search in lower-priority providers.

2. **Vaults (Airflow/Google/AWS/Azure)**: These are specialized providers that come
   after environment variables. They can provide configuration values and secrets.
   However, they typically focus on handling sensitive information.

3. **`secrets.toml` and `config.toml` Files**: These files are used for storing both
   configuration values and secrets. `secrets.toml` is dedicated to sensitive information,
   while `config.toml` contains non-sensitive configuration data.

4. Custom Providers added with `register_provider`: These are your own Provider implementation
   you can use to connect to any backend. See [adding custom providers](#adding-custom-providers) for more information.

5. **Default Argument Values**: These are the values specified in the function's signature.
   They have the lowest priority in the provider hierarchy.


### Example

```py
@dlt.source
def google_sheets(
    spreadsheet_id=dlt.config.value,
    tab_names=dlt.config.value,
    credentials=dlt.secrets.value,
    only_strings=False
):
    ...
```

In case of `google_sheets()` it will look
for: `spreadsheet_id`, `tab_names`, `credentials` and `only_strings`

Each provider has its own key naming convention, and dlt is able to translate between them.

**The argument name is a key in the lookup**.

At the top of the hierarchy are Environment Variables, then `secrets.toml` and
`config.toml` files. Providers like Airflow/Google/AWS/Azure Vaults will be inserted **after** the Environment
provider but **before** TOML providers.

For example, if `spreadsheet_id` is found in environment variable `SPREADSHEET_ID`, `dlt` will not look in TOML files
and below.

The values passed in the code **explicitly** are the **highest** in provider hierarchy. The **default values**
of the arguments have the **lowest** priority in the provider hierarchy.

:::info
Explicit Args **>** ENV Variables **>** Vaults: Airflow etc. **>** `secrets.toml` **>** `config.toml` **>** Default Arg Values
:::

Secrets are handled only by the providers supporting them. Some providers support only
secrets (to reduce the number of requests done by `dlt` when searching sections).

1. `secrets.toml` and environment may hold both config and secret values.
1. `config.toml` may hold only config values, no secrets.
1. Various vaults providers hold only secrets, `dlt` skips them when looking for values that are not
   secrets.

:::info
Context-aware providers will activate in the right environments i.e. on Airflow or AWS/GCP VMachines.
:::

### Adding Custom Providers

You can use the `CustomLoaderDocProvider` classes to supply a custom dictionary obtained from any source to dlt for use
as a source of `config` and `secret` values. The code  below demonstrates how to use a config stored in config.json.

```py
import dlt

from dlt.common.configuration.providers import CustomLoaderDocProvider

# create a function that loads a dict
def load_config():
   with open("config.json", "rb") as f:
      config_dict = json.load(f)

# create the custom provider
provider = CustomLoaderDocProvider("my_json_provider",load_config)

# register provider
dlt.config.register_provider(provider)
```

:::tip
Check our nice [example](../../examples/custom_config_provider) for a `yaml` based config provider that supports switchable profiles.
:::

## Provider key formats

### TOML vs. Environment Variables

Providers may use different formats for the keys. `dlt` will translate the standard format where
sections and key names are separated by "." into the provider-specific formats.

1. For TOML, names are case-sensitive and sections are separated with ".".
1. For Environment Variables, all names are capitalized and sections are separated with double
   underscore "__".

   Example: To override a token in "secrets.toml":

   ```toml
   [sources.github]
   access_token = "GITHUB_API_TOKEN"
   ```
   Use the following environment variable:
   ```sh
   export SOURCES__GITHUB__ACCESS_TOKEN="your_token_here"
   ```


1. When `dlt` evaluates the request `dlt.secrets["my_section.gcp_credentials"]` it must find the `private_key` for Google credentials. It looks for it as follows:
   1. It first searches for environment variable `MY_SECTION__GCP_CREDENTIALS__PRIVATE_KEY`  and if not found,
   1. in `secrets.toml` file under `my_section.gcp_credentials.private_key`.

   This way, `dlt` prioritizes security by using environment variables before looking into configuration files.



:::info
While using Google secrets provider please make sure your pipeline name
contains no whitespace or any other punctuation characters except "-" and "_".

Per Google the secret name can contain

    1. Uppercase and lowercase letters,
    2. Numerals,
    3. Hyphens,
    4. Underscores.
:::

### Environment provider

Looks for the values in the environment variables.

### TOML provider

The TOML provider in dlt utilizes two TOML files:

- `secrets.toml `- This file is intended for storing sensitive information, often referred to as "secrets".
- `config.toml `- This file is used for storing configuration values.

By default, the `.gitignore` file in the project prevents `secrets.toml` from being added to
version control and pushed. However, `config.toml` can be freely added to version control.

:::info
**TOML provider always loads those files from `.dlt` folder** which is looked **relative to the
current Working Directory**.
:::

Example: If your working directory is `my_dlt_project` and your project has the following structure:

```text
my_dlt_project:
  |
  pipelines/
    |---- .dlt/secrets.toml
    |---- google_sheets.py
```

and you run `python pipelines/google_sheets.py` then `dlt` will look for `secrets.toml` in
`my_dlt_project/.dlt/secrets.toml` and ignore the existing
`my_dlt_project/pipelines/.dlt/secrets.toml`.

If you change your working directory to `pipelines` and run `python google_sheets.py` it will look for
`my_dlt_project/pipelines/.dlt/secrets.toml` as (probably) expected.

:::caution
It's worth mentioning that the TOML provider also has the capability to read files from `~/.dlt/`
(located in the user's home directory) in addition to the local project-specific `.dlt` folder.
:::