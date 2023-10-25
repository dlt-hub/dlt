---
title: Configuration Providers
description: Configuration dlt Providers
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, provider]
---

# Configuration Providers
## The provider hierarchy

If function signature has arguments that may be injected, `dlt` looks for the argument values in
providers. **The argument name is a key in the lookup**.

Example:

```python
import dlt


@dlt.source
def google_sheets(
    spreadsheet_id,
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

In case of `google_sheets()` it will look
for: `tab_names`, `credentials` and `only_strings`.

Each provider has its own key naming convention, and dlt is able to translate between them.

Providers form a hierarchy. At the top are environment variables, then `secrets.toml` and
`config.toml` files. Providers like Google/AWS/Azure Vaults can be inserted after the environment
provider.

For example, if `spreadsheet_id` is in environment, dlt does not look into other providers.

The values passed in the code **explicitly** are the **highest** in provider hierarchy. The **default values**
of the arguments have the **lowest** priority in the provider hierarchy.

> **Summary of the hierarchy:**
>
> explicit args > env variables > ...vaults, airflow etc. > secrets.toml > config.toml > default arg values

Secrets are handled only by the providers supporting them. Some providers support only
secrets (to reduce the number of requests done by `dlt` when searching sections).

1. `secrets.toml` and environment may hold both config and secret values.
1. `config.toml` may hold only config values, no secrets.
1. Various vaults providers hold only secrets, `dlt` skips them when looking for values that are not
   secrets.

⛔ Context-aware providers will activate in right environments i.e. on Airflow or AWS/GCP VMachines.

## Provider key formats

### `toml` vs. Environment Variables

Providers may use different formats for the keys. `dlt` will translate the standard format where
sections and key names are separated by "." into the provider-specific formats.

1. For `toml`, names are case-sensitive and sections are separated with ".".
1. For Environment Variables, all names are capitalized and sections are separated with double
   underscore "\_\_".

Example: When `dlt` evaluates the request `dlt.secrets["my_section.gcp_credentials"]` it must find
the `private_key` for Google credentials. It will look

1. first in env variable `MY_SECTION__GCP_CREDENTIALS__PRIVATE_KEY` and if not found,
1. in `secrets.toml` with key `my_section.gcp_credentials.private_key`.

### Environment provider

Looks for the values in the environment variables.

### Toml provider

Tomls provider uses two `toml` files: `secrets.toml` to store secrets and `config.toml` to store
configuration values. The default `.gitignore` file prevents secrets from being added to source
control and pushed. The `config.toml` may be freely added.

> **Toml provider always loads those files from `.dlt` folder** which is looked **relative to the
> current Working Directory**.

Example: If your working directory is `my_dlt_project` and your project has the following structure:

```
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