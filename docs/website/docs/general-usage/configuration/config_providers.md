---
title: Secrets and Config Providers
description: Secrets and Config Providers
keywords: [credentials, secrets.toml, environment variables]
---

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
