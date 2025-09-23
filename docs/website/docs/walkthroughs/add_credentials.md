---
title: How to add credentials
description: How to add credentials locally and in production
keywords: [credentials, secrets.toml, environment variables]
---

# How to add credentials

## Adding credentials locally

When using a pipeline locally, we recommend using the `.dlt/secrets.toml` method.

To do so, open your dlt secrets file and match the source names and credentials to the ones in your script, for example:

```toml
[sources.pipedrive]
pipedrive_api_key = "pipedrive_api_key" # please set me up!

[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```

:::note
Keys in TOML files are case-sensitive and sections are separated with a period (`.`).
:::

For destination credentials, read the [documentation pages for each destination](../dlt-ecosystem/destinations) to create and configure credentials.

For Verified Source credentials, read the [Setup Guides](../dlt-ecosystem/verified-sources) for each source to find how to get credentials.

Once you have credentials for the source and destination, add them to the file above and save them.

Read more about [credential configuration.](../general-usage/credentials)

## Adding credentials to your deployment

To add credentials to your deployment,

- either use one of the `dlt deploy` commands;
- or follow the instructions to [pass credentials via code](../general-usage/credentials/advanced#configure-destination-credentials-in-code) or [environment](../general-usage/credentials/setup#environment-variables).

### Reading credentials from environment variables

`dlt` supports reading credentials from the environment. For example, our `.dlt/secrets.toml` might look like:

```toml
[sources.pipedrive]
pipedrive_api_key = "pipedrive_api_key" # please set me up!

[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```

If dlt tries to read this from environment variables, it will use a different naming convention.

For environment variables, all names are capitalized and sections are separated with a double underscore "__".

For example, for the secrets mentioned above, we would need to set them in the environment:

```sh
SOURCES__PIPEDRIVE__PIPEDRIVE_API_KEY
DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID
DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY
DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL
DESTINATION__BIGQUERY__LOCATION
```

## Retrieving credentials from Google Cloud Secret Manager

`dlt` supports reading credentials from Google Cloud Secret Manager. To enable this functionality you must provide
credentials with following access permissions:
* **roles/secretmanager.secretAccessor** to read particular secret
* **roles/secretmanager.secretViewer** to list available secrets (optional but highly recommended)

Example configuration:
```toml
[providers]
enable_google_secrets=true

[providers.google_secrets]
only_secrets=false
only_toml_fragments=false
list_secrets=true

[providers.google_secrets.credentials]
"project_id" = "<project_id>"
"private_key" = "-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n"
"client_email" = "....gserviceaccount.com"
```

### Allow to list secrets and use toml fragments to reduce calls to backend
We recommend enabling `list_secrets` to obtain a list of possible keys and avoid calls to the backends. We also recommend
to store configuration fragments, not single values to reduce the number of calls. Vault provider is able to fetch such fragments
and combine them into full configuration on the fly.

For example you can define:

**destination** secret to keep credentials for destinations:
```toml
[destination]
postgres.credentials="postgresql://loader:***@host:5432/postgres"

[destination.bigquery.credentials]
"project_id" = "<project_id>"
"private_key" = "-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n"
"client_email" = "....gserviceaccount.com"
```

or **destination-filesystem** to just store filesystem credentials
```toml
[destination.filesystem]
bucket_url="s3://bucket/path"
[destination.filesystem.credentials]
# s3 (same as athena)
region_name="eu-central-1"
aws_access_key_id="..."
aws_secret_access_key="..."
```

same for sources ie. **sources-mongodb** will store mongo credentials:
```toml
[sources.mongodb]
connection_url="mongodb+srv://temp_writer:***/dlt_data?authSource=admin&replicaSet=db-mongodb&tls=true"
```

Note that you still can store single values, in that case google vault works similarly to environment variables provider:
```sh
sources-pipedrive-pipedrive_api_key
destination-bigquery-credentials-project_id
destination-bigquery-credentials-private_key
destination-bigquery-credentials-client_email
destination-bigquery-location
```
This will obviously require several calls to Secrets backend.

:::warning
Vault provider will cache all retrieved keys internally and will not fetch those secrets again (until process is restarted). This
reduces number of calls to backend (which cost money) but will also not pick up changes at runtime.
:::

### Access secrets without list secrets permissions
Following settings will skip listing secrets and still minimize number of backend calls:
```toml
[providers.google_secrets]
only_secrets=true
only_toml_fragments=true
list_secrets=false
```

Vault will fetch only secret values (credentials, `dlt.secrets.value` marked arguments) and only the toml fragments as described
in the above section, without fetching single values.


:::warning
`dlt` probes several locations for a single value so if you disable `only_toml_fragments` you may receive large amount of calls
to Secrets backend.
:::

## Retrieving credentials from other vault types
Subclass `VaultDocProvider` and implement methods to fetch a secret and (optionally) to list secrets then
[register subclass as custom provider](../examples/custom_config_provider).
