---
title: Vault Providers
description: Learn how to configure Google Secrets and Airflow providers
---

## How vault providers work
`dlt` can read configuration and secrets from “vault” providers by reconstructing a secrets.toml-like document from one or more secrets stored in a vault. Internally this is handled by a vault-backed provider that:

* looks up entire TOML fragments (recommended) and single values (optional), then merges them into a working in-memory TOML document
* caches all retrieved values for the lifetime of the process to minimize repeated calls
* optionally pre-lists all available keys to avoid unnecessary lookups for non-existent secrets

Supported providers include:

* [Google Cloud Secret Manager](#configure-google-secret-provider)
* [Airflow Variables](#configure-airflow-variables-as-provider)

## Lookup and merge strategy
On first access to any configuration value, the vault provider tries to populate its in-memory TOML document by fetching and merging known fragments, in the following order:

1. Global `dlt_secrets_toml`. It first tries a special key `dlt_secrets_toml` that may contain an entire `secrets.toml` as a single secret.
2. Pipeline-scoped `dlt_secrets_toml`. If you request a value for a specific pipeline, it also attempts to fetch a pipeline-scoped `<pipeline_name>.dlt_secrets_toml`.
3. Known sections and names:
  * The provider knows about these top-level sections: **sources** and **destination**.
  * It will probe short-to-long paths so that more specific fragments override less specific ones:
    * **`sources`** and **`sources.<source_name>`**
    * **`destination`** and **`destination.<destination_name>`**
    * The above both globally and pipeline-scoped when a pipeline name is provided.

4. Single-value lookups (optional). If enabled, the provider may also fetch single values when a TOML fragment was not found. This can incur many calls because a single configuration may probe several possible locations.

### Merging behavior

Fragments are TOML documents and are merged into the in-memory configuration in the order they are found. More specific fragments (for example `destination.bigquery` over `destination`) override settings from more general fragments.

### Caching

Every successful or failed lookup (including “not found”) is cached for the lifetime of the process. Changes in the vault will not be picked up until the process restarts.

## Configure the vault provider
You can tune the provider to reduce the number of vault calls:

### only_secrets (default varies by provider)

When true, the provider only fetches values that are marked as secrets by `dlt` (for example, destination credentials and values annotated as secret). Non-secret settings are skipped.

### only_toml_fragments (default varies by provider)

When true, the provider fetches only the known TOML fragments (for example `destination`, `destination.bigquery`, `sources`, `sources.facebook`, and `dlt_secrets_toml`). Single-value lookups are skipped.

### list_secrets (default false)

When true, the provider lists all available secret names once and then avoids lookups for keys that do not exist. This can save many calls, but requires additional permissions (see Google Cloud Secret Manager example below).
If you enable `list_secrets` while also enabling `only_secrets` and/or `only_toml_fragments`, note that some lookups may still be skipped by design.

:::tip
* Prefer TOML fragments (**destination**, **destination.**, **sources**, **sources.**, **dlt_secrets_toml**) to minimize the number of round trips.
* Enable `list_secrets` whenever you can. It will radically reduce the number of calls to the vault's backend.
* Keep `only_secrets=true` when you want to restrict vault calls to secret-typed configuration (for example credentials), and provide non-secret config via environment or files.
:::

## Configure Google Secret Provider

Required permissions:

* `roles/secretmanager.secretAccessor` to read particular secrets (required)
* `roles/secretmanager.secretViewer` to list available secrets (required when list_secrets=true)

### Activate Google Secret Provider
To activate Google Secrets Provider, you must configure it first. Easiest way is to include such configuration in `secrets.toml`. You can skip the credentials sections if default google credentials with required permissions are available in your environment.

```toml
[providers]
enable_google_secrets = true  # google secrets provider is disabled by default

[providers.google_secrets]
only_secrets = false
only_toml_fragments = false
list_secrets = true  # we recommend pre-listing secrets to minimize calls to google backend

[providers.google_secrets.credentials]
project_id = "<project_id>"
private_key = "-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n"
client_email = "....gserviceaccount.com"
```

Alternative vault configuration when listing secrets is not available:

```toml
[providers.google_secrets]
only_secrets = true
only_toml_fragments = true
list_secrets = false
```

### Add secrets in Secrets Manager
Now you can add secrets to Google Secrets. Use TOML fragments to minimize backend calls. A few example of secrets.

**secret name: `destination`**

```toml 
[destination]
postgres.credentials = "postgresql://loader:***@host:5432/postgres"
```

**secret name: destination-bigquery-credentials**

```toml
[destination.bigquery.credentials]
project_id = "<project_id>"
private_key = "-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n"
client_email = "....gserviceaccount.com"
```


**secret name: destination-filesystem**

(whole `filesystem` destination configuration)
```toml
[destination.filesystem]
bucket_url = "s3://bucket/path"

[destination.filesystem.credentials]
region_name = "eu-central-1"
aws_access_key_id = "..."
aws_secret_access_key = "..."
```

**secret name: sources-mongodb**

```toml
[sources.mongodb]
connection_url = "mongodb+srv://user:***@host/db?authSource=admin&tls=true"
```

You can also store single values (more calls required):

For example, the following keys would be fetched similarly to environment variables:

* `sources-pipedrive-pipedrive_api_key`
* `destination-bigquery-credentials-project_id`
* `destination-bigquery-credentials-private_key`
* `destination-bigquery-credentials-client_email`
* `destination-bigquery-location`

:::caution
**Naming convention for Google Secrets**

Secret names are normalized to contain letters, digits, hyphens (-), and underscores (_).

* Punctuation (except `-` and `_`) and whitespace are removed.
* Sections are joined with hyphens, for example:
   * `destination.bigquery.credentials.project_id` → `destination-bigquery-credentials-project_id`
   * `sources.pipedrive.pipedrive_api_key` → `sources-pipedrive-pipedrive_api_key`
   * `destination.bigquery` → `destination-bigquery`
   * `my_pipeline.dlt_secrets_toml` → `my_pipeline-dlt_secrets_toml`
:::

### Notes on `list_secrets`:

When `list_secrets=true`, the provider will pre-list all secret names to skip lookups for non-existent keys.
If the service account lacks `roles/secretmanager.secretViewer`, listing will fail and the provider will raise a configuration error.


## Configure Airflow Variables as provider
You can use Airflow Variables to store secrets and TOML fragments.

### Activate and Configure Airflow Provider
This provider auto-activates when Airflow is installed and typically does not require any configuration to use. You are free to enable listing of variables (reduces number of calls to the backend - important for Airflow 3.0):

```toml
[providers.airflow_secrets]
list_secrets = true
```

:::tip
You can also disable Airflow provider ie. with environment variable:
`PROVIDERS__ENABLE_AIRFLOW_SECRETS=False`
:::

### Add Airflow Variables with secrets

Now you can create Airflow Variables whose values are either:

* entire TOML fragments (recommended), such as **destination**, **sources.**, or a pipeline-scoped `dlt_secrets_toml`
* single values (optional)

Examples of values stored in Airflow Variables with following names:

**my_pipeline.dlt_secrets_toml**

(entire secrets TOML scoped for a pipeline):
```toml
[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id = "<project_id>"
private_key = "-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n"
client_email = "....gserviceaccount.com"

[sources.pipedrive]
pipedrive_api_key = "..."
```

**destination**:
```toml
[destination]
postgres.credentials = "postgresql://loader:***@host:5432/postgres"
```

**sources.mongodb**:
```toml
[sources.mongodb]
connection_url = "mongodb+srv://user:***@host/db?authSource=admin&tls=true"
```

Provider behavior is the same as with other vaults:

* it probes `dlt_secrets_toml` first, then known sections (sources and destination), both globally and pipeline-scoped
* if `only_toml_fragments=true`, it will not attempt single-value lookups
* it caches lookups for the lifetime of the process
