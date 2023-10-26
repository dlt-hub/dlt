---
title: Add credentials
description: How to use dlt credentials
keywords: [credentials, secrets.toml, environment variables]
---

# How to add credentials

## Adding credentials locally

When using a pipeline locally, we recommend using the `.dlt/secrets.toml` method.

To do so, open your dlt secrets file and match the source names and credentials to the ones in your
script, for example:

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
> Note that for toml names are case-sensitive and sections are separated with ".".

For destination credentials, read the [documentation pages for each destination](../dlt-ecosystem/destinations) to create and configure
credentials.

For Verified Source credentials, read the [Setup Guides](../dlt-ecosystem/verified-sources) for each source to find how to get credentials.

Once you have credentials for the source and destination, add them to the file above and save them.

Read more about [credential configuration.](../general-usage/credentials)

## Adding credentials to your deployment

To add credentials to your deployment,

- either use one of the `dlt deploy` commands;
- or follow the instructions to [pass credentials via code](../general-usage/credentials/configuration#pass-credentials-as-code)
or [environment](../general-usage/credentials/config_providers#environment-provider).

### Reading credentials from environment variables

`dlt` supports reading credentials from environment. For example, our `.dlt/secrets.toml` might look like:

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

For environment variables all names are capitalized and sections are separated with double underscore "__".

For example, for the above secrets, we would need to put into environment:

```shell
SOURCES__PIPEDRIVE__PIPEDRIVE_API_KEY
DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID
DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY
DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL
DESTINATION__BIGQUERY__LOCATION
```
