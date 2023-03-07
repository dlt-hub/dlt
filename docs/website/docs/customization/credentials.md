---
sidebar_position: 2
---

# Credentials



# Adding credentials locally

When using a pipeline locally, we recommend using the `dlt/.secrets.toml` method.

To do so, open your dlt secrets file and match the source names and credentials to the ones in your script, for example


```
[sources.pipedrive]
pipedrive_api_key = "pipedrive_api_key" # please set me up!

[destination.bigquery.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
location = "US"
```

For destination credentials, read this [guide](./destinations/credentials.md) for how create and configure destination credentials.

For source credential, read the source's readme to find how to get credentials.

Once you have credentials for the source and destination, add them to the file above and save them

# Adding credentials to your deployment

To add credentials to your deployment,
- either use one of the dlt deploy commands
- or read the [credentials](./customization/credentials.md) section for your options on how to pass credentials via environment or code.

This page is a work in progress. If you have a question about credentials,
please send us an email at team@dlthub.com. We'd be happy to help you!