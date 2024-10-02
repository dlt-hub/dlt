---
title: Add credentials
description: How to use dlt credentials
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
- or follow the instructions to [pass credentials via code](../general-usage/credentials/advanced#examples) or [environment](../general-usage/credentials/setup#environment-variables).

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

To retrieve secrets from Google Cloud Secret Manager using Python and convert them into a dictionary format, you'll need to follow these steps. First, ensure that you have the necessary permissions to access the secrets on Google Cloud and have the `google-cloud-secret-manager` library installed. If not, you can install it using pip:

```sh
pip install google-cloud-secret-manager
```

[Google Cloud Documentation: Secret Manager client libraries.](https://cloud.google.com/secret-manager/docs/reference/libraries)

Here's how you can retrieve secrets and convert them into a dictionary:

1. **Set up the Secret Manager client**: Create a client that will interact with the Secret Manager API.
2. **Access the secret**: Use the client to access the secret's latest version.
3. **Convert to a dictionary**: If the secret is stored in a structured format (like JSON), parse it into a Python dictionary.

Assume we store secrets in JSON format with the name "temp-secret":
```json
{"api_token": "ghp_Kskdgf98dugjf98ghd...."}
```

Set `.dlt/secrets.toml` as:

```toml
[google_secrets.credentials]
"project_id" = "<project_id>"
"private_key" = "-----BEGIN PRIVATE KEY-----\n....\n-----END PRIVATE KEY-----\n"
"client_email" = "....gserviceaccount.com"
```
or set `GOOGLE_SECRETS__CREDENTIALS` to the path of your service account key file.

Retrieve the secrets stored in the Secret Manager as follows:

```py
import json as json_lib  # Rename the json import to avoid name conflict

import dlt
from dlt.sources.helpers import requests
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from google.cloud import secretmanager

@with_config(sections=("google_secrets",))
def get_secret_dict(secret_id: str, credentials: GcpServiceAccountCredentials = dlt.secrets.value) -> dict:
    """
    Retrieve a secret from Google Cloud Secret Manager and convert it to a dictionary.
    """
    # Create the Secret Manager client with provided credentials
    client = secretmanager.SecretManagerServiceClient(credentials=credentials.to_native_credentials())

    # Build the resource name of the secret version
    name = f"projects/{credentials.project_id}/secrets/{secret_id}/versions/latest"

    # Access the secret version
    response = client.access_secret_version(request={"name": name})

    # Decode the payload to a string and convert it to a dictionary
    secret_string = response.payload.data.decode("UTF-8")
    secret_dict = json_lib.loads(secret_string)

    return secret_dict

# Retrieve secret data as a dictionary for use in other functions.
secret_data = get_secret_dict("temp-secret")

# Set up the request URL and headers
url = "https://api.github.com/orgs/dlt-hub/repos"
headers = {
    "Authorization": f"token {secret_data['api_token']}",  # Use the API token from the secret data
    "Accept": "application/vnd.github+json",  # Set the Accept header for GitHub API
}

# Make a request to the GitHub API to get the list of repositories
response = requests.get(url, headers=headers)

# Set up the DLT pipeline
pipeline = dlt.pipeline(
    pipeline_name="quick_start", destination="duckdb", dataset_name="mydata"
)
# Run the pipeline with the data from the GitHub API response
load_info = pipeline.run(response.json())
# Print the load information to check the results
print(load_info)
```

### Points to note:

- **Permissions**: Ensure the service account or user credentials you are using have the necessary permissions to access the Secret Manager and the specific secrets.
- **Secret format**: This example assumes that the secret is stored in a JSON string format. If your secret is in a different format, you will need to adjust the parsing method accordingly.
- **Google Cloud authentication**: Make sure your environment is authenticated with Google Cloud. This can typically be done by setting credentials in `.dlt/secrets.toml` or setting the `GOOGLE_SECRETS__CREDENTIALS` environment variable to the path of your service account key file or the dict of credentials as a string.

