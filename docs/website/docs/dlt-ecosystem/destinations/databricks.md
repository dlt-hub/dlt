---

title: Databricks
description: Databricks `dlt` destination
keywords: [Databricks, destination, data warehouse]

---

# Databricks
*Big thanks to Evan Phillips and [swishbi.com](https://swishbi.com/) for contributing code, time, and a test environment.*

## Install dlt with Databricks

**To install the dlt library with Databricks dependencies:**

```sh
pip install "dlt[databricks]"
```

## Set up your Databricks workspace

To use the Databricks destination, you need:

* A Databricks workspace with a Unity Catalog metastore connected
* A Gen 2 Azure storage account and container

If you already have your Databricks workspace set up, you can skip to the [Loader setup guide](#loader-setup-guide).

### 1. Create a Databricks workspace in Azure

1. Create a Databricks workspace in Azure

    In your Azure Portal, search for Databricks and create a new workspace. In the "Pricing Tier" section, select "Premium" to be able to use the Unity Catalog.

2. Create an ADLS Gen 2 storage account

    Search for "Storage accounts" in the Azure Portal and create a new storage account.
    Make sure it's a Data Lake Storage Gen 2 account by enabling "hierarchical namespace" when creating the account. Refer to the [Azure documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account) for further information.

3. Create a container in the storage account

    In the storage account, create a new container. This will be used as a datastore for your Databricks catalog.

4. Create an Access Connector for Azure Databricks

    This will allow Databricks to access your storage account.
    In the Azure Portal, search for "Access Connector for Azure Databricks" and create a new connector.

5. Grant access to your storage container

    Navigate to the storage container you created earlier and select "Access control (IAM)" in the left-hand menu.

    Add a new role assignment and select "Storage Blob Data Contributor" as the role. Under "Members" select "Managed Identity" and add the Databricks Access Connector you created in the previous step.

### 2. Set up a metastore and Unity Catalog

1. Now go to your Databricks workspace

    To get there from the Azure Portal, search for "Databricks", select your Databricks, and click "Launch Workspace".

2. In the top right corner, click on your email address and go to "Manage Account"

3. Go to "Data" and click on "Create Metastore"

    Name your metastore and select a region.
    If you'd like to set up a storage container for the whole metastore, you can add your ADLS URL and Access Connector Id here. You can also do this on a granular level when creating the catalog.

    In the next step, assign your metastore to your workspace.

4. Go back to your workspace and click on "Catalog" in the left-hand menu

5. Click "+ Add" and select "Add Storage Credential"

    Create a name and paste in the resource ID of the Databricks Access Connector from the Azure portal.
    It will look something like this: `/subscriptions/<subscription_id>/resourceGroups/<resource_group>/providers/Microsoft.Databricks/accessConnectors/<connector_name>`


6. Click "+ Add" again and select "Add external location"

    Set the URL of your storage container. This should be in the form: `abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>`

    Once created, you can test the connection to make sure the container is accessible from Databricks.

7. Now you can create a catalog

    Go to "Catalog" and click "Create Catalog". Name your catalog and select the storage location you created in the previous step.

## Authentication

`dlt` currently supports two options for authentication:
1. [OAuth2](#oauth) (recommended) allows you to authenticate to Databricks using a service principal via OAuth2 M2M.
2. [Access token](#access_token) approach using a developer access token. This method may be deprecated in the future by Databricks.

### Using OAuth2

You can authenticate to Databricks using a service principal via OAuth2 M2M. To enable it:

1. Follow the instructions in the Databricks documentation: [Authenticate access to Databricks using OAuth M2M](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html)
to create a service principal and retrieve the `client_id` and `client_secret`.

2. Once you have the service principal credentials, update your credentials with any of the options shown below:

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "TOML config provider", "value": "toml"},
    {"label": "Environment variables", "value": "env"},
    {"label": "In the code", "value": "code"},
]}>

  <TabItem value="toml">

```toml
# secrets.toml
[destination.databricks.credentials]
server_hostname = "MY_DATABRICKS.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/12345"
catalog = "my_catalog"
client_id = "XXX"
client_secret = "XXX"
```
  </TabItem>

<TabItem value="env">

```sh
export DESTINATIONS__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME="MY_DATABRICKS.azuredatabricks.net"
export DESTINATIONS__DATABRICKS__CREDENTIALS__HTTP_PATH="/sql/1.0/warehouses/12345"
export DESTINATIONS__DATABRICKS__CREDENTIALS__CATALOG="my_catalog"
export DESTINATIONS__DATABRICKS__CREDENTIALS__CLIENT_ID="XXX"
export DESTINATIONS__DATABRICKS__CREDENTIALS__CLIENT_SECRET="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "MY_DATABRICKS.azuredatabricks.net"
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__HTTP_PATH"]="/sql/1.0/warehouses/12345"
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__CATALOG"]="my_catalog"
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__CLIENT_ID"]=os.environ.get("CLIENT_ID")
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__CLIENT_SECRET"]=os.environ.get("CLIENT_SECRET")
```
</TabItem>
</Tabs>

### Using access token

To create your access token:

1. Click your email in the top right corner and go to "User Settings". Go to "Developer" -> "Access Tokens".
Generate a new token and save it.
2. Set up credentials in a desired way:

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "TOML config provider", "value": "toml"},
    {"label": "Environment variables", "value": "env"},
    {"label": "In the code", "value": "code"},
]}>

  <TabItem value="toml">

```toml
# secrets.toml
[destination.databricks.credentials]
server_hostname = "MY_DATABRICKS.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/12345"
catalog = "my_catalog"
access_token = "XXX"
```
  </TabItem>

<TabItem value="env">

```sh
export DESTINATIONS__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME="MY_DATABRICKS.azuredatabricks.net"
export DESTINATIONS__DATABRICKS__CREDENTIALS__HTTP_PATH="/sql/1.0/warehouses/12345"
export DESTINATIONS__DATABRICKS__CREDENTIALS__CATALOG="my_catalog"
export DESTINATIONS__DATABRICKS__CREDENTIALS__ACCESS_TOKEN="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "MY_DATABRICKS.azuredatabricks.net"
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__HTTP_PATH"]="/sql/1.0/warehouses/12345"
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__CATALOG"]="my_catalog"
os.environ["DESTINATIONS__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"]=os.environ.get("ACCESS_TOKEN")
```
</TabItem>
</Tabs>

## Loader setup guide

**1. Initialize a project with a pipeline that loads to Databricks by running**

```sh
dlt init chess databricks
```

**2. Install the necessary dependencies for Databricks by running**

```sh
pip install -r requirements.txt
```

This will install dlt with the `databricks` extra, which contains the Databricks Python dbapi client.

**3. Enter your credentials into `.dlt/secrets.toml`.**

This should include your connection parameters and your authentication credentials.

You can find your server hostname and HTTP path in the Databricks workspace dashboard. Go to "SQL Warehouses", select your warehouse (default is called "Starter Warehouse"), and go to "Connection details".

Example:

```toml
[destination.databricks.credentials]
server_hostname = "MY_DATABRICKS.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/12345"
client_id = "XXX"
client_secret = "XXX"
catalog = "my_catalog"
```

You can find other options for specifying credentials in the [Authentication section](#authentication).

See [Staging support](#staging-support) for authentication options when `dlt` copies files from buckets.

## Write disposition
All write dispositions are supported.

## Data loading
To load data into Databricks, you must set up a staging filesystem by configuring an Amazon S3 or Azure Blob Storage bucket. Parquet is the default file format used for data uploads. As an alternative to Parquet, you can switch to using JSONL.

dlt will upload the data in Parquet files (or JSONL, if configured) to the bucket and then use `COPY INTO` statements to ingest the data into Databricks.

For more information on staging, see the [Staging support](#staging-support) section below.

## Supported file formats
* [Parquet](../file-formats/parquet.md) supported when staging is enabled.
* [JSONL](../file-formats/jsonl.md) supported when staging is enabled (see limitations below).

The JSONL format has some limitations when used with Databricks:

1. Compression must be disabled to load JSONL files in Databricks. Set `data_writer.disable_compression` to `true` in the dlt config when using this format.
2. The following data types are not supported when using the JSONL format with `databricks`: `decimal`, `json`, `date`, `binary`. Use `parquet` if your data contains these types.
3. The `bigint` data type with precision is not supported with the JSONL format.

## Staging support

Databricks supports both Amazon S3, Azure Blob Storage, and Google Cloud Storage as staging locations. `dlt` will upload files in Parquet format to the staging location and will instruct Databricks to load data from there.

### Databricks and Amazon S3

Please refer to the [S3 documentation](./filesystem.md#aws-s3) for details on connecting your S3 bucket with the `bucket_url` and `credentials`.

Example to set up Databricks with S3 as a staging destination:

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "TOML config provider", "value": "toml"},
    {"label": "Environment variables", "value": "env"},
    {"label": "In the code", "value": "code"},
]}>

  <TabItem value="toml">

```toml
# secrets.toml
[destination.filesystem]
bucket_url = "s3://your-bucket-name"

[destination.filesystem.credentials]
aws_access_key_id="XXX"
aws_secret_access_key="XXX"
```
  </TabItem>

<TabItem value="env">

```sh
export DESTINATIONS__FILESYSTEM__BUCKET_URL="s3://your-bucket-name"
export DESTINATIONS__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="XXX"
export DESTINATIONS__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATIONS__FILESYSTEM__BUCKET_URL"] = "s3://your-bucket-name"
os.environ["DESTINATIONS__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID")
os.environ["DESTINATIONS__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY")
```
</TabItem>
</Tabs>

### Databricks and Azure Blob Storage

Refer to the [Azure Blob Storage filesystem documentation](./filesystem.md#azure-blob-storage) for details on connecting your Azure Blob Storage container with the `bucket_url` and `credentials`.

To enable support for Azure Blob Storage with dlt, make sure to install the necessary dependencies by running:

```sh
pip install "dlt[az]"
```

:::note
Databricks requires that you use ABFS URLs in the following format: `abfss://container_name@storage_account_name.dfs.core.windows.net/path`.
dlt is able to adapt the other representation (i.e., `az://container-name/path`), but we recommend that you use the correct form.
:::

Example to set up Databricks with Azure as a staging destination:

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "TOML config provider", "value": "toml"},
    {"label": "Environment variables", "value": "env"},
    {"label": "In the code", "value": "code"},
]}>

  <TabItem value="toml">

```toml
# secrets.toml
[destination.filesystem]
bucket_url = "abfss://container_name@storage_account_name.dfs.core.windows.net/path"

[destination.filesystem.credentials]
azure_storage_account_name="XXX"
azure_storage_account_key="XXX"
```
  </TabItem>

<TabItem value="env">

```sh
export DESTINATIONS__FILESYSTEM__BUCKET_URL="abfss://container_name@storage_account_name.dfs.core.windows.net/path"
export DESTINATIONS__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME="XXX"
export DESTINATIONS__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATIONS__FILESYSTEM__BUCKET_URL"] = "abfss://container_name@storage_account_name.dfs.core.windows.net/path"
os.environ["DESTINATIONS__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
os.environ["DESTINATIONS__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY"] = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
```
</TabItem>
</Tabs>

### Databricks and Google Cloud Storage

In order to load from Google Cloud Storage stage, you must set up the credentials via a **named credential**. See below. Databricks does not allow you to pass Google Credentials explicitly in SQL statements.

### Use external locations and stored credentials
`dlt` forwards bucket credentials to the `COPY INTO` SQL command by default. You may prefer to use [external locations or stored credentials instead](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html#external-location) that are stored on the Databricks side.

If you set up an external location for your staging path, you can tell `dlt` to use it:
```toml
[destination.databricks]
is_staging_external_location=true
```

If you set up Databricks credentials named, for example, **credential_x**, you can tell `dlt` to use them:
```toml
[destination.databricks]
staging_credentials_name="credential_x"
```

Both options are available from code:
```py
import dlt

bricks = dlt.destinations.databricks(staging_credentials_name="credential_x")
```

## Additional destination capabilities

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-databricks](https://github.com/databricks/dbt-databricks).

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

### Databricks user agent
We enable Databricks to identify that the connection is created by `dlt`.
Databricks will use this user agent identifier to better understand the usage patterns associated with dlt integration. The connection identifier is `dltHub_dlt`.

<!--@@@DLT_TUBA databricks-->

