---

title: Databricks
description: Databricks `dlt` destination
keywords: [Databricks, destination, data warehouse]

---

# Databricks
*Big thanks to Evan Phillips and [swishbi.com](https://swishbi.com/) for contributing code, time, and a test environment.*

This Databricks destination will load your data into Databricks Delta tables using one of the supported [cloud storage options](#staging-support). You can access your data using Unity Catalog.

There are two options to run dlt pipelines and load data:

* Run dlt pipelines in any environment by providing credentials for both Databricks and your cloud storage.
* Run dlt pipelines directly within [Databricks notebooks](#direct-load-databricks-managed-volumes) without explicitly providing credentials.

Databricks supports both **Delta** (default) and **Apache Iceberg** table formats. See the [table_format](#apache-iceberg-tables) section for details on using Iceberg tables.

## Install dlt with Databricks

**To install the dlt library with Databricks dependencies:**

```sh
pip install "dlt[databricks]"
```

<!--@@@DLT_DESTINATION_CAPABILITIES databricks-->

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
1. [OAuth2](#using-oauth2) (recommended) allows you to authenticate to Databricks using a service principal via OAuth2 M2M.
2. [Access token](#using-access-token) approach using a developer access token. This method may be deprecated in the future by Databricks.

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
export DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME="MY_DATABRICKS.azuredatabricks.net"
export DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH="/sql/1.0/warehouses/12345"
export DESTINATION__DATABRICKS__CREDENTIALS__CATALOG="my_catalog"
export DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID="XXX"
export DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "MY_DATABRICKS.azuredatabricks.net"
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"]="/sql/1.0/warehouses/12345"
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"]="my_catalog"
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID"]=os.environ.get("CLIENT_ID")
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET"]=os.environ.get("CLIENT_SECRET")
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
export DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME="MY_DATABRICKS.azuredatabricks.net"
export DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH="/sql/1.0/warehouses/12345"
export DESTINATION__DATABRICKS__CREDENTIALS__CATALOG="my_catalog"
export DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "MY_DATABRICKS.azuredatabricks.net"
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"]="/sql/1.0/warehouses/12345"
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"]="my_catalog"
os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"]=os.environ.get("ACCESS_TOKEN")
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

### Using default credentials
If none of auth methods above is configured, `dlt` attempts to get authorization from the Databricks workspace context. The context may
come, for example, from a Notebook (runtime) or via standard set of env variables that Databricks Python sdk recognizes (ie. **DATABRICKS_TOKEN** or **DATABRICKS_HOST**)

`dlt` is able to set `server_hostname` and `http_path` from available warehouses. We use default warehouse id (**DATABRICKS_WAREHOUSE_ID**)
if set (via env variable), or a first one on warehouse's list.

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

1. The following data types are not supported when using the JSONL format with `databricks`: `decimal`, `json`, `date`, `binary`. Use `parquet` if your data contains these types.
2. The `bigint` data type with precision is not supported with the JSONL format.

## Direct Load (Databricks Managed Volumes)

`dlt` now supports **Direct Load**, enabling pipelines to run seamlessly from **Databricks Notebooks** without external staging. When executed in a Databricks Notebook, `dlt` uses the notebook context for configuration if not explicitly provided.

Direct Load also works **outside Databricks**, requiring explicit configuration of `server_hostname`, `http_path`, `catalog`, and authentication (`client_id`/`client_secret` for OAuth or `access_token` for token-based authentication).

The example below demonstrates how to load data directly from a **Databricks Notebook**. Simply specify the **Databricks catalog** and optionally a **fully qualified volume name** (recommended for production) – the remaining configuration comes from the notebook context:

```py
import dlt
from dlt.destinations import databricks
from dlt.sources.rest_api import rest_api_source

# Fully qualified Databricks managed volume (recommended for production)
# - dlt assumes the named volume already exists
staging_volume_name = "dlt_ci.dlt_tests_shared.static_volume"

bricks = databricks(credentials={"catalog": "dlt_ci"}, staging_volume_name=staging_volume_name)

pokemon_source = rest_api_source(
    {
        "client": {"base_url": "https://pokeapi.co/api/v2/"},
        "resource_defaults": {"endpoint": {"params": {"limit": 1000}}},
        "resources": ["pokemon"],
    }
)

pipeline = dlt.pipeline(
    pipeline_name="rest_api_example",
    dataset_name="rest_api_data",
    destination=bricks,
)

load_info = pipeline.run(pokemon_source)
print(load_info)
print(pipeline.dataset().pokemon.df())
```

- If **no** *staging_volume_name* **is provided**, dlt creates a **default volume** automatically.
- **For production**, explicitly setting *staging_volume_name* is recommended.
- The volume is used as a **temporary location** to store files before loading.

:::warning Module conflict
When using dlt within Databricks Notebooks, you may encounter naming conflicts with Databricks' built-in Delta Live Tables (DLT) module.
To avoid these conflicts, follow the steps in the [Troubleshooting section](#troubleshooting) below.
:::

:::tip
You can delete staged files **immediately** after loading by setting the following config option:
```toml
[destination.databricks]
keep_staged_files = false
```
:::

## Supported hints

### Supported table hints

Databricks supports the following table hints:

- `description` - Uses the description to add comment to the table. This can also be done by using the adapter parameter `table_comment`.

Databricks supports the following column hints:

- `primary_key` - adds a primary key constraint to the column in Unity Catalog.
- `description` - adds a description to the column. This can also be done by using the adapter parameter `table_comment`.
- `references` - adds a foreign key constraint to the column in Unity Catalog.
- `not_null` - adds a not null constraint to the column.
- `cluster` - adds a clustering constraint to the column. This can also be done by using the adapter parameter `cluster`.

:::note
If you want to enforce constraints on the tables, you can set the `create_indexes` option to `true`. This will add PRIMARY KEY and FOREIGN KEY constraints to the tables if the hints primary key and references are set.
```toml
[destination.databricks]
# Add PRIMARY KEY and FOREIGN KEY constraints to tables
create_indexes=true
```
:::

For additional hints specific to Databricks, see the [Databricks adapter](#databricks-adapter) section.

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
export DESTINATION__FILESYSTEM__BUCKET_URL="s3://your-bucket-name"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="XXX"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "s3://your-bucket-name"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = os.environ.get("AWS_ACCESS_KEY_ID")
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = os.environ.get("AWS_SECRET_ACCESS_KEY")
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
export DESTINATION__FILESYSTEM__BUCKET_URL="abfss://container_name@storage_account_name.dfs.core.windows.net/path"
export DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME="XXX"
export DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY="XXX"
```
  </TabItem>

<TabItem value="code">

```py
import os

# Do not set up the secrets directly in the code!
# What you can do is reassign env variables.
os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "abfss://container_name@storage_account_name.dfs.core.windows.net/path"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY"] = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
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

## Databricks adapter

You can use the `databricks_adapter` function to add Databricks-specific hints to a resource. These hints influence how data is loaded into Databricks tables, such as adding comments and tags. Hints can be defined at both the column level and table level.

The adapter updates the DltResource with metadata about the destination column and table DDL options.

### Supported hints

**Table-level hints:**
- `cluster`: Column name(s) to cluster the table by, or `"AUTO"` for automatic clustering
- `partition`: Column name(s) to partition the table by (Note: Cannot be used together with `cluster`)
- `table_format`: Table format - `"DELTA"` (default) or `"ICEBERG"` for Apache Iceberg tables
- `table_comment`: Adds a comment to the table. Supports basic markdown format [basic-syntax](https://www.markdownguide.org/cheat-sheet/#basic-syntax)
- `table_tags`: Adds tags to the table. Supports a list of strings and/or key-value pairs
- `table_properties`: Dictionary of table properties for Delta Lake optimization (TBLPROPERTIES)

**Column-level hints:**
- `column_hints`: Dictionary of column-specific hints
  - `column_comment`: Adds a comment to the column. Supports basic markdown format [basic-syntax](https://www.markdownguide.org/cheat-sheet/#basic-syntax)
  - `column_tags`: Adds tags to the column. Supports a list of strings and/or key-value pairs

### Use an adapter to apply hints to a resource

Here is an example of how to use the `databricks_adapter` function to apply hints to a resource on both the column level and table level:

```py
import dlt
from dlt.destinations.adapters import databricks_adapter

@dlt.resource(
    columns=[
        {"name": "event_date", "data_type": "date"},
        {"name": "user_id", "data_type": "bigint"},
        # Other columns.
    ]
)
def event_data():
    yield from [
        {"event_date": datetime.date.today() + datetime.timedelta(days=i)} for i in range(100)
    ]

# Apply table and column options.
databricks_adapter(
    event_data,

    # Table level options.
    table_comment="Dummy event data.",
    table_tags=["pii", {"cost_center": "12345"}],

    # Column level options.
    column_hints={
        "event_date": {"column_comment": "The date of the event"},
        "user_id": {
            "column_comment": "The id of the user",
            "column_tags": ["pii", {"cost_center": "12345"}]
        },
    },
)
```

### Advanced examples

#### Clustering and partitioning

```py
import dlt
from dlt.destinations.adapters import databricks_adapter

@dlt.resource
def sales_data():
    yield from [
        {"sale_date": "2024-01-01", "region": "US", "product_id": 1, "amount": 100.50},
        {"sale_date": "2024-01-02", "region": "EU", "product_id": 2, "amount": 200.75},
        # More data...
    ]

# Use AUTO clustering for automatic optimization
databricks_adapter(
    sales_data,
    cluster="AUTO",  # Let Databricks automatically choose optimal clustering
    table_comment="Sales data with automatic clustering"
)

# Or specify manual clustering and partitioning
@dlt.resource
def partitioned_sales():
    yield from sales_data()

databricks_adapter(
    partitioned_sales,
    partition=["year", "month"],  # Partition by year and month columns
    # Note: Cannot use cluster with partition in Databricks
    table_comment="Partitioned sales data"
)
```

#### Table properties for Delta Lake optimization

```py
import dlt
from dlt.destinations.adapters import databricks_adapter

@dlt.resource
def high_volume_events():
    yield from generate_events()  # Your event generator

databricks_adapter(
    high_volume_events,
    table_properties={
        # Delta Lake optimization properties
        "delta.appendOnly": True,  # Table is append-only
        "delta.logRetentionDuration": "30 days",  # Keep logs for 30 days
        "delta.dataSkippingStatsColumns": "event_date,user_id,event_type",  # Columns for data skipping
        "delta.autoOptimize.optimizeWrite": True,  # Enable optimize write
        "delta.autoOptimize.autoCompact": True,  # Enable auto compaction

        # Custom properties for metadata
        "custom.team": "data-engineering",
        "custom.sla": "tier-1",
        "custom.refresh_frequency": "hourly"
    },
    cluster="event_date",
    table_comment="High-volume event stream with Delta optimizations"
)
```

#### Apache Iceberg tables

Databricks supports Apache Iceberg table format, which provides benefits like better schema evolution and time travel capabilities.

**Important notes:**
- ICEBERG tables support the same data types as Delta tables
- Delta Lake-specific table properties (e.g., `delta.dataSkippingStatsColumns`, `delta.appendOnly`) cannot be used with ICEBERG tables - validation occurs at load time to prevent incompatible configurations

```py
import dlt
from dlt.destinations.adapters import databricks_adapter

@dlt.resource
def customer_data():
    yield from load_customers()  # Your customer data source

# Create an Iceberg table for better schema evolution
databricks_adapter(
    customer_data,
    table_format="ICEBERG",  # Use Apache Iceberg format
    table_properties={
        # Iceberg-specific properties (note: Delta properties are not supported)
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy"
    },
    table_comment="Customer data in Iceberg format for schema evolution"
)
```

#### Complete example with all features

```py
import dlt
from dlt.destinations.adapters import databricks_adapter

@dlt.resource(
    columns=[
        {"name": "transaction_date", "data_type": "date"},
        {"name": "customer_id", "data_type": "bigint"},
        {"name": "amount", "data_type": "decimal"},
        {"name": "region", "data_type": "text"},
        {"name": "year", "data_type": "bigint"},
        {"name": "month", "data_type": "bigint"}
    ]
)
def transactions():
    yield from load_transactions()

# Apply comprehensive Databricks optimizations
databricks_adapter(
    transactions,

    # Partitioning for efficient data organization
    partition=["year", "month"],

    # Note: Cannot use both partition and cluster together in Databricks
    # cluster=["region", "customer_id"],  # Would need separate table without partitioning

    # Table format (DELTA is default)
    table_format="DELTA",

    # Table properties for performance tuning
    table_properties={
        "delta.appendOnly": False,
        "delta.dataSkippingStatsColumns": "transaction_date,amount,customer_id",
        "delta.autoOptimize.optimizeWrite": True,
        "delta.targetFileSize": "128mb",
        "custom.owner": "finance-team"
    },

    # Table documentation
    table_comment="Financial transactions with optimized storage",
    table_tags=["financial", {"compliance": "sox"}],

    # Column documentation
    column_hints={
        "customer_id": {
            "column_comment": "Unique customer identifier",
            "column_tags": ["pii"]
        },
        "amount": {
            "column_comment": "Transaction amount in USD"
        }
    }
)
```

## Troubleshooting
Use the following steps to avoid conflicts with Databricks' built-in Delta Live Tables (DLT) module and enable dltHub integration.

### Enable dlt on serverless (16.x)
Live Tables (DLT) are not available on serverless but the import machinery that is patching DLT is still there in form of import hooks. You
can temporarily disable this machinery to import `dlt` and use it afterwards. In a notebook cell (assuming that `dlt` is already installed):

```sh
%restart_python
```

```py
import sys

# dlt patching hook is the first one on the list
metas = list(sys.meta_path)
sys.meta_path = metas[1:]

# remove RUNTIME - uncomment on dlt before 1.18.0
# import os
# del os.environ["RUNTIME"]

import dlt
sys.meta_path = metas  # restore post import hooks

# use dlt
info = dlt.run([1, 2, 3], destination=dlt.destinations.filesystem("_data"), table_name="digits")
print(info)
```

### Enable dlt on a cluster

#### 1. Add an `init` script
To ensure compatibility with the dltHub's dlt package in Databricks, add an `init` script that runs at cluster startup. This script installs the dlt package from dltHub, renames Databricks’ built-in DLT module to avoid naming conflicts, and updates internal references to allow continued use under the alias `dlt_dbricks`.

1. In your Databricks workspace directory, create a new file named `init.sh` and add the following content:
```sh
#! /bin/bash

# move Databricks' dlt package to a different folder name
mv /databricks/spark/python/dlt/ /databricks/spark/python/dlt_dbricks

# Replace all mentions of `dlt` with `dlt_dbricks` so that Databricks' dlt
# can be used as `dlt_dbricks` in the notebook instead
find /databricks/spark/python/dlt_dbricks/ -type f -exec sed -i 's/from dlt/from dlt_dbricks/g' {} \;

# Replace mentions of `dlt` with `dlt_dbricks` in DeltaLiveTablesHook.py to
# avoid import errors
sed -i "s/'dlt'/'dlt_dbricks'/g" /databricks/python_shell/dbruntime/DeltaLiveTablesHook.py
sed -i "s/from dlt/from dlt_dbricks/g" /databricks/python_shell/dbruntime/DeltaLiveTablesHook.py

# Install dltHub dlt
pip install dlt
```

2. Go to your cluster, click Edit, scroll down to Advanced Options, and open the Init Scripts tab.

3. Under Source, choose Workspace, then browse to your `init.sh` file and click Add.

4. Click Confirm to apply the change. The cluster will restart automatically.

:::warning
The location of `DeltaLiveTablesHook.py` might change when a new Databricks runtime version is released.

The following locations have been confirmed for the two latest LTS runtime versions:
- 16.4 LTS: /databricks/python_shell/lib/dbruntime/dlt/hook.py
- 15.4 LTS: /databricks/python_shell/lib/dbruntime/DeltaLiveTablesHook.py
:::

#### 2. Remove preloaded databricks modules in the notebook
After the cluster starts, Databricks may partially import its built-in Delta Live Tables (DLT) modules, which can interfere with the dlt package from dltHub.

To ensure a clean environment, add the following code at the top of your notebook:
```py
import sys
import types

# 1 Drop Databricks' post-import hook
sys.meta_path = [h for h in sys.meta_path if 'PostImportHook' not in repr(h)]

# 2️ Purge half-initialized Delta-Live-Tables modules
for name, module in list(sys.modules.items()):
    if not isinstance(module, types.ModuleType):
        continue
    if getattr(module, '__file__', '').startswith('/databricks/spark/python/dlt'):
        del sys.modules[name]
```
This ensures the dlt package from dltHub is used instead of the built-in Databricks DLT module.

:::warning
It is best practice to use an `init.sh` script.

Modifying `sys.meta_path` or `sys.modules` is fragile and may break after Databricks updates, potentially causing unexpected issues.
If this workaround is necessary, validate your setup after each platform upgrade.
:::

<!--@@@DLT_TUBA databricks-->

