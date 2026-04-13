---
title: dlt Connector App
description: How to use the dlt Connector App
keywords: [snowflake, native app, dlt connector app]
---

# dlt Connector App

The dlt Connector App is a Snowflake Native App that lets you move data from external SQL databases (PostgreSQL, MySQL, MSSQL) into Snowflake using a simple web UI. It runs entirely within your Snowflake account — no external infrastructure required.

You can: 
- connect an external [SQL database](../../dlt-ecosystem/verified-sources/sql_database) to Snowflake
- create one or more [pipelines](../../general-usage/pipeline) (each pipeline defines what to load and where)
- run pipelines on demand
- schedule pipelines to run automatically
- monitor runs and inspect logs 

without leaving Snowflake.



Under the hood, the app uses Snowflake-managed compute and [dlt](https://dlthub.com/product/dlt) to [extract](../../reference/explainers/how-dlt-works#extract), [normalize](../../reference/explainers/how-dlt-works#normalize), and [load](../../reference/explainers/how-dlt-works#load) data into your destination database, while keeping credentials stored securely in your account. 

This documentation explains how to set up sources, create and manage pipelines, monitor runs, and troubleshoot common issues.

---

# How to use the Snowflake Native App
## Prerequisites
Before creating your first pipeline, make sure you have:
1. A destination database in Snowflake where the loaded data should land
2. A role with permissions to approve External Access Integrations (`ACCOUNTADMIN`, or a role with that privilege)
3. Connection details for your source database, including:
    - host + port
    - database name / schema
    - username + password 
4. (Optional) An S3 bucket if you plan to stage data externally

## Install and open the app
Use the link we provided to you or find and install the app via the [Snowflake Marketplace](https://app.snowflake.com/marketplace).


## Set up connection

Before creating a [pipeline](../../general-usage/pipeline), you need to set up a connection — a secure, approved link between your Snowflake account and your source database.

Go to the Connections tab. It has three sub-tabs: **Create**, **Manage**, and **Approve**.

Snowflake controls outbound network access through [External Access Integrations (EAIs)](https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access). Creating one requires an `ACCOUNTADMIN` to approve the connection before it can be used. Once approved, the connection is available to every role that has access to the app.

The app handles this for you: it creates a specification requesting the EAI, and an admin approves it in the **Connections → Approve** tab.

![Create Connection](https://storage.googleapis.com/dlt-blog-images/sna_ui.png)

### Create a connection

In the **Connections → Create** tab, select the connection type:

- **database** - for PostgreSQL, MySQL, or MSSQL sources
- **stage** - for an S3 bucket used as an intermediate staging area


For a database connection, fill in:

- **Connection name** - a unique label (e.g. rfam_public_db)
- **Database type** - MySQL, PostgreSQL, or MSSQL
- **Host** - hostname of the source database (e.g. mysql-rfam-public.ebi.ac.uk)
- **Port** - leave empty to use the default port for the selected database type
- **Username** - database user
- **Password** - stored securely as a Snowflake secret


Click **Create connection**. This creates a Network Rule (specifying the allowed host and port), a Secret (storing the credentials), and an External Access Integration — all without requiring admin involvement. Finally, it submits an approval request that an `ACCOUNTADMIN` must review before the connection becomes active.

### Approve a connection
Once a connection is awaiting approval, an `ACCOUNTADMIN` must approve it. In the **Connections → Approve tab**, click Review pending connections to open Snowflake's native approval UI.

After approval, the connection status automatically changes to `ACTIVE` and it becomes available for use in pipelines.

### Manage connections
The **Connections → Manage** tab shows all your connections and their current status.
You can delete a connection from here. This removes all associated objects (Network Rule, Secret, External Access Integration, and Specification).


## Create a pipeline
1. Go to the pipeline tab in the UI
2. Enter a pipeline name
3. Click the **+** button to create the pipeline
4. Fill in the configurations

![add a pipeline](https://storage.googleapis.com/dlt-blog-images/sna_create_pipeline.png)



## Fill in the configuration
- **[Pipeline](../../general-usage/pipeline) name**  
  A unique ID for this pipeline. It is used to store configuration and to identify runs.


### Source settings

Source settings define **what to ingest** from your external SQL database and **how to connect** to it. These values are used to configure the `dlt.sources.sql_database` pipeline.

#### Required fields

- **Database type**  
  Select the type of [SQL database](../../dlt-ecosystem/verified-sources/sql_database) you want to ingest from. The selected type determines the driver and default connection behavior. 

  The following database types are currently supported:
  - MySQL
  - PostgreSQL
  - MSSQL

  If you need support for a different database type, please reach out via our [contact form](https://dlthub.com/contact).

- **Host and port**  
  Enter the hostname and port of your source database. If you leave the port empty, the app will use the database type’s default port.

- **Database name**  
  The name of the source database to connect to.

- **Database connection**
    Select an active connection from the dropdown. If no connections appear, go to the Connections tab and complete setup first.


#### Optional fields

- **Schema name**  
  Specify a [schema](../../general-usage/schema) to ingest from. Leave empty to use the default schema of the database/user.

- **Chunk size**  
  Number of rows fetched per batch during extraction. Larger values reduce round trips but increase memory usage.

- **Table backend**  
  Choose the backend used to introspect and read tables. If unsure, keep the default **SQLAlchemy**.  
  See the backend options in the [verified source docs](../../dlt-ecosystem/verified-sources/sql_database/configuration#configuring-the-backend).

- **JSON format configuration (advanced)**  
    Provide additional `sql_database` source options not exposed in the UI. Values are passed as keyword arguments to dlt’s `sql_database` source [function](../../api_reference/dlt/sources/sql_database/__init__) and override UI values if the same option is set in both places.

    Example:

    ```json
    {
        "chunk_size": 10000,
        "reflection_level": "minimal"
    }
    ```

### Table Settings
Table settings control **which tables are ingested** and allow **per-table configuration**.

![tables setting](https://storage.googleapis.com/dlt-blog-images/sna_table_setting_new.png)

#### Tables to ingest
Choose whether to ingest:

- **All** tables from the selected schema, or  
- a **Subset** of tables.

If you choose **Subset**, add the tables you want to ingest. If you choose **All**, you may still list specific tables below to customize their settings. Any tables not listed will be ingested using default configuration.


#### Optional fields (per configured table)

- **[Write disposition](../../general-usage/incremental-loading#choosing-a-write-disposition)**  
  Choose how dlt writes data to the destination (e.g., append, replace, merge) depending on what the UI offers.

- **Primary key**  
  Column (or list of columns) that uniquely identifies rows in the table. Used for deduplication and merge-based loading.

- **Max chunks**  
  Maximum number of chunks to extract. `max_chunks × chunk_size` defines the maximum rows extracted. Set to **0** for no limit. Passed to dlt as `max_items` via `add_limit()`.

- **[Incremental loading](../../general-usage/incremental-loading)**  
  Enable cursor-based incremental loading for the table. Only new or changed rows since the last run are extracted.

- **Table JSON configuration (advanced)**  
  Provide per-table hints not available in the UI. Values override UI settings when the same option is set in both places. These are passed as keyword arguments to dlt’s `apply_hints()` [function](../../api_reference/dlt/extract/hints#apply_hints) for the table resource.


    Example:
    ```json
    {
        "merge_key": "id",
        "columns": {
           "created_at": {
                "dedup_sort": "desc"
            },
            "deleted_flag": {
                "hard_delete": true
            }
        }
    }
    ```
### Destination Settings
Destination settings define where in Snowflake the data is loaded and allow advanced destination tuning.
- **Destination database**:
    Name of the Snowflake database to load into.
- **Check privileges**:
    Validates that the app has access to the destination database. If permissions are missing, the app will request/grant the required access.

![destination settings](https://storage.googleapis.com/dlt-blog-images/sna_check_privileges.png)

- **Destination JSON configuration**:
    Provide additional destination options not exposed in the UI. Values override UI settings when the same option is set in both places. These are passed as keyword arguments to dlt’s Snowflake destination [factory](../../api_reference/dlt/destinations/impl/snowflake/factory).
    
    Example:
    ```json
    {
        "create_indexes": true,
        "replace_strategy": "staging-optimized"
    }
    ```

### Pipeline Settings
This section contains optional pipeline-level defaults that affect how and where data is written.
- **Dataset name**:
    Destination schema name for the pipeline output. If not set, the app derives it from the pipeline name as `<pipeline_name>_dataset`.
- **[Dev mode](../../general-usage/pipeline#do-experiments-with-dev-mode)**:
    Enables dlt dev mode. When enabled, dlt appends a datetime-based suffix to the dataset name, producing a fresh schema on each run. This is intended for testing and avoids overwriting existing datasets.

- **[Staging](../../dlt-ecosystem/destinations/snowflake#staging-support)**  
  Staging location used during loading:
  - **Internal staging** in Snowflake, or  
  - **External staging** via a configured external stage (if supported).

### Compute settings

Compute settings control the Snowflake resources used to run and load the pipeline.  
Fields may vary depending on what the app exposes in your environment.

- **Compute pool**  
  The compute pool runs the job service responsible for [extracting](../../reference/explainers/how-dlt-works#extract) data from the source database and [normalizing](../../reference/explainers/how-dlt-works#normalize) it with dlt.

- **Warehouse**  
  The warehouse is used for [loading](../../reference/explainers/how-dlt-works#load) the extracted/normalized data into the destination database.

- **[Instance family](https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool)**  
  The instance family of the compute pool the ingestion job runs on. This determines the CPU/memory profile available to the job container.

- **Auto-suspend (compute pool)**  
  Number of seconds of inactivity after which the compute pool is automatically suspended.  
  Set to **0** to disable auto-suspend.


## Edit a pipeline
1. Open pipeline tab 
2. Click **edit** button
3. Fill in the configurations
4. Save


## Monitor runs and logs

Go to the **Runs** tab to see a history list of the Jobs. 

Click the **view details** button to view:
- pipeline name
- Job ID
- triggered by 
- triggered at
- started at
- ended at
- status
Statuses typically move through:
`STARTING → RUNNING → SUCCESS or FAILED`.

Click the **view logs** tab to see the logs of the job:
- When each pipeline stage (extract, normalize, load) starts and finishes
- Progress information for each stage 
- Performance metrics (processing time, memory usage, CPU, ...)

![schedule a pipeline](https://storage.googleapis.com/dlt-blog-images/sna_logs.png)



## Schedule a pipeline

The app uses Snowflake [Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro) to run pipelines on a schedule.

To schedule a pipeline:

1. Open the pipeline’s tab.
2. Click the **scheduling** button
3. Enable scheduling
4. Enter a schedule string
5. Click Save


![schedule a pipeline](https://storage.googleapis.com/dlt-blog-images/sna_schedul.png)


This operation creates or updates a Snowflake task that triggers the pipeline according to the specified schedule. To stop the task, set its status to `SUSPENDED`. You can do this either via SQL:

```sql
ALTER TASK [ IF EXISTS ] <name> RESUME | SUSPEND
```

or directly in the Snowflake UI by changing the task’s status:

![change task status](https://storage.googleapis.com/dlt-blog-images/sna_suspend.png)



