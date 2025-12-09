---
title: Database Connector App
description: How to use the database connector app
keywords: [snowflake, native app, database connector app]
---

# Database Connector App

The dltHub Database Connector App is a Snowflake Native App that lets you move data from your source database into Snowflake using a simple UI. 
You can: 
- define pipelines 
- configure connections 
- load settings
- run ingestions on demand or on a schedule
without leaving Snowflake. 
Under the hood, the app uses Snowflake-managed compute and dlt to extract, normalize, and load data into your destination database, while keeping credentials stored securely in your account. 
This documentation explains how to set up sources, create and manage pipelines, monitor runs, and troubleshoot common issues.


# How to use the Snowflake Native App
## What you can do with the app:
With the Database Connector App you can:
- connect an external SQL database to Snowflake
- create one or more pipelines (each pipeline defines what to load and where)
- run pipelines on demand
- schedule pipelines to run automatically
- monitor runs and inspect logs

## Prerequisites
Before creating your first pipeline, make sure you have:
1. A destination database in Snowflake where the loaded data should land
2. Role with permissions to create references requested by the app (External Access Integrations)
3. Connection details for your source database, including:
- host + port
- database name / schema
- username + password (or other supported auth)
4. (Optional) An S3 bucket if you plan to stage data externally

## Install and open the app
Use the link we provided to you or download the app via the [Snowflake Marketplace](https://app.snowflake.com/marketplace).

## Create a pipeline
1. Go to the pipeline tab in the UI
2. Enter a pipeline name
3. Click the **+**-button to create the pipeline
4. Fill in the configurations


## Fill in the configuration
- **Pipeline name**  
  A unique ID for this pipeline. It is used to store configuration and to identify runs.

---

### Source settings

Source settings define **what to ingest** from your external SQL database and **how to connect** to it. These values are used to configure the `dlt.sources.sql_database` pipeline.

#### Required fields

- **Database type**  
  Select the type of SQL database you want to ingest from (e.g. Postgres, MySQL, ...).  
  The selected type determines the driver and default connection behavior.

- **Host and port**  
  Enter the hostname and port of your source database.  
  If you leave the port empty, the app will use the database type’s default port.

- **Database name**  
  The name of the source database to connect to.

- **Network rule**  
  Allow external access to the source database endpoint.  
  This allows the app to reach your database through the configured External Access Integration.

- **Database credentials**  
  Enter the username and password for your source database.  
  Credentials are stored in your Snowflake account as secrets.


#### Optional fields

- **Schema name**  
  Specify a schema to ingest from.  
  Leave empty to use the default schema of the database/user.

- **Chunk size**  
  Number of rows fetched per batch during extraction.  
  Larger values reduce round trips but increase memory usage.

- **Table backend**  
  Choose the backend used to introspect and read tables.  
  If unsure, keep the default **SQLAlchemy**.  
  See the backend options in the [verified source docs](../../dlt-ecosystem/verified-sources/sql_database/configuration#configuring-the-backend).

- **JSON format configuration (advanced)**  
    Provide additional `sql_database` source options not exposed in the UI. 
    Values are passed as keyword arguments to dlt’s `sql_database` source function and override UI values if the same option is set in both places.

    Example:

    ```json
    {
        "chunk_size": 10000,
        "reflection_level": "minimal"
    }
    ```

### Table Settings
Table settings control **which tables are ingested** and allow **per-table configuration**.

#### Tables to ingest
Choose whether to ingest:

- **All** tables from the selected schema, or  
- a **Subset** of tables.

If you choose **Subset**, add the tables you want to ingest.  
If you choose **All**, you may still list specific tables below to customize their settings.  
Any tables not listed will be ingested using default configuration.


#### Optional fields (per configured table)

- **Write disposition**  
  Choose how dlt writes data to the destination (e.g., append, replace, merge) depending on what the UI offers.

- **Primary key**  
  Column (or list of columns) that uniquely identifies rows in the table.  
  Used for deduplication and merge-based loading.

- **Max chunks**  
  Maximum number of chunks to extract.  
  `max_chunks × chunk_size` defines the maximum rows extracted.  
  Set to **0** for no limit. Passed to dlt as `max_items` via `add_limit()`.

- **Incremental loading**  
  Enable cursor-based incremental loading for the table.  
  Only new or changed rows since the last run are extracted.

- **Table JSON configuration (advanced)**  
  Provide per-table hints not available in the UI.  
  Values override UI settings when the same option is set in both places.  
  These are passed as keyword arguments to dlt’s `apply_hints()` for the table resource.


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
- **Destination databse**:
    Name of the Snowflake database to load into.
- **Check privileges**:
    Validates that the app has access to the destination database.
    If permissions are missing, the app will request/grant the required access.
- **Destination JSON configuration**:
    Provide additional destination options not exposed in the UI.
    Values override UI settings when the same option is set in both places.
    These are passed as keyword arguments to dlt’s Snowflake destination factory.
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
    Destination schema name for the pipeline output.  
    If not set, the app derives it from the pipeline name as `<pipeline_name>_dataset`.
- **Dev mode**:
    Enables dlt dev mode. When enabled, dlt appends a datetime-based suffix to the dataset name, producing a fresh schema on each run. This is intended for testing and avoids overwriting existing datasets.

- **Staging**  
  Staging location used during loading:
  - **Internal staging** in Snowflake, or  
  - **External staging** via a configured external stage (if supported).

### Compute settings

Compute settings control the Snowflake resources used to run and load the pipeline.  
Fields may vary depending on what the app exposes in your environment.

- **Compute pool**  
  The compute pool runs the job service responsible for extracting data from the source database and normalizing it with dlt.

- **Warehouse**  
  The warehouse is used for loading the extracted/normalized data into the destination database.

- **Instance family**  
  The instance family of the compute pool the ingestion job runs on.  
  This determines the CPU/memory profile available to the job container.

- **Auto-suspend (compute pool)**  
  Number of seconds of inactivity after which the compute pool is automatically suspended.  
  Set to **0** to disable auto-suspend.


## Edit a pipeline
1. Open pipeline tab 
2. Click **edit** button
3. Fill in the configurations
4. Safe

## Run a pipeline manually
1. Open the pipeline tab
2. Click the **run now** button
3. The app will:
    - create a new run record
    TODO
    

## Monitor runs and logs

Go to the Runs tab
You’ll see a history list of the Jobs. 
- pipeline name
- Job ID
- triggered by (manual / task)
- timestamps
- status

Click a run to open details:
- current stage / progress
- warnings or errors
- log output

Statuses typically move through:
STARTING → RUNNING → SUCCESS or FAILED.

## Schedule a pipeline
Open a pipeline tab and click the **scheduling** button.
1. Enable scheduling.
2. Choose a schedule string (e.g. cron or “every X minutes/hours/days”, depending on UI).
3. Save.

This creates or updates a Snowflake task that triggers the pipeline on the given schedule. If you want to stop it, change the status to `suspended`.

