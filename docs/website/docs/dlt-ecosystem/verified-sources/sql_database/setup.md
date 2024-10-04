---
title: Setup
description: basic steps for setting up a dlt pipeline for SQL Database
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Setup

<Header/>

To connect to your SQL database using `dlt`, follow these steps:

1. Initialize a `dlt` project in the current working directory by running the following command:

    ```sh 
    dlt init sql_database duckdb
    ```

    This will add necessary files and configurations for a `dlt` pipeline with SQL database as the source and
   [DuckDB](../../destinations/duckdb.md) as the destination.

:::tip
If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../../destinations).
:::

2. Add credentials for your SQL database

    To connect to your SQL database, `dlt` would need to authenticate using necessary credentials. To enable this, paste your credentials in the `secrets.toml` file created inside the `.dlt/` folder in the following format:
    ```toml
    [sources.sql_database.credentials]
    drivername = "mysql+pymysql" # driver name for the database
    database = "Rfam" # database name
    username = "rfamro" # username associated with the database
    host = "mysql-rfam-public.ebi.ac.uk" # host address
    port = "4497" # port required for connection
    ```

    Alternatively, you can also authenticate using connection strings:
    ```toml
    [sources.sql_database.credentials]
    credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    ```

    To learn more about how to add credentials into your `sql_database` pipeline, see [here](./configuration#configuring-the-connection).  

3. Add credentials for your destination (if necessary)  

    Depending on which [destination](../../destinations) you're loading into, you might also need to add your destination credentials. For more information, read the [General Usage: Credentials.](../../../general-usage/credentials)

4. Install any necessary dependencies  

    ```sh
    pip install -r requirements.txt
    ```

5. Run the pipeline  

    ```sh
    python sql_database_pipeline.py
    ```

    Executing this command will run the example script `sql_database_pipeline.py` created in step 1. In order for this to run successfully, you will need to pass the names of the databases and/or tables you wish to load. 
    See the [section on configuring the sql_database source](./configuration#configuring-the-sql-database-source) for more details.


6. Make sure everything is loaded as expected with  
    ```sh
    dlt pipeline <pipeline_name> show
    ```

   :::note
   The pipeline_name for the above example is `rfam`, you may also use any
   custom name instead. 
   :::  

