---
title: Amazon Redshift
description: Amazon Redshift `dlt` destination
keywords: [redshift, destination, data warehouse]
---

# Amazon Redshift
Amazon Redshift is a fully managed, petabyte-scale data warehouse service in the cloud provided by Amazon Web Services (AWS). 

Redshift can be used as a destination for your data pipelines using `dlt`.

## Supported File Format
[SQL INSERT](https://dlthub.com/docs/dlt-ecosystem/file-formats/insert-format) file format is the default format `dlt` supports loading data into Redshift. 

## Setup Guide
### 1. Initialize the dlt project
Let's start by initializing a new dlt project as follows:
```
dlt init chess redshift
```
> 💡 This command will initialize your pipeline with chess as the source and Redshift as the destination.

The above command generates several files and directories, including `secrets.toml` and a requirements file for Redshift. You can install the necessary dependencies specified in the requirements file by executing it as follows:
```bash
pip install -r requirements.txt
```
or with `pip install dlt[redshift]` which installs the `dlt` library and the necessary dependencies for working with Amazon Redshift as a destination.


### 2. Setup Redshift cluster

To load the data into Redshift, you must create a Redshift cluster and allow access to your IP address via the VPC inbound rules used in the cluster.

1. You can use an existing cluster or create a new one.
2. To create a new cluster, navigate to the 'Provisioned Cluster Dashboard' and click 'Create Cluster’.
3. Specify the required details such as 'Cluster Identifier', 'Node Type', 'Admin User Name', 'Admin Password’, and 'Database Name’.
4. In the 'Network and Security' section, you can configure the cluster's VPC (Virtual Private Cloud). Remember to add your IP address to the inbound rules of the VPC on AWS.

### 3. Add credentials

1. Next, set up the Redshift credentials in the `.dlt/secrets.toml` file as shown below:
    ```toml
    [destination.redshift.credentials]
    database = "please set me up!" # copy your database name here
    password = "please set me up!" # keep your redshift db instance password here
    username = "please set me up!" # keep your redshift db instance username here
    host = "please set me up!" # copy your redshift host from cluster endpoint here
    port = "please set me up!" # enter redshift db instance port number
    connect_timeout = 15 # enter the timeout value
    ```
    
2. The host is derived from the cluster endpoint specified in the “General Configuration.” For example:
    
    ```bash
    # If the endpoint is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com:5439/your_database_name
    # Then the host is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com
    ```
    
3. The `connect_timeout` is the number of minutes the pipeline will wait before the timeout.

4. If your credentials are stored in ~/.aws/credentials, remove the [destination.redshift.credentials] section above, and dlt will revert to your default profile in the local credentials. If you want to change the profile, pass the profile name as follows (here: dlt-ci-user):
   ```toml
   [destination.redshift.credentials]
   aws_profile="dlt-ci-user"
   ```

### Write disposition[](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#write-disposition)

Redshift destination handles the write dispositions as follows:

- `replace`: completely overwrites the data in the destination dataset with the data produced by the source. Use `write_disposition='replace'` in your resources to achieve this.
- `append`: appends the new data to the destination leaving the already loaded data unchanged. Use `write_disposition='append'` in your resources to achieve this.
- `merge`: merges new data to the destination using `merge_key` and deduplicates/upserts new data using `private_key`. Use `write_disposition='merge'` in your resources to achieve this.

This destination also supports incremental loading. To read more about incremental loading, please review the [documentation here.](https://dlthub.com/docs/general-usage/incremental-loading)

### Data Loading

All tables are saved in the designated database, with the table structures, child tables, and column schemas derived from the data. The dataset name you provide to the pipeline's `run` or `load` methods is used. 

> When `dlt` processes data from various endpoints, it generates tables corresponding to those endpoints and creates additional tables within the dataset. This is part of the [normalization process.](https://dlthub.com/docs/general-usage/schema#data-normalizer)

In addition to the tables corresponding to the endpoints, the following tables are also generated in the dataset:

- `_dlt_loads`: The table tracks complete loads, enabling subsequent transformations. It includes load package information identified by load_id and status (0 for completed loads).
- `_dlt_pipeline_state`: This table stores the version information of the `dlt` library used for loading the data. It helps ensure compatibility and track changes in the library.
- `_dlt_version`: This table stores pipeline state, including pipeline details, run information, and state blob. It facilitates incremental loading and synchronization of the pipeline state with the destination.
  
### Supported Column Hints

Amazon Redshift column hints provide additional information or instructions about handling the data. Specifically:

- `cluster` hint defines the table's distribution in Redshift.
    - When this hint is applied to a column, it becomes the DISTKEY for the table in Redshift. This means that Redshift will distribute the rows of the table across its nodes based on the values in this column.
    - This can significantly impact the performance of queries and joins. You can find more information about this in the [best practices for distribution style](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html).
- `sort` hint specifies that a column should be sortable or have an order.
    - In Redshift, this translates to the column being a SORTKEY.
    - Redshift uses the SORTKEY to physically order the rows on disk, which can improve the speed of range queries and joins. More details can be found in the [choose best sort key practices](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html).

So that you know, each destination can interpret these hints in its way. For example, while Redshift uses the `cluster` hint for table distribution, Redshift uses it to specify a cluster column. Other databases like Duckdb and Postgres ignore it when creating tables. You can find more information about this in the [dlt documentation](https://dlthub.com/docs/general-usage/schema#column-hint-rules).

### Additional destination options

### dbt support

- This destination [integrates with dbt](https://dlthub.com/docs/dlt-ecosystem/transformations/dbt) via [dbt-redshift](https://github.com/dbt-labs/dbt-redshift).
- If explicitly defined, credentials are shared with dbt and other settings such as location, retries, and timeouts.
- In the case of implicit credentials (i.e., available in the cloud feature), dlt shares the project_id and delegates the retrieval of certificates to the dbt adapter.

### Syncing of `dlt` state

- Redshift supports the recovery of the pipeline state. The state is stored in the `_dlt_pipeline_state` table at the target and contains information about the pipeline, the pipeline run (to which the state belongs), and the state blob.
- dlt has a `dlt pipeline sync` command to [get the state back from this table](https://dlthub.com/docs/reference/command-line-interface#sync-pipeline-with-the-destination).
- To read more about the state, please read our [documentation here.](https://dlthub.com/docs/general-usage/state)
