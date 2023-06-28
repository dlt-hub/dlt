# Redshift

The redshift connector is a powerful tool for seamlessly loading data into a database hosted on the redshift cluster. It efficiently transfers data using the [SQL INSERT file format](https://dlthub.com/docs/dlt-ecosystem/file-formats/insert-format), making it highly compatible and easy to work with. 

By using this connector, you can easily build a robust and scalable data warehouse on the redshift platform. Its capabilities streamline the process of populating and normalizing data, ensuring optimal performance and efficient analysis within your data warehousing environment.

## Setup Guide

### 1. Initialize the dlt project
```
dlt init chess redshift
```
> 💡 This command will initialise your pipeline with chess as the source and the AWS redshift as the destination.


The command above creates sample `secrets.toml` and requirements file for redshift. You can install those dependencies by running:
```bash
pip install -r requirements.txt
```
or with `pip install dlt[redshift]` which installs the `dlt` library along with the necessary dependencies for working with AWS redshift as a destination.

### 2. Setup redshift cluster

To load the data to redshift you need to create a redshift cluster with and also allow your IP address access via the VPC inbound rules used in the cluster. 

1. You have the option of using an existing cluster or creating a new one.
2. To create a new cluster, navigate to the 'Provisioned Cluster Dashboard' and click 'Create Cluster'.
3. Specify the required details such as 'Cluster Identifier', 'Node Type', 'Admin User Name', 'Admin Password' and 'Database Name'.
4. In the 'Network and Security' section, you can configure the VPC (Virtual Private Cloud) to be used for the cluster. Remember to add your IP address to the inbound rules of the VPC on AWS.

### 3. Add credentials

1. Next, set up the redshift credentials in the `.dlt/secrets.toml` file as shown below:
    ```toml
    [destination.redshift.credentials]
    database = "please set me up!" # copy your database name here
    password = "please set me up!" # keep your redshift db instance password here
    username = "please set me up!" # keep your redshift db instance username here
    host = "please set me up!" # copy your redshift host from cluster endpoint here
    port = "please set me up!" # enter redshift db instance port number
    connect_timeout = 15 # enter the timeout value
    ```
    
2. The host is derived from the cluster endpoint specified in the cluster's General Configuration. For example:
    ```bash
    # If the endpoint is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com:5439/your_database_name
    # Then the host is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com
    ```
    
3. The connect_timeout is the number of minutes the pipeline will wait before timeout.
4. If you have your credentials stored in `~/.aws/credentials`, simply remove the [destination.redshift.credentials] section above and `dlt` will revert to your default profile in the local       credentials. If you want to change the profile, pass the profile name as follows (here: dlt-ci-user):
    ```toml
    [destination.redshift.credentials]
    aws_profile="dlt-ci-user"
    ```

## Data Loading

All tables are stored in the specified database, with tables, child tables and column schemas derived from the data. With the name of the dataset that you have passed to the `run` or `load` methods of the pipeline. The data is loaded in the following hierarchy: 
```bash
Cluster > Database > Dataset > Table
```
> Note that dlt uses [SQL Insert file format](https://dlthub.com/docs/dlt-ecosystem/file-formats/insert-format) to load data to redshift.

It is important to note that in addition to the tables generated from endpoints, `dlt` will contact additional tables within the dataset.
- `_dlt_loads`: The table tracks complete loads, enabling subsequent transformations. It includes load package information identified by load_id and status (0 for completed loads).
- `_dlt_pipeline_state`: This table stores the version information of the `dlt` library used for loading the data. It helps ensure compatibility and track changes in the library.
- `_dlt_version`: This table stores pipeline state, including pipeline details, run information, and state blob. It facilitates incremental loading and synchronization of the pipeline state with the destination.

## Write disposition
`redshift` destination handles the write dispositions as follows:

- **Full load**: replaces the destination dataset with whatever the source produced on this run. To achieve this, use `write_disposition='replace'` in your resources.
- **Append**: appends the new data to the destination. Use `write_disposition='append'`.
- **Merge**: Merges new data to the destination using `merge_key` and/or deduplicates/upserts new data using `private_key`. Use `write_disposition='merge'`.

This destination also supports incremental loading to read more about incremental loading, refer to the [documentation here.](https://dlthub.com/docs/general-usage/incremental-loading)

## Syncing of `dlt` state
Redshift supports the recovery of pipeline state. The state is stored in the `_dlt_pipeline_state` table at the target and contains information about the pipeline, the pipeline run (to which the state belongs) and the state blob.

dlt has a `dlt pipeline sync` command where you can [get the state back from this table](https://dlthub.com/docs/reference/command-line-interface#sync-pipeline-with-the-destination).

To read more about the state, please read our [documentation here.](https://dlthub.com/docs/general-usage/state) 

## dbt support

This destination [integrates with dbt](https://dlthub.com/docs/dlt-ecosystem/transformations/transforming-the-data#transforming-the-data-using-dbt.) via [dbt-redshift](https://github.com/dbt-labs/dbt-redshift)
