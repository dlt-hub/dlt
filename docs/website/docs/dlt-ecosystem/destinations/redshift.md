---
title: Amazon Redshift
description: Amazon Redshift `dlt` destination
keywords: [redshift, destination, data warehouse]
---

# Amazon Redshift

## Setup Guide
### 1. Initialize the dlt project

Let's start by initializing a new dlt project as follows:

```bash
dlt init chess redshift
```
> 💡 This command will initialize your pipeline with chess as the source and Redshift as the destination.

The above command generates several files and directories, including `.dlt/secrets.toml` and a requirements file for Redshift. You can install the necessary dependencies specified in the requirements file by executing it as follows:
```bash
pip install -r requirements.txt
```
or with `pip install dlt[redshift]`, which installs the `dlt` library and the necessary dependencies for working with Amazon Redshift as a destination.

### 2. Setup Redshift cluster
To load data into Redshift, it is necessary to create a Redshift cluster and enable access to your IP address through the VPC inbound rules associated with the cluster. While we recommend asking our GPT-4 assistant for details, we have provided a general outline of the process below:

1. You can use an existing cluster or create a new one.
2. To create a new cluster, navigate to the 'Provisioned Cluster Dashboard' and click 'Create Cluster'.
3. Specify the required details such as 'Cluster Identifier', 'Node Type', 'Admin User Name', 'Admin Password', and 'Database Name'.
4. In the 'Network and Security' section, you can configure the cluster's VPC (Virtual Private Cloud). Remember to add your IP address to the inbound rules of the VPC on AWS.

### 3. Add credentials

1. Next, set up the Redshift credentials in the `.dlt/secrets.toml` file as shown below:

    ```toml
    [destination.redshift.credentials]
    database = "please set me up!" # copy your database name here
    password = "please set me up!" # keep your redshift db instance password here
    username = "please set me up!" # keep your redshift db instance username here
    host = "please set me up!" # copy your redshift host from cluster endpoint here
    port = 5439
    connect_timeout = 15 # enter the timeout value
    ```

2. The "host" is derived from the cluster endpoint specified in the “General Configuration.” For example:

    ```bash
    # If the endpoint is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com:5439/your_database_name
    # Then the host is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com
    ```

3. The `connect_timeout` is the number of minutes the pipeline will wait before the timeout.

## Write disposition

All [write dispositions](../../general-usage/incremental-loading#choosing-a-write-disposition) are supported.

## Supported file formats
[SQL Insert](../file-formats/insert-format) is used by default.

## Supported column hints

Amazon Redshift supports the following column hints:

- `cluster` - hint is a Redshift term for table distribution. Applying it to a column makes it the "DISTKEY," affecting query and join performance. Check the following [documentation](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html) for more info.
- `sort` - creates SORTKEY to order rows on disk physically. It is used to improve a query and join speed in Redshift, please read the [sort key docs](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html) to learn more.

## Additional destination options
### dbt support

- This destination [integrates with dbt](../transformations/dbt) via [dbt-redshift](https://github.com/dbt-labs/dbt-redshift). Credentials and timeout settings are shared automatically with `dbt`.

### Syncing of `dlt` state
- This destination fully supports [dlt state sync.](../../general-usage/state#syncing-state-with-destination)
