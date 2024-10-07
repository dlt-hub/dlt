---
title: Amazon Redshift
description: Amazon Redshift `dlt` destination
keywords: [redshift, destination, data warehouse]
---

# Amazon Redshift

## Install dlt with Redshift
**To install the dlt library with Redshift dependencies:**
```sh
pip install "dlt[redshift]"
```

## Setup guide
### 1. Initialize the dlt project

Let's start by initializing a new dlt project as follows:

```sh
dlt init chess redshift
```
> 💡 This command will initialize your pipeline with chess as the source and Redshift as the destination.

The above command generates several files and directories, including `.dlt/secrets.toml` and a requirements file for Redshift. You can install the necessary dependencies specified in the requirements file by executing it as follows:
```sh
pip install -r requirements.txt
```
or with `pip install "dlt[redshift]"`, which installs the `dlt` library and the necessary dependencies for working with Amazon Redshift as a destination.

### 2. Setup Redshift cluster
To load data into Redshift, you need to create a Redshift cluster and enable access to your IP address through the VPC inbound rules associated with the cluster. While we recommend asking our GPT-4 assistant for details, we have provided a general outline of the process below:

1. You can use an existing cluster or create a new one.
2. To create a new cluster, navigate to the 'Provisioned Cluster Dashboard' and click 'Create Cluster'.
3. Specify the required details such as 'Cluster Identifier', 'Node Type', 'Admin User Name', 'Admin Password', and 'Database Name'.
4. In the 'Network and Security' section, you can configure the cluster's VPC (Virtual Private Cloud). Remember to add your IP address to the inbound rules of the VPC on AWS.

### 3. Add credentials

1. Next, set up the Redshift credentials in the `.dlt/secrets.toml` file as shown below:

    ```toml
    [destination.redshift.credentials]
    database = "please set me up!" # Copy your database name here
    password = "please set me up!" # Keep your Redshift db instance password here
    username = "please set me up!" # Keep your Redshift db instance username here
    host = "please set me up!" # Copy your Redshift host from cluster endpoint here
    port = 5439
    connect_timeout = 15 # Enter the timeout value
    ```

2. The "host" is derived from the cluster endpoint specified in the “General Configuration.” For example:

    ```sh
    # If the endpoint is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com:5439/your_database_name
    # Then the host is:
    redshift-cluster-1.cv3cmsy7t4il.us-east-1.redshift.amazonaws.com
    ```

3. The `connect_timeout` is the number of minutes the pipeline will wait before timing out.

You can also pass a database connection string similar to the one used by the `psycopg2` library or [SQLAlchemy](https://docs.sqlalchemy.org/en/20/core/engines.html#postgresql). The credentials above will look like this:
```toml
# Keep it at the top of your TOML file, before any section starts
destination.redshift.credentials="redshift://loader:<password>@localhost/dlt_data?connect_timeout=15"
```

## Write disposition

All [write dispositions](../../general-usage/incremental-loading#choosing-a-write-disposition) are supported.

## Supported file formats
[SQL Insert](../file-formats/insert-format) is used by default.

When staging is enabled:
* [jsonl](../file-formats/jsonl.md) is used by default.
* [parquet](../file-formats/parquet.md) is supported.

> ❗ **Redshift cannot load `VARBYTE` columns from `json` files**. `dlt` will fail such jobs permanently. Switch to `parquet` to load binaries.

> ❗ **Redshift cannot load `TIME` columns from `json` or `parquet` files**. `dlt` will fail such jobs permanently. Switch to direct `insert_values` to load time columns.

> ❗ **Redshift cannot detect compression type from `json` files**. `dlt` assumes that `jsonl` files are gzip compressed, which is the default.

> ❗ **Redshift loads `json` types as strings into SUPER with `parquet`**. Use `jsonl` format to store JSON in SUPER natively or transform your SUPER columns with `PARSE_JSON`.

## Supported column hints

Amazon Redshift supports the following column hints:

- `cluster` - This hint is a Redshift term for table distribution. Applying it to a column makes it the "DISTKEY," affecting query and join performance. Check the following [documentation](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html) for more info.
- `sort` - This hint creates a SORTKEY to order rows on disk physically. It is used to improve query and join speed in Redshift. Please read the [sort key docs](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html) to learn more.

### Table and column identifiers
Redshift **by default** uses case-insensitive identifiers and **will lower case all the identifiers** that are stored in the INFORMATION SCHEMA. Do not use
[case-sensitive naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations). Letter casing will be removed anyway, and you risk generating identifier collisions, which are detected by `dlt` and will fail the load process.

You can [put Redshift in case-sensitive mode](https://docs.aws.amazon.com/redshift/latest/dg/r_enable_case_sensitive_identifier.html). Configure your destination as below in order to use case-sensitive naming conventions:
```toml
[destination.redshift]
has_case_sensitive_identifiers=true
```


## Staging support

Redshift supports s3 as a file staging destination. `dlt` will upload files in the parquet format to s3 and ask Redshift to copy their data directly into the db. Please refer to the [S3 documentation](./filesystem.md#aws-s3) to learn how to set up your s3 bucket with the bucket_url and credentials. The `dlt` Redshift loader will use the AWS credentials provided for s3 to access the s3 bucket if not specified otherwise (see config options below). Alternatively to parquet files, you can also specify jsonl as the staging file format. For this, set the `loader_file_format` argument of the `run` command of the pipeline to `jsonl`.

## Identifier names and case sensitivity
* Up to 127 characters
* Case insensitive
* Stores identifiers in lower case
* Has case-sensitive mode, if enabled you must [enable case sensitivity in destination factory](../../general-usage/destination.md#control-how-dlt-creates-table-column-and-other-identifiers)

### Authentication IAM Role

If you would like to load from s3 without forwarding the AWS staging credentials but authorize with an IAM role connected to Redshift, follow the [Redshift documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html) to create a role with access to s3 linked to your Redshift cluster and change your destination settings to use the IAM role:

```toml
[destination]
staging_iam_role="arn:aws:iam::..."
```

### Redshift/S3 staging example code

```py
# Create a dlt pipeline that will load
# chess player data to the Redshift destination
# via staging on S3
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='redshift',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
```

## Additional destination options
### dbt support

- This destination [integrates with dbt](../transformations/dbt) via [dbt-redshift](https://github.com/dbt-labs/dbt-redshift). Credentials and timeout settings are shared automatically with `dbt`.

### Syncing of `dlt` state
- This destination fully supports [dlt state sync.](../../general-usage/state#syncing-state-with-destination)

## Supported loader file formats

Supported loader file formats for Redshift are `sql` and `insert_values` (default). When using a staging location, Redshift supports `parquet` and `jsonl`.

<!--@@@DLT_TUBA redshift-->

