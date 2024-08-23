---
title: Staging
description: Configure an s3 or gcs bucket for staging before copying into the destination
keywords: [staging, destination]
---
# Staging

The goal of staging is to bring the data closer to the database engine so the modification of the destination (final) dataset happens faster and without errors. `dlt`, when asked, creates two
staging areas:
1. A **staging dataset** used by the [merge and replace loads](../general-usage/incremental-loading.md#merge-incremental_loading) to deduplicate and merge data with the destination.
2. A **staging storage** which is typically a s3/gcp bucket where [loader files](file-formats/) are copied before they are loaded by the destination.

## Staging dataset
`dlt` creates a staging dataset when write disposition of any of the loaded resources requires it. It creates and migrates required tables exactly like for the
main dataset. Data in staging tables is truncated when load step begins and only for tables that will participate in it.
Such staging dataset has the same name as the dataset passed to `dlt.pipeline` but with `_staging` suffix in the name. Alternatively, you can provide your own staging dataset pattern or use a fixed name, identical for all the
configured datasets.
```toml
[destination.postgres]
staging_dataset_name_layout="staging_%s"
```
Entry above switches the pattern to `staging_` prefix and for example for dataset with name **github_data** `dlt` will create **staging_github_data**.

To configure static staging dataset name, you can do the following (we use destination factory)
```py
import dlt

dest_ = dlt.destinations.postgres(staging_dataset_name_layout="_dlt_staging")
```
All pipelines using `dest_` as destination will use **staging_dataset** to store staging tables. Make sure that your pipelines are not overwriting each other's tables.

### Cleanup up staging dataset automatically
`dlt` does not truncate tables in staging dataset at the end of the load. Data that is left after contains all the extracted data and may be useful for debugging.
If you prefer to truncate it, put the following line in `config.toml`:

```toml
[load]
truncate_staging_dataset=true
```

## Staging storage
`dlt` allows to chain destinations where the first one (`staging`) is responsible for uploading the files from local filesystem to the remote storage. It then generates followup jobs for the second destination that (typically) copy the files from remote storage into destination.

Currently, only one destination the [filesystem](destinations/filesystem.md) can be used as a staging. Following destinations can copy remote files:

1. [Azure Synapse](destinations/synapse#staging-support)
1. [Athena](destinations/athena#staging-support)
1. [Bigquery](destinations/bigquery.md#staging-support)
1. [Dremio](destinations/dremio#staging-support)
1. [Redshift](destinations/redshift.md#staging-support)
1. [Snowflake](destinations/snowflake.md#staging-support)

### How to use
In essence, you need to set up two destinations and then pass them to `dlt.pipeline`. Below we'll use `filesystem` staging with `parquet` files to load into `Redshift` destination.

1. **Set up the s3 bucket and filesystem staging.**

    Please follow our guide in [filesystem destination documentation](destinations/filesystem.md). Test the staging as standalone destination to make sure that files go where you want them. In your `secrets.toml` you should now have a working `filesystem` configuration:
    ```toml
    [destination.filesystem]
    bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

    [destination.filesystem.credentials]
    aws_access_key_id = "please set me up!" # copy the access key here
    aws_secret_access_key = "please set me up!" # copy the secret access key here
    ```

2. **Set up the Redshift destination.**

    Please follow our guide in [redshift destination documentation](destinations/redshift.md). In your `secrets.toml` you added:
    ```toml
    # keep it at the top of your toml file! before any section starts
    destination.redshift.credentials="redshift://loader:<password>@localhost/dlt_data?connect_timeout=15"
    ```

3. **Authorize Redshift cluster to access the staging bucket.**

    By default `dlt` will forward the credentials configured for `filesystem` to the `Redshift` COPY command. If you are fine with this, move to the next step.

4. **Chain staging to destination and request `parquet` file format.**

    Pass the `staging` argument to `dlt.pipeline`. It works like the destination `argument`:
    ```py
    # Create a dlt pipeline that will load
    # chess player data to the redshift destination
    # via staging on s3
    pipeline = dlt.pipeline(
        pipeline_name='chess_pipeline',
        destination='redshift',
        staging='filesystem', # add this to activate the staging location
        dataset_name='player_data'
    )
    ```
    `dlt` will automatically select an appropriate loader file format for the staging files. Below we explicitly specify `parquet` file format (just to demonstrate how to do it):
    ```py
    info = pipeline.run(chess(), loader_file_format="parquet")
    ```

5. **Run the pipeline script.**

    Run the pipeline script as usual.

:::tip
Please note that `dlt` does not delete loaded files from the staging storage after the load is complete, but it trunkate previously loaded files.
:::

### How to prevent staging files truncation

Before `dlt` loads data to staging storage it truncates previously loaded files. To prevent it and keep the whole history
of loaded files you can use the following parameter:

```toml
[destination.redshift]
truncate_table_before_load_on_staging_destination=false
```

:::caution
[Athena](destinations/athena#staging-support) destination only truncate not iceberg tables with `replace` merge_disposition.
Therefore, parameter `truncate_table_before_load_on_staging_destination` only control truncation of corresponding files for these tables.
:::
