---
title: Staging
description: Configure an s3 or gcs bucket for staging before copying into the destination
keywords: [staging, destination]
---
# Staging

The goal of staging is to bring the data closer to the database engine so the modification of the destination (final) dataset happens faster and without errors. `dlt`, when asked, creates two
staging areas:
1. A **staging dataset** used by the [merge and replace loads](../general-usage/incremental-loading.md#merge-incremental_loading) to deduplicate and merge data with the destination. Such staging dataset has the same name as the dataset passed to `dlt.pipeline` but with `_staging` suffix in the name. As a user you typically never see and directly interact with it.
2. A **staging storage** which is typically a s3/gcp bucket where [loader files](file-formats/) are copied before they are loaded by the destination.

## Staging storage
`dlt` allows to chain destinations where the first one (`staging`) is responsible for uploading the files from local filesystem to the remote storage. It then generates followup jobs for the second destination that (typically) copy the files from remote storage into destination.

Currently, only one destination the [filesystem](destinations/filesystem.md) can be used as a staging. Following destinations can copy remote files:
1. [Redshift.](destinations/redshift.md#staging-support)
2. [Bigquery.](destinations/bigquery.md#staging-support)
3. [Snowflake.](destinations/snowflake.md#staging-support)

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
    ```python
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
    ```python
    info = pipeline.run(chess(), loader_file_format="parquet")
    ```

5. **Run the pipeline script.**

    Run the pipeline script as usual.

> 💡 Please note that `dlt` does not delete loaded files from the staging storage after the load is complete.
