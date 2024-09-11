"""
---
title: Custom destination with BigQuery
description: Learn how use the custom destination to load to bigquery and use credentials
keywords: [destination, credentials, example, bigquery, custom destination]
---

In this example, you'll find a Python script that demonstrates how to load to BigQuery with the custom destination.

We'll learn how to:
- Use [built-in credentials.](../general-usage/credentials/complex_types#gcp-credentials)
- Use the [custom destination.](../dlt-ecosystem/destinations/destination.md)
- Use pyarrow tables to create nested column types on BigQuery.
- Use BigQuery `autodetect=True` for schema inference from parquet files.

"""

import dlt
import pandas as pd
import pyarrow as pa
from google.cloud import bigquery

from dlt.common.configuration.specs import GcpServiceAccountCredentials

# constants
OWID_DISASTERS_URL = (
    "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
    "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020)/"
    "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020).csv"
)
# this table needs to be manually created in your gc account
# format: "your-project.your_dataset.your_table"
BIGQUERY_TABLE_ID = "chat-analytics-rasa-ci.ci_streaming_insert.natural-disasters"


# dlt sources
@dlt.resource(name="natural_disasters")
def resource(url: str):
    # load pyarrow table with pandas
    table = pa.Table.from_pandas(pd.read_csv(url))
    # we add a list type column to demonstrate bigquery lists
    table = table.append_column(
        "tags",
        pa.array(
            [["disasters", "earthquakes", "floods", "tsunamis"]] * len(table),
            pa.list_(pa.string()),
        ),
    )
    # we add a struct type column to demonstrate bigquery structs
    table = table.append_column(
        "meta",
        pa.array(
            [{"loaded_by": "dlt"}] * len(table),
            pa.struct([("loaded_by", pa.string())]),
        ),
    )
    yield table


# dlt bigquery custom destination
# we can use the dlt provided credentials class
# to retrieve the gcp credentials from the secrets
@dlt.destination(
    name="bigquery", loader_file_format="parquet", batch_size=0, naming_convention="snake_case"
)
def bigquery_insert(
    items, table=BIGQUERY_TABLE_ID, credentials: GcpServiceAccountCredentials = dlt.secrets.value
) -> None:
    client = bigquery.Client(
        credentials.project_id, credentials.to_native_credentials(), location="US"
    )
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    )
    # since we have set the batch_size to 0, we get a filepath and can load the file directly
    with open(items, "rb") as f:
        load_job = client.load_table_from_file(f, table, job_config=job_config)
    load_job.result()  # Waits for the job to complete.


if __name__ == "__main__":
    # run the pipeline and print load results
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_bigquery_insert",
        destination=bigquery_insert,
        dataset_name="mydata",
        dev_mode=True,
    )
    load_info = pipeline.run(resource(url=OWID_DISASTERS_URL))

    print(load_info)
