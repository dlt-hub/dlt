import dlt
import pytest
import os
from random import randrange, choice
from typing import Any, Dict, Tuple

from dlt.common.destination.dataset import SupportsReadableDataset

from tests.load.utils import (
    FILE_BUCKET,
    destinations_configs,
    SFTP_BUCKET,
    MEMORY_BUCKET,
    DestinationTestConfiguration,
)


def get_job_types(p: dlt.Pipeline, stage: str = "loaded") -> Dict[str, Dict[str, Any]]:
    """
    gets a list of loaded jobs by type for each table
    this is useful for testing to check that the correct transformations were used
    """

    jobs = []
    if stage == "loaded":
        jobs = p.last_trace.last_load_info.load_packages[0].jobs["completed_jobs"]
    elif stage == "extracted":
        jobs = p.last_trace.last_extract_info.load_packages[0].jobs["new_jobs"]

    tables: Dict[str, Dict[str, Any]] = {}

    for j in jobs:
        file_format = j.job_file_info.file_format
        table_name = j.job_file_info.table_name
        if table_name.startswith("_dlt"):
            continue
        tables.setdefault(table_name, {})[file_format] = (
            tables.get(table_name, {}).get(file_format, 0) + 1
        )

    return tables


def transformation_configs(only_duckdb: bool = False):
    return destinations_configs(
        default_sql_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_filesystem_configs=True,
        exclude=[
            "athena",  # NOTE: athena iceberg will probably work, we need to implement the model files for it
            # TODO: can we enable it and just disable specific tests
            "sqlalchemy_sqlite-no-staging",  # NOTE: sqlalchemy has no uuid support
            (  # NOTE: duckdb parquet does not need to be tested explicitely if we have the regular
                "duckdb-parquet-no-staging"
            ),
            "synapse",  # should probably work? no model file support at the moment. revisit soon :)
            "dremio",  # should probably work? no model file support at the moment. different type of sql client
        ],
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
        subset=(
            [
                "duckdb",
            ]
            if only_duckdb
            else None
        ),
    )


def setup_transformation_pipelines(
    destination_config: DestinationTestConfiguration,
) -> Tuple[dlt.Pipeline, dlt.Pipeline]:
    """Set up a source and a destination pipeline for transformation tests"""

    # Disable unique indexing for postgres, otherwise there will be a not null constraint error
    # because we're copying from the same table, should be fixed in hints forwarding
    if destination_config.destination_type == "postgres":
        os.environ["DESTINATION__POSTGRES__CREATE_INDEXES"] = "false"

    # setup incoming data and transformed data pipelines
    fruit_p = destination_config.setup_pipeline(
        "fruit_pipeline",
        dataset_name="source",
        use_single_dataset=False,
        dev_mode=True,
    )
    dest_p = destination_config.setup_pipeline(
        "fruit_pipeline",
        dataset_name="transformed",
        use_single_dataset=False,
        # for filesystem destination we load to a duckdb on python transformations
        destination="duckdb" if destination_config.destination_type == "filesystem" else None,
        dev_mode=True,
    )

    return fruit_p, dest_p
