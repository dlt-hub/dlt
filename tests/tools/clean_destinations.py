"""script to clean datasets from destinations used by our ci"""

from typing import Any
import argparse
import os

import dlt
from dlt.cli import echo as fmt
from dlt.destinations.sql_client import SqlClientBase

from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTerminalException,
    DatabaseTransientException,
)

SKIP_DATASETS = ["pyicebergdemodb"]


# all destinations that can be cleaned by this script
ALL_DESTINATIONS = ["bigquery", "redshift", "postgres", "snowflake", "athena"]

# limit the number of datasets to clean per run
LIMIT = 100000


def get_datasets_command(pipeline: dlt.Pipeline, destination: str) -> tuple[str, int]:
    """Get the command to get all datasets from a destination"""

    destination_client = pipeline._get_destination_clients(pipeline._get_schema_or_create())[0]

    if destination in ["athena"]:
        return "SHOW DATABASES", 0
    elif destination in ["snowflake"]:
        return "SHOW SCHEMAS;", 1
    elif destination in ["redshift"]:
        return "SHOW SCHEMAS;", 1
    elif destination == "bigquery":
        project_id = destination_client.config.credentials.project_id  # type: ignore
        return f"SELECT schema_name FROM `{project_id}.INFORMATION_SCHEMA.SCHEMATA`;", 0
    elif destination in ["postgres"]:
        return (
            """SELECT schema_name
FROM information_schema.schemata
WHERE schema_owner = current_user
ORDER BY schema_name;""",
            0,
        )
    else:
        raise ValueError(f"Destination {destination} is not supported")


def drop_dataset_command(destination: str, dataset_name: str) -> str:
    """Get the command to drop a dataset from the destination, we could also use the drop command from the destination client"""
    if destination in ["athena"]:
        return f"DROP DATABASE {dataset_name} CASCADE;"
    elif destination in ["redshift", "postgres", "snowflake"]:
        return f"DROP SCHEMA IF EXISTS {dataset_name} CASCADE;"
    elif destination in ["bigquery"]:
        return f"DROP SCHEMA {dataset_name} CASCADE;"
    else:
        raise ValueError(f"Destination {destination} is not supported")


if __name__ == "__main__":
    os.environ["BUCKET_URL"] = "SOME_BUCKET"
    # parse input args
    parser = argparse.ArgumentParser(description="Clean datasets from destinations used by our ci")
    parser.add_argument(
        "--destination",
        type=str,
        required=False,
        help="The destination to clean, defaults to all if not set: " + ", ".join(ALL_DESTINATIONS),
    )
    args = parser.parse_args()

    selected_destination = [args.destination] if args.destination else ALL_DESTINATIONS

    for destination in selected_destination:
        fmt.echo("====")
        fmt.echo(f"Cleaning {destination}...")
        fmt.echo("====")

        # we use the pipeline to access the sql client and configs
        pipeline = dlt.pipeline(pipeline_name="clean_destinations", destination=destination)

        # get datasets
        datasets_command, result_index = get_datasets_command(pipeline, destination)
        with pipeline.sql_client() as client:
            with client.execute_query(datasets_command) as cur:
                datasets = [row[result_index] for row in cur.fetchall()]

        # info
        total = len(datasets)
        fmt.echo(f"Found {total} datasets to clean, will clean {LIMIT} of them")

        # clean datasets
        with pipeline.sql_client() as client:
            count = 0
            for dataset in datasets[:LIMIT]:
                if dataset in SKIP_DATASETS:
                    fmt.echo(f"Skipping dataset {dataset}")
                    continue
                count += 1
                fmt.echo(f"Cleaning dataset {count} of {total}: {dataset}, ")
                command = drop_dataset_command(destination, dataset)
                try:
                    with client.execute_query(command) as cur:
                        pass
                except Exception as e:
                    print(f"Could not delete schema: {str(e)}")
