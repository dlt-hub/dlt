"""
---
title: Backfill to Filesystem with partial replace
description: Load chess game data from Chess.com into a filesystem destination, while deleting old backfill files.
keywords: [incremental loading, REST API, dlt, chess.com, data pipeline, backfill management, filesystem]
---

This script interacts with the Chess.com REST API to extract game data for a specific user on a monthly basis.
The script retrieves game data for a specified time range, and when additional data is loaded for a different time range,
it automatically handles de-duplication by deleting any previously loaded files for overlapping time range.

We'll learn:

- How to configure a [REST API source](../dlt-ecosystem/verified-sources/rest_api/basic.md) using
 the `dlt` library.
- How to manage and delete old backfill files for de-duplication.
- How to use [Filesystem](../dlt-ecosystem/destinations/filesystem.md) as a destination for storing extracted data.
"""

import os
import re
from dlt.common import pendulum as p
from typing import Dict, List, Iterator

import dlt
from dlt.sources import DltResource
from dlt.common.pipeline import LoadInfo
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


@dlt.source
def chess_com_source(username: str, months: List[Dict[str, str]]) -> Iterator[DltResource]:
    """
    Configures and yields resources to fetch chess game data for a given user across specified months.

    Args:
        username (str): Chess.com username to fetch games for.
        months (List[Dict[str, str]]): List of dictionaries containing 'year' and 'month' keys.

    Yields:
        dlt.Resource: Resource objects containing fetched game data.
    """
    for month in months:
        year = month["year"]
        month_str = month["month"]
        # Configure REST API endpoint for the specific month
        config: RESTAPIConfig = {
            "client": {
                "base_url": "https://api.chess.com/pub/",  # Base URL for Chess.com API
            },
            "resources": [
                {
                    "name": f"chess_com_games_{year}_{month_str}",  # Unique resource name
                    "write_disposition": "append",
                    "endpoint": {
                        "path": f"player/{username}/games/{year}/{month_str}",  # API endpoint path
                    },
                    "primary_key": ["url"],  # Primary key to prevent duplicates
                }
            ],
        }
        yield from rest_api_resources(config)


def generate_months(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> Iterator[Dict[str, str]]:
    """
    Generates a list of months between the start and end dates.

    Args:
        start_year (int): Starting year.
        start_month (int): Starting month.
        end_year (int): Ending year.
        end_month (int): Ending month.

    Yields:
        Dict[str, str]: Dictionary containing 'year' and 'month' as strings.
    """
    start_date = p.datetime(start_year, start_month, 1)
    end_date = p.datetime(end_year, end_month, 1)
    current_date = start_date
    while current_date <= end_date:
        yield {"year": str(current_date.year), "month": f"{current_date.month:02d}"}
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)


def delete_old_backfills(load_info: LoadInfo, p: dlt.Pipeline, table_name: str) -> None:
    """
    Deletes old backfill files that do not match the current load ID to maintain data integrity.

    Args:
        load_info (LoadInfo): Information about the current load.
        p (dlt.Pipeline): The dlt pipeline instance.
        table_name (str): Name of the table to clean up backfills for.
    """
    # Fetch current load id
    load_id = load_info.loads_ids[0]
    pattern = re.compile(rf"{load_id}")  # Compile regex pattern for the current load ID

    # Initialize the filesystem client
    fs_client: FilesystemClient = p._get_destination_clients()[0]  # type: ignore

    # Construct the table directory path
    table_dir = os.path.join(fs_client.dataset_path, table_name)

    # Check if the table directory exists
    if fs_client.fs_client.exists(table_dir):
        # Traverse the table directory
        for root, _dirs, files in fs_client.fs_client.walk(table_dir, maxdepth=None):
            for file in files:
                # Construct the full file path
                file_path = os.path.join(root, file)
                # If the file does not match the current load ID, delete it
                if not pattern.search(file_path):
                    try:
                        fs_client.fs_client.rm(file_path)  # Remove the old backfill file
                        print(f"Deleted old backfill file: {file_path}")
                    except Exception as e:
                        print(f"Error deleting file {file_path}: {e}")
    else:
        # Inform if the table directory does not exist
        print(f"Table directory does not exist: {table_dir}")


def load_chess_data():
    """
    Sets up and runs the dlt pipeline to load chess game data, then manages backfills.
    """
    # Initialize the dlt pipeline with filesystem destination
    pipeline = dlt.pipeline(
        pipeline_name="chess_com_data", destination="filesystem", dataset_name="chess_games"
    )

    # Generate the list of months for the desired date range
    months = list(generate_months(2023, 1, 2023, 12))

    # Create the source with all specified months
    source = chess_com_source("MagnusCarlsen", months)

    # Run the pipeline to fetch and load data
    info = pipeline.run(source)
    print(info)

    # After the run, delete old backfills for each table to maintain data consistency
    for month in months:
        table_name = f"chess_com_games_{month['year']}_{month['month']}"
        delete_old_backfills(info, pipeline, table_name)


if __name__ == "__main__":
    load_chess_data()
