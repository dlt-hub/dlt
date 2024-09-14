import pytest

from tests.utils import skipifgithubfork


"""
---
title: Load parent table records into child table
description: Learn how to integrate custom parent keys into child records
keywords: [parent child relationship, parent key]
---

This example demonstrates handling data with parent-child relationships using the `dlt` library.
You learn how to integrate specific fields (e.g., primary, foreign keys) from a parent record into each child record.

In this example, we'll explore how to:

- Add `parent_id` into each child record using `add_parent_id` function
- Use the [`add_map` function](https://dlthub.com/docs/api_reference/extract/resource#add_map) to apply this
custom logic to every record in the dataset

:::note important
Please note that dlt metadata, including `_dlt_id` and `_dlt_load_id`, will still be loaded into the tables.
:::
"""

from typing import List, Dict, Any, Generator
import dlt


# Define a dlt resource with write disposition to 'merge'
@dlt.resource(name="parent_with_children", write_disposition={"disposition": "merge"})
def data_source() -> Generator[List[Dict[str, Any]], None, None]:
    # Example data
    data = [
        {
            "parent_id": 1,
            "parent_name": "Alice",
            "children": [
                {"child_id": 1, "child_name": "Child 1"},
                {"child_id": 2, "child_name": "Child 2"},
            ],
        },
        {
            "parent_id": 2,
            "parent_name": "Bob",
            "children": [{"child_id": 3, "child_name": "Child 3"}],
        },
    ]

    yield data


# Function to add parent_id to each child record within a parent record
def add_parent_id(record: Dict[str, Any]) -> Dict[str, Any]:
    parent_id_key = "parent_id"
    for child in record["children"]:
        child[parent_id_key] = record[parent_id_key]
    return record


@skipifgithubfork
@pytest.mark.forked
def test_parent_child_relationship():
    # Create and configure the dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="generic_pipeline",
        destination="duckdb",
        dataset_name="dataset",
    )

    # Run the pipeline
    load_info = pipeline.run(data_source().add_map(add_parent_id), primary_key="parent_id")
    # Output the load information after pipeline execution
    print(load_info)
