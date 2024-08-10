"""
---
title: Load parent table keys into child table
description: Learn how integrate custom parent keys into child records
keywords: [parent child relationship, parent key]
---

This example demonstrates handling data with parent-child relationships using the `dlt` library. This process is about integrating specific fields (e.g. primary, foreign keys) from a parent record into each child record, ensuring that we can use self defined parent keys in the child table. 

In this example, we'll explore how to:

- Integrate a custom `parent_id` into each child record.
- Ensure every child is correctly linked to its `parent_id` using a tailored function, `add_parent_id`.
- Use the [`add_map` function](https://dlthub.com/docs/api_reference/extract/resource#add_map) to apply this custom logic to every record in our dataset.
"""

from typing import Dict, Any
import dlt

# Define a dlt resource with write disposition to 'merge'
@dlt.resource(name='parent_with_children', write_disposition={"disposition": "merge"})
def data_source() -> Dict[str, Any]:
    # Example data
    data = [
        {
            'parent_id': 1,
            'parent_name': 'Alice',
            'children': [
                {'child_id': 1, 'child_name': 'Child 1'},
                {'child_id': 2, 'child_name': 'Child 2'}
            ]
        },
        {
            'parent_id': 2,
            'parent_name': 'Bob',
            'children': [
                {'child_id': 3, 'child_name': 'Child 3'}
            ]
        }
    ]

    yield data

# Function to add parent_id to each child record within a parent record
def add_parent_id(record: Dict[str, Any]) -> Dict[str, Any]:
    parent_id_key = "parent_id"
    for child in record["children"]:
        child[parent_id_key] = record[parent_id_key]
    return record

if __name__ == "__main__":
    # Create and configure the dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name='generic_pipeline',
        destination='duckdb',
        dataset_name='dataset',
    )

    # Run the pipeline
    load_info = pipeline.run(
        data_source()
        .add_map(add_parent_id),
        primary_key="parent_id"
    )
    # Output the load information after pipeline execution
    print(load_info)

