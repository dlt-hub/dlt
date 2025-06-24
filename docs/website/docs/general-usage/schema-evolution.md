---
title: Schema evolution
description: A small guide to elaborate on how schema evolution works
keywords: [schema evolution, schema, dlt schema]
---

## Schema evolution with `dlt`

`dlt` automatically infers the initial schema for your first pipeline run. However, in most cases, the schema tends to change over time, which makes it critical for downstream consumers to adapt to schema changes.

As the structure of data changes, such as the addition of new columns or changing data types, `dlt` handles these schema changes, enabling you to adapt to changes without losing velocity.

## Inferring a schema from nested data

The first run of a pipeline will scan the data that goes through it and generate a schema. To convert nested data into a relational format, `dlt` flattens dictionaries and unpacks nested lists into sub-tables.

We'll review some examples here and figure out how `dlt` creates the initial schema and how normalization works. Consider a pipeline that loads the following schema:

```py
data = [{
    "organization": "Tech Innovations Inc.",
    "address": {
        'building': 'r&d',
        "room": 7890,
    },
    "Inventory": [
        {"name": "Plasma ray", "inventory nr": 2411},
        {"name": "Self-aware Roomba", "inventory nr": 268},
        {"name": "Type-inferrer", "inventory nr": 3621}
    ]
}]

# Run `dlt` pipeline
dlt.pipeline("organizations_pipeline", destination="duckdb").run(data, table_name="org")
```

The schema of data above is loaded to the destination as follows:
<iframe width="560" height="315" src='https://dbdiagram.io/e/65e5c68bcd45b569fb7805e8/65e7ff92cd45b569fba253d9'> </iframe>

### What did the schema inference engine do?

As you can see above, the dlt's inference engine generates the structure of the data based on the source and provided hints. It normalizes the data, creates tables and columns, and infers data types.

For more information, you can refer to the [Schema](./schema) and [Adjust a Schema](../walkthroughs/adjust-a-schema) sections in the documentation.

## Evolving the schema

For a typical data source, the schema tends to change over time, and dlt handles this changing schema seamlessly.

Let’s add the following 4 cases:

- A column is added: a field named “CEO” was added.
- A column type is changed: The datatype of the column named “inventory_nr” was changed from integer to string.
- A column is removed: a field named “room” was commented out/removed.
- A column is renamed: a field “building” was renamed to “main_block”.

Please update the pipeline for the cases discussed above.
```py
data = [{
    "organization": "Tech Innovations Inc.",
    # Column added:
    "CEO": "Alice Smith",
    "address": {
        # 'building' renamed to 'main_block'
        'main_block': 'r&d',
	      # Removed room column
        # "room": 7890,
    },
    "Inventory": [
        # Type change: 'inventory_nr' changed to string from int
        {"name": "Plasma ray", "inventory nr": "AR2411"},
        {"name": "Self-aware Roomba", "inventory nr": "AR268"},
        {"name": "Type-inferrer", "inventory nr": "AR3621"}
    ]
}]

# Run `dlt` pipeline
dlt.pipeline("organizations_pipeline", destination="duckdb").run(data, table_name="org")
```

Let’s load the data and look at the tables:
<iframe width="560" height="315" src='https://dbdiagram.io/e/65e80303cd45b569fba28e9d/65e80556cd45b569fba2b8ab'> </iframe>

What happened?

- Added column:
    - A new column named `ceo` is added to the “org” table.
- Variant column:
    - A new column named `inventory_nr__v_text` is added as the datatype of the column was changed from “integer” to “string”.
- Removed column stopped loading:
    - New data to column `room` is not loaded.
- Column stopped loading and new one was added:
    - A new column `address__main_block` was added and now data will be loaded to that and stop loading in the column `address__building`.

## Alert schema changes to curate new data

By separating the technical process of loading data from curation, you free the data engineer to do engineering, and the analytics to curate data without technical obstacles. So, the analyst must be kept in the loop.

**Tracking column lineage**

The column lineage can be tracked by loading the 'load_info' to the destination. The 'load_info' contains information about columns’ data types, add times, and load id. To read more please see [the data lineage article](https://dlthub.com/blog/dlt-data-lineage) we have on the blog.

**Getting notifications**

We can read the load outcome and send it to a Slack webhook with dlt.
```py
# Import the send_slack_message function from the dlt library
from dlt.common.runtime.slack import send_slack_message

# Define the URL for your Slack webhook
hook = "https://hooks.slack.com/services/xxx/xxx/xxx"

# Iterate over each package in the load_info object
for package in load_info.load_packages:
    # Iterate over each table in the schema_update of the current package
    for table_name, table in package.schema_update.items():
        # Iterate over each column in the current table
        for column_name, column in table["columns"].items():
            # Send a message to the Slack channel with the table
						# and column update information
            send_slack_message(
                hook,
                message=(
                    f"\tTable updated: {table_name}: "
                    f"Column changed: {column_name}: "
                    f"{column['data_type']}"
                )
            )
```
This script sends Slack notifications for schema updates using the `send_slack_message` function from the `dlt` library. It provides details on the updated table and column.

## How to control evolution

`dlt` allows schema evolution control via its schema and data contracts. Refer to our **[documentation](./schema-contracts)** for details.

### How to test for removed columns - applying "not null" constraint

A column not existing and a column being null are two different things. However, when it comes to APIs and JSON, it’s usually all treated the same - the key-value pair will simply not exist.

To remove a column, exclude it from the output of the resource function. Subsequent data inserts will treat this column as null. Verify column removal by applying a not null constraint. For instance, after removing the "room" column, apply a not null constraint to confirm its exclusion.

```py
data = [{
    "organization": "Tech Innovations Inc.",
    "address": {
        'building': 'r&d'
        #"room": 7890,
    },
    "Inventory": [
        {"name": "Plasma ray", "inventory nr": 2411},
        {"name": "Self-aware Roomba", "inventory nr": 268},
        {"name": "Type-inferrer", "inventory nr": 3621}
    ]
}]

pipeline = dlt.pipeline("organizations_pipeline", destination="duckdb")
# Adding not null constraint
pipeline.run(data, table_name="org", columns={"room": {"data_type": "bigint", "nullable": False}})
```
During pipeline execution, a data validation error indicates that a removed column is being passed as null.

## Some schema changes in the data

The data in the pipeline mentioned above is modified.

```py
data = [{
    "organization": "Tech Innovations Inc.",
    "CEO": "Alice Smith",
    "address": {'main_block': 'r&d'},
    "Inventory": [
        {"name": "Plasma ray", "inventory nr": "AR2411"},
        {"name": "Self-aware Roomba", "inventory nr": "AR268"},
        {
            "name": "Type-inferrer", "inventory nr": "AR3621",
            "details": {
                "category": "Computing Devices",
                "id": 369,
                "specifications": [{
                    "processor": "Quantum Core",
                    "memory": "512PB"
                }]
            }
        }
    ]
}]

# Run `dlt` pipeline
dlt.pipeline("organizations_pipeline", destination="duckdb").run(data, table_name="org")
```
The schema of the data above is loaded to the destination as follows:
<iframe width="560" height="315" src='https://dbdiagram.io/e/65e80b31cd45b569fba33169/65e81055cd45b569fba3aa20'> </iframe>

## What did the schema evolution engine do?

The schema evolution engine in the `dlt` library is designed to handle changes in the structure of your data over time. For example:

- As above in continuation of the inferred schema, the “specifications” are nested in "details", which are nested in “Inventory”, all under the table name “org”. So the table created for projects is `org__inventory__details__specifications`.

This is a simple example of how schema evolution works.

## Schema evolution using schema and data contracts

Demonstrating schema evolution without talking about schema and data contracts is only one side of the coin. Schema and data contracts dictate the terms of how the schema being written to the destination should evolve.

Schema and data contracts can be applied to entities such as ‘tables’, ‘columns’, and ‘data_types’ using contract modes such as ‘evolve’, ‘freeze’, ‘discard_rows’, and ‘discard_columns’ to tell dlt how to apply contracts for a particular entity. To read more about **schema and data contracts**, read our [documentation](./schema-contracts).

## Common schema evolution challenges

This section addresses common issues that arise during schema evolution and outlines some possible approaches to resolving them.

### Inconsistent data types

When a column’s data type changes across pipeline runs (e.g., from integer to string), dlt creates a variant column (e.g., `value__v_text`) to store the new type, while preserving the original (e.g., value). Downstream processes must account for both.

#### Recommended Approaches:

#### Enforce consistent types with `apply_hints`:

Define expected types explicitly to avoid schema drift and variant columns caused by type inference.
```py
# Force all values to be treated as text
resource.apply_hints(columns={"value": {"data_type": "text"}})
```

#### Validate data before processing:

Validate and normalize data upfront to catch type issues early and enforce consistency across runs.
```py
def validate_value(value):
    if not isinstance(value, (int, str)):
        raise TypeError(f"Expected int or str, got {type(value)}")
    return str(value)  # Convert to consistent type
```
Apply to your data:
```py
data = [{"id": 1, "value": validate_value(42)}]
```
Ensures values match expected types, reducing the likelihood of variant columns due to type inconsistencies.

### Nested data evolution challenges

As your data sources evolve, nested structures can become increasingly complex, leading to deeply nested tables that are difficult to manage or query.

#### Recommended Approaches:

#### Control nesting depth during evolution:

Limit how deeply nested structures are normalized to manage the level of nesting in the schema.
```py
# Set max_table_nesting on the source
@dlt.source(max_table_nesting=2)
def my_data():
    return dlt.resource(data, name="my_table")

...
```

#### Flatten complex structures before processing:

Convert nested fields to flat representations to simplify schema inference.
```py
import json as jsonlib

def flatten_nested_data(data):
    # Preprocess to reduce nesting complexity
    return {"id": data["id"], "details_json": jsonlib.dumps(data["complex_nested_field"])}

# Use in your pipeline
flattened_data = [flatten_nested_data(item) for item in original_data]
pipeline.run(flattened_data, table_name="my_table")
```

For more details on nested tables, see [nested tables documentation](destination-tables.md#nested-tables).
