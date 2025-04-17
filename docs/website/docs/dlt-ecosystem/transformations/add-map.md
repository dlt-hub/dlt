---
title: Transforming data with `add_map`
description: Apply lightweight python transformations to your data inline using `add_map`.
keywords: [add_map, transform data, remove columns]
---

`add_map` is a method in dlt used to apply custom logic to each data item after extraction. It is typically used to modify records before they continue through the pipeline or are loaded to the destination. Common examples include transforming, enriching, validating, cleaning, restructuring, or anonymizing data early in the pipeline.


## Method signature
### `add_map` method
```py
def add_map(item_map: ItemTransformFunc[TDataItem],
            insert_at: int = None) -> TDltResourceImpl:
    ...
```

Use `add_map` to apply a function to each item extracted by a resource. It runs your logic on every record before it continues through the pipeline.

**Arguments**:

- `item_map`: A function that takes a single data item (and optionally metadata) and returns a modified item. If the resource yields a list, `add_map` applies the function to each item automatically.
- `insert_at` (optional): An integer that specifies where to insert the function in the pipeline. For example, if data yielding is at index 0, your transformation at index 1, and incremental processing at index 2, you should set `insert_at=1`.

This page covers how `add_map` works, where it fits in the pipeline, and how to use it in different scenarios.

## Related methods

In addition to `add_map`, dlt provides:

- **`add_filter`**: Excludes records based on a condition. Works like a filter function that removes items you don't want to load. ([`resource.add_filter`)](../../../api_reference/dlt/extract/resource#add_filter).
- **`add_yield_map`**: Produces multiple outputs from a single input item. Returns an iterator instead of a single item. ([`resource.add_yield_map`](../../../api_reference/dlt/extract/resource#add_yield_map)).
- **`add_limit`**: Limits the number of records processed by a resource. Useful for testing or reducing data volume during development. ([`resource.add_limit`](../../../api_reference/dlt/extract/resource#add_limit)).

These methods help you control the shape and flow of data during transformation.

## `add_map` vs `@dlt.transformer`

dlt offers two primary ways to handle data operations during extraction:

- **`add_map`**: Ideal for simple, item-level operations within a single resource. Typical examples include masking sensitive fields, formatting dates, or computing additional fields for each record individually as it's extracted.
- **`@dlt.transformer`**: Defines a separate transformer resource that enriches or transforms data during extraction, often by fetching related information concurrently or performing complex operations involving other endpoints or external APIs.

If your needs are straightforward and focused on single-record modifications or operations, `add_map` is usually the simplest, convenient and much more efficient choice.

## Common use cases for `add_map`

- **Data cleaning and enrichment:**
    Modify fields in each record as they are pulled from the source. For example, standardize date formats, compute new fields, or enrich records with additional info.
    
- **Anonymizing or masking sensitive data:**
    Before data is loaded into your warehouse, you might want to pseudonymize personally identifiable information (PII) or for GDPR compliance. Read docs here: [Pseudonymizing columns.](../../general-usage/customising-pipelines/pseudonymizing_columns)
    
- **Removing or renaming fields:**
    If certain fields from the source are not needed or should have different names, you can modify the record dictionaries in-place. Please find the docs here:
    
    - [Removing columns.](../../general-usage/customising-pipelines/removing_columns)
    - [Renaming columns.](../../general-usage/customising-pipelines/renaming_columns)
- **Incremental loading:**
    When using incremental loading, you may need to adjust records before the incremental logic runs. This includes filling in missing timestamp or ID fields used as cursors, or dropping records that don’t meet criteria. The `add_map` function with the `insert_at` parameter lets you run these transformations at the right stage in the pipeline.
    
## Examples
### Using `add_map` to prepare data for incremental loading

Use `add_map` to ensure records are compatible with incremental loading.

If the incremental cursor field (e.g., `updated_at`) is missing, you can provide a fallback like `created_at`. This ensures all records have a valid cursor value and can be processed correctly by the incremental step.

For example, i[n this example](../../general-usage/incremental-loading#transform-records-before-incremental-processing), the third record is made incremental-ready by assigning it a fallback `updated_at` value. This ensures it isn't skipped by the incremental loader.

### Controlling transformation order with `insert_at`

dlt pipelines execute in multiple stages. For example, data is typically yielded at step index `0`, transformations like `add_map` are applied at index `1`, and incremental processing occurs in subsequent steps.

To ensure your transformations are applied before the incremental logic kicks in, it’s important to control the execution order using the `insert_at` parameter of the `add_map` function. This parameter lets you define exactly where your transformation logic is inserted within the pipeline.

```py
import dlt
import hashlib

@dlt.resource
def user_data():
    yield {"id": 1, "first_name": "John", "last_name": "Doe", "email": "john.doe@example.com"}
    yield {"id": 2, "first_name": "Jane", "last_name": "Smith", "email": "jane.smith@example.com"}

# First transformation: mask email addresses
def mask_email(record):
    record["email"] = hashlib.sha256(record["email"].encode()).hexdigest()
    return record

# Second transformation: enrich with full name
def enrich_full_name(record):
    record["full_name"] = f"{record['first_name']} {record['last_name']}"
    return record

# Attach transformations explicitly controlling their order
transformed_users = (
    user_data()
    .add_map(enrich_full_name)  # By default, this would be at the end
    .add_map(mask_email, insert_at=1)  # Explicitly run masking first after extraction
)

# Verify the transformed data
for user in transformed_users:
    print(user)
```

**Expected output**

```py
{'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'email': '<hashed_value>', 'full_name': 'John Doe'}
{'id': 2, 'first_name': 'Jane', 'last_name': 'Smith', 'email': '<hashed_value>', 'full_name': 'Jane Smith'}
```

By explicitly using `insert_at=1`, the email masking step (`mask_email`) is executed right after data extraction and before enrichment. This ensures sensitive data is handled securely at the earliest stage possible.

### Note on Incremental behavior

Use `insert_at` to control when your transformation runs in the pipeline and ensure it executes before incremental filtering. See [incremental loading documentation](../../general-usage/incremental-loading#transform-records-before-incremental-processing) for more details.

## `add_map` vs `add_yield_map`

The difference between `add_map` and `add_yield_map` matters when a transformation returns multiple records from a single input.

#### Example

```py
import dlt
import pandas as pd

@dlt.resource
def orders():
    yield {"order_id": 100, "items": ["Laptop", "Mouse", "Keyboard"]}
    yield {"order_id": 101, "items": ["Monitor"]}

# Function to split items into separate records
def split_items(order):
    return [{"order_id": order["order_id"], "item": item} for item in order["items"]]

mapped_orders = orders().add_map(split_items)
yield_mapped_orders = orders().add_yield_map(split_items)

print("Using add_map:")
print(pd.DataFrame(mapped_orders))

print("Using add_yield_map:")
print(pd.DataFrame(yield_mapped_orders))
```

#### Output
Both methods produce the same output, because `dlt` automatically flattens the list returned by `add_map`.

```text
Using add_map:
0  {'order_id': 100, 'item': 'Laptop'}
1  {'order_id': 100, 'item': 'Mouse'}
2  {'order_id': 100, 'item': 'Keyboard'}
3  {'order_id': 101, 'item': 'Monitor'}

Using add_yield_map:
0  {'order_id': 100, 'item': 'Laptop'}
1  {'order_id': 100, 'item': 'Mouse'}
2  {'order_id': 100, 'item': 'Keyboard'}
3  {'order_id': 101, 'item': 'Monitor'}
```

#### Key Difference

- **`add_map`**: The function should return a list. `dlt` flattens this list into separate records.
- **`add_yield_map`**: The function should yield one record at a time.

If flattening did *not* occur, the output would differ:

```text
Using add_map:
0  [{'order_id': 100, 'item': 'Laptop'}, {'order_id': 100, 'item': 'Mouse'}, {'order_id': 100, 'item': 'Keyboard'}]
1  [{'order_id': 101, 'item': 'Monitor'}]

Using add_yield_map:
0  {'order_id': 100, 'item': 'Laptop'}
1  {'order_id': 100, 'item': 'Mouse'}
2  {'order_id': 100, 'item': 'Keyboard'}
3  {'order_id': 101, 'item': 'Monitor'}
```

#### When to Use Which

- Use **`add_map`** if your function returns a list of records.
- Use **`add_yield_map`** if your function yields records one at a time (e.g. streaming, filtering).

`add_yield_map` lets you yield records one at a time, giving better control over processing. This is helpful when your logic requires conditional output, filtering, or more granular handling.

## Best practices for using `add_map`

- **Keep transformations simple:**
    Functions passed to `add_map` run on each record and should be stateless and lightweight. Use them for tasks like string cleanup or basic calculations. For heavier operations (like per-record API calls), batch the work or move it outside `add_map`, for example into a transformer resource or a post-load step.
    
- **Use the right tool for the job:**
    Use `add_map` for one-to-one record transformations. If you need to drop records, use `add_filter` instead of returning `None` in a map function. To split or expand one record into many, use `add_yield_map`.
    
- **Chain transformations when needed:**
    Since `add_map` and `add_filter` return the resource object, you can chain multiple transformations in sequence. Just be mindful of the order, they execute in the order they are added unless you explicitly control it using `insert_at`.
    
- **Ordering with `insert_at`:**
    When using multiple transforms and built-in steps (like incremental loading), control their order with `insert_at`.
    
    Pipeline steps are zero-indexed in the order they are added. Index `0` is usually the initial data extraction. To run a custom map first, set `insert_at=1`. For multiple custom steps, assign different indices (e.g., one at `1`, another at `2`). If you're unsure about the order, iterate over the resource or check the `dlt` logs to confirm how steps are applied.
    
- **Advanced consideration - data formats**
    Most `dlt` sources yield dictionaries or lists of them. However, some backends, such as PyArrow, may return data as NumPy arrays or Arrow tables. In these cases, your `add_map` or `add_yield_map` function must handle the input format, possibly by converting it to a list of dicts or a pandas DataFrame. This is an advanced scenario, but important if your transformation fails due to unexpected input types.