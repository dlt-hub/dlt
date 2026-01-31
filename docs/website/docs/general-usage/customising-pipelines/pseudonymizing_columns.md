---
title: Pseudonymizing columns
description: Pseudonymizing (or anonymizing) columns by replacing the special characters
keywords: [pseudonymize, anonymize, columns, special characters]
---

# Pseudonymizing columns

Pseudonymization is a deterministic way to hide personally identifiable information (PII), enabling us to consistently achieve the same mapping. If instead you wish to anonymize, you can delete the data or replace it with a constant. In the example below, we create a dummy source with a PII column called "name", which we replace with deterministic hashes (i.e., replacing the German umlaut).

```py
import dlt
import hashlib

@dlt.source
def dummy_source(prefix: str = None):
    @dlt.resource
    def dummy_data():
        for _ in range(3):
            yield {'id': _, 'name': f'Jane Washington {_}'}
    return dummy_data(),

def pseudonymize_name(doc):
    '''
    Pseudonymization is a deterministic type of PII-obscuring.
    Its role is to allow identifying users by their hash,
    without revealing the underlying info.
    '''
    # add a constant salt to generate
    salt = 'WI@N57%zZrmk#88c'
    salted_string = doc['name'] + salt
    sh = hashlib.sha256()
    sh.update(salted_string.encode())
    hashed_string = sh.digest().hex()
    doc['name'] = hashed_string
    return doc

# run it as is
for row in dummy_source().dummy_data.add_map(pseudonymize_name):
    print(row)

#{'id': 0, 'name': '96259edb2b28b48bebce8278c550e99fbdc4a3fac8189e6b90f183ecff01c442'}
#{'id': 1, 'name': '92d3972b625cbd21f28782fb5c89552ce1aa09281892a2ab32aee8feeb3544a1'}
#{'id': 2, 'name': '443679926a7cff506a3b5d5d094dc7734861352b9e0791af5d39db5a7356d11a'}

# Or create an instance of the data source, modify the resource and run the source.

# 1. Create an instance of the source so you can edit it.
source_instance = dummy_source()
# 2. Modify this source instance's resource
data_resource = source_instance.dummy_data.add_map(pseudonymize_name)
# 3. Inspect your result
for row in source_instance:
    print(row)

pipeline = dlt.pipeline(pipeline_name='example', destination='bigquery', dataset_name='normalized_data')
load_info = pipeline.run(source_instance)
```



## Advanced Data Masking for SQL Database Sources

The example above works well for simple cases with hardcoded column names. For production use cases, you might want a more flexible function that:

- Takes column names as parameters
- Supports multiple masking methods (mask with asterisks or nullify)
- Works with different data backends (PyArrow, Pandas, SQL)

Here's a more realistic data masking example:

```py
from enum import StrEnum
import pyarrow as pa
import pandas as pd
import dlt

class MaskingMethod(StrEnum):
    MASK = "mask"
    NULLIFY = "nullify"

def mask_sql_db_columns(
    columns: list[str],
    method: MaskingMethod | None = None,
    mask: str = "*******",
) -> pa.Table | pd.DataFrame | dict:
    """Mask specified columns in a SQL Database table.
    
    Args:
        columns (list[str]): The list of columns to mask.
        method (MaskingMethod): The masking method to use. Defaults to MASK.
        mask (str): The mask string to use. Defaults to '*******'.
    
    Returns:
        pa.Table | pd.DataFrame | dict[str, Any]: The table with columns masked.
    """
    if method is None:
        method = MaskingMethod.MASK
        
    def mask_columns(
        table_or_row: pa.Table | pd.DataFrame | dict[str, Any]
    ) -> pa.Table | pd.DataFrame | dict[str, Any]:
        # Handle `pyarrow` and `connectorx` backends
        if isinstance(table_or_row, pa.Table):
            table = table_or_row
            for col in table.schema.names:
                if col in columns:
                    if method == MaskingMethod.MASK:
                        replace_with = pa.array([mask] * table.num_rows)
                    elif method == MaskingMethod.NULLIFY:
                        replace_with = pa.nulls(
                            table.num_rows, type=table.schema.field(col).type
                        )
                    table = table.set_column(
                        table.schema.get_field_index(col), col, replace_with
                    )
            return table

        # Handle `pandas` backend
        if isinstance(table_or_row, pd.DataFrame):
            table = table_or_row
            for col in table.columns:
                if col in columns:
                    if method == MaskingMethod.MASK:
                        table[col] = mask
                    elif method == MaskingMethod.NULLIFY:
                        table[col] = None
            return table

        # Handle `sqlalchemy` backend
        if isinstance(table_or_row, dict):
            row = table_or_row
            for col in row:
                if col in columns:
                    if method == MaskingMethod.MASK:
                        row[col] = mask
                    elif method == MaskingMethod.NULLIFY:
                        row[col] = None
            return row

        msg = f"Unsupported table type: {type(table_or_row)}"
        raise NotImplementedError(msg)
    
    return mask_columns
```

Then, it can be used with `sql_table` source like so:

```py
from dlt.sources.sql_database import sql_table

table = sql_table(table="users")
table.add_map(mask_sql_db_columns(columns=["email", "phone"]))

pipeline = dlt.pipeline()
load_info = pipeline.run(table)
```

This implementation:
- Takes a list of column names to mask, making it reusable across different tables
- Supports two masking methods: `MASK` (replace with asterisks) and `NULLIFY` (set to null)
- Works with PyArrow tables, Pandas DataFrames, and dictionary rows
- Can be easily extended to support additional backends
