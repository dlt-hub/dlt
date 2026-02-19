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

## Masking columns in SQL database sources

For more realistic use cases, especially when working with SQL database sources, you'll want a reusable function that can mask multiple columns and handle different backends. The example below shows a production-ready approach that:

- Takes column names as parameters (reusable across different tables)
- Supports multiple backends: `pyarrow`, `connectorx`, `pandas`, and `sqlalchemy`
- Offers both masking (replacing with a mask string) and nullifying (setting to NULL) methods
- Uses a closure pattern to capture the column configuration

```py
from enum import Enum
import pyarrow as pa
import pandas as pd
import dlt
from dlt.sources.sql_database import sql_table

class MaskingMethod(str, Enum):
    MASK = "mask"
    NULLIFY = "nullify"

def mask_columns(
    columns: list[str],
    method: MaskingMethod = MaskingMethod.MASK,
    mask: str = "******",
):
    """Create a masking function for specified columns in a SQL Database table.
    
    This function returns a closure that can be used with add_map() to mask
    sensitive data across different SQL database backends.
    
    All backends supported by the sql_database source are handled:
    - pyarrow (Arrow tables)
    - connectorx (Arrow tables)
    - pandas (DataFrames)
    - sqlalchemy (dict rows)
    
    Args:
        columns: List of column names to mask
        method: Either 'mask' (replace with mask string) or 'nullify' (set to NULL)
        mask: String to use when masking (default: "******")
    
    Returns:
        A function that can be passed to add_map()
    
    Example:
        >>> table = sql_table(table="users")
        >>> table.add_map(mask_columns(columns=["email", "phone"]))
        >>> pipeline = dlt.pipeline(destination="duckdb")
        >>> pipeline.run(table)
    """
    def mask_table_or_row(
        table_or_row: pa.Table | pd.DataFrame | dict,
    ) -> pa.Table | pd.DataFrame | dict:
        # Handle PyArrow tables (pyarrow and connectorx backends)
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
                        table.schema.get_field_index(col),
                        col,
                        replace_with,
                    )
            return table
        
        # Handle Pandas DataFrames (pandas backend)
        if isinstance(table_or_row, pd.DataFrame):
            table = table_or_row.copy()
            for col in table.columns:
                if col in columns:
                    if method == MaskingMethod.MASK:
                        table[col] = mask
                    elif method == MaskingMethod.NULLIFY:
                        table[col] = None
            return table
        
        # Handle dict rows (sqlalchemy backend)
        if isinstance(table_or_row, dict):
            row = table_or_row.copy()
            for col in row:
                if col in columns:
                    if method == MaskingMethod.MASK:
                        row[col] = mask
                    elif method == MaskingMethod.NULLIFY:
                        row[col] = None
            return row
        
        # Unsupported type
        raise NotImplementedError(
            f"Unsupported table type: {type(table_or_row)}. "
            f"Supported backends: pyarrow, connectorx, pandas, sqlalchemy"
        )
    
    return mask_table_or_row

# Example usage with SQL database source
table = sql_table(
    table="customers",
    backend="pandas"  # or "pyarrow", "connectorx", "sqlalchemy"
)

# Mask sensitive columns
table.add_map(mask_columns(columns=["email", "phone", "ssn"], method=MaskingMethod.MASK))

# Or use nullify method
table.add_map(mask_columns(columns=["email"], method=MaskingMethod.NULLIFY))

pipeline = dlt.pipeline(
    pipeline_name="masked_data",
    destination="duckdb",
    dataset_name="analytics"
)
load_info = pipeline.run(table)
```

### Understanding closures in data masking

The `mask_columns` function above uses a **closure** pattern - it returns an inner function (`mask_table_or_row`) that "remembers" the parameters (`columns`, `method`, `mask`) from its outer scope. This is a powerful pattern because:

1. **Reusability**: You can create multiple masking functions with different configurations
2. **Clean syntax**: `add_map(mask_columns(["email"]))` is more readable than passing parameters each time
3. **Encapsulation**: The masking logic is self-contained and configurable

```py
# Create different masking functions for different needs
mask_pii = mask_columns(columns=["ssn", "tax_id"], method=MaskingMethod.MASK)
nullify_optional = mask_columns(columns=["middle_name"], method=MaskingMethod.NULLIFY)

# Apply them to different tables
customers_table.add_map(mask_pii)
employees_table.add_map(mask_pii)
contacts_table.add_map(nullify_optional)
```

