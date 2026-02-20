---
title: Pseudonymizing columns
description: Pseudonymizing (or anonymizing) columns by replacing the special characters
keywords: [pseudonymize, anonymize, columns, special characters]
---

# Pseudonymizing columns

Pseudonymization is a deterministic way to hide personally identifiable information (PII), enabling us to consistently achieve the same mapping. If instead you wish to anonymize, you can delete the data or replace it with a constant.

## Masking columns with a closure

In real-world scenarios, you typically want to mask specific columns dynamically rather than hardcoding column names. The example below shows a realistic approach using a closure that works with all backends supported by the `sql_database` source (PyArrow, ConnectorX, Pandas, and SQLAlchemy).

```py
from enum import Enum
from typing import Any, Callable, Optional

import pyarrow as pa
import pandas as pd
import dlt


class MaskingMethod(str, Enum):
    MASK = "mask"
    NULLIFY = "nullify"


def mask_columns(
    columns: list[str],
    method: Optional[MaskingMethod] = None,
    mask: str = "******",
) -> Callable[..., Any]:
    """Mask specified columns in a table or row.

    All backends supported by the sql_database source, as of version 1.4.1, are
    supported. See https://github.com/dlt-hub/dlt/blob/devel/dlt/sources/sql_database/helpers.py#L50

    Args:
        columns (list[str]): The list of columns to mask.
        method (Optional[MaskingMethod]): The masking method to use (MASK or NULLIFY).
        mask (str): The mask string to use when method is MASK.

    Returns:
        Callable: A function that masks the specified columns in a table or row.

    """
    resolved_method: MaskingMethod = method if method is not None else MaskingMethod.MASK

    def apply_masking(
        table_or_row: Any,
    ) -> Any:
        # Handle `pyarrow` and `connectorx` backends.
        if isinstance(table_or_row, pa.Table):
            table = table_or_row
            for col in table.schema.names:
                if col in columns:
                    if resolved_method == MaskingMethod.MASK:
                        replace_with = pa.array([mask] * table.num_rows)
                    elif resolved_method == MaskingMethod.NULLIFY:
                        replace_with = pa.nulls(
                            table.num_rows, type=table.schema.field(col).type
                        )
                    table = table.set_column(
                        table.schema.get_field_index(col),
                        col,
                        replace_with,
                    )
            return table

        # Handle `pandas` backend.
        if isinstance(table_or_row, pd.DataFrame):
            table = table_or_row

            for col in table.columns:
                if col in columns:
                    if resolved_method == MaskingMethod.MASK:
                        table[col] = mask
                    elif resolved_method == MaskingMethod.NULLIFY:
                        table[col] = None
            return table

        # Handle `sqlalchemy` backend.
        if isinstance(table_or_row, dict):
            row = table_or_row
            for col in row:
                if col in columns:
                    if resolved_method == MaskingMethod.MASK:
                        row[col] = mask
                    elif resolved_method == MaskingMethod.NULLIFY:
                        row[col] = None
            return row

        # Handle unsupported table types.
        msg = f"Unsupported table type: {type(table_or_row)}. Supported backends: (pyarrow, connectorx, pandas, sqlalchemy)."
        raise NotImplementedError(msg)

    return apply_masking


# Usage example with sql_database source
from dlt.sources.sql_database import sql_table

# Mask the 'password' and 'ssn' columns in a table
table = sql_table(table="users")
table.add_map(mask_columns(columns=["password", "ssn"]))

pipeline = dlt.pipeline(pipeline_name='example', destination='bigquery', dataset_name='masked_data')
load_info = pipeline.run(table)

# Or use NULLIFY method to replace with NULL instead of a mask string
table = sql_table(table="users")
table.add_map(mask_columns(columns=["password", "ssn"], method=MaskingMethod.NULLIFY))

pipeline = dlt.pipeline(pipeline_name='example', destination='bigquery', dataset_name='masked_data')
load_info = pipeline.run(table)
```

This approach uses a closure to capture the `columns`, `method`, and `mask` parameters, allowing you to reuse the same masking logic across different resources with different column configurations. The function automatically handles different data formats (PyArrow tables, Pandas DataFrames, and dictionaries) depending on the backend used by your source.
