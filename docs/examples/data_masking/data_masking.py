"""
---
title: Data masking for SQL database sources
description: Learn how to mask sensitive columns using closures that work with all sql_database backends
keywords: [data masking, pseudonymize, anonymize, sql_database, example]
---

This example shows how to build a reusable column-masking function for the
`sql_database` source. The function uses a closure to capture masking
configuration, so you can apply it to any resource via `add_map()`.

Based on an implementation by [Michał Zawadzki](https://github.com/trymzet)
and [Varun Chawla](https://github.com/veeceey).

We'll learn how to:

- Use a [closure](https://docs.python.org/3/faq/programming.html#why-am-i-getting-an-unboundlocalerror-when-the-variable-has-a-value) to create a configurable masking callback
- Handle all backends supported by [sql_database](../dlt-ecosystem/verified-sources/sql_database/index.md): PyArrow, ConnectorX, Pandas, and SQLAlchemy
- Support two masking strategies: replace with a mask string, or nullify

## Usage with `sql_table`

```py
from dlt.sources.sql_database import sql_table

table = sql_table(table="users")
table.add_map(mask_columns(columns=["email", "ssn"]))

pipeline = dlt.pipeline(
    pipeline_name="masked_data",
    destination="duckdb",
    dataset_name="mydata",
)
load_info = pipeline.run(table)
```

"""

from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import pyarrow as pa
import pandas as pd


class MaskingMethod(str, Enum):
    MASK = "mask"
    NULLIFY = "nullify"


def mask_columns(
    columns: List[str],
    method: Optional[MaskingMethod] = None,
    mask: str = "******",
) -> Callable[..., Any]:
    """Return a mapping function that masks the specified columns.

    Args:
        columns (List[str]): Column names to mask.
        method (Optional[MaskingMethod]): MASK replaces with `mask` string,
            NULLIFY sets to None. Defaults to MASK.
        mask (str): Replacement string used when method is MASK.

    Returns:
        Callable: A function suitable for `resource.add_map()`.
    """
    resolved_method: MaskingMethod = (
        method if method is not None else MaskingMethod.MASK
    )

    def _apply(
        table_or_row: Union[pa.Table, pd.DataFrame, Dict[str, Any]],
    ) -> Union[pa.Table, pd.DataFrame, Dict[str, Any]]:
        # pyarrow / connectorx backends
        if isinstance(table_or_row, pa.Table):
            table = table_or_row
            for col in table.schema.names:
                if col in columns:
                    if resolved_method == MaskingMethod.MASK:
                        replacement = pa.array([mask] * table.num_rows)
                    else:
                        replacement = pa.nulls(
                            table.num_rows, type=table.schema.field(col).type
                        )
                    table = table.set_column(
                        table.schema.get_field_index(col), col, replacement
                    )
            return table

        # pandas backend
        if isinstance(table_or_row, pd.DataFrame):
            df = table_or_row
            for col in df.columns:
                if col in columns:
                    df[col] = mask if resolved_method == MaskingMethod.MASK else None
            return df

        # sqlalchemy backend (dict rows)
        if isinstance(table_or_row, dict):
            row = table_or_row
            for col in row:
                if col in columns:
                    row[col] = mask if resolved_method == MaskingMethod.MASK else None
            return row

        raise NotImplementedError(f"Unsupported data type: {type(table_or_row)}")

    return _apply


if __name__ == "__main__":
    import dlt

    # create a dummy source with sensitive columns
    @dlt.resource(write_disposition="replace")
    def users():
        yield [
            {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "ssn": "123-45-6789",
            },
            {"id": 2, "name": "Bob", "email": "bob@example.com", "ssn": "987-65-4321"},
            {
                "id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "ssn": "555-12-3456",
            },
        ]

    # mask email and ssn before loading
    masked_users = users()
    masked_users.add_map(mask_columns(columns=["email", "ssn"]))

    pipeline = dlt.pipeline(
        pipeline_name="data_masking_example",
        destination="duckdb",
        dataset_name="mydata",
    )
    load_info = pipeline.run(masked_users)
    print(load_info)

    # verify: sensitive columns are masked, other columns are untouched
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT id, name, email, ssn FROM mydata.users ORDER BY id"
        )

    for row in rows:
        assert row[2] == "******", f"email should be masked, got {row[2]}"
        assert row[3] == "******", f"ssn should be masked, got {row[3]}"
    assert rows[0][1] == "Alice"
    assert rows[1][1] == "Bob"

    # now demonstrate NULLIFY method on a second table
    @dlt.resource(
        write_disposition="replace",
        columns={"phone": {"data_type": "text"}},
    )
    def customers():
        yield [
            {"id": 1, "name": "Dana", "phone": "555-0001"},
            {"id": 2, "name": "Eve", "phone": "555-0002"},
        ]

    nullified_customers = customers()
    nullified_customers.add_map(
        mask_columns(columns=["phone"], method=MaskingMethod.NULLIFY)
    )

    load_info = pipeline.run(nullified_customers)
    print(load_info)

    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT id, name, phone FROM mydata.customers ORDER BY id"
        )

    for row in rows:
        assert row[2] is None, f"phone should be nullified, got {row[2]}"
    assert rows[0][1] == "Dana"

    print("All masking pipeline checks passed.")
