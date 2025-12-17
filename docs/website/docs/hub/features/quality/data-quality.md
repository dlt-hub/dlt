---
title: "Data quality 🧪"
description: Validate your data and control its quality
keywords: ["dlthub", "data quality", "contracts", "check", "metrics"]
---

:::warning
🚧 This feature is under development. Interested in becoming an early tester? [Join dltHub early access](https://info.dlthub.com/waiting-list).
:::

dltHub will allow you to define data validation rules in Python. This ensures your data meets expected quality standards at the ingestion step.

## Key features
With dltHub, you will be able to:

* Define data tests and quality contracts in Python.
* Apply both row-level and batch-level validation.
* Enforce constraints on distributions, boundaries, and expected values.

Stay tuned for updates as we expand these capabilities! 🚀

## Checks
A **data quality check** or **check** is a function applied to data that returns a **check result** or **result** (can be boolean, integer, float, etc.). The result that is converted to a success / fail **check outcome** or **outcome** (boolean) based on a **decision**.

:::info
A **test** verifies that **code** behaves as expected. A **check** verifies that the **data** meets some expectations. Code tests enable you to make changes with confidence and data checks help monitor your live systems.
:::

For example, the check `is_in(column_name, accepted_values)` verifies that the column only includes accepted values. Running the check counts successful records to compute the success rate (**result**). The **outcome** will be a success if success rate is 100%, i.e., all records succeeded the check (**decision**).

This snippet shows a single `is_in()` check being ran against the `orders` table in the `point_of_sale` dataset.

```py
import dlt
from dlt.hub import data_quality as dq

dataset = dlt.dataset("duckdb", "point_of_sale")
checks = [
    dq.checks.is_in("payment_methods", ["card", "cash", "voucher"])
]

# prepare a query to execute all checks
check_plan = dq.prepare_checks(dataset["orders"], checks=checks)
# execute checks and get results in-memory
check_plan.df()
```


### Check level
The **check level** indicates the granularity of the **check result**. For instance:
- **Row-level** checks produce a result per record. It's possible to inspect which specific records pass / failed the check.

- **Table-level** checks produce a result per table (e.g., result is "the number of unique values" and decision is "is this greater than 5?"). 

    These checks can often be rewritten as row-level checks (e.g., "is this value unique?")

- **Dataset-level** checks produce a result per dataset. This typically involves multiple tables, temporal comparisons, or pipeline information (e.g., "the number of rows in 'orders' is higher than the number of rows in 'customers')

:::important
Notice that the **check level** relates to the result and not the **input data** of the check. For instance, a row-level check can involve multiple tables as input.
:::


### Built-in checks
The library `dlthub` includes many built-in checks: `is_in()`, `is_unique()`, `is_primary_key()`, and more. The built-in `case()` and `where()` simplify custom row and table-level checks respectively.

For example, the following are equivalent:

```py
from dlt.hub import data_quality as dq

dq.checks.is_unique("foo")
dq.checks.case("COUNT(*) OVER (PARTITION BY foo) > 1")
```


### Custom checks (WIP)
Can be implemented as a `dlt.hub.transformation` that matches a specific schema or as subclass of `_BaseCheck` for full control. This allows to use any language supported by transformations, allowing eager/lazy and in-memory/on-backend execution.

Notes:
- Should have utilities to statically validate check definitions (especially lazy)
- Should have testing utilities that makes it easy to unit test checks (same utilities as transformations)


### Lifecycle
Data quality checks can be executed at different stages of the pipeline lifecycle. This choice has several impacts, including:
- the **input data** available for the check
- the compute resources used
- the **actions** available (e.g., drop invalid load)

<!--How does this affect transactions? How do we handle errors in the data quality part-->

#### Post-load
The post-load execution is the simplest option. The pipeline goes through `Extract -> Normalize -> Load` as usual. Then, the checks are executed on the destination.

Properties:
- Failed records can't be dropped or quarantined before load. All records must be written, checked, and then handled. This only works with `write_disposition="append"` or destinations supporting snapshots (e.g. `iceberg`, `ducklake`).
- Checks have access to the full dataset. This includes current and past loads + internal dlt tables.
- Computed directly on the destination. This scales well with the size of the data and the complexity of the checks.
- Results and outcome are directly stored on the dataset. No data movement is required.

```mermaid
sequenceDiagram
    participant Resource
    participant Pipeline
    participant Dataset

    Resource->>Pipeline: Extract
    Pipeline->>Pipeline: Normalize
    Pipeline->>Dataset: Load
    Dataset->>Dataset: Run Checks
```

#### Pre-load (staging)
The pre-load execution via staging dataset allows to execute checks on the destination and trigger actions before data is loaded into the dataset. This is effectively using **post-load** checks before a 2nd load phase.

:::info
`dlt` uses staging datasets for other features such as `merge` and `replace` write dispositions.
:::

Properties:
- Failed records can be dropped or quarantined before load. This works with all `write_disposition`
- Requires a destination that supports staging datasets.
- Checks have access to the current load. 
    - If the staging dataset is on the same destination, checks can access the full dataset. 
    - If the staging dataset is on a different destination, communication between the staging dataset and the dataset.
- Computed on the staging destination.  This scales well with the size of the data and the complexity of the checks.
- Data and checks results & outcome can be safely stored on the staging dataset until review. This helps human-in-the-loop workflows without reprocessing the full pipeline.


```mermaid
sequenceDiagram
    participant Resource
    participant Pipeline
    participant Staging as Staging Dataset
    participant Dataset

    Resource->>Pipeline: Extract
    Pipeline->>Pipeline: Normalize
    Pipeline->>Staging: Load
    Staging->>Staging: Run Checks
    Staging->>Dataset: Load
```


#### Pre-load (in-memory)

The pre-load execution in-memory will execute checks using `duckdb` against the load packages (i.e., temporary files) stored on the machine that runs `dlt`. This allows to trigger actions before data is loaded into the destination.

:::note
This is equivalent to using a staging destination that is the local filesystem. This section highlights the trade-offs of this choice.
:::

Properties:
- Failed records can be dropped or quarantined before load. This works with all `write_disposition
- Checks only have access to the current load. Checking against the full dataset requires communication between the staging destination and the main destination.
- Computed on the machine running the pipeline. The resource need to match the compute requirements.
- Data and checks results & outcome may be lost if the runtime is ephemeral (e.g., AWS Lambda timeout). In this case, the pipeline must process the data again.

```mermaid
sequenceDiagram
    participant Resource
    participant Pipeline
    participant Dataset

    Resource->>Pipeline: Extract
    Pipeline->>Pipeline: Normalize
    Pipeline->>Pipeline: Run checks
    Pipeline->>Dataset: Load
```

## Migration and versioning (WIP)

As the real-world change, their can be addition, removal, or modification of data quality checks for your pipeline / dataset. This is require for proper auditing.

For example, the check `is_in("division", ["europe", "america"])` defined in 2024 could evolve to `is_in("division", ["europe", "america", "asia"])` in 2026.

Notes:
- checks need to be serialized and hashed (trivial for lazy checks)
- checks can be stored on schema (consequently on the destination too)
- this is the same challenge as versioning transformations

## Action (WIP)
After running checks, **actions** can be triggered based on the **check result** or **check outcome**. 

Notes:
- actions can be configured globally or per-check
- planned actions: drop data, quarantine data (move to a special dataset), resolve (e.g., fill value, set default, apply transformation), fail (prevents load), raise/alert (sends notification)
- This needs to be configurable from outside the source code (e.g., via `config.toml`). The same checks would require different action during development vs. prod


## Metrics



