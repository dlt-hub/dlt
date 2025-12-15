---
title: Data Quality Lifecycle
description: End-to-end data quality checks across the dlt pipeline lifecycle
keywords: [data quality, validation, contracts, pydantic, schema, monitoring, governance]
---

# Data Quality Lifecycle

The data quality lifecycle has rarely been achievable 
in a single tool due to the runtime constraints of 
traditional ETL vendors. 

## One library, end-to-end

Because `dlt` together with `dltHub` span the entire pipeline, starting from ingestion, passing through a portable staging layer, and extending into the transformation, it uniquely bridges these gaps.

Instead of stitching together four or five separate tools, you write Python code that works across the entire pipeline. No glue scripts. No context lost between systems.


![Data Quality Lifecycle](https://storage.googleapis.com/dlt-blog-images/docs-DQ-lifecycle.png)

**The three checkpoints for data quality:**

1. **In-flight:** Check individual records as data is extracted, before loading it.
2. **Staging:** We optionally load the data to an optionally transient staging area where we can test it without breaking production.
3. **Destination:** Check properties of the full dataset currently written to the destination.

## The five pillars of data quality

`dlt` addresses quality across five core dimensions, offering support for implementing these checks across the entire data lifecycle.

1. **Structural Integrity:** Does the data fit the destination schema and types?
2. **Semantic Validity:** Does the data make business sense?
3. **Uniqueness & Relations:** Is the dataset consistent with itself?
4. **Privacy & Governance:** Is the data safe and compliant?
5. **Operational Health:** Is the pipeline running correctly?

![Five Pillars of Data Quality](https://storage.googleapis.com/dlt-blog-images/docs-DQ-pillars.png)

---

### 1. Structural Integrity

*Does the data fit the destination schema and types?*

These checks ensure incoming data conforms to the expected shape and technical types before loading, preventing broken pipelines and "garbage" tables.

| Job to be Done | dlt Solution | Learn More | Availability |
|----------------|--------------|------------|--------------|
| Prevent unexpected columns | **Schema Contracts (Frozen Mode):** Set your schema to `frozen` to raise an immediate error if the source API adds an undocumented field. | [Schema Contracts](../general-usage/schema-contracts.md) | dlt |
| Enforce data types | **Type Coercion:** `dlt` automatically coerces compatible types (e.g., string `"100"` to int `100`) and rejects non-coercible values to ensure column consistency. | [Schema](../general-usage/schema.md) | dlt |
| Fix naming errors | **Normalization:** `dlt` automatically cleans table and column names (converting to `snake_case`) to prevent SQL syntax errors in the destination. | [Naming Convention](../general-usage/naming-convention.md) | dlt |
| Enforce required fields | **Nullability Constraints:** Mark fields as `nullable=False` in your resource hints to drop or error on records missing critical keys. | [Resource](../general-usage/resource.md) | dlt |

---

### 2. Semantic Validity

*Does the data make business sense?*

These checks verify the content of the data against business logic. While structural checks handle types (*is it a number?*), semantic checks handle meaning (*is it a valid age?*).

| Job to be Done | dlt Solution | Learn More | Availability |
|----------------|--------------|------------|--------------|
| Validate logic & ranges | **Pydantic Models:** Attach Pydantic models to your resources to enforce logic like `age > 0` or email format validation in-stream. | [Schema Contracts](../general-usage/schema-contracts.md#use-pydantic-models-for-data-validation) | dlt |
| Filter bad rows | **`add_filter`:** Apply a predicate function to exclude records that don't meet criteria (e.g., `lambda x: x["status"] != "deleted"`). | [Transform with add_map](../dlt-ecosystem/transformations/add-map.md) | dlt |
| Check batch anomalies | **Staging Tests:** Use the portable runtime (e.g., Ibis/DuckDB) to query the staging buffer. Example: "Alert if the average order value in this batch is > $10k." | [Staging](../dlt-ecosystem/staging.md) | dlt |
| Built-in data checks | **Data Quality Checks:** Use built-in checks like `is_in()`, `is_unique()`, `is_primary_key()` with pre-load or post-load execution, plus actions on failure (drop, quarantine, alert). | [Data Quality](https://dlthub.com/docs/hub/features/quality/data-quality) | dlthub |

---

### 3. Uniqueness & Relations

*Is the dataset consistent with itself?*

These checks manage duplication and preserve relationships between different tables in your dataset.

| Job to be Done | dlt Solution | Learn More | Availability |
|----------------|--------------|------------|--------------|
| Prevent duplicates | **Merge Disposition:** Define `primary_key` and `write_disposition='merge'` to automatically upsert records. `dlt` handles the deduping logic for you. | [Incremental Loading](../general-usage/incremental-loading.md) | dlt |
| Track historical changes | **SCD2 Strategy:** Use `write_disposition={"disposition": "merge", "strategy": "scd2"}` to automatically maintain validity windows (`_dlt_valid_from`, `_dlt_valid_to`) for dimension tables. | [Merge Loading](../general-usage/merge-loading.md#scd2-strategy) | dlt |
| Link parent/child data | **Automatic Lineage:** `dlt` automatically generates foreign keys (`_dlt_parent_id`) when unnesting complex JSON, preserving the link between parent and child tables. | [Destination Tables](../general-usage/destination-tables.md#nested-tables) | dlt |
| Find orphan keys | **Post-Load Assertions:** Run SQL tests on the destination to identify child records missing a valid parent (e.g., Orders without Customers). | [SQL Transformations](../dlt-ecosystem/transformations/sql.md) | dlt |

---

### 4. Privacy & Governance

*Is the data safe and compliant?*

Data quality also means compliance. These features ensure sensitive data is handled correctly before it becomes a liability in your warehouse.

| Job to be Done | dlt Solution | Learn More | Availability |
|----------------|--------------|------------|--------------|
| Mask/Hash PII | **Transformation Hooks:** Use `add_map` to hash emails or redact names in-stream. Data is sanitized in memory before it ever touches the disk. | [Pseudonymizing Columns](../general-usage/customising-pipelines/pseudonymizing_columns.md) | dlt |
| Drop sensitive columns | **Column Removal:** Use `add_map` to completely remove columns (e.g., `ssn`, `credit_card`) before they ever reach the destination. | [Removing Columns](../general-usage/customising-pipelines/removing_columns.md) | dlt |
| Enforce PII Contracts | **Pydantic Models:** Use Pydantic schemas to strictly define and detect sensitive fields (e.g., `EmailStr`), ensuring they are caught and hashed before loading. | [Schema Contracts](../general-usage/schema-contracts.md) | dlt |
| Join on private data | **Deterministic Hashing:** Use a secret salt via `dlt.secrets` to deterministically hash IDs, allowing you to join tables on "User ID" without exposing the actual user identity. | [Credentials Setup](../general-usage/credentials/setup.md) | dlt |
| Track PII through transformations | **Column-Level Hint Forwarding:** PII hints (e.g., `x-annotation-pii`) are automatically propagated through SQL transformations, so downstream tables retain knowledge of sensitive origins. | [Transformations](https://dlthub.com/docs/hub/features/transformations#column-level-hint-forwarding) | dlthub |

---

### 5. Operational Health

*Is the pipeline running correctly?*

Monitoring the reliability of the delivery mechanism itself. Even perfectly valid data is "bad quality" if it arrives 24 hours late.

| Job to be Done | dlt Solution | Learn More | Availability |
|----------------|--------------|------------|--------------|
| Detect empty loads | **Volume Monitoring:** Inspect `load_info` metrics after a run. Trigger an alert if `row_count` drops to zero unexpectedly. | [Running in Production](running.md#inspect-and-save-the-load-info-and-trace) | dlt |
| Collect custom metrics | **Custom Metrics:** Track business-specific statistics during extraction (e.g., page counts, API call counts) using `dlt.current.resource_metrics()`. | [Resource](../general-usage/resource.md#collect-custom-metrics) | dlt |
| Monitor Freshness (SLA) | **Load Metadata:** Query the `_dlt_loads` table in your destination to verify the `inserted_at` timestamp meets your freshness SLA. | [Destination Tables](../general-usage/destination-tables.md#load-packages-and-load-ids) | dlt |
| Audit Schema Drift | **Schema History:** Even in permissive modes, `dlt` tracks every schema change. Use the audit trail to see exactly when a new column was introduced. | [Schema Evolution](../general-usage/schema-evolution.md) | dlt |
| Alert on schema changes | **Schema Update Alerts:** Inspect `load_info.load_packages[].schema_update` after each run to detect new tables/columns and trigger alerts (e.g., Slack notification when schema drifts). | [Running in Production](running.md#inspect-save-and-alert-on-schema-changes) | dlt |
| Trace Lineage | **Load IDs:** Every row in your destination is tagged with `_dlt_load_id`. You can trace any specific record back to the exact pipeline run that produced it. | [Destination Tables](../general-usage/destination-tables.md#data-lineage) | dlt |
| Alert on failures | **Slack Integration:** Send pipeline success/failure notifications via Slack incoming webhooks configured in `dlt.secrets`. | [Alerting](alerting.md) | dlt |



## Validate data quality during development

Use the [dlt Dashboard](../general-usage/dashboard.md) to interactively inspect your pipeline during development. The dashboard lets you:

- Query loaded data and verify row counts match expectations
- Inspect schemas, columns, and all column hints
- Check incremental state of each resource
- Review load history and trace information
- Catch issues like pagination bugs (suspiciously round counts) before they reach production

```sh
dlt pipeline {pipeline_name} show
```

---

## Get the full lifecycle with dltHub

The features marked `dlt` in the tables above are available today in the open-source library. **dltHub** provides a managed runtime and additional data quality capabilities:

- **Run dlt on the dltHub runtime** — Execute all your existing dlt pipelines with managed infrastructure, scheduling, and observability built-in.
- **Built-in data quality checks** — Use `is_in()`, `is_unique()`, `is_primary_key()`, and more with row-level and batch-level validation.
- **Pre-load and post-load execution** — Run checks in staging before data hits your warehouse, or validate after load with full dataset access.
- **Follow-up actions on failure** — Bad data quarantine to enable faster debugging.
- **Column-level hint forwarding** — Track PII and other sensitive column hints through SQL transformations.

:::tip Early Access
Interested in the full data quality lifecycle? [Join dltHub early access](https://info.dlthub.com/waiting-list) to get started.
:::

[Learn more about dltHub Data Quality →](https://dlthub.com/docs/hub/features/quality/data-quality)

---

## Related documentation

- [dlt Dashboard](../general-usage/dashboard.md) — Inspect pipelines, schemas, and data during development
- [Schema Contracts](../general-usage/schema-contracts.md) — Schema contracts and Pydantic validation
- [Running in Production](running.md) — Metrics, load info, and debugging
- [Monitoring](monitoring.md) — Run and data monitoring
- [Alerting](alerting.md) — Setting up alerts for pipeline issues
