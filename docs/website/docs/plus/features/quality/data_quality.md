---
title: "🧪 Data quality"
description: Validate your data and control its quality
keywords: ["dlt+", "data quality", "contracts"]
---

:::note
🚧 This feature is under development. Interested in becoming an early tester? [Join dlt+ early access](https://info.dlthub.com/waiting-list)
:::

dlt+ will allow you to define data validation rules at the YAML level or using Pydantic models. This ensures your data meets expected quality standards at the ingestion step.

## Example: Defining a quality contract in YAML

You can specify quality contracts to enforce constraints on your data, such as expected value ranges and nullability.

```yaml
engine_version: 10
name: scd_type_3
tables:
  customers:
    columns:
      category:
        data_type: bigint
        nullable: false
        quality_contracts:
          expect_column_max_to_be_between:
            min_value: 1
            max_value: 100
```

## Key features
With dlt+, you will be able to:

* Define data tests and quality contracts using YAML configuration or Pydantic models.
* Apply both row-level and batch-level validation.
* Enforce constraints on distributions, boundaries, and expected values.

Stay tuned for updates as we expand these capabilities! 🚀

