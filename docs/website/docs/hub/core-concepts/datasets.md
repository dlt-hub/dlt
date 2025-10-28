# Datasets

A dataset is a physical collection of data and dlt metadata, including the schema on a destination. One destination can have multiple datasets; for now, datasets are bound to a physical destination, but this may change in future iterations.

By treating datasets as individual entities, dltHub enables data cataloging and data governance.

#### Data cataloging

Datasets automatically create data catalogs that can be used to discover schema and [read and write data](../features/data-access.md).

#### Data governance

Datasets are a fundamental unit of governance in the dltHub Project. Using the declarative interface of dltHub, you can control:
1. Where they are materialized: you can specify which destinations you would like to materialize the datasets in.
2. Who can access them: you can enable and disable them per profile.
3. Ways in which the schema can be modified: you can also set [schema contracts](../../general-usage/schema-contracts.md) per profile.
