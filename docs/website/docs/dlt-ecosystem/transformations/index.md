---
title: Overview
description: dlt supports both ETL and ELT transformation patterns
keywords: [elt, etl, transformer, transformations]
---

`dlt` supports both Extract, Transform, Load (ETL) and Extract, Load, Transform (ELT) patterns.

In ETL, the data is transformed before being loaded into the destination. This is useful for light processing such as adding columns, removing sensitive data, or type casting. `dlt` offers built-in utilities like `add_map()` and custom processors via `@dlt.transformer`

In ELT, the data is loaded as-is in the destination. This raw data is transformed directly on the destination where more powerful compute is available (e.g., data warehouse, data lake). `dlt` supports this via several patterns.

The two approaches can be used together in a single project.
