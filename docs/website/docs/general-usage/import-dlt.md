---
title: Import dlt
description: How to import dlt
keywords: [import, importing dlt]
---

# Import `dlt`

To use `dlt` functions, please import the `dlt` library:

```python
import dlt
```

In the `dlt` module you can:

1. Define and run [pipelines](pipeline.md) with `dlt.pipeline`.
1. Declare [sources](source.md) and [resources](resource.md) using `@dlt.source` and `@dlt.resource`
   decorators.
1. Define and access [configuration](configuration.md) values and [credentials](credentials.md)
   with `dlt.config` and `dlt.secrets`.
1. Access the current pipeline [state](state.md) using `dlt.current.state` in your sources and
   resources as well as the current schema and pipeline.
