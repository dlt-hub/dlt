---
title: Performance
description: Scale-up, parallelize and finetune dlt pipelines
keywords: [scaling, parallelism, finetuning]
---

# Performance

## Yield pages instead of rows

If you can, yield pages when producing data. This makes some processes more effective by lowering the necessary function calls

## Memory /disk management

dlt likes resources that yield data because it can request data into a buffer before processing and releasing it. This makes it possible to manage the amount of resources used. In order to configure this option, you can specify buffer size via env variables or by adding to the config.toml

globally: `DATA_WRITER__BUFFER_MAX_ITEMS=100`

or specifically:

`NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS = 100`

`SOURCES__DATA_WRITER__BUFFER_MAX_ITEMS = 100`

The default buffer is actually set to a moderately low value, so unless you are trying to run dlt on IOT sensors or other tiny infrastructures, you might actually want to increase it to speed up processing.

Keep in mind load packages are buffered to disk and are left for any troubleshooting, so you can clear disk pace with the config.toml option `load.delete_completed_jobs=true` or the equivalent env var.

To troubleshoot memory usage you can add the env var `PROGRESS=log`.

### Parallelism

Parallelism can be limited with the config option `max_parallel_items = 5` that you can place under a source. As dlt is a library can also leverage parallelism outside of dlt such as by placing tasks in parallel in a dag.
