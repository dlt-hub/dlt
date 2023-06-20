---
title: Performance
description: Scale-up, parallelize and finetune dlt pipelines
keywords: [scaling, parallelism, finetuning]
---

# Performance

## Yield pages instead of rows

If you can, yield pages when producing data. This makes some processes more effective by lowering
the necessary function calls.

## Memory/disk management

`dlt` likes resources that yield data because it can request data into a buffer before processing
and releasing it. This makes it possible to manage the amount of resources used. In order to
configure this option, you can specify buffer size via env variables or by adding to the
`config.toml`.

Globally in your `config.toml`:

```toml
[data_writer]
max_buffer_items=100
```

or specifically for the normalization and source:

```toml
[normalize.data_writer]
max_buffer_items=100

[sources.data_writer]
max_buffer_items=200
```

The default buffer is actually set to a moderately low value, so unless you are trying to run `dlt`
on IOT sensors or other tiny infrastructures, you might actually want to increase it to speed up
processing.

Keep in mind load packages are buffered to disk and are left for any troubleshooting, so you can
clear disk space with the `config.toml` option `load.delete_completed_jobs=true` or the equivalent env
variable.

To troubleshoot memory usage you can add the env variable `PROGRESS=log`.

## Parallelism

Parallelism can be limited with the config option `max_parallel_items = 5` that you can place under
a source. As `dlt` is a library can also leverage parallelism outside of `dlt` such as by placing
tasks in parallel in a dag.

```toml
[extract] # global setting
max_parallel_items=5

[sources.my_pipeline.extract] # setting for the "my_pipeline" pipeline
max_parallel_items=5
```

## Resources loading, `fifo` vs. `round robin`

When extracting from resources, you have two options to determine what the order of queries to your
resources are: `fifo` and `round_robin`.

`fifo` is the default option and will result in every resource being fully extracted before the next
resource is extracted in the order that you added them to your source.

`round_robin` will result in extraction of one item from the first resource, then one item from the
second resource etc, doing as many rounds as necessary until all resources are fully extracted.

You can change this setting in your `config.toml` as follows:

```toml
[extract] # global setting
next_item_mode=round_robin

[sources.my_pipeline.extract] # setting for the "my_pipeline" pipeline
next_item_mode=5
```
