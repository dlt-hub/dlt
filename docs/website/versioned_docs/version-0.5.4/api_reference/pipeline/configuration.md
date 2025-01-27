---
sidebar_label: configuration
title: pipeline.configuration
---

## PipelineConfiguration Objects

```python
@configspec
class PipelineConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/configuration.py#L14)

### dataset\_name\_layout

Layout for dataset_name, where %s is replaced with dataset_name. For example: 'prefix_%s'

### restore\_from\_destination

Enables the `run` method of the `Pipeline` object to restore the pipeline state and schemas from the destination

### enable\_runtime\_trace

Enables the tracing. Tracing saves the execution trace locally and is required by `dlt deploy`.

### use\_single\_dataset

Stores all schemas in single dataset. When False, each schema will get a separate dataset with `{dataset_name}_{schema_name}

### full\_refresh

Deprecated. Use `dev_mode` instead. When set to True, each instance of the pipeline with the `pipeline_name` starts from scratch when run and loads the data to a separate dataset.

### dev\_mode

When set to True, each instance of the pipeline with the `pipeline_name` starts from scratch when run and loads the data to a separate dataset.

### refresh

Refresh mode for the pipeline to fully or partially reset a source during run. See docstring of `dlt.pipeline` for more details.

