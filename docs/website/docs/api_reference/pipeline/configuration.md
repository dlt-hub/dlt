---
sidebar_label: configuration
title: pipeline.configuration
---

## PipelineConfiguration Objects

```python
@configspec
class PipelineConfiguration(BaseConfiguration)
```

#### restore\_from\_destination

Enables the `run` method of the `Pipeline` object to restore the pipeline state and schemas from the destination

#### enable\_runtime\_trace

Enables the tracing. Tracing saves the execution trace locally and is required by `dlt deploy`.

#### use\_single\_dataset

Stores all schemas in single dataset. When False, each schema will get a separate dataset with `{dataset_name}_{schema_name}

#### full\_refresh

When set to True, each instance of the pipeline with the `pipeline_name` starts from scratch when run and loads the data to a separate dataset.

