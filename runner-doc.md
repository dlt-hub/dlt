# Pipeline Run Configurations

## run_from_clean_folder

The `run_from_clean_folder` parameter controls whether the pipeline should start from a clean state before running. If set to `true`, the pipeline's local state is wiped and synchronized with the destination to get the latest schema and state. This is useful for ensuring a fresh start, but means any pending or local-only data will be lost.

**YAML Example:**

```yaml
run_config:
  run_from_clean_folder: true
```

- If `false` (default), any pending loads or traces from previous runs are finalized before running.
- If `true`, the pipeline's local folders are wiped before running.

## store_trace_info

The `store_trace_info` parameter controls whether trace information about pipeline runs is is automatically saved to the destination as well. This is useful for debugging, auditing, or monitoring pipeline executions.

**YAML Example:**

```yaml
run_config:
  store_trace_info: true
```

- If `true`, trace information is saved after each run and can be loaded into a trace pipeline for analysis.
- If `false` (default), trace information is not stored.

Trace files are written to the pipeline's storage under the `traces/` directory after the run and will be deleted from there once they've successfully been loaded via the trace pipeline.

**Dedicated Trace Pipeline:**

By default this happens via a pipeline that is automatically created with the name `_trace_<pipeline_name>` and that writes to the same destination as the main pipeline.
Optionally, you can also provide a custom trace pipeline to use for storing the trace information, e.g. to write to a remote append-only filesystem.

```yaml
pipelines:
  trace_pipeline:
    source: my_source
    destination: filesystem
    ...
    
  my_pipeline:
    source: my_source
    destination: duckdb
    run_config:
      store_trace_info: trace_pipeline
    ...
```

**Trace Table Naming:**

By default, traces are loaded into a table named `<pipeline_name>_trace`, where `<pipeline_name>` is the name of your pipeline. For example, if your pipeline is named `my_pipeline`, the trace table will be `my_pipeline_trace`. 

## Retry Policy

You can configure the retry policy for the runner to control how it handles errors during pipeline execution.
It is applied to when finalizing any pending data from previous loads,
when running the pipeline with the given data an when loading the trace info.

At the moment we support the following policies:

| Policy Type | YAML Example Keys/Values | Resulting Policy |
|-------------|-------------------------|-----------------|
| None        | `type: none`            | No retry (single attempt) |
| Fixed       | `type: fixed`,<br>`max_attempts: 3` | 3 attempts, no backoff |
| Backoff     | `type: backoff`,<br>`max_attempts: 5`,<br>`multiplier: 2`,<br>`min: 1`,<br>`max: 10` | Exponential backoff (5 attempts, multiplier 2, min 1, max 10) |

**Full YAML Examples:**

```yaml
# No retry
run_config:
  retry_policy:
    type: none
```

```yaml
# Fixed attempts
run_config:
  retry_policy:
    type: fixed
    max_attempts: 3
```

```yaml
# Exponential backoff
run_config:
  retry_policy:
    type: backoff
    max_attempts: 5
    multiplier: 2
    min: 1
    max: 10
```

---

### Configuring `retry_pipeline_steps`

The `retry_pipeline_steps` parameter controls which pipeline steps will be retried according to the retry policy. It should be a list of step names. Possible values are `extract`, `normalize`, and `load`.

| Example YAML | Meaning |
|--------------|---------|
| `retry_pipeline_steps: ["load"]` | Only retry the load step (default) |
| `retry_pipeline_steps: ["normalize", "load"]` | Retry both normalize and load steps |
| `retry_pipeline_steps: ["extract", "normalize", "load"]` | Retry all main steps |

**Full YAML Example:**

```yaml
run_config:
  retry_policy:
    type: backoff
    max_attempts: 5
    multiplier: 2
    min: 1
    max: 10
  retry_pipeline_steps: ["extract", "normalize", "load"]
```
