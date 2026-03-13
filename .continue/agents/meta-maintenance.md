---
name: Meta Maintenance
description: Ensures that files, rules and skills are updated if the PR has changes affecting them.
---

# Meta Maintenance

## Context

This project contains several files that must follow changes in the code. We cannot apply those changes
algorithmically with the Python script. When the project evolves, these files must be updated too.

## What to Check

### 1. Optional dependency change
Task disabled for now

### 2. Update continue agents from claude skills
task disabled for now

### 3. Annotate pipeline trace schema contract
 **Update `tests/pipeline/cases/contracts/trace.schema.yaml` to match the current trace structure.**                                                                                                                                                                                                                                  
  The schema represents the **normalized (flattened) output** of pipeline trace data. To understand the structure, trace the serialization from top to bottom:                                                                               
  1. **Entry point**: `dlt/pipeline/trace.py` — the `PipelineTrace` and `PipelineStepTrace` classes and their `asdict()` methods. This is where the top-level dict shape is defined.

  2. **Step info classes**: `dlt/common/pipeline.py` — `ExtractInfo`, `NormalizeInfo`, `LoadInfo` each override `asdict()` to flatten their metrics differently. The step type determines which `*_info` sub-tables appear (e.g.
  `extract_info__job_metrics`).

  3. **Metrics**: `dlt/common/metrics.py` — the underlying metric NamedTuples (`DataWriterMetrics`, `LoadJobMetrics`, etc.) define the leaf-level columns in metrics tables.

  4. **Load packages**: `dlt/common/storages/load_package.py` — `LoadPackageInfo.asdict()` flattens jobs and schema_update into sub-tables with their own columns.

  5. **Exceptions**: `dlt/common/exceptions.py` — `ExceptionTrace` TypedDict defines the exception trace columns.

  6. **Execution context**: `dlt/common/runtime/typing.py` and `dlt/common/runtime/exec_info.py` — define the runtime environment fields.

  When in doubt, the most reliable way to see the exact output shape is to inspect the `asdict()` methods — they often rename keys, pop fields, or inject extra columns (like `load_id`, `extract_idx`) that don't exist on the original
  class.

  **Annotation guidelines**

  Every **table** must have a `description:` field. Columns that are self-descriptive (e.g. `file_path`, `file_size`, `started_at`, `pipeline_name`, `table_name`, `schema_name`) should NOT be annotated — noise drowns out signal.

  Annotate a column when any of these apply:
  - **Non-obvious meaning**: `transaction_id` (what transaction?), `span_id`, `extract_idx`, `engine_version`
  - **Non-obvious type/encoding**: `created` (unix timestamp, not datetime), `exception_attrs` (JSON string), `columns` (JSON string)
  - **Domain jargon**: `is_terminal`, `variant`, `first_run`, `x_normalizer__seen_data`
  - **Ambiguous in context**: `state` means different things in packages vs jobs; `parent` in a tables table vs `parent_name` in a DAG

  Keep descriptions to **one line**. Describe what the value represents, not how it's computed. Good:
  ```yaml
  description: Whether the exception is non-retryable (terminal)
  ```
  Bad:
  ```yaml
  description: Set to True when exception has TerminalException mixin via get_exception_trace_chain()
  ```