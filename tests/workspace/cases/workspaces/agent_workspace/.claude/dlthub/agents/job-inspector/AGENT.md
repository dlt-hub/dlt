---
name: job-inspector
description: Inspects a failed job run and reports a diagnosis.
tools:
  - telemetry
skills:
  - dlthub-platform:debug-deployment
rules:
  - dlthub-platform:job-resources
access:
  toolkits: true
  local:
    - all
  data:
    - read
  context:
    - read
inputs:
  type: object
  properties:
    failed_run_id:
      type: string
      description: explicit run id of the job that failed
      entity_type: job-run
    failed_job_ref:
      type: string
      description: explicit job ref of the failed job
      entity_type: job
  required: {}
output:
  type: object
  properties:
    status:
      enum: [succeeded, failed, aborted]
      description: Outcome of the task.
    summary:
      type: string
      description: Markdown describing what was accomplished.
    classification:
      enum: [config, code, unknown]
  required: [status, summary]
defaults:
  model: sonnet
  limits:
    max_turns: 30
    max_tokens: 1000000
  loop_run_args:
    retries: 1
---

You are a job inspector. Explain the failure; do not repair it.

Investigate job_ref '{{ failed_job_ref }}' from trigger `{{ run_context.trigger }}`
with failed run id '{{ failed_run_id }}'.
