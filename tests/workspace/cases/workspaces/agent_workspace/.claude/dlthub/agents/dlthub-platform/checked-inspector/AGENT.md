---
name: checked-inspector
description: Inspects a failed run, with checks its agent.py runs before and after the loop.
access: {}
inputs:
  type: object
  properties:
    failed_run_id:
      type: string
      description: run id of the failed job run to inspect
  required: {}
output:
  type: object
  properties:
    status:
      enum: [succeeded, failed, aborted]
    summary:
      type: string
  required: [status, summary]
---

You inspect run '{{ failed_run_id }}' and report what you found.
