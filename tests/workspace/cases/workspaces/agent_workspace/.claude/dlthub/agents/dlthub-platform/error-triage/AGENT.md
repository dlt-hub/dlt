---
name: error-triage
description: Puts one error message of a failed run into a category.
access: {}
inputs:
  type: object
  properties:
    error_message:
      type: string
      description: error message of the failed run
  required: [error_message]
output:
  type: object
  properties:
    status:
      enum: [succeeded, failed, aborted]
    summary:
      type: string
    category:
      enum: [config, data, infra]
  required: [status, summary, category]
---

Put this error into a category: {{ error_message }}
