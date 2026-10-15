---
name: find-crash
description: Test agent used by the toolkit install tests.
access:
  local:
    - read
  data:
    - read
inputs:
  type: object
  properties: {}
  required: {}
output:
  type: object
  properties:
    status:
      enum: [succeeded, failed, aborted]
      description: Outcome.
    summary:
      type: string
      description: What happened.
  required: [status, summary]
---

You are a test agent.
