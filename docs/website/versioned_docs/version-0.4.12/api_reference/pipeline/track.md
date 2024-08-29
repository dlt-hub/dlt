---
sidebar_label: track
title: pipeline.track
---

Implements SupportsTracking

## slack\_notify\_load\_success

```python
def slack_notify_load_success(incoming_hook: str, load_info: LoadInfo,
                              trace: PipelineTrace) -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/pipeline/track.py#L33)

Sends a markdown formatted success message and returns http status code from the Slack incoming hook

