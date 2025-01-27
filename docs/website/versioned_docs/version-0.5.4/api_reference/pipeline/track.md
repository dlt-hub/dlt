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

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/track.py#L33)

Sends a markdown formatted success message and returns http status code from the Slack incoming hook

