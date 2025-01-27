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

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/track.py#L33)

Sends a markdown formatted success message and returns http status code from the Slack incoming hook

