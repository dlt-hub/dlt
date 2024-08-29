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

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/pipeline/track.py#L32)

Sends a markdown formatted success message and returns http status code from the Slack incoming hook

