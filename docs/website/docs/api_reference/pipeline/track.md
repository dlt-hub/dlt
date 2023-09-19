---
sidebar_label: track
title: pipeline.track
---

Implements SupportsTracking

#### slack\_notify\_load\_success

```python
def slack_notify_load_success(incoming_hook: str, load_info: LoadInfo,
                              trace: PipelineTrace) -> int
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/pipeline/track.py#L32)

Sends a markdown formatted success message and returns http status code from the Slack incoming hook

