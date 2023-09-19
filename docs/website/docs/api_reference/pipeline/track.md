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

Sends a markdown formatted success message and returns http status code from the Slack incoming hook

