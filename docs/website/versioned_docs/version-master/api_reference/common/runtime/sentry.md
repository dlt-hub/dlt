---
sidebar_label: sentry
title: common.runtime.sentry
---

## before\_send

```python
def before_send(event: DictStrAny,
                _unused_hint: Optional[StrAny] = None) -> Optional[DictStrAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/runtime/sentry.py#L59)

Called by sentry before sending event. Does nothing, patch this function in the module for custom behavior

