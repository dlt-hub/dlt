---
sidebar_label: sentry
title: common.runtime.sentry
---

## before\_send

```python
def before_send(event: DictStrAny,
                _unused_hint: Optional[StrAny] = None) -> Optional[DictStrAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/runtime/sentry.py#L59)

Called by sentry before sending event. Does nothing, patch this function in the module for custom behavior

