---
sidebar_label: sentry
title: common.runtime.sentry
---

## before\_send

```python
def before_send(event: DictStrAny,
                _unused_hint: Optional[StrAny] = None) -> Optional[DictStrAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/runtime/sentry.py#L59)

Called by sentry before sending event. Does nothing, patch this function in the module for custom behavior

