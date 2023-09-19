---
sidebar_label: sentry
title: common.runtime.sentry
---

#### before\_send

```python
def before_send(event: DictStrAny,
                _unused_hint: Optional[StrAny] = None) -> Optional[DictStrAny]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/sentry.py#L55)

Called by sentry before sending event. Does nothing, patch this function in the module for custom behavior

