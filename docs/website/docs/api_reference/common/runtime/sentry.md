---
sidebar_label: sentry
title: common.runtime.sentry
---

#### before\_send

```python
def before_send(event: DictStrAny,
                _unused_hint: Optional[StrAny] = None) -> Optional[DictStrAny]
```

Called by sentry before sending event. Does nothing, patch this function in the module for custom behavior

