---
sidebar_label: versioned_state
title: common.versioned_state
---

## bump\_state\_version\_if\_modified

```python
def bump_state_version_if_modified(
        state: TVersionedState,
        exclude_attrs: List[str] = None) -> Tuple[int, str, str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/common/versioned_state.py#L30)

Bumps the `state` version and version hash if content modified, returns (new version, new hash, old hash) tuple

