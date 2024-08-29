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

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/versioned_state.py#L28)

Bumps the `state` version and version hash if content modified, returns (new version, new hash, old hash) tuple

