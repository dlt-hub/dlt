---
sidebar_label: normalize_storage
title: common.storages.normalize_storage
---

## NormalizeStorage Objects

```python
class NormalizeStorage(VersionedStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/normalize_storage.py#L18)

### list\_files\_to\_normalize\_sorted

```python
def list_files_to_normalize_sorted() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/normalize_storage.py#L44)

Gets all data files in extracted packages storage. This method is compatible with current and all past storages

