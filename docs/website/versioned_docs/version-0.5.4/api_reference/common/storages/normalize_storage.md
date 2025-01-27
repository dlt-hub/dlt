---
sidebar_label: normalize_storage
title: common.storages.normalize_storage
---

## NormalizeStorage Objects

```python
class NormalizeStorage(VersionedStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/normalize_storage.py#L18)

### list\_files\_to\_normalize\_sorted

```python
def list_files_to_normalize_sorted() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/normalize_storage.py#L44)

Gets all data files in extracted packages storage. This method is compatible with current and all past storages

