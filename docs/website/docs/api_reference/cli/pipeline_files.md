---
sidebar_label: pipeline_files
title: cli.pipeline_files
---

#### find\_conflict\_files

```python
def find_conflict_files(
        local_index: TVerifiedSourceFileIndex,
        remote_new: Dict[str, TVerifiedSourceFileEntry],
        remote_modified: Dict[str, TVerifiedSourceFileEntry],
        remote_deleted: Dict[str, TVerifiedSourceFileEntry],
        dest_storage: FileStorage) -> Tuple[List[str], List[str]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/cli/pipeline_files.py#L221)

Use files index from .sources to identify modified files via sha3 content hash

