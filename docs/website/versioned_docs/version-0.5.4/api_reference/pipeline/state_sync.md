---
sidebar_label: state_sync
title: pipeline.state_sync
---

## mark\_state\_extracted

```python
def mark_state_extracted(state: TPipelineState, hash_: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/state_sync.py#L37)

Marks state as extracted by setting last extracted hash to hash_ (which is current version_hash)

`_last_extracted_hash` is kept locally and never synced with the destination

## force\_state\_extract

```python
def force_state_extract(state: TPipelineState) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/state_sync.py#L46)

Forces `state` to be extracted by removing local information on the most recent extraction

