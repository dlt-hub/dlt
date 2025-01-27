---
sidebar_label: state_sync
title: pipeline.state_sync
---

## mark\_state\_extracted

```python
def mark_state_extracted(state: TPipelineState, hash_: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/state_sync.py#L38)

Marks state as extracted by setting last extracted hash to hash_ (which is current version_hash)

`_last_extracted_hash` is kept locally and never synced with the destination

## force\_state\_extract

```python
def force_state_extract(state: TPipelineState) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/state_sync.py#L47)

Forces `state` to be extracted by removing local information on the most recent extraction

