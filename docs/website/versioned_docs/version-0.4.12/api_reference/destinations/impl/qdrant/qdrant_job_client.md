---
sidebar_label: qdrant_job_client
title: destinations.impl.qdrant.qdrant_job_client
---

## QdrantClient Objects

```python
class QdrantClient(JobClientBase, WithStateSync)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/qdrant_job_client.py#L154)

Qdrant Destination Handler

### drop\_storage

```python
def drop_storage() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/qdrant_job_client.py#L242)

Drop the dataset from the Qdrant instance.

Deletes all collections in the dataset and all data associated.
Deletes the sentinel collection.

If dataset name was not provided, it deletes all the tables in the current schema

### get\_stored\_state

```python
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/qdrant_job_client.py#L311)

Loads compressed state from destination storage
By finding a load id that was completed

### get\_stored\_schema

```python
def get_stored_schema() -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/e9c9ecfa8a644fdb516dd74aabca3bf75bafb154/dlt/destinations/impl/qdrant/qdrant_job_client.py#L377)

Retrieves newest schema from destination storage

