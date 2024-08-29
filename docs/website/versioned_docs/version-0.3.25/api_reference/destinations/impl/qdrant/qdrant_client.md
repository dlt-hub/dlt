---
sidebar_label: qdrant_client
title: destinations.impl.qdrant.qdrant_client
---

## QdrantClient Objects

```python
class QdrantClient(JobClientBase, WithStateSync)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/qdrant/qdrant_client.py#L139)

Qdrant Destination Handler

### drop\_storage

```python
def drop_storage() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/qdrant/qdrant_client.py#L236)

Drop the dataset from the Qdrant instance.

Deletes all collections in the dataset and all data associated.
Deletes the sentinel collection.

If dataset name was not provided, it deletes all the tables in the current schema

### get\_stored\_state

```python
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/qdrant/qdrant_client.py#L302)

Loads compressed state from destination storage
By finding a load id that was completed

### get\_stored\_schema

```python
def get_stored_schema() -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/qdrant/qdrant_client.py#L351)

Retrieves newest schema from destination storage

