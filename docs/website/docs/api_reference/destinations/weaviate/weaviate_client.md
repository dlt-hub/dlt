---
sidebar_label: weaviate_client
title: destinations.weaviate.weaviate_client
---

## LoadWeaviateJob Objects

```python
class LoadWeaviateJob(LoadJob)
```

#### load\_batch

```python
@wrap_weaviate_error
def load_batch(f: IO[str]) -> None
```

Load all the lines from stream `f` in automatic Weaviate batches.
Weaviate batch supports retries so we do not need to do that.

## WeaviateClient Objects

```python
class WeaviateClient(JobClientBase, WithStateSync)
```

Weaviate client implementation.

#### make\_qualified\_class\_name

```python
def make_qualified_class_name(table_name: str) -> str
```

Make a full Weaviate class name from a table name by prepending
the dataset name if it exists.

#### get\_class\_schema

```python
def get_class_schema(table_name: str) -> Dict[str, Any]
```

Get the Weaviate class schema for a table.

#### create\_class

```python
def create_class(class_schema: Dict[str, Any],
                 full_class_name: Optional[str] = None) -> None
```

Create a Weaviate class.

**Arguments**:

- `class_schema` - The class schema to create.
- `full_class_name` - The full name of the class to create. If not
  provided, the class name will be prepended with the dataset name
  if it exists.

#### create\_class\_property

```python
def create_class_property(class_name: str, prop_schema: Dict[str,
                                                             Any]) -> None
```

Create a Weaviate class property.

**Arguments**:

- `class_name` - The name of the class to create the property on.
- `prop_schema` - The property schema to create.

#### delete\_class

```python
def delete_class(class_name: str) -> None
```

Delete a Weaviate class.

**Arguments**:

- `class_name` - The name of the class to delete.

#### delete\_all\_classes

```python
def delete_all_classes() -> None
```

Delete all Weaviate classes from Weaviate instance and all data
associated with it.

#### query\_class

```python
def query_class(class_name: str, properties: List[str]) -> GetBuilder
```

Query a Weaviate class.

**Arguments**:

- `class_name` - The name of the class to query.
- `properties` - The properties to return.
  

**Returns**:

  A Weaviate query builder.

#### create\_object

```python
def create_object(obj: Dict[str, Any], class_name: str) -> None
```

Create a Weaviate object.

**Arguments**:

- `obj` - The object to create.
- `class_name` - The name of the class to create the object on.

#### drop\_storage

```python
def drop_storage() -> None
```

Drop the dataset from Weaviate instance.

Deletes all classes in the dataset and all data associated with them.
Deletes the sentinel class as well.

If dataset name was not provided, it deletes all the tables in the current schema

#### get\_stored\_state

```python
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

Loads compressed state from destination storage

#### get\_stored\_schema

```python
def get_stored_schema() -> Optional[StorageSchemaInfo]
```

Retrieves newest schema from destination storage

#### make\_weaviate\_class\_schema

```python
def make_weaviate_class_schema(table_name: str) -> Dict[str, Any]
```

Creates a Weaviate class schema from a table schema.

