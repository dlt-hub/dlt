---
sidebar_label: models
title: destinations.impl.lancedb.models
---

## PatchedOpenAIEmbeddings Objects

```python
@register("openai_patched")
class PatchedOpenAIEmbeddings(OpenAIEmbeddings)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/models.py#L10)

### sanitize\_input

```python
def sanitize_input(texts: TEXT) -> Union[List[str], np.ndarray]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/models.py#L13)

Replace empty strings with a placeholder value.

### generate\_embeddings

```python
def generate_embeddings(texts: Union[List[str], np.ndarray]) -> List[np.array]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/models.py#L21)

Generate embeddings, treating the placeholder as an empty result.

