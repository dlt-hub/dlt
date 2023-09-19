---
sidebar_label: weaviate_adapter
title: destinations.weaviate.weaviate_adapter
---

#### TTokenizationSetting

Maps column names to tokenization types supported by Weaviate

#### weaviate\_adapter

```python
def weaviate_adapter(data: Any,
                     vectorize: TColumnNames = None,
                     tokenization: TTokenizationSetting = None) -> DltResource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/weaviate/weaviate_adapter.py#L16)

Prepares data for the Weaviate destination by specifying which columns
should be vectorized and which tokenization method to use.

Vectorization is done by Weaviate's vectorizer modules. The vectorizer module
can be configured in dlt configuration file under
`[destination.weaviate.vectorizer]` and `[destination.weaviate.module_config]`.
The default vectorizer module is `text2vec-openai`. See also:
https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules

**Arguments**:

- `data` _Any_ - The data to be transformed. It can be raw data or an instance
  of DltResource. If raw data, the function wraps it into a DltResource
  object.
- `vectorize` _TColumnNames, optional_ - Specifies columns that should be
  vectorized. Can be a single column name as a string or a list of
  column names.
- `tokenization` _TTokenizationSetting, optional_ - A dictionary mapping column
  names to tokenization methods supported by Weaviate. The tokenization
  methods are one of the values in `TOKENIZATION_METHODS`:
  - 'word',
  - 'lowercase',
  - 'whitespace',
  - 'field'.
  

**Returns**:

- `DltResource` - A resource with applied Weaviate-specific hints.
  

**Raises**:

- `ValueError` - If input for `vectorize` or `tokenization` is invalid
  or neither is specified.
  

**Examples**:

  >>> data = [{"name": "Alice", "description": "Software developer"}]
  >>> weaviate_adapter(data, vectorize="description", tokenization={"description": "word"})
  [DltResource with hints applied]

