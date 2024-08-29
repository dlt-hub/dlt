---
sidebar_label: extractors
title: extract.extractors
---

## Extractor Objects

```python
class Extractor()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extractors.py#L39)

### item\_format

```python
@staticmethod
def item_format(items: TDataItems) -> Optional[TLoaderFileFormat]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extractors.py#L73)

Detect the loader file format of the data items based on type.
Currently this is either 'arrow' or 'puae-jsonl'

**Returns**:

  The loader file format or `None` if if can't be detected.

### write\_items

```python
def write_items(resource: DltResource, items: TDataItems, meta: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/extract/extractors.py#L89)

Write `items` to `resource` optionally computing table schemas and revalidating/filtering data

