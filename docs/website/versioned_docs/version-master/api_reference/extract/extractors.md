---
sidebar_label: extractors
title: extract.extractors
---

## MaterializedEmptyList Objects

```python
class MaterializedEmptyList(List[Any])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/extractors.py#L42)

A list variant that will materialize tables even if empty list was yielded

## materialize\_schema\_item

```python
def materialize_schema_item() -> MaterializedEmptyList
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/extractors.py#L48)

Yield this to materialize schema in the destination, even if there's no data.

## with\_file\_import

```python
def with_file_import(
        file_path: str,
        file_format: TLoaderFileFormat,
        items_count: int = 0,
        hints: Union[TResourceHints, TDataItem] = None) -> DataItemWithMeta
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/extractors.py#L70)

Marks file under `file_path` to be associated with current resource and imported into the load package as a file of
type `file_format`.

You can provide optional `hints` that will be applied to the current resource. Note that you should avoid schema inference at
runtime if possible and if that is not possible - to do that only once per extract process. Use `make_hints` in `mark` module
to create hints. You can also pass Arrow table or Pandas data frame form which schema will be taken (but content discarded).
Create `TResourceHints` with `make_hints`.

If number of records in `file_path` is known, pass it in `items_count` so `dlt` can generate correct extract metrics.

Note that `dlt` does not sniff schemas from data and will not guess right file format for you.

## Extractor Objects

```python
class Extractor()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/extractors.py#L97)

### write\_items

```python
def write_items(resource: DltResource, items: TDataItems, meta: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/extractors.py#L126)

Write `items` to `resource` optionally computing table schemas and revalidating/filtering data

## ObjectExtractor Objects

```python
class ObjectExtractor(Extractor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/extractors.py#L289)

Extracts Python object data items into typed jsonl

## ArrowExtractor Objects

```python
class ArrowExtractor(Extractor)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/extract/extractors.py#L295)

Extracts arrow data items into parquet. Normalizes arrow items column names.
Compares the arrow schema to actual dlt table schema to reorder the columns and to
insert missing columns (without data). Adds _dlt_load_id column to the table if
`add_dlt_load_id` is set to True in normalizer config.

We do things that normalizer should do here so we do not need to load and save parquet
files again later.

Handles the following types:
- `pyarrow.Table`
- `pyarrow.RecordBatch`
- `pandas.DataFrame` (is converted to arrow `Table` before processing)

