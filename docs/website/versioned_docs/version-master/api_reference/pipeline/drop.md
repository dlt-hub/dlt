---
sidebar_label: drop
title: pipeline.drop
---

## drop\_resources

```python
def drop_resources(
    schema: Schema,
    state: TPipelineState,
    resources: Union[Iterable[Union[str, TSimpleRegex]],
                     Union[str, TSimpleRegex]] = (),
    state_paths: jsonpath.TAnyJsonPath = (),
    drop_all: bool = False,
    state_only: bool = False,
    sources: Optional[Union[Iterable[Union[str, TSimpleRegex]],
                            Union[str, TSimpleRegex]]] = None
) -> _DropResult
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/pipeline/drop.py#L77)

Generate a new schema and pipeline state with the requested resources removed.

**Arguments**:

- `schema` - The schema to modify. Note that schema is changed in place.
- `state` - The pipeline state to modify. Note that state is changed in place.
- `resources` - Resource name(s) or regex pattern(s) matching resource names to drop.
  If empty, no resources will be dropped unless `drop_all` is True.
- `state_paths` - JSON path(s) relative to the source state to drop.
- `drop_all` - If True, all resources will be dropped (supersedes `resources`).
- `state_only` - If True, only modify the pipeline state, not schema
- `sources` - Only wipe state for sources matching the name(s) or regex pattern(s) in this list
  If not set all source states will be modified according to `state_paths` and `resources`
  

**Returns**:

  A 3 part tuple containing the new schema, the new pipeline state, and a dictionary
  containing information about the drop operation.

