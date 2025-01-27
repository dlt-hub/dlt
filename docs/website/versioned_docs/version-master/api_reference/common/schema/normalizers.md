---
sidebar_label: normalizers
title: common.schema.normalizers
---

## configured\_normalizers

```python
@with_config(spec=SchemaConfiguration, sections=_section_for_schema)
def configured_normalizers(
        naming: TNamingConventionReferenceArg = dlt.config.value,
        json_normalizer: TJSONNormalizer = dlt.config.value,
        allow_identifier_change_on_table_with_data: bool = None,
        use_break_path_on_normalize: Optional[bool] = None,
        schema_name: Optional[str] = None) -> TNormalizersConfig
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/normalizers.py#L43)

Gets explicitly onfigured normalizers without any defaults or capabilities injection. If `naming`
is a module or a type it will get converted into string form via import.

If `schema_name` is present, a section ("sources", schema_name, "schema") is used to inject the config

## import\_normalizers

```python
@with_config
def import_normalizers(
    explicit_normalizers: TNormalizersConfig,
    default_normalizers: TNormalizersConfig = None
) -> Tuple[TNormalizersConfig, NamingConvention,
           Type[DataItemNormalizer[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/normalizers.py#L67)

Imports the normalizers specified in `normalizers_config` or taken from defaults. Returns the updated config and imported modules.

`destination_capabilities` are used to get naming convention, max length of the identifier and max nesting level.

## naming\_from\_reference

```python
def naming_from_reference(
        names: TNamingConventionReferenceArg,
        max_length: Optional[int] = None) -> NamingConvention
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/normalizers.py#L124)

Resolves naming convention from reference in `names` and applies max length if specified

Reference may be: (1) shorthand name pointing to `dlt.common.normalizers.naming` namespace
(2) a type name which is a module containing `NamingConvention` attribute (3) a type of class deriving from NamingConvention

## serialize\_reference

```python
def serialize_reference(
        naming: Optional[TNamingConventionReferenceArg]) -> Optional[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/schema/normalizers.py#L182)

Serializes generic `naming` reference to importable string.

