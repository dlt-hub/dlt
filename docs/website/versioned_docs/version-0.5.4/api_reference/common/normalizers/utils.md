---
sidebar_label: utils
title: common.normalizers.utils
---

## explicit\_normalizers

```python
@with_config(spec=NormalizersConfiguration, sections=_section_for_schema)
def explicit_normalizers(
        naming: TNamingConventionReferenceArg = dlt.config.value,
        json_normalizer: TJSONNormalizer = dlt.config.value,
        allow_identifier_change_on_table_with_data: bool = None,
        schema_name: Optional[str] = None) -> TNormalizersConfig
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/utils.py#L45)

Gets explicitly configured normalizers without any defaults or capabilities injection. If `naming`
is a module or a type it will get converted into string form via import.

If `schema_name` is present, a section ("sources", schema_name, "schema") is used to inject the config

## import\_normalizers

```python
@with_config
def import_normalizers(
    explicit_normalizers: TNormalizersConfig,
    default_normalizers: TNormalizersConfig = None,
    destination_capabilities: DestinationCapabilitiesContext = None
) -> Tuple[TNormalizersConfig, NamingConvention,
           Type[DataItemNormalizer[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/utils.py#L66)

Imports the normalizers specified in `normalizers_config` or taken from defaults. Returns the updated config and imported modules.

`destination_capabilities` are used to get naming convention, max length of the identifier and max nesting level.

## naming\_from\_reference

```python
def naming_from_reference(
    names: TNamingConventionReferenceArg,
    destination_capabilities: DestinationCapabilitiesContext = None
) -> NamingConvention
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/utils.py#L111)

Resolves naming convention from reference in `names` and applies max length from `destination_capabilities`

Reference may be: (1) shorthand name pointing to `dlt.common.normalizers.naming` namespace
(2) a type name which is a module containing `NamingConvention` attribute (3) a type of class deriving from NamingConvention

## serialize\_reference

```python
def serialize_reference(
        naming: Optional[TNamingConventionReferenceArg]) -> Optional[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/normalizers/utils.py#L178)

Serializes generic `naming` reference to importable string.

