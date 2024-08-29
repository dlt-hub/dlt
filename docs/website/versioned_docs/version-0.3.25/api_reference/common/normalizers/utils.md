---
sidebar_label: utils
title: common.normalizers.utils
---

## explicit\_normalizers

```python
@with_config(spec=NormalizersConfiguration)
def explicit_normalizers(
        naming: str = dlt.config.value,
        json_normalizer: TJSONNormalizer = dlt.config.value
) -> TNormalizersConfig
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/normalizers/utils.py#L19)

Gets explicitly configured normalizers - via config or destination caps. May return None as naming or normalizer

## import\_normalizers

```python
@with_config
def import_normalizers(
    normalizers_config: TNormalizersConfig,
    destination_capabilities: DestinationCapabilitiesContext = None
) -> Tuple[TNormalizersConfig, NamingConvention,
           Type[DataItemNormalizer[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/normalizers/utils.py#L27)

Imports the normalizers specified in `normalizers_config` or taken from defaults. Returns the updated config and imported modules.

`destination_capabilities` are used to get max length of the identifier.

