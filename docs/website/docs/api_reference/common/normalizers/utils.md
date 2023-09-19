---
sidebar_label: utils
title: common.normalizers.utils
---

#### explicit\_normalizers

```python
@with_config(spec=NormalizersConfiguration)
def explicit_normalizers(
        naming: str = dlt.config.value,
        json_normalizer: TJSONNormalizer = dlt.config.value
) -> TNormalizersConfig
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/utils.py#L17)

Gets explicitly configured normalizers - via config or destination caps. May return None as naming or normalizer

#### import\_normalizers

```python
@with_config
def import_normalizers(
    normalizers_config: TNormalizersConfig,
    destination_capabilities: DestinationCapabilitiesContext = None
) -> Tuple[TNormalizersConfig, NamingConvention,
           Type[DataItemNormalizer[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/normalizers/utils.py#L26)

Imports the normalizers specified in `normalizers_config` or taken from defaults. Returns the updated config and imported modules.

`destination_capabilities` are used to get max length of the identifier.

