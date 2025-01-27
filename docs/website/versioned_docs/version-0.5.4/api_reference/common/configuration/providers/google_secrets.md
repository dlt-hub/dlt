---
sidebar_label: google_secrets
title: common.configuration.providers.google_secrets
---

## normalize\_key

```python
def normalize_key(in_string: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/google_secrets.py#L17)

Replaces punctuation characters in a string

Note: We exclude `_` and `-` from punctuation characters

**Arguments**:

- `in_string(str)` - input string
  

**Returns**:

- `(str)` - a string without punctuatio characters and whitespaces

## GoogleSecretsProvider Objects

```python
class GoogleSecretsProvider(VaultDocProvider)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/google_secrets.py#L36)

### get\_key\_name

```python
@staticmethod
def get_key_name(key: str, *sections: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/providers/google_secrets.py#L47)

Make key name for the secret

Per Google the secret name can contain, so we will use snake_case normalizer

    1. Uppercase and lowercase letters,
    2. Numerals,
    3. Hyphens,
    4. Underscores.

