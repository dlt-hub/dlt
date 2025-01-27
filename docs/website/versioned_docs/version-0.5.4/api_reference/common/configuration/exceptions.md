---
sidebar_label: exceptions
title: common.configuration.exceptions
---

## ContainerException Objects

```python
class ContainerException(DltException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L23)

base exception for all exceptions related to injectable container

## ConfigProviderException Objects

```python
class ConfigProviderException(ConfigurationException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L29)

base exceptions for all exceptions raised by config providers

## ConfigFieldMissingException Objects

```python
class ConfigFieldMissingException(KeyError, ConfigurationException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L43)

raises when not all required config fields are present

## UnmatchedConfigHintResolversException Objects

```python
class UnmatchedConfigHintResolversException(ConfigurationException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L93)

Raised when using `@resolve_type` on a field that doesn't exist in the spec

## FinalConfigFieldException Objects

```python
class FinalConfigFieldException(ConfigurationException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L110)

rises when field was annotated as final ie Final[str] and the value is modified by config provider

## ConfigValueCannotBeCoercedException Objects

```python
class ConfigValueCannotBeCoercedException(ConfigurationValueError)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L119)

raises when value returned by config provider cannot be coerced to hinted type

## ConfigFileNotFoundException Objects

```python
class ConfigFileNotFoundException(ConfigurationException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L141)

thrown when configuration file cannot be found in config folder

## ConfigFieldMissingTypeHintException Objects

```python
class ConfigFieldMissingTypeHintException(ConfigurationException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L148)

thrown when configuration specification does not have type hint

## ConfigFieldTypeHintNotSupported Objects

```python
class ConfigFieldTypeHintNotSupported(ConfigurationException)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/configuration/exceptions.py#L159)

thrown when configuration specification uses not supported type in hint

