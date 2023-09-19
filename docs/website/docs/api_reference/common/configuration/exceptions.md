---
sidebar_label: exceptions
title: common.configuration.exceptions
---

## ContainerException Objects

```python
class ContainerException(DltException)
```

base exception for all exceptions related to injectable container

## ConfigProviderException Objects

```python
class ConfigProviderException(ConfigurationException)
```

base exceptions for all exceptions raised by config providers

## ConfigFieldMissingException Objects

```python
class ConfigFieldMissingException(KeyError, ConfigurationException)
```

raises when not all required config fields are present

## UnmatchedConfigHintResolversException Objects

```python
class UnmatchedConfigHintResolversException(ConfigurationException)
```

Raised when using `@resolve_type` on a field that doesn't exist in the spec

## FinalConfigFieldException Objects

```python
class FinalConfigFieldException(ConfigurationException)
```

rises when field was annotated as final ie Final[str] and the value is modified by config provider

## ConfigValueCannotBeCoercedException Objects

```python
class ConfigValueCannotBeCoercedException(ConfigurationValueError)
```

raises when value returned by config provider cannot be coerced to hinted type

## ConfigFileNotFoundException Objects

```python
class ConfigFileNotFoundException(ConfigurationException)
```

thrown when configuration file cannot be found in config folder

## ConfigFieldMissingTypeHintException Objects

```python
class ConfigFieldMissingTypeHintException(ConfigurationException)
```

thrown when configuration specification does not have type hint

## ConfigFieldTypeHintNotSupported Objects

```python
class ConfigFieldTypeHintNotSupported(ConfigurationException)
```

thrown when configuration specification uses not supported type in hint

