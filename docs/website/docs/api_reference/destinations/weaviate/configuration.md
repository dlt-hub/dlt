---
sidebar_label: configuration
title: destinations.weaviate.configuration
---

## WeaviateCredentials Objects

```python
@configspec
class WeaviateCredentials(CredentialsConfiguration)
```

#### \_\_str\_\_

```python
def __str__() -> str
```

Used to display user friendly data location

## WeaviateClientConfiguration Objects

```python
@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration)
```

#### destination\_name

type: ignore

#### dataset\_name

type: ignore

#### fingerprint

```python
def fingerprint() -> str
```

Returns a fingerprint of host part of a connection string

