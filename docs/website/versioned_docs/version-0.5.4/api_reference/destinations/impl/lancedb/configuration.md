---
sidebar_label: configuration
title: destinations.impl.lancedb.configuration
---

## LanceDBCredentials Objects

```python
@configspec
class LanceDBCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/configuration.py#L15)

### uri

LanceDB database URI. Defaults to local, on-disk instance.

The available schemas are:

- `/path/to/database` - local database.
- `db://host:port` - remote database (LanceDB cloud).

### api\_key

API key for the remote connections (LanceDB cloud).

### embedding\_model\_provider\_api\_key

API key for the embedding model provider.

## LanceDBClientOptions Objects

```python
@configspec
class LanceDBClientOptions(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/configuration.py#L37)

### max\_retries

`EmbeddingFunction` class wraps the calls for source and query embedding
generation inside a rate limit handler that retries the requests with exponential
backoff after successive failures.

You can tune it by setting it to a different number, or disable it by setting it to 0.

## LanceDBClientConfiguration Objects

```python
@configspec
class LanceDBClientConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/configuration.py#L66)

### dataset\_separator

Character for the dataset separator.

### options

LanceDB client options.

### embedding\_model\_provider

Embedding provider used for generating embeddings. Default is "cohere". You can find the full list of
providers at https://github.com/lancedb/lancedb/tree/main/python/python/lancedb/embeddings as well as
https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/.

### embedding\_model

The model used by the embedding provider for generating embeddings.
Check with the embedding provider which options are available.
Reference https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/.

### embedding\_model\_dimensions

The dimensions of the embeddings generated. In most cases it will be automatically inferred, by LanceDB,
but it is configurable in rare cases.

Make sure it corresponds with the associated embedding model's dimensionality.

### vector\_field\_name

Name of the special field to store the vector embeddings.

### id\_field\_name

Name of the special field to manage deduplication.

### sentinel\_table\_name

Name of the sentinel table that encapsulates datasets. Since LanceDB has no
concept of schemas, this table serves as a proxy to group related dlt tables together.

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/lancedb/configuration.py#L106)

Returns a fingerprint of a connection string.

