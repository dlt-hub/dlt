---
sidebar_label: configuration
title: destinations.impl.clickhouse.configuration
---

## ClickHouseCredentials Objects

```python
@configspec(init=False)
class ClickHouseCredentials(ConnectionStringCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/clickhouse/configuration.py#L14)

### host

Host with running ClickHouse server.

### port

Native port ClickHouse server is bound to. Defaults to 9440.

### http\_port

HTTP Port to connect to ClickHouse server's HTTP interface.
The HTTP port is needed for non-staging pipelines.
 Defaults to 8123.

### username

Database user. Defaults to 'default'.

### database

database connect to. Defaults to 'default'.

### secure

Enables TLS encryption when connecting to ClickHouse Server. 0 means no encryption, 1 means encrypted.

### connect\_timeout

Timeout for establishing connection. Defaults to 10 seconds.

### send\_receive\_timeout

Timeout for sending and receiving data. Defaults to 300 seconds.

### gcp\_access\_key\_id

When loading from a gcp bucket, you need to provide gcp interoperable keys

### gcp\_secret\_access\_key

When loading from a gcp bucket, you need to provide gcp interoperable keys

## ClickHouseClientConfiguration Objects

```python
@configspec
class ClickHouseClientConfiguration(
        DestinationClientDwhWithStagingConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/clickhouse/configuration.py#L72)

### dataset\_table\_separator

Separator for dataset table names, defaults to '___', i.e. 'database.dataset___table'.

### table\_engine\_type

The default table engine to use. Defaults to 'merge_tree'. Other implemented options are 'shared_merge_tree' and 'replicated_merge_tree'.

### dataset\_sentinel\_table\_name

Special table to mark dataset as existing

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/clickhouse/configuration.py#L91)

Returns a fingerprint of the host part of a connection string.

