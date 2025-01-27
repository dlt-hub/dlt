---
sidebar_label: configuration
title: destinations.impl.synapse.configuration
---

## SynapseCredentials Objects

```python
@configspec(init=False)
class SynapseCredentials(MsSqlCredentials)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/synapse/configuration.py#L17)

### drivername

type: ignore

## SynapseClientConfiguration Objects

```python
@configspec
class SynapseClientConfiguration(MsSqlClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/synapse/configuration.py#L33)

### destination\_type

type: ignore

### default\_table\_index\_type

Table index type that is used if no table index type is specified on the resource.
This only affects data tables, dlt system tables ignore this setting and
are always created as "heap" tables.

### create\_indexes

Whether `primary_key` and `unique` column hints are applied.

### staging\_use\_msi

Whether the managed identity of the Synapse workspace is used to authorize access to the staging Storage Account.

