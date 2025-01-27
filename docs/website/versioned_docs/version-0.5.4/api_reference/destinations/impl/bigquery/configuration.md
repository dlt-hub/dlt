---
sidebar_label: configuration
title: destinations.impl.bigquery.configuration
---

## BigQueryClientConfiguration Objects

```python
@configspec
class BigQueryClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                  )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/bigquery/configuration.py#L13)

### destination\_type

type: ignore

### project\_id

Note, that this is BigQuery project_id which could be different from credentials.project_id

### has\_case\_sensitive\_identifiers

If True then dlt expects to load data into case sensitive dataset

### should\_set\_case\_sensitivity\_on\_new\_dataset

If True, dlt will set case sensitivity flag on created datasets that corresponds to naming convention

### http\_timeout

connection timeout for http request to BigQuery api

### file\_upload\_timeout

a timeout for file upload when loading local files

### retry\_deadline

How long to retry the operation in case of error, the backoff 60 s.

### batch\_size

Number of rows in streaming insert batch

### autodetect\_schema

Allow BigQuery to autodetect schemas and create data tables

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/destinations/impl/bigquery/configuration.py#L40)

Returns a fingerprint of project_id

