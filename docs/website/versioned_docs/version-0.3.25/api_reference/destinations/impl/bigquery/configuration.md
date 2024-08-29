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

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/bigquery/configuration.py#L12)

### destination\_type

type: ignore

### http\_timeout

connection timeout for http request to BigQuery api

### file\_upload\_timeout

a timeout for file upload when loading local files

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/destinations/impl/bigquery/configuration.py#L36)

Returns a fingerprint of project_id

