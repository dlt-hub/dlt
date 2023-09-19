---
sidebar_label: configuration
title: destinations.bigquery.configuration
---

## BigQueryClientConfiguration Objects

```python
@configspec
class BigQueryClientConfiguration(DestinationClientDwhWithStagingConfiguration
                                  )
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/bigquery/configuration.py#L12)

#### http\_timeout

connection timeout for http request to BigQuery api

#### file\_upload\_timeout

a timeout for file upload when loading local files

#### retry\_deadline

how long to retry the operation in case of error, the backoff 60s

#### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/destinations/bigquery/configuration.py#L31)

Returns a fingerprint of project_id

