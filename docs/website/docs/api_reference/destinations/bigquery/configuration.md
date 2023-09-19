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

Returns a fingerprint of project_id

