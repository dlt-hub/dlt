---
title: Verified sources
description: List of verified sources
keywords: ['verified source']
---
import DocCardList from '@theme/DocCardList';
import Link from '../../_book-onboarding-call.md';

Choose from our collection of verified sources, developed and maintained by the dlt team and community. Each source is rigorously tested against a real API and provided as Python code for easy customization.

Planning to use dlt in production and need a source that isn't listed? We're happy to help you build it: <Link />.

### Popular sources

- [SQL databases](sql_database). Supports PostgreSQL, MySQL, MS SQL Server, BigQuery, Redshift, and more.
- [REST API generic source](rest_api). Loads data from REST APIs using declarative configuration.
- [OpenAPI source generator](openapi-generator). Generates a source from an OpenAPI 3.x spec using the REST API source.
- [Cloud and local storage](filesystem). Retrieves data from AWS S3, Google Cloud Storage, Azure Blob Storage, local files, and more.

### Full list of verified sources

<DocCardList />

:::tip
If you're looking for a source that isn't listed and it provides a REST API, be sure to check out our [REST API generic source](rest_api)
 source.
:::


### Get help

* Source missing? [Request a new verified source.](https://github.com/dlt-hub/verified-sources/issues/new?template=source-request.md)
* Missing endpoint or a feature? [Request or contribute](https://github.com/dlt-hub/verified-sources/issues/new?template=extend-a-source.md)
* [Join our Slack community](https://dlthub.com/community) and ask in the technical-help channel.
