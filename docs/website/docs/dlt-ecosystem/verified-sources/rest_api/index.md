---
title: REST APIs
description: Loads data from REST APIs using a declarative configuration
keywords: [rest api, restful api]
---

You can use the REST API source to extract data from any REST API. Using a [declarative configuration](./basic.md#source-configuration), you can define:

* the API endpoints to pull data from,
* their [relationships](./basic.md#define-resource-relationships),
* how to handle [pagination](./basic.md#pagination),
* [authentication](./basic.md#authentication).

dlt will take care of the rest: unnesting the data, inferring the schema, etc., and writing to the destination.

import DocCardList from '@theme/DocCardList';

<DocCardList />

