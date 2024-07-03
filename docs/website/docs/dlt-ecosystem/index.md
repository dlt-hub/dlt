---
title: Integrations
description: List of integrations
keywords: ['integrations, sources, destinations']
---
import DocCardList from '@theme/DocCardList';
import Link from '../_book-onboarding-call.md';

Speed up the process of creating data pipelines by using dlt's multiple pre-built sources and destinations:

- Each [dlt verified source](verified-sources) allows you to create [pipelines](../general-usage/pipeline) that extract data from a particular source: a database, a cloud service, or an API.
- [Destinations](destinations) are where you want to load your data. dlt supports a variety of destinations, including databases, data warehouses, and data lakes.

<DocCardList />

:::tip
Most source-destination pairs work seamlessly together. If the merge [write disposition](../general-usage/incremental-loading#choosing-a-write-disposition) is not supported by a destination (for example, [file sytem destination](destinations/filesystem)), dlt will automatically fall back to the [append](../general-usage/incremental-loading#append) write disposition.
:::