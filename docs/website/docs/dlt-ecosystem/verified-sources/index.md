---
title: Sources
description: Available sources
keywords: ['source']
---
import Link from '../../_book-onboarding-call.md';
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

Planning to use `dlt` in production and need a source that isn't listed? We're happy to help you build it: <Link/>.

### Core sources

<DocCardList items={useCurrentSidebarCategory().items.filter(
item => item.label === '30+ SQL Databases' || item.label === 'REST APIs' || item.label === 'Filesystem & cloud storage'
)} />

### Verified sources

Choose from our collection of verified sources, developed and maintained by the `dlt` team and community. Each source is rigorously tested against a real API and provided as Python code for easy customization.

:::tip
If you couldn't find a source implementation, you can easily create your own. Check out the [resource page](../../general-usage/resource) to learn how!
:::

<DocCardList items={useCurrentSidebarCategory().items.filter(
item => item.label !== '30+ SQL Databases' && item.label !== 'REST APIs' && item.label !== 'Filesystem & cloud storage'
)} />

### What's the difference between core and verified sources?

The main difference between the [core sources](#core-sources) and [verified sources](#verified-sources) lies in their structure.
Core sources are generic collections, meaning they can connect to a variety of systems. For example, the [SQL Database source](sql_database) can connect to any
database that supports SQLAlchemy.

According to our telemetry, core sources are the most widely used among our users!

It's also important to note that core sources are integrated into the `dlt` core library,
whereas verified sources are maintained in a separate [repository](https://github.com/dlt-hub/verified-sources).
To use a verified source, you need to run the `dlt` init command, which will download the verified source code to
your working directory.


### Get help

* Source missing? [Request a new verified source.](https://github.com/dlt-hub/verified-sources/issues/new?template=source-request.md)
* Missing endpoint or a feature? [Request or contribute](https://github.com/dlt-hub/verified-sources/issues/new?template=extend-a-source.md)
* [Join our Slack community](https://dlthub.com/community) and ask in the technical-help channel.

