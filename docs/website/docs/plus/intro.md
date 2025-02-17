---
title: Introduction
description: Introduction to dlt+
---

import Link from '../_plus_admonition.md';

<Link/>

# What is dlt+?

![dlt+](/img/slot-machine-gif.gif)

dlt+ is a framework for running dlt pipelines in production at scale. It is the commercial extension to the open source data load tool (dlt). Features include:

* [Projects](../plus/features/projects.md): a declarative yaml interface that allows any team member to easily define sources, destinations and pipelines
* [Local transformations](../plus/features/transformations/index.md): a staging layer for data transformations, combining a local cache with schema enforcement, debugging tools, and integration with existing data workflows.
* [Data quality & tests](../plus/features/quality/tests.md)
* [Iceberg support](../plus/ecosystem/iceberg.md)
* [Secure data access and sharing](../plus/features/data-access.md)
* [AI workflows](../plus/features/ai.md): agents to augment your data engineering team

To get started with dlt+, install the library using pip (Python 3.9-3.12):

```sh
pip install dlt-plus
```

:::caution
dlt+ requires a license to run, if you would like a trial, please join our [waiting list](https://info.dlthub.com/waiting-list).
:::