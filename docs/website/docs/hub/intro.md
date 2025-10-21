---
title: Introduction
description: Introduction to dltHub
---

# What is dltHub?

![dltHub](/img/slot-machine-gif.gif)

dltHub is a commercial extension to the open-source data load tool (dlt). It augments it with a set of features like transformations, data validations,
iceberg with full catalog support and provides a yaml interface to define data platforms. dltHub features include:

- [@dlt.hub.transformation](features/transformations/index.md) - powerful Python decorator to build transformation pipelines and notebooks
- [dbt transformations](features/transformations/dbt-transformations.md): a staging layer for data transformations, combining a local cache with schema enforcement, debugging tools, and integration with existing data workflows.
- [Iceberg support](ecosystem/iceberg.md)
- [Secure data access and sharing](features/data-access.md)
- [AI workflows](features/ai.md): agents to augment your data engineering team.

To get started with dltHub, install the library using pip (Python 3.9-3.12):

```sh
pip install dlthub
```

You can try out any features by self-issuing a trial license. You can use such license for evaluation, development and testing.
Trial license are issued off-line using `dlt license` command:

1. Display a list of available features
```sh
dlt license scopes
```

2. Issue license for the feature you want to test.

```sh
dlt license issue dlthub.transformation
```

The command above will enable access to new `@dlt.hub.transformation` decorator. Note that you may
self issue licenses several times and the command above will carry-over features from previously issued license.

3. Do not forget to read our [EULA](EULA.md) and [Special Terms](EULA.md#specific-terms-for-the-self-issued-trial-license-self-issued-trial-terms)
for self issued licenses.
