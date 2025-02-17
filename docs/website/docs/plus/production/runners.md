---
title: Runners
description: Run pipelines in production
keywords: [runners, lambda, airflow]
---

import Link from '../../_plus_admonition.md';

<Link/>

# Runners

With dlt+ you can now run pipelines directly from the command line, allowing you to go to production faster:

```sh
dlt pipeline my_pipeline run
```

These can also be run in different environments with the use of [profiles](../core-concepts/profiles.md):

```sh
dlt project --profile prod my_pipeline run
```

We are working on specialized runners for environments like Airflow, Dagster, Prefect and more. If you're interested, feel free to [join our early access program](https://info.dlthub.com/waiting-list).