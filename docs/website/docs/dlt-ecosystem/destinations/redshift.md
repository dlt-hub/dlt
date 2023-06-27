---
title: Amazon Redshift
description: Amazon Redshift `dlt` destination
keywords: [redshift, destination, data warehouse]
---

# Amazon Redshift

**1. Initialize a project with a pipeline that loads to Redshift by running**

```
dlt init chess redshift
```

**2. Install the necessary dependencies for Redshift by running**

```
pip install -r requirements.txt
```

This will install dlt with **redshift** extra which contains `psycopg2` client.

**3. Edit the `dlt` credentials file with your info**

```
open .dlt/secrets.toml
```

## dbt support

This destination
[integrates with dbt](../transformations/dbt.md)
via [dbt-redshift](https://github.com/dbt-labs/dbt-redshift).
