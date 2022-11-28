---
sidebar_position: 8
---

# Destinations

`dlt` supports three destinations at the moment:
- [Google BigQuery](./destinations#google-bigquery)
- [Postgres](./destinations#postgres)
- [Amazon Redshift](./destinations#amazon-redshift)

## Google BigQuery

You can initalize a project with a pipeline that loads to BigQuery by running
```
dlt init <source> bigquery
```

You will then need to install the necessary dependencies for BigQuery by running
```
pip install -r requirements.txt
```

Finally, you will want to edit the `dlt` credentials file with your service account info
```
open ./.dlt/secrets.toml
```

## Postgres

You can initalize a project with a pipeline that loads to Postgres by running
```
dlt init <source> postgres
```

You will then need to install the necessary dependencies for Postgres by running
```
pip install -r requirements.txt
```

Finally, you will want to edit the `dlt` credentials file with your connection info
```
open ./.dlt/secrets.toml
```

## Amazon Redshift

You can initalize a project with a pipeline that loads to Redshift by running
```
dlt init <source> redshift
```

You will then need to install the necessary dependencies for Redshift by running
```
pip install -r requirements.txt
```

Finally, you will want to edit the `dlt` credentials file with your info
```
open ./.dlt/secrets.toml
```