---
title: 30+ SQL Databases
description: PostgreSQL, MySQL, MS SQL, BigQuery, Redshift, and more
keywords: [sql connector, sql database pipeline, sql database]
---
import Header from '../_source-info-header.md';

# 30+ SQL databases

<Header/>

SQL databases are management systems (DBMS) that store data in a structured format, commonly used for efficient and reliable data retrieval.

The SQL Database verified source loads data to your specified destination using one of the following backends: SQLAlchemy, PyArrow, pandas, or ConnectorX.

Sources and resources that can be loaded using this verified source are:

| Name         | Description                                                          |
| ------------ | -------------------------------------------------------------------- |
| sql_database | Reflects the tables and views in an SQL database and retrieves the data |
| sql_table    | Retrieves data from a particular SQL database table                  |
|              |                                                                      |

:::tip
If you prefer to skip the tutorial and see the code example right away, check out the pipeline example [here](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py).
:::

### Supported databases

We support all [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/20/dialects/), which include, but are not limited to, the following database engines:

* [PostgreSQL](./troubleshooting#postgres--mssql)
* [MySQL](./troubleshooting#mysql)
* SQLite
* [Oracle](./troubleshooting#oracle)
* [Microsoft SQL Server](./troubleshooting#postgres--mssql)
* MariaDB
* [IBM DB2 and Informix](./troubleshooting#db2)
* Google BigQuery
* Snowflake
* Redshift
* Apache Hive and Presto
* SAP Hana
* CockroachDB
* Firebird
* Teradata Vantage

:::note
Note that there are many unofficial dialects, such as [DuckDB](https://duckdb.org/).
:::

