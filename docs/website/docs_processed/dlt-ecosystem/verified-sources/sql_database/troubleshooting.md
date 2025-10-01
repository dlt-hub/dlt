---
title: Troubleshooting
description: common troubleshooting use-cases for the sql_database source
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Troubleshooting

<Header/>

## Timezone-aware and Non-aware Data Types

### I see a UTC datetime column in my destination, but my data source has a naive datetime column
Use `full` or `full_with_precision` reflection level to get an explicit `timezone` hint in reflected table schemas. Without that
hint, `dlt` will coerce all timestamps into timezone-aware UTC ones.

### I have an incremental cursor on a datetime column and I see query errors
Queries used to query data in the `sql_database` are created from an `Incremental` instance attached to the table resource. [Initial end and last values
must match timezone-awareness of the cursor column](setup.md) because they will be used as parameters in the `WHERE` clause.

In rare cases where the last value is already stored in the pipeline state and has incorrect timezone-awareness, you may not be able to recover your pipeline automatically. You can
modify the local pipeline state (after syncing with destination) to add/remove timezone information.

## Troubleshooting connection

### Pipeline state grows extremely large or I get deduplication state warnings when using incremental
If you set incremental column on low resolution column (ie. of type **date**) then `dlt` will [deduplicate](../../../general-usage/incremental/cursor.md#deduplicate-overlapping-ranges) such data by default. For low resolution column you may have many rows associated with a single
cursor value and since hashes of such rows are stored in state - you will get large pipeline state. You can avoid that in many ways:
1. [set the comparison to exclusive](advanced.md#inclusive-and-exclusive-filtering) but make sure that rows are not added with the last cursor column value
between run (ie. if you have column on a **day** column and between runs, rows are added to that day - with open range those rows will be skipped)
2. use high resolution cursor columns (ie. **datetime** type) so not many rows are associated with single value.
3. disable deduplication [explicitly](../../../general-usage/incremental/cursor.md#deduplicate-overlapping-ranges)

### Connecting to MySQL with SSL 
Here, we use the `mysql` and `pymysql` dialects to set up an SSL connection to a server, with all information taken from the [SQLAlchemy docs](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#ssl-connections).

1. To enforce SSL on the client without a client certificate, you may pass the following DSN:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca="
   ```

1. You can also pass the server's public certificate (potentially bundled with your pipeline) and disable host name checks:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca=server-ca.pem&ssl_check_hostname=false"
   ```

1. For servers requiring a client certificate, provide the client's private key (a secret value). In Airflow, this is usually saved as a variable and exported to a file before use. The server certificate is omitted in the example below:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@35.203.96.191:3306/mysql?ssl_ca=&ssl_cert=client-cert.pem&ssl_key=client-key.pem"
   ```

### SQL Server connection options

**To connect to an `mssql` server using Windows authentication**, include `trusted_connection=yes` in the connection string.

```toml
sources.sql_database.credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver 17+for+SQL+Server"
```

**To connect to a local SQL server instance running without SSL**, pass the `encrypt=no` parameter:
```toml
sources.sql_database.credentials="mssql+pyodbc://loader:loader@localhost/dlt_data?encrypt=no&driver=ODBC+Driver 17+for+SQL+Server"
```

**To allow a self-signed SSL certificate** when you are getting `certificate verify failed: unable to get local issuer certificate`:
```toml
sources.sql_database.credentials="mssql+pyodbc://loader:loader@localhost/dlt_data?TrustServerCertificate=yes&driver=ODBC+Driver 17+for+SQL+Server"
```

**To use long strings (>8k) and avoid collation errors**:
```toml
sources.sql_database.credentials="mssql+pyodbc://loader:loader@localhost/dlt_data?LongAsMax=yes&driver=ODBC+Driver 17+for+SQL+Server"
```

**To fix MS SQL Server connection issues with ConnectorX**:

Some users have reported issues with MS SQL Server and Connector X. The problems are not caused by dlt, but by how connections are made. A big thanks to [Mark-James M](https://github.com/markjamesm) for suggesting a solution.

To fix connection issues with ConnectorX and MS SQL Server, include both `Encrypt=yes` and `encrypt=true` in your connection string:
```toml
sources.sql_database.credentials="mssql://user:password@server:1433/database?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&encrypt=true"
```
This approach can help resolve connection-related issues.

## Troubleshooting backends

### Notes on specific databases

#### Oracle
1. When using the `oracledb` dialect in thin mode, we are getting protocol errors. Use thick mode or the `cx_oracle` (old) client.
2. Mind that `SQLAlchemy` translates Oracle identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
3. `Connectorx` is for some reason slower for Oracle than the `PyArrow` backend.  
  
See [here](https://github.com/dlt-hub/sql_database_benchmarking/tree/main/oracledb#installing-and-setting-up-oracle-db) for information and code on setting up and benchmarking on Oracle.

#### DB2
1. Mind that `SQLAlchemy` translates DB2 identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
2. The DB2 type `DOUBLE` gets incorrectly mapped to the Python type `float` (instead of the `SQLAlchemy` type `Numeric` with default precision). This requires `dlt` to perform additional casts. The cost of the cast, however, is minuscule compared to the cost of reading rows from the database.  

See [here](https://github.com/dlt-hub/sql_database_benchmarking/tree/main/db2#installing-and-setting-up-db2) for information and code on setting up and benchmarking on DB2.

#### MySQL
1. The `SQLAlchemy` dialect converts doubles to decimals. (This can be disabled via the table adapter argument as shown in the code example [here](./configuration#pyarrow))

#### Postgres / MSSQL
No issues were found for these databases. Postgres is the only backend where we observed a 2x speedup with `ConnectorX` (see [here](https://github.com/dlt-hub/sql_database_benchmarking/tree/main/postgres) for the benchmarking code). On other db systems, it performs the same as (or sometimes worse than) the `PyArrow` backend.

### Notes on specific data types

#### JSON

In the `SQLAlchemy` backend, the JSON data type is represented as a Python object, and in the `PyArrow` backend, it is represented as a JSON string. At present, it does not work correctly with `pandas` and `ConnectorX`, which cast Python objects to `str`, generating invalid JSON strings that cannot be loaded into the destination.

#### UUID  
UUIDs are represented as strings by default. You can switch this behavior by using `table_adapter_callback` to modify properties of the UUID type for a particular column. (See the code example [here](./configuration#pyarrow) for how to modify the data type properties of a particular column.)

### Notes on data loading

#### Arrow loading

Using the configuration `sql_database(backend="pyarrow")` improves performance by pivoting all records (row-major) into a arrow table (column-major). The library `pyarrow` doesn't have a utility for this pivot operation, which is why `numpy` or `pandas` is necessary. Either library is required for this pivot operation only, then `pyarrow` handles the rest of type conversion and applying dlt constraints. 
