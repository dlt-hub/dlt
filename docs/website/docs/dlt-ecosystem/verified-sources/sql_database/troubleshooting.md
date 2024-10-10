---
title: Troubleshooting
description: common troubleshooting use-cases for the sql_database source
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Troubleshooting

<Header/>

## Troubleshooting connection

#### Connecting to MySQL with SSL 
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

#### SQL Server connection options

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

