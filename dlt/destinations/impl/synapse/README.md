# Set up loader user
Execute the following SQL statements to set up the [loader](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/data-loading-best-practices#create-a-loading-user) user:
```sql
-- on master database

CREATE LOGIN loader WITH PASSWORD = 'YOUR_LOADER_PASSWORD_HERE';
```

```sql
-- on minipool database

CREATE USER loader FOR LOGIN loader;

-- DDL permissions
GRANT CREATE TABLE ON DATABASE :: minipool TO loader;
GRANT CREATE VIEW ON DATABASE :: minipool TO loader;

-- DML permissions
GRANT SELECT ON DATABASE :: minipool TO loader;
GRANT INSERT ON DATABASE :: minipool TO loader;
GRANT ADMINISTER DATABASE BULK OPERATIONS TO loader;
```

```sql
-- https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-workload-isolation

CREATE WORKLOAD GROUP DataLoads
WITH ( 
    MIN_PERCENTAGE_RESOURCE = 0
    ,CAP_PERCENTAGE_RESOURCE = 50
    ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 25
);

CREATE WORKLOAD CLASSIFIER [wgcELTLogin]
WITH (
    WORKLOAD_GROUP = 'DataLoads'
    ,MEMBERNAME = 'loader'
);
```

# config.toml
```toml
[destination.synapse.credentials]
database = "minipool"
username = "loader"
host = "dlt-synapse-ci.sql.azuresynapse.net"
port = 1433
driver = "ODBC Driver 18 for SQL Server"

[destination.synapse]
create_indexes = false
```

# secrets.toml
```toml
[destination.synapse.credentials]
password = "YOUR_LOADER_PASSWORD_HERE"
```