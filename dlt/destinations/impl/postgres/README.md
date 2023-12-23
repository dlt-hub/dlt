# loader account setup

1. Create new database `CREATE DATABASE dlt_data`
2. Create new user, set password `CREATE USER loader WITH PASSWORD 'loader';`
3. Set as database owner (we could set lower permission) `ALTER DATABASE dlt_data OWNER TO loader`
