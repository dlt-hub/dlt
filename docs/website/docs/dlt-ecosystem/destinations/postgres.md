---
title: Postgres
description: Postgres `dlt` destination
keywords: [postgres, destination, data warehouse]
---

# Postgres

**1. Initialize a project with a pipeline that loads to Postgres by running**
```
dlt init chess postgres
```

**2. Install the necessary dependencies for Postgres by running**
```
pip install -r requirements.txt
```
This will install dlt with **postgres** extra which contains `psycopg2` client.

**3. Create a new database after setting up a Postgres instance and `psql` / query editor by running**
```
CREATE DATABASE dlt_data;
```

Add `dlt_data` database to `.dlt/secrets.toml`.

**4. Create a new user by running**
```
CREATE USER loader WITH PASSWORD '<password>';
```

Add `loader` user and `<password>` password to `.dlt/secrets.toml`.

**5. Give the `loader` user owner permissions by running**
```
ALTER DATABASE dlt_data OWNER TO loader;
```

It is possible to set more restrictive permissions (e.g. give user access to a specific schema).

**6. Enter your credentials into `.dlt/secrets.toml`.**
It should now look like
```toml
[destination.postgres.credentials]

database = "dlt_data"
username = "loader"
password = "<password>" # replace with your password
host = "localhost" # or the IP address location of your database
port = 5432
connect_timeout = 15
```

You can also pass a database connection string similar to the one used by `psycopg2` library or [SQLAlchemy](https://docs.sqlalchemy.org/en/20/core/engines.html#postgresql). Credentials above will look like this:
```toml
destination.postgres.credentials="postgresql://loader:<password>@localhost/dlt_data?connect_timeout=15"
```

To pass credentials directly you can use `credentials` argument passed to `dlt.pipeline` or `pipeline.run` methods.
```python
pipeline = dlt.pipeline(pipeline_name='chess', destination='postgres', dataset_name='chess_data', credentials="postgresql://loader:<password>@localhost/dlt_data")
```

## Additional destination options
Postgres destination creates UNIQUE indexes by default on columns with `unique` hint (ie. `_dlt_id`). To disable this behavior
```toml
[destination.postgres]
create_indexes=false
```