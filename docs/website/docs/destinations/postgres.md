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

**6. Your `.dlt/secrets.toml` should now look like**
```
[destination.postgres.credentials]

database = "dlt_data"
username = "loader"
password = "<password>" # replace with your password
host = "localhost" # or the IP address location of your database
port = 5432
connect_timeout = 15
```