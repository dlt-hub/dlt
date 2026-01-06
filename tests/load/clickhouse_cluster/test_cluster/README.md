This folder contains the files to run a local ClickHouse cluster using Docker compose.
It is based on https://clickhouse.com/docs/architecture/replication, with these adjustments:
- ports changed, to not interfere with single-node ClickHouse container
- environment variables added, to create `dlt_data` database and `loader` user
- `users.xml` config files removed, because we create the user with environment variables