This folder contains the files to run a local ClickHouse cluster using Docker compose.

## Cluster characteristics
The cluster has five nodes:
- two data nodes
- three keeper nodes

The cluster has two different configurations:
- `cluster_1S_2R`: one shard, two replications (based on https://clickhouse.com/docs/architecture/replication)
- `cluster_2S_1R`: two shards, one replication (based on https://clickhouse.com/docs/architecture/horizontal-scaling)

## Notes

These adjustements are made to the cluster definition files (compared to ClickHouse docs references linked in previous section):
- ports changed, to not interfere with single-node ClickHouse container
- environment variables added, to create `dlt_data` database and `loader` user
- `users.xml` config files removed, because we create the user with environment variables