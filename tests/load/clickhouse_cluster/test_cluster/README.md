This folder contains the files to run a local ClickHouse cluster using Docker compose.

## Cluster characteristics
The cluster has seven nodes:
- four data nodes: `clickhouse-01` ... `clickhouse-04`
- three keeper nodes: `clickhouse-keeper-01` ... `clickhouse-keeper-03`

The cluster has three different configurations:
- `cluster_1S_2R`: one shard, two replications (based on https://clickhouse.com/docs/architecture/replication)
- `cluster_2S_1R`: two shards, one replication (based on https://clickhouse.com/docs/architecture/horizontal-scaling)
- `cluster_2S_2R`: two shards, two replications (based on https://clickhouse.com/docs/architecture/cluster-deployment)

*The third and fourth data nodes (i.e. `clickhouse-03` and `clickhouse-04`) are only used in configuration `cluster_2S_2R`.*

## Notes

These adjustements are made to the cluster definition files (compared to ClickHouse docs references linked in previous section):
- ports changed, to not interfere with single-node ClickHouse container
- environment variables added, to create `dlt_data` database and `loader` user
- `users.xml` config files removed, because we create the user with environment variables
- swapped data nodes two and three in configuration `cluster_2S_2R`, so it matches configuration `cluster_1S_2R`, so we can use the same `macros` definition for both