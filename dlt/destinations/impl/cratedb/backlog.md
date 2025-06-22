# CrateDB destination adapter backlog

## Iteration +1

- Check if the Redshift literal escaper is the right choice for all
  data types it is handling.

- Currently only works with CrateDB's default dataset / schema `doc`,
  that's why out-of-the-box `dlt init chess cratedb` will fail like
  `ERROR: Schema 'chess_players_games_data' unknown`. Why!?
  Thoughts: Most probably, dlt introspects the available schemas,
  doesn't find it in CrateDB, and fails. It doesn't know that CrateDB
  would create the schema transparently, without needing an explicit
  `CREATE SCHEMA ...` operation.

- ERROR: mismatched input 'ABORT' expecting {....

- Documentation: How JSONB/ARRAYs are mapped to CrateDB

- The `merge` followup job is currently defunct with CrateDB.
  It has been replaced by  a `replace" [sic!] job.

- Troubleshooting: Workloads specifying certain write dispositions currently
  need to be invoked twice. On the first invocation, this happens:

      dlt.pipeline.exceptions.PipelineStepFailed: Pipeline execution failed at stage load when processing package 1750628689.026369 with exception:

      <class 'dlt.common.destination.exceptions.DestinationSchemaTampered'>
      Schema frankfurter content was changed - by a loader or by destination code - from the moment it was retrieved by load package. Such schema cannot reliably be updated nor saved. Current version hash: ZUJEaD/gSBvY9vbgx3urdi93mJppp7CMlRaLuJkxJCs= != stored version hash Cocqcq+4Qzo8zLOc9R3uYjhjLZr6D/QwgS03TtFG2wI=. If you are using destination client directly, without storing schema in load package, you should first save it into schema storage. You can also use schema._bump_version() in test code to remove modified flag.

- Is it possible to use an `adapter` to better support CrateDB's special
  data types? For example, full round-tripping using `sys.summits` isn't
  possible just yet, due to the `coordinates` column, for example when
  selecting DuckDB.

- Is it possible to provide other direct loaders from the `sqlalchemy`
  adapter?

- Addressing per `cratedb://` will raise errors, e.g. in `ingestr`:
  `Following fields are missing: ['password', 'username', 'host'] in configuration with spec CrateDbCredentials`

- Add staging support
  > CrateDB supports Amazon S3, Google Cloud Storage, and Azure Blob
  > Storage as file staging destinations.

## Verify

- > Data is loaded into CrateDB using the most efficient method
  > depending on the data source.

- > CrateDB does not support the `binary` datatype. Binary will be loaded to a `text` column.

## Iteration +2

- The `UNIQUE` constraint is dearly missing. Is it possible to emulate?
  Otherwise, when loading data multiple times, duplicates will happen.
  ```
  cr> select * from users;
  +----+-------+-------------------+----------------+
  | id | name  |     __dlt_load_id | __dlt_id       |
  +----+-------+-------------------+----------------+
  |  2 | Bob   | 1749406972.57687  | NFPmX5Pw4gZ7fA |
  |  1 | Alice | 1749406985.809837 | 6/Xoe8jhlBUbAQ |
  |  2 | Bob   | 1749406985.809837 | VnP4S8AsQ/ujOg |
  |  1 | Alice | 1749406972.57687  | 4HCWs9jqfwTyTQ |
  +----+-------+-------------------+----------------+
  SELECT 4 rows in set (0.003 sec)
  ```

- Provide Ibis dataset access, see
  tests/load/test_read_interfaces.py

- Software tests about geospatial concerns have been removed.
  See `postgres` adapter for bringing them back.
