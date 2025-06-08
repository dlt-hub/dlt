# CrateDB destination adapter

## What's inside

- The `cratedb` adapter is heavily based on the `postgres` adapter.
- The `CrateDbSqlClient` deviates from the original `Psycopg2SqlClient` by
  accounting for [CRATEDB-15161] per `SystemColumnWorkaround`.

## Backlog

A few items need to be resolved, see [backlog](./backlog.md).


[CRATEDB-15161]: https://github.com/crate/crate/issues/15161
