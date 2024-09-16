"""WARNING: Running this script will drop add schemas in the redshift destination set up in your secrets.toml"""

import dlt
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTerminalException,
    DatabaseTransientException,
)

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="drop_redshift", destination="redshift")

    with pipeline.sql_client() as client:
        with client.execute_query("""select s.nspname as table_schema,
        s.oid as schema_id,
        u.usename as owner
        from pg_catalog.pg_namespace s
        join pg_catalog.pg_user u on u.usesysid = s.nspowner
        order by table_schema;""") as cur:
            dbs = [row[0] for row in cur.fetchall()]
        for db in dbs:
            if db.startswith("<"):
                continue
            sql = f"DROP SCHEMA {db} CASCADE;"
            try:
                print(sql)
                with client.execute_query(sql):
                    pass  #
            except (
                DatabaseUndefinedRelation,
                DatabaseTerminalException,
                DatabaseTransientException,
            ):
                print("Could not delete schema")
