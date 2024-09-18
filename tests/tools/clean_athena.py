"""WARNING: Running this script will drop add schemas in the athena destination set up in your secrets.toml"""

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="drop_athena", destination="athena")

    with pipeline.sql_client() as client:
        with client.execute_query("SHOW DATABASES") as cur:
            dbs = cur.fetchall()
        for db in dbs:
            db = db[0]
            sql = f"DROP SCHEMA `{db}` CASCADE;"
            try:
                print(sql)
                with client.execute_query(sql):
                    pass  #
            except DatabaseUndefinedRelation:
                print("Could not delete schema")
