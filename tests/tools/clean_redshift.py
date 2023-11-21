from dlt.destinations.impl.postgres.postgres import PostgresClient
from dlt.destinations.impl.postgres.sql_client import psycopg2
from psycopg2.errors import InsufficientPrivilege, InternalError_, SyntaxError

CONNECTION_STRING = ""

if __name__ == '__main__':
    # connect
    connection = psycopg2.connect(CONNECTION_STRING)
    connection.set_isolation_level(0)

    # list all schemas
    with connection.cursor() as curr:
        curr.execute("""select s.nspname as table_schema,
        s.oid as schema_id,
        u.usename as owner
        from pg_catalog.pg_namespace s
        join pg_catalog.pg_user u on u.usesysid = s.nspowner
        order by table_schema;""")
        schemas = [row[0] for row in curr.fetchall()]

    # delete all schemas, skipp expected errors
    with connection.cursor() as curr:
        print(f"Deleting {len(schemas)} schemas")
        for schema in schemas:
            print(f"Deleting {schema}...")
            try:
                curr.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            except (InsufficientPrivilege, InternalError_, SyntaxError):
                pass
            print(f"Deleted {schema}...")
