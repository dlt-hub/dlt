from adbc_driver_flightsql.dbapi import connect

with connect(
    uri="grpc://dremio:32010",
    db_kwargs={
        "username": "dremio",
        "password": "dremio123",
    },
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("select 'a' as foo")
        result = cursor.fetchall()  # <- hangs indefinitely
        print(result)
