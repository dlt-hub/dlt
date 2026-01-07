from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient


def get_table_engine(sql_client: ClickHouseSqlClient, table_name: str) -> str:
    qry = "SELECT engine FROM system.tables WHERE database = %s AND name = %s;"
    table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    with sql_client:
        result = sql_client.execute_sql(qry, database, name)

    return result[0][0]
