# def test_query_iterator_gcp(gcp_client: BigQueryClient) -> None:
#     gcp_client.update_storage_schema()
#     load_id = "182879721.182912"
#     gcp_client.complete_load(load_id)
#     load_table = gcp_client.fully_qualified_table_name(Schema.LOADS_TABLE_NAME)
#     with gcp_client.execute_query(f"SELECT * FROM {Schema.LOADS_TABLE_NAME}") as curr:
#         columns = [c[0] for c in curr.description]
#         import pandas
#         from pandas.io.sql import _wrap_result
#         df: pandas.DataFrame = _wrap_result(curr.fetchall(), columns)
#         print(df.columns)
#         print(df.dtypes)
#         print(df)
    # print(list(gcp_client.execute_query(f"SELECT * FROM {load_table}")))
    # print(list(gcp_client.execute_query(f"SELECT none FROM {Schema.LOADS_TABLE_NAME}")))