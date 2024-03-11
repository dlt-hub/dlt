# import streamlit as st

# load_ids = loads_df["load_id"].to_list()
# st.text(load_ids)
# chart_data = []
# for load_id in load_ids:
#     stack_items = []
#     for table in table_names:
#         stack_items.append(_query_table_count(table_name=table, load_id=load_id))
#     chart_data.append(stack_items)
# st.json(chart_data, expanded=False)
# st.text(table_names)
# load_data = pd.DataFrame(chart_data, columns=table_names)
# st.bar_chart(load_data, x=load_ids)
# def _query_table_count(table_name: str, load_id: str) -> int:
#     query = (
#         f"SELECT COUNT(1) AS rows_count FROM {table_name} " f"WHERE _dlt_load_id = '{load_id}'"
#     )
#     rows_counts_df = _query_data(query)
#     return int(rows_counts_df["rows_count"][0])
