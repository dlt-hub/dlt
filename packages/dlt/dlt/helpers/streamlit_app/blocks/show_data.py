import dlt
import streamlit as st

from dlt.helpers.streamlit_app.utils import query_data


def show_data_button(pipeline: dlt.Pipeline, table_name: str) -> None:
    if st.button("SHOW DATA", key=table_name):
        df = query_data(pipeline, f"SELECT * FROM {table_name}", chunk_size=2048)
        if df is None:
            st.text("No rows returned")
        else:
            rows_count = df.shape[0]
            if df.shape[0] < 2048:
                st.text(f"All {rows_count} row(s)")
            else:
                st.text(f"Top {rows_count} row(s)")

            st.dataframe(df)
