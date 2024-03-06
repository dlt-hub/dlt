from pathlib import Path
from typing import Optional

import dlt
import pandas as pd
import streamlit as st

from dlt.pipeline.exceptions import SqlClientNotAvailable

HERE = Path(__file__).absolute().parent

if hasattr(st, "cache_data"):
    cache_data = st.cache_data
else:
    cache_data = st.experimental_memo


# FIXME: make something to DRY the code
def query_data(pipeline: dlt.Pipeline, query: str, schema_name: str = None) -> pd.DataFrame:
    @cache_data(ttl=600)
    def query_data(query: str, schema_name: str = None) -> Optional[pd.DataFrame]:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df()
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")

    return query_data(query, schema_name)


def query_data_live(pipeline: dlt.Pipeline, query: str, schema_name: str = None) -> pd.DataFrame:
    @cache_data(ttl=5)
    def query_data(query: str, schema_name: str = None) -> Optional[pd.DataFrame]:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df()
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")

    return query_data(query, schema_name)
