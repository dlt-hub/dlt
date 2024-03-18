import os
import sys

from pathlib import Path
from typing import Optional, Callable

import dlt
import pandas as pd
import streamlit as st

from dlt.cli import echo as fmt
from dlt.pipeline.exceptions import SqlClientNotAvailable

HERE = Path(__file__).absolute().parent

if hasattr(st, "cache_data"):
    cache_data = st.cache_data
else:
    cache_data = st.experimental_memo


def attach_to_pipeline(pipeline_name: str) -> dlt.Pipeline:
    st.session_state["pipeline_name"] = pipeline_name
    pipelines_dir = os.getenv("DLT_PIPELINES_DIR")
    pipeline = dlt.attach(pipeline_name, pipelines_dir=pipelines_dir)
    return pipeline


def render_with_pipeline(render_func: Callable[..., None]) -> None:
    if test_pipeline_name := os.getenv("DLT_TEST_PIPELINE_NAME"):
        fmt.echo(f"RUNNING TEST PIPELINE: {test_pipeline_name}")
        pipeline_name = test_pipeline_name
    else:
        pipeline_name = sys.argv[1]

    pipeline = attach_to_pipeline(pipeline_name)
    render_func(pipeline)


# FIXME: make something to DRY the code
def query_data(
    pipeline: dlt.Pipeline,
    query: str,
    schema_name: str = None,
    chunk_size: Optional[int] = None,
) -> pd.DataFrame:
    @cache_data(ttl=600)
    def query_data(  # type: ignore[return]
        query: str,
        schema_name: str = None,
        chunk_size: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df(chunk_size=chunk_size)
        except SqlClientNotAvailable:
            st.error("🚨 Cannot load data - SqlClient not available")

    return query_data(query, schema_name, chunk_size=chunk_size)


def query_data_live(
    pipeline: dlt.Pipeline,
    query: str,
    schema_name: str = None,
    chunk_size: Optional[int] = None,
) -> pd.DataFrame:
    @cache_data(ttl=5)
    def query_data(  # type: ignore[return]
        query: str,
        schema_name: str = None,
        chunk_size: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df(chunk_size=chunk_size)
        except SqlClientNotAvailable:
            st.error("🚨 Cannot load data - SqlClient not available")

    return query_data(query, schema_name, chunk_size=chunk_size)
