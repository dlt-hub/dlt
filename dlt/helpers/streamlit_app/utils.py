import argparse
import os

from pathlib import Path
from typing import Optional, Callable, Tuple

import dlt
import pandas as pd
import streamlit as st

from dlt.cli import echo as fmt
from dlt.common.destination.exceptions import SqlClientNotAvailable

HERE = Path(__file__).absolute().parent


def parse_args() -> Tuple[str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("pipeline_name", nargs=1)
    parser.add_argument(
        "--pipelines-dir",
        help="Pipelines working directory",
        default=None,
    )
    known_options, _ = parser.parse_known_args()
    return known_options.pipeline_name[0], known_options.pipelines_dir


def render_with_pipeline(render_func: Callable[..., None]) -> None:
    pipeline_name, pipelines_dir = parse_args()
    if test_pipeline_name := os.getenv("DLT_TEST_PIPELINE_NAME"):
        fmt.echo(f"RUNNING TEST PIPELINE: {test_pipeline_name}")
        pipeline_name = test_pipeline_name

    st.session_state["pipeline_name"] = pipeline_name
    # use pipelines dir from env var or try to resolve it using get_dlt_pipelines_dir
    pipeline = dlt.attach(pipeline_name, pipelines_dir=pipelines_dir)
    render_func(pipeline)


def query_using_cache(pipeline: dlt.Pipeline, ttl: int) -> Callable[..., Optional[pd.DataFrame]]:
    @st.cache_data(ttl=ttl)
    def do_query(  # type: ignore[return]
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

    return do_query  # type: ignore[unused-ignore, no-any-return]


def query_data(
    pipeline: dlt.Pipeline,
    query: str,
    schema_name: str = None,
    chunk_size: Optional[int] = None,
) -> pd.DataFrame:
    query_maker = query_using_cache(pipeline, ttl=60)
    return query_maker(query, schema_name, chunk_size=chunk_size)


def query_data_live(
    pipeline: dlt.Pipeline,
    query: str,
    schema_name: str = None,
    chunk_size: Optional[int] = None,
) -> pd.DataFrame:
    query_maker = query_using_cache(pipeline, ttl=5)
    return query_maker(query, schema_name, chunk_size=chunk_size)
