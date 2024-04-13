from typing import Optional
import dlt
import streamlit as st

from dlt.common.exceptions import MissingDependencyException
from dlt.helpers.streamlit_app.utils import query_data


def maybe_run_query(
    pipeline: dlt.Pipeline,
    show_charts: bool = True,
    example_query: Optional[str] = "",
) -> None:
    st.subheader("Run your query")
    sql_query = st.text_area("Enter your SQL query", value=example_query)
    if st.button("Run Query"):
        if sql_query:
            try:
                # run the query from the text area
                df = query_data(pipeline, sql_query, chunk_size=2048)
                if df is None:
                    st.text("No rows returned")
                else:
                    rows_count = df.shape[0]
                    st.text(f"{rows_count} row(s) returned")
                    st.dataframe(df)
                    try:
                        # now if the dataset has supported shape try to display the bar or altair chart
                        if df.dtypes.shape[0] == 1 and show_charts:
                            # try barchart
                            st.bar_chart(df)
                        if df.dtypes.shape[0] == 2 and show_charts:
                            # try to import altair charts
                            try:
                                import altair as alt
                            except ModuleNotFoundError:
                                raise MissingDependencyException(
                                    "dlt Streamlit Helpers",
                                    ["altair"],
                                    "dlt Helpers for Streamlit should be run within a streamlit"
                                    " app.",
                                )

                            # try altair
                            bar_chart = (
                                alt.Chart(df)
                                .mark_bar()
                                .encode(
                                    x=f"{df.columns[1]}:Q", y=alt.Y(f"{df.columns[0]}:N", sort="-x")
                                )
                            )
                            st.altair_chart(bar_chart, use_container_width=True)
                    except Exception as ex:
                        st.error(f"Chart failed due to: {ex}")
            except Exception as ex:
                st.text("Exception when running query")
                st.exception(ex)
