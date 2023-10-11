import sys
from typing import Dict, List
import humanize


from dlt.common import pendulum
from dlt.common.typing import AnyFun
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.reference import WithStateSync

from dlt.helpers.pandas_helper import pd
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import CannotRestorePipelineException, SqlClientNotAvailable
from dlt.pipeline.state_sync import load_state_from_destination

try:
    import streamlit as st
    # from streamlit import SECRETS_FILE_LOC, secrets
except ModuleNotFoundError:
    raise MissingDependencyException("DLT Streamlit Helpers", ["streamlit"], "DLT Helpers for Streamlit should be run within a streamlit app.")


# use right caching function to disable deprecation message
if hasattr(st, "cache_data"):
    cache_data = st.cache_data
else:
    cache_data = st.experimental_memo

# def restore_pipeline() -> Pipeline:
#     """Restores Pipeline instance and associated credentials from Streamlit secrets

#         Current implementation requires that pipeline working dir is available at the location saved in secrets.

#     Raises:
#         PipelineBackupNotFound: Raised when pipeline backup is not available
#         CannotRestorePipelineException: Raised when pipeline working dir is not found or invalid

#     Returns:
#         Pipeline: Instance of pipeline with attached credentials
#     """
#     if "dlt" not in secrets:
#         raise PipelineException("You must backup pipeline to Streamlit first")
#     dlt_cfg = secrets["dlt"]
#     credentials = deepcopy(dict(dlt_cfg["destination"]))
#     if "default_schema_name" in credentials:
#         del credentials["default_schema_name"]
#     credentials.update(dlt_cfg["credentials"])
#     pipeline = Pipeline(dlt_cfg["pipeline_name"])
#     pipeline.restore_pipeline(credentials_from_dict(credentials), dlt_cfg["working_dir"])
#     return pipeline


# def backup_pipeline(pipeline: Pipeline) -> None:
#     """Backups pipeline state to the `secrets.toml` of the Streamlit app.

#     Pipeline credentials and working directory will be added to the Streamlit `secrets` file. This allows to access query the data loaded to the destination and
#     access definitions of the inferred schemas. See `restore_pipeline` and `write_data_explorer_page` functions in the same module.

#     Args:
#         pipeline (Pipeline): Pipeline instance, typically restored with `restore_pipeline`
#     """
#     # save pipeline state to project .config
#     # config_file_name = file_util.get_project_streamlit_file_path("config.toml")

#     # save credentials to secrets
#     if os.path.isfile(SECRETS_FILE_LOC):
#         with open(SECRETS_FILE_LOC, "r", encoding="utf-8") as f:
#             # use whitespace preserving parser
#             secrets_ = tomlkit.load(f)
#     else:
#         secrets_ = tomlkit.document()

#     # save general settings
#     secrets_["dlt"] = {
#         "working_dir": pipeline.working_dir,
#         "pipeline_name": pipeline.pipeline_name
#     }

#     # get client config
#     # TODO: pipeline api v2 should provide a direct method to get configurations
#     CONFIG: BaseConfiguration = pipeline._loader_instance.load_client_cls.CONFIG  # type: ignore
#     CREDENTIALS: CredentialsConfiguration = pipeline._loader_instance.load_client_cls.CREDENTIALS  # type: ignore

#     # save client config
#     # print(dict_remove_nones_in_place(CONFIG.as_dict(lowercase=False)))
#     dlt_c = cast(TomlContainer, secrets_["dlt"])
#     dlt_c["destination"] = dict_remove_nones_in_place(dict(CONFIG))
#     dlt_c["credentials"] = dict_remove_nones_in_place(dict(CREDENTIALS))

#     with open(SECRETS_FILE_LOC, "w", encoding="utf-8") as f:
#         # use whitespace preserving parser
#         tomlkit.dump(secrets_, f)


def write_load_status_page(pipeline: Pipeline) -> None:
    """Display pipeline loading information. Will be moved to dlt package once tested"""

    @cache_data(ttl=600)
    def _query_data(query: str, schema_name: str = None) -> pd.DataFrame:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df()
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")

    @cache_data(ttl=5)
    def _query_data_live(query: str, schema_name: str = None) -> pd.DataFrame:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df()
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")

    try:
        st.header("Pipeline info")
        credentials = pipeline.sql_client().credentials
        st.markdown(f"""
        * pipeline name: **{pipeline.pipeline_name}**
        * destination: **{str(credentials)}** in **{pipeline.destination.__name__}**
        * dataset name: **{pipeline.dataset_name}**
        * default schema name: **{pipeline.default_schema_name}**
        """)

        st.header("Last load info")
        col1, col2, col3 = st.columns(3)
        loads_df = _query_data_live(
            f"SELECT load_id, inserted_at FROM {pipeline.default_schema.loads_table_name} WHERE status = 0 ORDER BY inserted_at DESC LIMIT 101 "
        )
        loads_no = loads_df.shape[0]
        if loads_df.shape[0] > 0:
            rel_time = humanize.naturaldelta(pendulum.now() - pendulum.from_timestamp(loads_df.iloc[0, 1].timestamp())) + " ago"
            last_load_id = loads_df.iloc[0, 0]
            if loads_no > 100:
                loads_no = "> " + str(loads_no)
        else:
            rel_time = "---"
            last_load_id = "---"
        col1.metric("Last load time", rel_time)
        col2.metric("Last load id", last_load_id)
        col3.metric("Total number of loads", loads_no)

        st.markdown("**Number of loaded rows:**")
        selected_load_id = st.selectbox("Select load id", loads_df)
        schema = pipeline.default_schema

        # construct a union query
        query_parts = []
        for table in schema.data_tables():
            if "parent" in table:
                continue
            table_name = table["name"]
            query_parts.append(f"SELECT '{table_name}' as table_name, COUNT(1) As rows_count FROM {table_name} WHERE _dlt_load_id = '{selected_load_id}'")
            query_parts.append("UNION ALL")
        query_parts.pop()
        rows_counts_df = _query_data("\n".join(query_parts))

        st.markdown(f"Rows loaded in **{selected_load_id}**")
        st.dataframe(rows_counts_df)

        st.markdown("**Last 100 loads**")
        st.dataframe(loads_df)

        st.header("Schema updates")
        schemas_df = _query_data_live(
            f"SELECT schema_name, inserted_at, version, version_hash FROM {pipeline.default_schema.version_table_name} ORDER BY inserted_at DESC LIMIT 101 "
            )
        st.markdown("**100 recent schema updates**")
        st.dataframe(schemas_df)

        st.header("Pipeline state info")
        with pipeline.destination_client() as client:
            if isinstance(client, WithStateSync):
                remote_state = load_state_from_destination(pipeline.pipeline_name, client)
        local_state = pipeline.state

        col1, col2 = st.columns(2)
        if remote_state:
            remote_state_version = remote_state["_state_version"]
        else:
            remote_state_version = "---"  # type: ignore

        col1.metric("Local state version", local_state["_state_version"])
        col2.metric("Remote state version", remote_state_version)

        if remote_state_version != local_state["_state_version"]:
            st.warning("Looks like that local state is not yet synchronized or synchronization is disabled")

    except CannotRestorePipelineException as restore_ex:
        st.error("Seems like the pipeline does not exist. Did you run it at least once?")
        st.exception(restore_ex)

    except ConfigFieldMissingException as cf_ex:
        st.error("Pipeline credentials/configuration is missing. This most often happen when you run the streamlit app from different folder than the `.dlt` with `toml` files resides.")
        st.text(str(cf_ex))

    except Exception as ex:
        st.error("Pipeline info could not be prepared. Did you load the data at least once?")
        st.exception(ex)



def write_data_explorer_page(pipeline: Pipeline, schema_name: str = None, show_dlt_tables: bool = False, example_query: str = "", show_charts: bool = True) -> None:
    """Writes Streamlit app page with a schema and live data preview.

    #### Args:
        pipeline (Pipeline): Pipeline instance to use.
        schema_name (str, optional): Name of the schema to display. If None, default schema is used.
        show_dlt_tables (bool, optional): Should show dlt internal tables. Defaults to False.
        example_query (str, optional): Example query to be displayed in the SQL Query box.
        show_charts (bool, optional): Should automatically show charts for the queries from SQL Query box. Defaults to True.

    Raises:
        MissingDependencyException: Raised when a particular python dependency is not installed
    """

    @cache_data(ttl=60)
    def _query_data(query: str, chunk_size: int = None) -> pd.DataFrame:
        try:
            with pipeline.sql_client(schema_name) as client:
                with client.execute_query(query) as curr:
                    return curr.df(chunk_size=chunk_size)
        except SqlClientNotAvailable:
            st.error("Cannot load data - SqlClient not available")


    if schema_name:
        schema = pipeline.schemas[schema_name]
    else:
        schema = pipeline.default_schema
    st.title(f"Available tables in {schema.name} schema")

    for table in schema.data_tables():
        table_name = table["name"]
        st.header(table_name)
        if "description" in table:
            st.text(table["description"])
        table_hints: List[str] = []
        if "parent" in table:
            table_hints.append("parent: **%s**" % table["parent"])
        if "resource" in table:
            table_hints.append("resource: **%s**" % table["resource"])
        if "write_disposition" in table:
            table_hints.append("write disposition: **%s**" % table["write_disposition"])

        st.markdown(" | ".join(table_hints))

        # table schema contains various hints (like clustering or partition options) that we do not want to show in basic view
        essentials_f = lambda c: {k:v for k, v in c.items() if k in ["name", "data_type", "nullable"]}

        st.table(map(essentials_f, table["columns"].values()))
        # add a button that when pressed will show the full content of a table
        if st.button("SHOW DATA", key=table_name):
            df = _query_data(f"SELECT * FROM {table_name}", chunk_size=2048)
            if df is None:
                st.text("No rows returned")
            else:
                rows_count = df.shape[0]
                if df.shape[0] < 2048:
                    st.text(f"All {rows_count} row(s)")
                else:
                    st.text(f"Top {rows_count} row(s)")
                st.dataframe(df)

    st.title("Run your query")
    sql_query = st.text_area("Enter your SQL query", value=example_query)
    if st.button("Run Query"):
        if sql_query:
            try:
                # run the query from the text area
                df = _query_data(sql_query)
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
                                    "DLT Streamlit Helpers",
                                    ["altair"],
                                    "DLT Helpers for Streamlit should be run within a streamlit app."
                                )

                            # try altair
                            bar_chart = alt.Chart(df).mark_bar().encode(
                                x=f'{df.columns[1]}:Q',
                                y=alt.Y(f'{df.columns[0]}:N', sort='-x')
                            )
                            st.altair_chart(bar_chart, use_container_width=True)
                    except Exception as ex:
                        st.error(f"Chart failed due to: {ex}")
            except Exception as ex:
                st.text("Exception when running query")
                st.exception(ex)


def display(pipeline_name: str) -> None:
    import dlt

    pipeline = dlt.attach(pipeline_name)

    pages: Dict[str, AnyFun] = {
        "Explore data": write_data_explorer_page,
        "Load info": write_load_status_page,
    }

    st.title(f"Show {pipeline_name} pipeline")

    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", list(pages.keys()))
    page = pages[selection]

    with st.spinner("Loading Page ..."):
        page(pipeline)


if __name__ == "__main__":
    display(sys.argv[1])
