from dlt.pipeline import Pipeline
from dlt.common.exceptions import MissingDependencyException
from dlt.helpers.pandas import query_results_to_df, pd

try:
    import streamlit as st
    # from streamlit import SECRETS_FILE_LOC, secrets
except ImportError:
    raise MissingDependencyException("DLT Streamlit Helpers", ["streamlit"], "DLT Helpers for Streamlit should be run within a streamlit app.")


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


# def write_load_status_page(pipeline: Pipeline) -> None:



def write_data_explorer_page(pipeline: Pipeline, schema_name: str = None, show_dlt_tables: bool = False, example_query: str = "", show_charts: bool = True) -> None:
    """Writes Streamlit app page with a schema and live data preview.

    Args:
        pipeline (Pipeline): Pipeline instance to use.
        schema_name (str, optional): Name of the schema to display. If None, default schema is used.
        show_dlt_tables (bool, optional): Should show DLT internal tables. Defaults to False.
        example_query (str, optional): Example query to be displayed in the SQL Query box.
        show_charts (bool, optional): Should automatically show charts for the queries from SQL Query box. Defaults to True.

    Raises:
        MissingDependencyException: Raised when a particular python dependency is not installed
    """
    @st.experimental_memo(ttl=600)
    def run_query(query: str) -> pd.DataFrame:
        # dlt pipeline exposes configured sql client that (among others) let's you make queries against the warehouse
        with pipeline.sql_client(schema_name) as client:
            df = query_results_to_df(client, query)
            return df

    if schema_name:
        schema = pipeline.schemas[schema_name]
    else:
        schema = pipeline.default_schema
    st.title(f"Available tables in {schema.name} schema")
    # st.text(schema.to_pretty_yaml())

    for table in schema.all_tables(with_dlt_tables=show_dlt_tables):
        table_name = table["name"]
        st.header(table_name)
        if "description" in table:
            st.text(table["description"])
        if "parent" in table:
            st.text("Parent table: " + table["parent"])

        # table schema contains various hints (like clustering or partition options) that we do not want to show in basic view
        essentials_f = lambda c: {k:v for k, v in c.items() if k in ["name", "data_type", "nullable"]}

        st.table(map(essentials_f, table["columns"].values()))
        # add a button that when pressed will show the full content of a table
        if st.button("SHOW DATA", key=table_name):
            st.text(f"Full {table_name} table content")
            st.dataframe(run_query(f"SELECT * FROM {table_name}"))

    st.title("Run your query")
    sql_query = st.text_area("Enter your SQL query", value=example_query)
    if st.button("Run Query"):
        if sql_query:
            st.text("Results of a query")
            try:
                # run the query from the text area
                df = run_query(sql_query)
                # and display the results
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
                        except ImportError:
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
