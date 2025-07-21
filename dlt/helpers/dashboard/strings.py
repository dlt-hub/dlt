#
# App general
#
import marimo as _mo

# Reusable string parts
_credentials_info = (
    "Have you run your pipeline and are your credentials available to the dltHub pipeline"
    " dashboard?"
)

#
# App general
#
app_title = """
# Welcome to the dltHub pipeline dashboard...
"""
app_intro = """
<p align="center">...the hackable data platform for `dlt` developers.</p>
"""
app_pipeline_select_label = "Pipeline:"
app_no_pipeline_selected = "No pipeline selected"

#
# Home section
#
home_quick_start_title = """
### Quick start - Select one of your recently used pipelines:

{}

### Or select from all available pipelines:
"""

home_basics_text = """
## dltHub pipeline dashboard basics

We have found `{}` pipelines in local directory `{}`. When you select a pipeline to inspect, you will be able to:

* See the current pipeline schema
* See the pipeline state
* Browse information about past loads and traces
* Browse the data in the pipeline's dataset (requires credentials available to the dltHub pipeline dashboard)

To sync the current schema and state from the destination dataset or inspect data in the destination dataset, your destination credentials need to be available to the dltHub pipeline dashboard. Either provide them as environment variables or start the dltHub pipeline dashboard from the directory containing your `.dlt` folder where the credentials are stored.

If the dltHub pipeline dashboard can't connect to the destination, you will receive a warning and can browse the locally stored information about the pipeline.

## dltHub pipeline dashboard CLI commands

* `dlt pipeline <pipeline_name> show --dashboard` - Start the pipeline dashboard for the selected pipeline
* `dlt pipeline <pipeline_name> show --dashboard --edit` - Start a local copy of the pipeline dashboard for the selected pipeline in edit mode

## Learn more

* [marimo docs](https://docs.marimo.io/) - Learn all about marimo, the amazing framework that powers the dltHub pipeline dashboard

<small>
2025 [dltHub](https://dlthub.com)
</small>

"""
app_title_pipeline = "Pipeline `{}`"

#
# Sync status section
#
sync_status_title = "Sync Status"
sync_status_subtitle = "Sync the pipeline state from the destination"
sync_status_subtitle_long = (
    "Sync the pipeline state from the destination. This will update the pipeline state and "
    "schema from the destination."
)
sync_status_success_text = "Pipeline state synced successfully from `{}`."
sync_status_spinner_text = "Syncing pipeline state from destination..."

sync_status_error_text = (
    f"Error syncing pipeline from destination. {_credentials_info} Switching to local mode."
)

#
# Overview section
#
overview_title = "Pipeline Overview"
overview_subtitle = "Overview of the selected pipeline"

#
# Schema page
#
schema_title = "Schema Browser"
schema_subtitle = "Browse the default schema of the selected pipeline"
schema_subtitle_long = (
    "Browse the default schema of the selected pipeline. The following list shows all tables "
    "found in the dlt schema of the selected pipeline. Note that in some cases the dlt "
    "schema may differ from the actual schema materialized in the destination."
)

schema_no_default_available_text = (
    "No default schema available. Does your pipeline have a completed load?"
)
schema_table_details_title = "Table Details for Selected Tables"
schema_table_columns_title = "`{}` columns"
schema_raw_yaml_title = "Raw Schema as YAML"
schema_show_raw_yaml_text = "Show raw schema as YAML"

# Schema UI controls
ui_show_dlt_tables = "Show `_dlt` tables"
ui_show_child_tables = "Show child tables"
ui_load_row_counts = "Load row counts"
ui_show_dlt_columns = "Show `_dlt` columns"
ui_show_type_hints = "Show type hints"
ui_show_other_hints = "Show other hints"
ui_show_custom_hints = "Show custom hints (x-)"
ui_clear_cache = "Clear cache"
ui_limit_to_1000_rows = "Limit to 1000 rows"

#
# Browse data page
#
browse_data_title = "Browse Pipeline Data"
browse_data_subtitle = "Browse data from the current pipeline."
browse_data_subtitle_long = (
    "Browse data from the current pipeline. Select a table from the list to start or write a SQL"
    " query in the text area below. Clicking the row counts button will load the row counts for all"
    " tables from the destination. To reload the row count, just click the button again."
)

browse_data_error_text = f"Error connecting to destination. {_credentials_info}"

browse_data_explorer_title = """
<small>Select a table above or write a SQL query in the text area below to explore the data in the destination. The query will be executed on the destination and the results will be displayed in a table. All queries are cached, please clear the cache if you need to refresh the results for a query.</small>
"""

browse_data_query_result_title = "Query Result"

browse_data_query_history_title = "Query History"
browse_data_query_history_subtitle = (
    "The following list shows all queries that have been executed on the destination and are"
    " present in the cache. Select one or more to see the results again below and compare."
)

browse_data_query_error = "Error executing SQL query:"

browse_data_query_hint = """SELECT \n  * \nFROM dataset.table \nLIMIT 1000"""

browse_data_run_query_button = "Run Query"
browse_data_run_query_tooltip = "Run the query in the editor"
browse_data_loading_spinner_text = "Loading data from destination"

#
# State page
#
state_title = "Pipeline State"
state_subtitle = "A raw view of the currently stored pipeline state."


#
# Last trace page
#
trace_title = "Last Trace"
trace_subtitle = (
    "An overview of the last load trace from the most recent successful run of the selected"
    " pipeline, if available."
)
trace_show_raw_trace_text = "Show"
trace_no_trace_text = """
No local trace available for this pipeline. This probably means that your pipeline has never been run on this computer.
"""

trace_overview_title = "Trace Overview"
trace_execution_context_title = "Execution Context"
trace_execution_context_subtitle = (
    "Information about the environment in which the pipeline was executed."
)
trace_steps_overview_title = "Steps Overview"
trace_steps_overview_subtitle = (
    "Select a step to see execution details, such as rows processed, "
    "running times, and information about load jobs."
)
trace_step_details_title = "{} Details"
trace_resolved_config_title = "Resolved Config Values"
trace_resolved_config_subtitle = (
    "A list of config values that were resolved for this pipeline run. You can use "
    "this to find errors in your configuration, such as config values that you set "
    "up but were not used. The values of the resolved configs are not displayed "
    "for security reasons."
)
trace_raw_trace_title = "Raw Trace"

#
# Loads page
#
loads_title = "Pipeline Loads"
loads_subtitle = (
    "View a list of all loads that have been executed on the destination dataset of the "
    "selected pipeline."
)
loads_subtitle_long = (
    "View a list of all loads that have been executed on the destination dataset of the selected"
    " pipeline. Select one to see all available details. Additional data will be loaded from the"
    " destination, such as the row count for that load and the schema for this load. Depending on"
    " the destination and your data, this might take some time."
)
loads_loading_failed_text = f"Failed to load loads from destination. {_credentials_info}"
loads_loading_spinner_text = "Loading loads from destination..."

# loads details
loads_details_title = "Load Details for Load ID: {}"
loads_details_row_counts_title = "Row Counts"
loads_details_row_counts_subtitle = (
    "The row counts associated with this load. Only shows tables that have a _dlt_load_id column,"
    " which excludes child tables and some system tables."
)
loads_details_schema_version_title = "Schema Details"
loads_details_schema_version_subtitle = (
    "The full schema that was the result of this load. This schema and version <strong>{}</strong>"
    " the current default schema."
)
loads_details_loading_spinner_text = "Loading row counts and schema..."

loads_details_error_text = "Error loading load details"

#
# Ibis backend page
#
ibis_backend_title = "Ibis Backend"
ibis_backend_subtitle = (
    "Select to automatically connect to the Ibis backend of the selected pipeline. This will make "
    "the destination available in the marimo datasources panel."
)
ibis_backend_connected_text = (
    "Ibis Backend connected successfully. If you are in marimo edit mode, you can now see "
    "the connected database in the datasources panel."
)
ibis_backend_error_text = f"Error connecting to Ibis Backend. {_credentials_info}"
ibis_backend_connecting_spinner_text = "Connecting to Ibis Backend..."
