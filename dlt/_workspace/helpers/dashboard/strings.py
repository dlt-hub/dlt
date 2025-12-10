#
# App general
#

# Reusable string parts

_help_url = "https://dlthub.com/docs/general-usage/dataset-access/dashboard"
_credentials_help_url = _help_url + "#credentials"
_sync_help_url = "https://dlthub.com/docs/reference/command-line-interface#dlt-pipeline-sync"


_credentials_info = (
    "Have you run your pipeline, and are your credentials available to the dltHub pipeline"
    f" dashboard? Learn more in the [Dashboard Credentials Docs]({_credentials_help_url})."
)


#
# App general
#
app_section_name = "workspace_home"
app_title = """
# Welcome to the dltHub workspace dashboard...
"""
app_intro = """
<p align="center">...the hackable data platform for `dlt` developers.</p>
"""
app_pipeline_select_label = "Pipeline:"
app_no_pipeline_selected = "No pipeline selected"
app_schema_select_label = "Schema:"

app_pipeline_not_found = f"""
## Pipeline not found

You requested to view a pipeline named `{{}}` but it does not exist in the pipelines directory at `{{}}`. To fix this, you can do one of the following:

1. Select a different pipeline in the dropdown above.
2. Run a pipeline with this name on this machine, then click the refresh button.
3. Ensure you have set the correct pipelines directory (using the `pipelines_dir` CLI argument).
4. Restore a pipeline with this name from a destination using [`dlt pipeline sync`]({_sync_help_url})

This page will automatically refresh with your pipeline data once you have run a pipeline with this name on this machine.

"""


#
# Home section
#
home_section_name = "home_section"
home_quick_start_title = """
### Quick start: select one of your recently used pipelines:

{}

### Or select from all available pipelines:
"""

home_basics_text = f"""
## dltHub workspace dashboard basics

We found `{{}}` pipelines in the local directory `{{}}`. When you select a pipeline to inspect, you can:

* See an overview of your pipeline
* See the current pipeline schema and incremental state
* Browse the data in the pipeline's dataset (requires credentials to be available to the dltHub workspace dashboard)
* View the pipeline state locally and on the destination
* Browse information about past loads and traces

To inspect data in the destination dataset, ensure your destination credentials are available to the dltHub workspace dashboard. Either provide them as environment variables, or start the dltHub workspace dashboard from the directory that contains your `.dlt` folder, where the credentials are stored.

If the dltHub workspace dashboard cannot connect to the destination, you will receive a warning and will only be able to browse the locally stored information about the pipeline.

## dltHub workspace dashboard CLI commands

* `dlt pipeline <pipeline_name> show` - Start the workspace dashboard for the selected pipeline
* `dlt pipeline <pipeline_name> show --edit` - Start a local copy of the workspace dashboard for the selected pipeline in edit mode

## Learn more

* [dlt dashboard docs]({_help_url}) - Dashboard docs
* [dlt pipeline sync]({_sync_help_url}) command - Learn how to restore a pipeline locally to be able to see it in the dashboard
* [Marimo docs](https://docs.marimo.io/) - Learn more about Marimo, the framework that powers the dltHub workspace dashboard

<small>
2025 [dltHub](https://dlthub.com)
</small>

"""
app_title_pipeline = """
## Pipeline `{}`
"""

view_load_packages_text = "Status of load packages from last execution"


#
# Overview section
#
overview_section_name = "overview_section"
overview_title = "Pipeline Info"
overview_subtitle = "Basic properties of the selected pipeline"
overview_remote_state_title = "Remote state"
overview_remote_state_subtitle = (
    "The remote state and schemas of the pipeline as discovered on the destination"
)
overview_remote_state_button = "Load and show remote state"

#
# Schema page
#
schema_section_name = "schema_section"
schema_title = "Dataset Browser: Schema"
schema_subtitle = "Browse the default schema of the selected pipeline"
schema_subtitle_long = (
    "Browse the selected schema of the current pipeline. The following list shows all tables "
    "found in the dlt schema of the current pipeline. In some cases, the dlt schema may differ "
    "from the actual schema materialized in the destination."
)

schema_no_default_available_text = f"No schemas found for this pipeline. {_credentials_info}"
schema_table_details_title = "Table Details for Selected Tables"
schema_table_columns_title = "`{}` Columns"
schema_raw_yaml_title = "Raw Schema as YAML"
schema_show_raw_yaml_text = "Show raw schema as YAML"

# Schema UI controls
ui_show_dlt_tables = "Show internal tables"
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
browse_data_section_name = "browse_data_section"
browse_data_title = "Dataset Browser: Data and Source/Resource State"
browse_data_subtitle = "Browse data from the current pipeline."
browse_data_subtitle_long = (
    "Browse data from the current pipeline. Select a table from the list to start, or write an SQL"
    " query in the text area below. Clicking the row counts button will load the row counts for all"
    " tables from the destination. To reload the row counts, click the button again."
    " The resource state of the currently selected table will also be displayed if it is available."
)

browse_data_error_text = "Dashboard is not able to read data from this destination. "

browse_data_explorer_title = """
<small>Select a table above or write an SQL query in the text area below to explore the data in the destination. The query will be executed on the destination and the results will be displayed in a table. All queries are cached. Please clear the cache if you need to refresh the results for a query.</small>
"""

browse_data_query_result_title = "Query Result"

browse_data_query_history_title = "Query History"
browse_data_query_history_subtitle = (
    "The following list shows all queries that have been executed on the destination and are"
    " present in the cache. Select one or more to see the results again below and compare."
)

browse_data_query_error = "Error executing SQL query. "

browse_data_query_hint = """SELECT
  *
FROM dataset.table
LIMIT 1000"""

browse_data_run_query_button = "Run Query"
browse_data_run_query_tooltip = "Run the query in the editor"
browse_data_loading_spinner_text = "Loading data from destination..."

#
# State page
#
state_section_name = "state_section"
state_title = "Pipeline State"
state_subtitle = "A raw view of the currently stored pipeline state."

#
# Data quality page
#
data_quality_section_name = "data_quality_section"
data_quality_title = "Data Quality"
data_quality_subtitle = "View the results of your data quality checks"

#
# Last trace page
#
trace_section_name = "trace_section"
trace_title = "Last Pipeline Run Trace"
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
    "run times, and information about load jobs."
)
trace_step_details_title = "{} Details"
trace_resolved_config_title = "Resolved Config Values"
trace_resolved_config_subtitle = (
    "A list of config values that were resolved for this pipeline run. You can use "
    "this to find errors in your configuration, such as config values that you set "
    "up but that were not used. The values of the resolved configs are not displayed "
    "for security reasons."
)
trace_raw_trace_title = "Raw Trace"

#
# Loads page
#
loads_section_name = "loads_section"
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
loads_loading_failed_text = "Failed to load loads from destination. "
no_loads_found_text = (
    "No loads found. Either this pipeline was never run or there was a problem connecting to the"
    " destination."
)
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
    "The full schema that was the result of this load. This schema version <strong>{}</strong> the"
    " current default schema."
)
loads_details_loading_spinner_text = "Loading row counts and schema..."

loads_details_error_text = "Error loading load details. "

#
# Ibis backend page
#
ibis_backend_section_name = "ibis_backend_section"
ibis_backend_title = "Ibis Backend"
ibis_backend_subtitle = (
    "Connect to the Ibis backend for the selected pipeline. This will make "
    "the destination available in the Marimo datasources panel."
)
ibis_backend_connected_text = (
    "The Ibis backend connected successfully. If you are in Marimo edit mode, you can now see "
    "the connected database in the datasources panel."
)
ibis_backend_error_text = "Error connecting to Ibis backend. "
ibis_backend_connecting_spinner_text = "Connecting to Ibis backend..."
