#
# App general
#
import marimo as _mo

# Reusable string parts
_credentials_info = "Has your pipeline been run and are your credentials in scope of dltHub studio?"

#
# App general
#
app_title = """
# Welcome to dltHub Studio...
"""
app_intro = """
<p align="center">...the hackable data platform for `dlt` developers. Learn how to modify this app and create your personal platform at <a href="https://dlthub.com/docs/studio/overview">dlthub.com</a>.</p>
"""
app_pipeline_select_label = "Pipeline:"
app_no_pipeline_selected = "No pipeline selected"

#
# Home section
#
home_quick_start_title = """
### Quick start - Select one of your recently used pipelines:

{}

### Or one of all available pipelines:
"""

home_basics_text = """
## dltHub Studio basics

`dlt studio` has found `{}` pipelines in local directory `{}`. If you select one of the pipelines to inspect. You will be able to:

* See the current pipeline schema
* See the pipeline state
* Browse information about past loads and traces
* Browse the data in the pipeline's dataset (requires credentials in scope of `dltHub studio`)

If you want to sync the current schema and state from the destination dataset or inspect data in the destination dataset, the credentials for your destination need to be in scope of `dltHub studio`. Either provide them as environment variables or start dltHub studio from the directory with your `.dlt` folder where the credentials are stored.

If dlthub Studio can't connect to the destination, you will receive a warning and you can browse the locally stored information about the pipeline.

## dltHub Studio CLI commands

* `dlt studio` - Start the studio, this will take you to this place
* `dlt studio -p <dlt_pipeline_name>` - directly jump to the pipeline page on launch
* `dlt studio eject` - Will eject the code for dltHub Studio and allow you to create your own hackable version of the app :)

## Learn more

* [marimo docs](https://docs.marimo.io/) - Learn all about marimo, the amazing framework that powers dltHub Studio
* [dltHub Studio docs](https://dlthub.com/docs/studio/overview) - Learn all about dltHub Studio, the hackable data platform for `dlt` developers

<small>
2025 [dltHub](https://dlthub.com)
</small>

"""
app_title_pipeline = "Pipeline `{}`"

#
# Sync status section
#
sync_status_title = "Sync status"
sync_status_subtitle = "Sync the pipeline state from the destination"
sync_status_subtitle_long = (
    "Sync the pipeline state from the destination. This will update the pipeline state and the"
    " schema in the destination."
)
sync_status_success_text = "Pipeline state synced successfully from `{}`."

sync_status_error_text = (
    f"Error syncing pipeline from destination. {_credentials_info} Switching to local mode."
)

#
# Overview section
#
overview_title = "Pipeline overview"
overview_subtitle = "Overview of the selected pipeline"

#
# Schema page
#
schema_title = "Schema Browser"
schema_subtitle = "Browse the default schema of the selected pipeline"
schema_subtitle_long = (
    "Browse the default schema of the selected pipeline. The following list shows all the tables"
    " found in the dlt schema of the selected pipeline. Please note that in some cases the dlt"
    " schema can differ from the actual schema materialized in the destination."
)
#
# Browse data page
#
browse_data_title = "Browse data of pipeline"
browse_data_subtitle = """Browse data of the current pipeline."""
browse_data_subtitle_long = """Browse data of the current pipeline. Select a table from the list to start or write a SQL query in the text area below. Toggling row counts will load the row counts for all tables from the destination. To reload, toggle the switch again."""

browse_data_error_text = f"Error connecting to destination. {_credentials_info}"

browse_data_explorer_title = """
<small>Select a table above or write a SQL query in the text area below to explore the data in the destination. The query will be executed on the destination and the results will be displayed in a table. If you disable query caching, all cached queries will be purged.</small>
"""

browse_data_query_result_title = """Browse data: Query result"""

browse_data_query_history_title = """Browse data: Cached queries history"""
browse_data_query_history_subtitle = """The following list shows all the queries that have been executed on the destination and are present in the cache. Select one or more to see the result again below and compare."""

browse_data_query_error = """Error executing SQL query:"""

browse_data_query_hint = """SELECT \n  * \nFROM dataset.table \nLIMIT 1000"""

#
# State page
#
state_title = "Pipeline state"
state_subtitle = "A raw view of the currently stored pipeline state."


#
# Last trace page
#
trace_title = "Last trace"
trace_subtitle = (
    "An overview of the last load trace of the last successful run of the selected pipeline if"
    " available."
)
trace_show_raw_trace_text = "Show"
trace_no_trace_text = """
No local trace available for this pipeline. This probably means that your pipeline has never been run on this computer.
"""

#
# Loads page
#
loads_title = "Pipeline Loads"
loads_subtitle = (
    "View a list of all the loads that have been executed on the destination dataset of the"
    " selected pipeline."
)
loads_subtitle_long = (
    "View a list of all the loads that have been executed on the destination dataset of the"
    " selected pipeline. Select one to see all available details. More data will be loaded from the"
    " destination, such as the row count for that load and the schema for this load. Depending on"
    " the destination and your data, this might take a long time."
)
loads_loading_failed_text = f"Loading loads from destination failed. {_credentials_info}"

# loads details
loads_details_row_counts_title = """Row counts"""
loads_details_row_counts_subtitle = """The row counts associated with this load. Will only show tables that have a _dlt_load_id column, which excludes child tables and some of the system tables."""
loads_details_schema_version_title = """Schema details"""
loads_details_schema_version_subtitle = """The full schema that was the result of this load. This schema and version <strong>{}</strong> the current default schema."""

loads_details_error_text = """Error loading load details"""

#
# Ibis backend page
#
ibis_backend_title = "Ibis Backend"
ibis_backend_subtitle = (
    "Select to automatically connect to the ibis backend of the selected pipeline. This will make"
    " the destination available in the marimo datasources panel. </small>"
)
ibis_backend_connected_text = (
    "Ibis Backend connected successfully. If you are in marimo edit mode, you can now see"
    " the connected database in the datasources panel."
)
ibis_backend_error_text = f"Error connecting to Ibis Backend. {_credentials_info}"
