#
# App general
#
import marimo as _mo

app_tab_overview = "Overview"
app_tab_schema = "Schema"
app_tab_browse_data = "Browse Data"
app_tab_state = "State"
app_tab_last_trace = "Last Trace"
app_tab_ibis_browser = "Ibis-Browser"
app_tab_loads = "Loads"
app_tab_mapping = {
    app_tab_overview: f"{_mo.icon('lucide:home')} {app_tab_overview}",
    app_tab_schema: f"{_mo.icon('lucide:table-properties')} {app_tab_schema}",
    app_tab_browse_data: f"{_mo.icon('lucide:database')} {app_tab_browse_data}",
    app_tab_state: f"{_mo.icon('lucide:file-chart-column')} {app_tab_state}",
    app_tab_last_trace: f"{_mo.icon('lucide:file-chart-column')} {app_tab_last_trace}",
    app_tab_loads: f"{_mo.icon('lucide:file-chart-column')} {app_tab_loads}",
    app_tab_ibis_browser: f"{_mo.icon('lucide:view')} {app_tab_ibis_browser}",
}
app_tab_mapping_reverse = {v: k for k, v in app_tab_mapping.items()}

app_title = """
# Welcome to dltHub Studio...
"""
app_intro = """
<p align="center">...the hackable data platform for `dlt` developers. Learn how to modify this app and create your personal platform at <a href="https://dlthub.com/docs/studio/overview">dlthub.com</a>.</p>
"""

#
# Welcome page
#
app_quick_start_title = """
### Quick start - Select one of your recently used pipelines:

{}

### Or one of all available pipelines:
"""
app_basics_text = """
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
app_description = (
    "The hackable data platform for `dlt` developers. Learn how to modify this app and"
    " create your personal platform at"
    " [dlthub.com](https://dlthub.com/docs/studio/overview)."
)
app_title_pipeline = "Pipeline `{}`"

# studio
pipeline_select_label = "Pipeline:"
no_pipeline_selected = "No pipeline selected"

#
# Pipeline overview page
#
pipeline_sync_status = "## Pipeline sync status"
pipeline_sync_error_text = (
    "Error syncing pipeline from destination. Make sure the credentials are in scope of"
    " `dltHub studio`. Switching to local mode."
)
pipeline_details = "## Pipeline details"
pipeline_sync_status = """
## Pipeline sync status
<small>Dlt studio will try to sync the pipeline state and schema from the destination. If the sync fails, studio use local mode and inspect the locally stored information about the pipeline.</small>
"""
pipeline_sync_success_text = "Pipeline state synced successfully from `{}`."

#
# Schema page
#
schema_raw_title = "## Raw schema"
schema_table_overview = """
    ## Table overview for default schema `{}` version `{}`
    <small>The following list shows all the tables found in the dlt schema of the selected pipeline. Please note that in some cases the dlt schema can differ from the actual schema materialized in the destination.
    <strong>Select one or more tables to see their full schema.</strong></small>
    """

#
# Browse data page
#
browse_data_title = """## Browse data of schema `{}` in dataset `{}`
<small>Browse data of the current pipeline. Select a table from the list to start or write a sql query in the text area below. Toggeling row counts will load the row counts for all tables from the destination. To reload, toggle the switch again.</small>
"""

browse_data_error = (
    "Error connecting to destination. Has your pipeline been run and are your credentials"
    " in scope of `dltHub studio`?"
)

browse_data_explorer_title = """
<small>Select a table above or write a sql query in the text area below to explore the data in the destination. The query will be executed on the destination and the results will be displayed in a table. If you disable query caching, all cached queries will be purged.</small>
"""

browse_data_query_result_title = """
## Last successful query result
<small>`Query: {}`</small>
"""

browse_data_query_history_title = """
## Cached Queries history
<small>The following list shows all the queries that have been executed on the destination and are present in the cache. Select one or more to see the result again below and compare.</small>
"""

browse_data_query_error = """Error executing sql query:"""

#
# State page
#
state_raw_title = """
## Raw state
<small>A raw view of the currently stored pipeline state.</small>
"""

last_trace_no_trace = """
No local trace available for the selected pipeline.
"""

#
# Last trace page
#
last_trace_title = """
## Last trace
<small>A raw view of the last load trace of the selected pipeline if available.</small>
"""

#
# Loads page
#
loads_title = """
## Loads
<small>A list of all the loads that have been executed on the destination dataset of the selected pipeline. Select one to see all available details. More data will be loaded from the destination, such as the row count for that load and the schema for this load. Depending on the destination and your datat this might take a long time.</small>
"""

loads_details_title = """
## Details for load {}
"""

loads_details_row_counts = """
### Row counts for this load
<small>The row counts associated with this load. Will only show tables that have a _dlt_load_id column, which excludes child tables and some of the system tables.</small>
"""

loads_details_schema_version = """
### Schema `{}` version `{}`
<small>The full schema that was the result of this load. This schema and version <strong>{}</strong> the current default schema.</small>
"""

loads_details_error = """Error loading load details"""
#
# Ibis backend page
#
ibis_backend_title = """
## Connect to Ibis Backend
<small>This page will automatically connect to the ibis backend of the selected pipeline. This will make the destination available in the datasources panel. Please note that this is a raw view on all tables and data in the destination and might differ from the tables you see in the dlt schema.</small>
"""
ibis_backend_connected = (
    "Ibis Backend connected successfully. If you are in marimo edit mode, you can now see"
    " the connected database in the datasources panel."
)
ibis_connect_error = (
    "Error connecting to Ibis Backend. Has your pipeline been run and are your credentials"
    " in scope of `dltHub studio`?"
)
