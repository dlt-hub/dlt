---
sidebar_label: streamlit_helper
title: helpers.streamlit_helper
---

#### write\_load\_status\_page

```python
def write_load_status_page(pipeline: Pipeline) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/helpers/streamlit_helper.py#L96)

Display pipeline loading information. Will be moved to dlt package once tested

#### write\_data\_explorer\_page

```python
def write_data_explorer_page(pipeline: Pipeline,
                             schema_name: str = None,
                             show_dlt_tables: bool = False,
                             example_query: str = "",
                             show_charts: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/helpers/streamlit_helper.py#L205)

Writes Streamlit app page with a schema and live data preview.

### Args:
pipeline (Pipeline): Pipeline instance to use.
schema_name (str, optional): Name of the schema to display. If None, default schema is used.
show_dlt_tables (bool, optional): Should show DLT internal tables. Defaults to False.
example_query (str, optional): Example query to be displayed in the SQL Query box.
show_charts (bool, optional): Should automatically show charts for the queries from SQL Query box. Defaults to True.

**Raises**:

- `MissingDependencyException` - Raised when a particular python dependency is not installed

