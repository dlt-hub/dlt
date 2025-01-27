---
sidebar_label: dashboard
title: helpers.streamlit_app.pages.dashboard
---

## write\_data\_explorer\_page

```python
def write_data_explorer_page(pipeline: Pipeline,
                             schema_name: str = None,
                             example_query: str = "",
                             show_charts: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/helpers/streamlit_app/pages/dashboard.py#L12)

Writes Streamlit app page with a schema and live data preview.

**Arguments**:

- `pipeline` _Pipeline_ - Pipeline instance to use.
- `schema_name` _str, optional_ - Name of the schema to display. If None, default schema is used.
- `example_query` _str, optional_ - Example query to be displayed in the SQL Query box.
- `show_charts` _bool, optional_ - Should automatically show charts for the queries from SQL Query box. Defaults to True.
  

**Raises**:

- `MissingDependencyException` - Raised when a particular python dependency is not installed

