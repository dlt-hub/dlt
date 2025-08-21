import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    import dlt
    from dlt.common.storages import FileStorage
    from dlt.common.pipeline import get_dlt_pipelines_dir


@app.cell
def _():
    _storage = FileStorage(get_dlt_pipelines_dir())

    try:
        _pipelines = _storage.list_folder_dirs(".", to_root=False)
    except Exception:
        _pipelines = []

    _pipelines = sorted(_pipelines)

    select_pipeline = mo.ui.dropdown(
        _pipelines,
        value=_pipelines[0],
        label="Pipeline",
    )
    select_pipeline
    return (select_pipeline,)


@app.function
def pipeline_details(pipeline: dlt.Pipeline) -> list[dict]:
    """
    Get the details of a pipeline.
    """
    try:
        credentials = str(pipeline.dataset().destination_client.config.credentials)
    except Exception:
        credentials = "Could not resolve credentials"

    details_dict = {
        "Pipeline name": pipeline.pipeline_name,
        "Destination": (
            pipeline.destination.destination_description
            if pipeline.destination
            else "No destination set"
        ),
        "Credentials": credentials,
        "Dataset name": pipeline.dataset_name,
        "Schema name": (
            pipeline.default_schema_name
            if pipeline.default_schema_name
            else "No default schema set"
        ),
        "Pipeline working dir": pipeline.working_dir,
    }

    return [{"Key": k, "Value": v} for k, v in details_dict.items()]


@app.cell
def _():
    from typing import Any, cast
    from itertools import chain

    from dlt.common.utils import without_none

    def _align_dict_keys(items: list[dict]) -> list[dict]:
        """
        Makes sure all dicts have the same keys, sets "-" as default. Makes for nicer rendering in marimo table
        """
        items = cast(list[dict], [without_none(i) for i in items])
        all_keys = set(chain.from_iterable(i.keys() for i in items))
        for i in items:
            i.update({key: "-" for key in all_keys if key not in i})
        return items

    def style_cell(row_id: str, name: str, __: Any) -> dict[str, str]:
        """
        Style a cell in a table.

        Args:
            row_id (str): The id of the row.
            name (str): The name of the column.
            __ (Any): The value of the cell.

        Returns:
            Dict[str, str]: The css style of the cell.
        """
        style = {}  # "background-color": "white" if (int(row_id) % 2 == 0) else "#f4f4f9"}
        if name.lower() == "name":
            style["font-weight"] = "bold"
        return style

    @mo.cache
    def create_table_list(
        pipeline: dlt.Pipeline,
        show_internals: bool = False,
        show_child_tables: bool = True,
    ) -> list[dict[str, str]]:
        """Create a list of tables for the pipeline.

        Args:
            pipeline_name (str): The name of the pipeline to create the table list for.
        """
        FILL_VALUE = "-"

        # get tables and filter as needed
        tables = list(pipeline.default_schema.tables.values())
        if not show_child_tables:
            tables = [t for t in tables if t.get("parent") is None]

        table_list = []
        keys = ("name", "parent", "resource", "write_disposition", "description")
        for table in tables:
            info = {key.capitalize().replace("_", " "): table.get(key, FILL_VALUE) for key in keys}
            table_list.append(info)

        table_list.sort(key=lambda x: str(x["Name"]))
        if not show_internals:
            table_list = [t for t in table_list if not str(t["Name"]).lower().startswith("_dlt")]

        return _align_dict_keys(table_list)

    return create_table_list, style_cell


@app.cell
def _(dlt_schema_table_list, pipeline):
    schema_dict = pipeline.default_schema.to_dict().copy()
    schema_dict["tables"] = {
        col_name: col
        for col_name, col in schema_dict["tables"].items()
        if col_name in [table["Name"].lower() for table in dlt_schema_table_list.value]
    }

    dlt.Schema.from_dict(schema_dict)
    return


@app.cell
def _(create_table_list, pipeline, style_cell):
    table_list = create_table_list(
        pipeline,
        show_internals=True,
        show_child_tables=True,
    )

    dlt_schema_table_list = mo.ui.table(
        table_list,  # type: ignore[arg-type]
        style_cell=style_cell,
        initial_selection=list(range(0, len(table_list))),
    )
    dlt_schema_table_list
    return (dlt_schema_table_list,)


@app.cell
def _(select_pipeline):
    pipeline = dlt.attach(select_pipeline.value)
    return (pipeline,)


@app.cell
def _(pipeline):
    raw_schema = mo.lazy(
        mo.json(pipeline.default_schema.to_dict(), label=pipeline.default_schema_name)
    )

    raw_schema_yaml = mo.lazy(
        mo.ui.code_editor(
            pipeline.default_schema.to_pretty_yaml(),
            language="yaml",
        )
    )

    raw_schema_json = mo.lazy(
        mo.ui.code_editor(
            pipeline.default_schema.to_pretty_json(),
            language="json",
        )
    )

    raw_schema_dbml = mo.lazy(
        mo.ui.code_editor(
            value=pipeline.default_schema.to_dbml(),
        )
    )

    raw_schema_dot = mo.lazy(
        mo.ui.code_editor(
            value=pipeline.default_schema.to_dot(),
        )
    )

    raw_schema_format_tabs = mo.ui.tabs(
        {
            "dict": raw_schema,
            "JSON": raw_schema_json,
            "YAML": raw_schema_yaml,
            "DBML": raw_schema_dbml,
            "DOT": raw_schema_dot,
        },
    )
    raw_schema_format_tabs
    return


if __name__ == "__main__":
    app.run()
