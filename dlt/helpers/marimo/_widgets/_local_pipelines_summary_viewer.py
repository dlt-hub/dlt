import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")

with app.setup:
    import pathlib
    import datetime
    import functools
    from typing import Optional, Union

    import marimo as mo

    from dlt.common.json import json
    from dlt.common.storages import FileStorage
    from dlt.common.pipeline import get_dlt_pipelines_dir


@app.function
def get_pipeline_options(pipelines_dir: str):
    _storage = FileStorage(pipelines_dir)

    try:
        _pipelines = _storage.list_folder_dirs(".", to_root=False)
    except Exception:
        _pipelines = []

    return {p: _storage.storage_path + "/" + p for p in sorted(_pipelines)}


@app.cell
def _():
    pipelines_dir_textbox = mo.ui.text(
        value=get_dlt_pipelines_dir(), full_width=True, label="Pipelines directory"
    )
    pipelines_dir_textbox
    return (pipelines_dir_textbox,)


@app.cell
def _(pipelines_dir_textbox):
    pipelines_options = {}
    _msg = None

    _pipelines_dir = pathlib.Path(pipelines_dir_textbox.value)
    if _pipelines_dir.exists() and _pipelines_dir.is_dir():
        pipelines_options = get_pipeline_options(_pipelines_dir)
    else:
        _msg = mo.md(f"No directory found at `{_pipelines_dir.resolve()}`")

    _msg
    return (pipelines_options,)


@app.function
def open_directory(*args, path: Union[str, pathlib.Path]) -> None:
    import platform
    import os
    import subprocess

    path = pathlib.Path(path).resolve()
    if not path.is_dir():
        raise NotADirectoryError(f"`{path}` is not a valid directory.")

    system = platform.system()

    try:
        if system == "Windows":
            os.startfile(path)
        elif system == "Darwin":  # macOS
            subprocess.run(["open", path])
        elif system == "Linux":
            subprocess.run(["xdg-open", path])
        else:
            raise NotImplementedError(f"Unsupported OS: `{system}`")

    except Exception:
        raise


@app.function
def pipeline_state(pipeline_path: str) -> dict:
    return json.typed_loads(pathlib.Path(pipeline_path, "state.json").read_text())


@app.function
def last_extracted_date(pipeline_state: dict) -> Optional:
    return pipeline_state["_local"].get("_last_extracted_at")


@app.function
def destination_type(pipeline_state: dict) -> Optional[str]:
    return (
        pipeline_state.get("destination_type").rpartition(".")[2]
        if pipeline_state.get("destination_type")
        else None
    )


@app.function
def destination_name(pipeline_state: dict) -> Optional[str]:
    return (
        pipeline_state.get("destination_name").rpartition(".")[2]
        if pipeline_state.get("destination_name")
        else None
    )


@app.function
def last_run_directory(pipeline_state: dict) -> Optional[str]:
    if pipeline_state.get("_local").get("last_run_context"):
        return pipeline_state["_local"]["last_run_context"].get("run_dir")

    return None


@app.function
def open_pipeline_dir_btn(full_path: str) -> mo.ui.button:
    return mo.ui.button(
        on_click=functools.partial(open_directory, path=full_path),
        label="open",
    )


@app.function
def open_last_run_dir_btn(last_run_dir: Optional[str]) -> Optional[mo.ui.button]:
    if last_run_dir is None:
        return None

    return mo.ui.button(
        on_click=functools.partial(open_directory, path=last_run_dir),
        label=last_run_dir,
    )


@app.cell
def _(pipelines_options):
    data = []
    for _pipeline_name, _full_p in pipelines_options.items():
        _p_stats = pathlib.Path(_full_p).stat()
        _p_state = pipeline_state(_full_p)

        _d = {
            "name": _pipeline_name,
            " ": open_pipeline_dir_btn(_full_p),
            "created_at": datetime.datetime.fromtimestamp(_p_stats.st_mtime),
            "dataset": _p_state["dataset_name"],
            "destination": destination_type(_p_state),
            "last_extracted_at": last_extracted_date(_p_state),
            "last_run_dir": open_last_run_dir_btn(last_run_directory(_p_state)),
        }

        data.append(_d)

    data = sorted(data, key=lambda d: d["created_at"], reverse=True)
    return (data,)


@app.cell
def _(data):
    _FORMATTING = (
        {
            "created_at": lambda r: r.strftime("%Y-%m-%d %H:%M:%S"),
            "last_extracted_at": lambda r: (
                r.strftime("%Y-%m-%d %H:%M:%S") if r is not None else None
            ),
        },
    )

    mo.ui.table(
        data,
        label="Local pipelines",
        freeze_columns_left=["name", " "],
        selection=None,
        format_mapping=_FORMATTING,
    )
    return


if __name__ == "__main__":
    app.run()
