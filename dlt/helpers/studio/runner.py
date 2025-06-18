import subprocess
import os
import pkg_resources

from dlt.common.exceptions import MissingDependencyException

# keep this, will raise if user tries to run studio without dependencies
try:
    import marimo
    import pandas
    import ibis
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt marimo app",
        ["marimo", "pandas", "ibis-framework"],
        "the dlt marimo app requires additional dependencies, such as marimo, pandas and"
        " ibis-framework.",
    )

EJECTED_APP_FILE_NAME = "dlt_app.py"
STYLE_FILE_NAME = "dlt_app_styles.css"


def run_studio(pipeline_name: str = None, edit: bool = False) -> None:
    from dlt.helpers.studio import dlt_app

    ejected_app_path = os.path.join(os.getcwd(), EJECTED_APP_FILE_NAME)
    ejected_css_path = os.path.join(os.getcwd(), STYLE_FILE_NAME)
    ejected_app_exists = os.path.exists(ejected_app_path)

    # when editing, eject the app with styles to the cwd if not present already
    if edit and not ejected_app_exists:
        with open(dlt_app.__file__, "r", encoding="utf-8") as f:
            app_code = f.read()
            with open(ejected_app_path, "w", encoding="utf-8") as f:
                f.write(app_code)
        css_file_path = pkg_resources.resource_filename("dlt", f"helpers/studio/{STYLE_FILE_NAME}")
        with open(css_file_path, "r", encoding="utf-8") as f:
            css_content = f.read()
            with open(os.path.join(os.getcwd(), ejected_css_path), "w", encoding="utf-8") as f:
                f.write(css_content)

    if edit:
        studio_cmd = ["marimo", "edit", ejected_app_path]
    # run will open ejected app if present, else default app
    else:
        studio_cmd = [
            "marimo",
            "run",
            dlt_app.__file__ if not ejected_app_exists else ejected_app_path,
        ]

    if pipeline_name:
        studio_cmd.append("--")
        studio_cmd.append("--pipeline")
        studio_cmd.append(pipeline_name)

    subprocess.run(studio_cmd)
