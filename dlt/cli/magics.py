import typing as t

from IPython.core.display import HTML, display
from IPython.core.magic import Magics, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from dlt.cli import echo
from dlt.common.runtime.exec_info import is_databricks, is_ipython

DEFAULT_VERIFIED_SOURCES_REPO = "https://github.com/dlt-hub/verified-sources.git"
DLT_SCHEMA_URL = "https://dlthub.com/docs/general-usage/schema"


def register_notebook_magics() -> None:
    """
    Registers custom IPython magic commands for the notebook environment.

    This function checks if the current environment is an IPython environment (like Jupyter notebooks).
    If it is, it attempts to register custom magic commands. The registration process is wrapped
    in a try-except block to gracefully handle cases where necessary dependencies for the magic
    commands might not be present.

    Raises:
        ImportError: If there is an issue importing the required modules for the magic commands.
                     The exception is silently passed, and the function does not perform any action.
    """
    if is_ipython():
        try:
            register_magics()
        except ImportError:
            pass


@magics_class
class DltMagics(Magics):
    @property
    def display(self) -> t.Any:
        if is_databricks():
            # Assume Databricks' 'display' is a callable with an unknown signature
            databricks_display = self.shell.user_ns.get("display")
            if callable(databricks_display):
                return databricks_display
            else:
                raise RuntimeError(
                    "Expected a callable for Databricks' display, got something else."
                )
        return display  # Assuming 'display' is a predefined callable

    def success_message(self, message: str) -> t.Any:
        msg = f'<div><span style="color: green; font-weight: bold">{message}</span></div>'
        return self.display(HTML(msg))

    def on_exception(self, ex: str, info: str) -> t.Any:
        msg = (
            f'<div><span style="color: red; font-weight: bold">{ex}</span><span style="color:'
            f" green; font-weight: bold>Please refer to {info} for further assistance</span></div>"
        )
        return self.display(HTML(msg))

    @magic_arguments()
    @argument(
        "--enable-telemetry",
        action="store_true",
        help="Enables telemetry before command is executed",
    )
    @argument(
        "--disable-telemetry",
        action="store_true",
        help="Disables telemetry before command is executed",
    )
    @line_magic
    def settings(self, line: str) -> int:
        from dlt.cli._dlt import telemetry_change_status_command_wrapper

        """
        A dlt line magic command to set global settings like telemetry
        """
        args = parse_argstring(self.settings, line)

        if args.enable_telemetry:
            return telemetry_change_status_command_wrapper(True)
        if args.disable_telemetry:
            return telemetry_change_status_command_wrapper(False)
        return 0

    @magic_arguments()
    @argument(
        "--source_name",
        type=str,
        help="Name of data source for which to create a pipeline.",
    )
    @argument(
        "--destination_name",
        type=str,
        help="Name of a destination, e.g., bigquery or redshift.",
    )
    @argument(
        "--repo_location",
        type=str,
        help="Advanced. Uses a specific url or local path to verified sources repository.",
    )
    @argument(
        "--use_generic_template",
        action="store_true",
        help="Use a generic template with all the dlt loading code present.",
    )
    @argument("--branch", type=str, default=None, help="Use a default branch for the source.")
    @line_magic
    def init(self, line: str) -> t.Any:
        """
        A dlt line magic command for initializing a dlt project.
        """
        from dlt.cli._dlt import init_command_wrapper

        args = parse_argstring(self.init, line)

        with echo.always_choose(False, always_choose_value=True):
            return init_command_wrapper(
                source_name=args.source_name,
                destination_type=args.destination_name,
                use_generic_template=args.use_generic_template,
                repo_location=(
                    args.repo_location
                    if args.repo_location is not None
                    else DEFAULT_VERIFIED_SOURCES_REPO
                ),
                branch=args.branch if args.branch is not None else None,
            )

    @magic_arguments()
    @argument(
        "--operation",
        type=str,
        default=None,
        help="Operation to perform on the pipeline",
    )
    @argument("--pipeline_name", type=str, default=None, help="Name of the pipeline")
    @argument("--pipelines_dir", type=str, default=None, help="Directory of the pipeline")
    @argument("--verbosity", type=int, default=0, help="Verbosity level")
    @line_magic
    def pipeline(self, line: str) -> t.Any:
        """
        A dlt line magic command for pipeline operations.
        """
        from dlt.cli._dlt import pipeline_command_wrapper

        args = parse_argstring(self.pipeline, line)

        if args.operation == "list-pipelines":
            args.operation = "list"

        with echo.always_choose(False, always_choose_value=True):
            return pipeline_command_wrapper(
                operation=args.operation,
                pipeline_name=args.pipeline_name,
                pipelines_dir=args.pipelines_dir,
                verbosity=args.verbosity,
            )

    @magic_arguments()
    @argument("--file_path", type=str, help="Schema file name, in yaml or json format")
    @argument("--format", type=str, choices=["json", "yaml"], default="yaml", help="File format")
    @argument("--remove_defaults", type=str, help="Remove defaults")
    @line_magic
    def schema(self, line: str) -> t.Any:
        """
        A dlt line magic command for handling schemas.
        """
        from dlt.cli._dlt import schema_command_wrapper

        args = parse_argstring(self.schema, line)

        try:
            with echo.always_choose(False, always_choose_value=True):
                schema_command_wrapper(
                    file_path=args.file_path,
                    format_=args.format,
                    remove_defaults=args.remove_defaults,
                )
                self.success_message("dlt schema magic ran successfully.")
                return 0
        except Exception as ex:
            self.on_exception(str(ex), DLT_SCHEMA_URL)
            return -1

    @line_magic
    def dlt_version(self, line: str) -> t.Any:
        """
        A dlt line magic command to display version information.
        """
        from dlt.version import __version__

        try:
            self.success_message(f"dlt version: {__version__}")
            return 0
        except Exception as ex:
            self.on_exception(str(ex), "")
            return -1


def register_magics() -> None:
    try:
        shell = get_ipython()  # type: ignore
        shell.register_magics(DltMagics)
    except NameError:
        pass
