import os
import traceback
import typing as t

from hyperscript import h
from IPython.core.display import HTML, display
from IPython.core.magic import Magics, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from dlt.cli import echo, echo as fmt
from dlt.common.runtime.exec_info import is_ipython

DEBUG_FLAG = False

DLT_INIT_DOCS_URL = "https://dlthub.com/docs/reference/command-line-interface#dlt-init"
DEFAULT_VERIFIED_SOURCES_REPO = "https://github.com/dlt-hub/verified-sources.git"
DLT_DEPLOY_DOCS_URL = "https://dlthub.com/docs/walkthroughs/deploy-a-pipeline"


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
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            # Assume Databricks' 'display' is a callable with an unknown signature
            databricks_display = self.shell.user_ns.get("display")
            if callable(databricks_display):
                return databricks_display
            else:
                raise RuntimeError(
                    "Expected a callable for Databricks' display, got something else."
                )
        return display  # Assuming 'display' is a predefined callable

    def success_message(self, messages: t.Dict[str, str]) -> HTML:
        unstyled = messages.get("unstyled")
        msg = str(
            h(
                "div",
                h(
                    "span",
                    messages.get("green-bold"),
                    {"style": {"color": "green", "font-weight": "bold"}},
                ),
                h("span", unstyled) if unstyled else "",
            )
        )
        return HTML(msg)

    def on_exception(self, ex: str, info: str) -> t.Any:
        msg = str(
            h(
                "div",
                h(
                    "span",
                    str(ex),
                    {"style": {"color": "red", "font-weight": "bold"}},
                ),
                h(
                    "span",
                    " Please refer to %s for further assistance" % fmt.bold(info),
                ),
            )
        )
        out = display(HTML(msg))

        if DEBUG_FLAG:
            # Display the full traceback in a preformatted style
            traceback_html = h(
                "pre", traceback.format_exc(), {"style": {"color": "gray"}}
            )
            display(HTML(str(traceback_html)))
            raise ex

        return out

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
    @argument(
        "--debug", action="store_true", help="Displays full stack traces on exceptions."
    )
    @line_magic
    def settings(self, line: str) -> int:
        """
        A dlt line magic command to set global settings like telemetry, debug mode, etc.
        """
        args = parse_argstring(self.settings, line)
        global DEBUG_FLAG
        try:
            from dlt.cli._dlt import telemetry_change_status_command_wrapper

            if args.enable_telemetry:
                telemetry_change_status_command_wrapper(True)
            if args.disable_telemetry:
                telemetry_change_status_command_wrapper(False)
            if args.debug:
                DEBUG_FLAG = True
            return 0
        except:
            return -1

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
    @argument(
        "--branch", type=str, default=None, help="Use a default branch for the source."
    )
    @line_magic
    def init(self, line: str) -> t.Any:
        """
        A dlt line magic command for initializing a DLT project.
        """
        args = parse_argstring(self.init, line)
        try:
            from dlt.cli._dlt import init_command_wrapper

            with echo.always_choose(False, always_choose_value=True):
                out = init_command_wrapper(
                    source_name=args.source_name,
                    destination_type=args.destination_name,
                    use_generic_template=args.use_generic_template,
                    repo_location=args.repo_location
                    if args.repo_location is not None
                    else DEFAULT_VERIFIED_SOURCES_REPO,
                    branch=args.branch if args.branch is not None else None,
                )
                if out == -1:
                    self.display(
                        self.on_exception(
                            "Failure due to...",
                            "Default value for init is 'No' for safety reasons.",
                        )
                    )
                    return -1
                else:
                    self.display(
                        self.success_message(
                            {"green-bold": "DLT project initialized successfully."}
                        )
                    )
                    return 0
        except Exception as ex:
            self.on_exception(str(ex), DLT_INIT_DOCS_URL)
            return -1

    @magic_arguments()
    @argument(
        "--operation",
        type=str,
        default=None,
        help="Operation to perform on the pipeline",
    )
    @argument("--pipeline_name", type=str, default=None, help="Name of the pipeline")
    @argument(
        "--pipelines_dir", type=str, default=None, help="Directory of the pipeline"
    )
    @argument("--verbosity", type=int, default=0, help="Verbosity level")
    @line_magic
    def pipeline(self, line: str) -> t.Any:
        """
        A dlt line magic command for pipeline operations.
        """
        from dlt.cli._dlt import DLT_PIPELINE_COMMAND_DOCS_URL, pipeline_command_wrapper

        args = parse_argstring(self.pipeline, line)
        if args.operation == "list-pipelines":
            args.operation = "list"
        try:
            with echo.always_choose(False, always_choose_value=True):
                pipeline_command_wrapper(
                    operation=args.operation,
                    pipeline_name=args.pipeline_name,
                    pipelines_dir=args.pipelines_dir,
                    verbosity=args.verbosity,
                )
                return 0
        except Exception as ex:
            self.on_exception(str(ex), DLT_PIPELINE_COMMAND_DOCS_URL)
            return -2

    @magic_arguments()
    @argument("--file_path", type=str, help="Schema file name, in yaml or json format")
    @argument("--format", type=str, help="File format")
    @argument("--remove_defaults", type=str, help="Remove defaults")
    @line_magic
    def schema(self, line: str) -> t.Any:
        """
        A dlt line magic command for handling schemas.
        """
        args = parse_argstring(self.schema, line)
        try:
            from dlt.cli._dlt import schema_command_wrapper

            with echo.always_choose(False, always_choose_value=True):
                schema_command_wrapper(
                    file_path=args.file_path,
                    format_=args.format,
                    remove_defaults=args.remove_defaults,
                )
                self.display(
                    self.success_message(
                        {"green-bold": "DLT schema magic ran successfully."}
                    )
                )
                return 0
        except Exception as ex:
            self.on_exception(str(ex), "Schema Documentation URL")
            return -1

    @line_magic
    def dlt_version(self, line) -> t.Any:
        """
        A dlt line magic command to display version information.
        """
        from dlt.version import __version__
        try:
            self.display(
                self.success_message(
                    {"green-bold": f"{self.__class__.__name__} version: {__version__}"}
                )
            )
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
