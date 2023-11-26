from argparse import ArgumentParser

from IPython.core.display import HTML, display
from IPython.core.magic import (
    Magics,
    cell_magic,
    line_cell_magic,
    line_magic,
    magics_class, register_line_magic,
)
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring


# from dlt.cli._dlt import telemetry_change_status_command_wrapper, init_command_wrapper, on_exception
# from .deploy_command import COMMAND_DEPLOY_REPO_LOCATION, DLT_DEPLOY_DOCS_URL
from dlt.cli.init_command import init_command
# from .pipeline_command import DLT_PIPELINE_COMMAND_DOCS_URL
# from dlt.cli.init_command import init_command
import typing as t
from hyperscript import h
import os
import traceback

import click

DEBUG_FLAG = False

DLT_INIT_DOCS_URL = "https://dlthub.com/docs/reference/command-line-interface#dlt-init"
DEFAULT_VERIFIED_SOURCES_REPO = "https://github.com/dlt-hub/verified-sources.git"




@magics_class
class DltMagics(Magics):

    @property
    def display(self) -> t.Callable:
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            # Use Databricks' special display instead of the normal IPython display
            return self._shell.user_ns["display"]
        return display

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

    def on_exception(self, ex: Exception, info: str) -> None:
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
                    " Please refer to %s for further assistance" % fmt.bold(info)
                ),
            )
        )
        display(HTML(msg))

        if DEBUG_FLAG:
            # Display the full traceback in a preformatted style
            traceback_html = h("pre", traceback.format_exc(), {"style": {"color": "gray"}})
            display(HTML(str(traceback_html)))
            raise ex


    @magic_arguments()
    @argument('--enable-telemetry', action='store_true', help="Enables telemetry before command is executed")
    @argument('--disable-telemetry', action='store_true', help="Disables telemetry before command is executed")
    @argument('--non-interactive', action='store_true', help="Non interactive mode. Default choices are automatically made for confirmations and prompts.")
    @argument('--debug', action='store_true', help="Displays full stack traces on exceptions.")
    @line_magic
    @register_line_magic
    def settings(self, line):
        """
        A DLT line magic command to set global settings like telemetry, debug mode, etc.
        """
        args = parse_argstring(self.settings, line)
        global DEBUG_FLAG

        if args.enable_telemetry:
            telemetry_change_status_command_wrapper(True)
        if args.disable_telemetry:
            telemetry_change_status_command_wrapper(False)
        if args.non_interactive:
            fmt.ALWAYS_CHOOSE_DEFAULT = True
        if args.debug:
            DEBUG_FLAG = True
        return "Settings updated"
    @magic_arguments()
    @argument('--source', type=str, help="Name of data source for which to create a pipeline.")
    @argument('--destination', type=str, help="Name of a destination, e.g., bigquery or redshift.")
    @argument('--location', type=str, help="Advanced. Uses a specific url or local path to verified sources repository.")
    @argument('--generic', action='store_true', help="Use a generic template with all the dlt loading code present.")
    @argument('--branch', action='store_true', help="Use a default branch for the source.")
    @line_magic
    @register_line_magic
    def init(self, line):
        """
        A DLT line magic command for initializing a DLT project.
        """
        args = parse_argstring(self.init, line)
        try:
            init_command(
                source_name=args.source,
                destination_name=args.destination,
                use_generic_template=args.generic,
                repo_location=args.location if args.location is not None else DEFAULT_VERIFIED_SOURCES_REPO,
                branch=args.branch
            )
            return self.display(self.success_message({"green-bold": "DLT project initialized successfully."}))


        except Exception as ex:
            self.on_exception(ex, DLT_INIT_DOCS_URL)
            return -1
    @magic_arguments()
    @argument('--pipeline_script_path', type=str, help="Path to a pipeline script")
    @argument('--deployment_method', type=str, help="Deployment method")
    @argument('--repo_location', type=str, help="Repository location")
    @argument('--branch', type=str, help="Deployment method")
    @line_magic
    @register_line_magic
    def deploy(self, line):
        """
        A DLT line magic command for deploying a pipeline.
        """
        args = parse_argstring(self.deploy, line)
        try:
            deploy_command_wrapper(
                pipeline_script_path=args.pipeline_script_path,
                deployment_method=args.deployment_method,
                repo_location=args.repo_location if args.repo_location is not None else COMMAND_DEPLOY_REPO_LOCATION,
                branch=args.branch
            )
            return self.display(self.success_message({"green-bold": "DLT project deployed."}))
        except Exception as ex:
            self.on_exception(ex, DLT_DEPLOY_DOCS_URL)
            return -1
    @magic_arguments()
    @argument('--operation', type=str, help="Operation to perform on the pipeline")
    @argument('--pipeline_name', type=str, help="Name of the pipeline")
    @argument('--pipelines_dir', type=str, help="Directory of the pipeline")
    @argument('--verbosity', type=str, help="Verbosity level")
    @line_magic
    @register_line_magic
    def pipeline(self, line):
        """
        A DLT line magic command for pipeline operations.
        """
        args = parse_argstring(self.pipeline, line)
        try:
            pipeline_command_wrapper(
                operation=args.operation,
                pipeline_name=args.pipeline_name,
                pipelines_dir=args.pipeline_dir,
                verbosity=args.verbosity

            )
            return self.display(self.success_message({"green-bold": "DLT pipeline created."}))
        except Exception as ex:
            self.on_exception(ex, DLT_PIPELINE_COMMAND_DOCS_URL)
            return -2
    @magic_arguments()
    @argument('--file_path', type=str, help="Schema file name, in yaml or json format")
    @argument('--format', type=str, help="File format")
    @argument('--remove_defaults', type=str, help="Remove defaults")
    @line_magic
    @register_line_magic
    def schema(self, line):
        """
        A DLT line magic command for handling schemas.
        """
        args = parse_argstring(self.schema, line)
        try:
            schema_command_wrapper(
                file_path=args.file_path,
                format_=args.format,
                remove_defaults=args.remove_defaults
            )
            return self.display(self.success_message({"green-bold": "DLT schema created."}))
        except Exception as ex:
            self.on_exception(ex, "Schema Documentation URL")  # Replace with actual URL
            return -1
    @line_magic
    @register_line_magic
    def dlt_version(self):
        """
        A DLT line magic command to display version information.
        """
        from dlt.version import __version__
        return f"{self.__class__.__name__} version: {__version__}"

    @magic_arguments()
    @argument("--line", "-s", type=str, help="String to reverse.")
    @register_line_magic
    @line_magic
    def reverse(self, line=None):
        """
        A simple IPython line magic command that reverses the input string.
        """
        # parser = ArgumentParser()
        # parser.add_argument("--line", "-s", type=str, help="Start date to render.")
        #
        # # Parse the arguments
        # args = parser.parse_args(line.split())
        return line[::-1]






def register_magics() -> None:
    from dlt.cli import echo as fmt
    fmt.echo("got here")
    try:
        fmt.echo("got here2")
        shell = get_ipython()  # type: ignore
        shell.register_magics(DltMagics)

        line_magics = shell.magics_manager.magics['line']
        fmt.echo("Registered Line Magics:" + str(line_magics))
    except NameError:
        pass



from ..common.runtime.exec_info import is_notebook
from dlt.cli import echo as fmt

def check_notebook_runtime():
    fmt.echo("Checking if notebook")
    if is_notebook():
        try:
            fmt.echo("Registering magics")
            register_magics()
        except ImportError:
            pass