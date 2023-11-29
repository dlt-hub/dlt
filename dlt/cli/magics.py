from argparse import ArgumentParser

# from dlt.cli._dlt import telemetry_change_status_command_wrapper, init_command_wrapper, on_exception
# from .deploy_command import COMMAND_DEPLOY_REPO_LOCATION, DLT_DEPLOY_DOCS_URL

# from dlt.cli._dlt import init_command_wrapper
# from .pipeline_command import DLT_PIPELINE_COMMAND_DOCS_URL
# from dlt.cli.init_command import init_command

from IPython.core.display import HTML, display
from IPython.core.magic import (
    Magics,
    cell_magic,
    line_cell_magic,
    line_magic,
    magics_class, register_line_magic,
)
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring


from ..common.runtime.exec_info import is_notebook, is_ipython
from dlt.cli import echo as fmt


import typing as t
from hyperscript import h
import os
import traceback

import click

DEBUG_FLAG = False

DLT_INIT_DOCS_URL = "https://dlthub.com/docs/reference/command-line-interface#dlt-init"
DEFAULT_VERIFIED_SOURCES_REPO = "https://github.com/dlt-hub/verified-sources.git"
DLT_DEPLOY_DOCS_URL = "https://dlthub.com/docs/walkthroughs/deploy-a-pipeline"



@magics_class
class DltMagics(Magics):

    @property
    def display(self) -> t.Callable[..., t.Any]:
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            # Assume Databricks' 'display' is a callable with an unknown signature
            databricks_display = self._shell.user_ns.get("display")
            if callable(databricks_display):
                return databricks_display
            else:
                raise RuntimeError("Expected a callable for Databricks' display, got something else.")
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

    def on_exception(self, ex: str, info: str) -> str:
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
        out = display(HTML(msg))

        if DEBUG_FLAG:
            # Display the full traceback in a preformatted style
            traceback_html = h("pre", traceback.format_exc(), {"style": {"color": "gray"}})
            display(HTML(str(traceback_html)))
            raise ex

        return out


    @magic_arguments()
    @argument('--enable-telemetry', action='store_true', help="Enables telemetry before command is executed")
    @argument('--disable-telemetry', action='store_true', help="Disables telemetry before command is executed")
    @argument('--non-interactive', action='store_true', help="Non interactive mode. Default choices are automatically made for confirmations and prompts.")
    @argument('--debug', action='store_true', help="Displays full stack traces on exceptions.")
    @line_magic
    @register_line_magic
    def settings(self, line:str)->str:
        """
        A DLT line magic command to set global settings like telemetry, debug mode, etc.
        """
        args = parse_argstring(self.settings, line)
        global DEBUG_FLAG
        try:
            from dlt.cli._dlt import telemetry_change_status_command_wrapper
            if args.enable_telemetry:
                telemetry_change_status_command_wrapper(True)
            if args.disable_telemetry:
                telemetry_change_status_command_wrapper(False)
            if args.non_interactive:
                fmt.ALWAYS_CHOOSE_DEFAULT = True
            if args.debug:
                DEBUG_FLAG = True
            return "Settings updated"
        except:
            pass


    @magic_arguments()
    @argument('--source_name', type=str, help="Name of data source for which to create a pipeline.")
    @argument('--destination_name', type=str, help="Name of a destination, e.g., bigquery or redshift.")
    @argument('--repo_location', type=str, help="Advanced. Uses a specific url or local path to verified sources repository.")
    @argument('--use_generic_template', action='store_true', help="Use a generic template with all the dlt loading code present.")
    @argument('--branch', type=str,default=None,  help="Use a default branch for the source.")
    @register_line_magic
    @line_magic
    def init(self, line:str) -> t.Any:
        """
        A DLT line magic command for initializing a DLT project.
        """
        args = parse_argstring(self.init, line)
        try:
            from dlt.cli._dlt import init_command_wrapper

            out = init_command_wrapper(
                source_name=args.source_name,
                destination_name=args.destination_name,
                use_generic_template=args.use_generic_template,
                repo_location=args.repo_location if args.repo_location is not None else DEFAULT_VERIFIED_SOURCES_REPO,
                branch=args.branch if args.branch is not None else None
            )
            if out == -1:
                return self.display(self.on_exception("Failure due to...", "Default value for init is 'No' for safety reasons."))
            else:
                return self.display(self.success_message({"green-bold": "DLT project initialized successfully."}))
        except Exception as ex:
            self.on_exception(ex, DLT_INIT_DOCS_URL)
            return -1

    @magic_arguments()
    @argument('--pipeline_script_path', type=str, help="Path to a pipeline script")
    @argument('--deployment_method', type=str, help="Deployment method")
    @argument('--repo_location', type=str, help="Repository location")
    @argument('--branch', type=str, help="Git branch")
    @argument('--schedule', type=str, default=None, help="Schedule for the deployment (optional)")
    @argument('--run_on_push', type=bool, default=False, help="Run on push to Git (optional)")
    @argument('--run_manually', type=bool, default=False, help="Run manually on Git (optional)")
    @register_line_magic
    @line_magic
    def deploy(self, line:str) ->t.Any :
        """
        A DLT line magic command for deploying a pipeline.
        """
        args = parse_argstring(self.deploy, line)
        try:
            from dlt.cli._dlt import deploy_command_wrapper
            from dlt.cli.deploy_command import PipelineWasNotRun, DLT_DEPLOY_DOCS_URL, DeploymentMethods, COMMAND_DEPLOY_REPO_LOCATION, SecretFormats
            def words_to_cron(description):
                mapping = {
                    "5 minutes": "*/5 * * * *",
                    "15 minutes": "*/15 * * * *",
                    "30 minutes": "*/30 * * * *",
                    "45 minutes": "*/45 * * * *",
                    "hour": "0 * * * *",
                    # Add more mappings as needed
                }
                cron_expression = mapping.get(description.lower())
                if cron_expression:
                    return cron_expression
                else:
                    return "Invalid description"

            # Initialize an empty kwargs dictionary
            kwargs = {}

            if args.schedule is not None:
                kwargs['schedule'] = words_to_cron(args.schedule)
            if args.run_on_push is not None:
                kwargs['run_on_push'] = args.run_on_push
            if args.run_manually is not None:
                kwargs['run_manually'] = args.run_manually

            deploy_command_wrapper(
                pipeline_script_path=args.pipeline_script_path,
                deployment_method=args.deployment_method,
                repo_location=args.repo_location if args.repo_location is not None else COMMAND_DEPLOY_REPO_LOCATION,
                branch=args.branch,
                **kwargs
            )
            return self.display(self.success_message({"green-bold": "DLT deploy magic ran successfully."}))
        except Exception as ex:

            self.on_exception(ex, DLT_DEPLOY_DOCS_URL)
            return -1
    @magic_arguments()
    @argument('--operation', type=str,default=None,  help="Operation to perform on the pipeline")
    @argument('--pipeline_name', type=str, default=None, help="Name of the pipeline")
    @argument('--pipelines_dir', type=str, default=None,  help="Directory of the pipeline")
    @argument('--verbosity', type=int, default=0,  help="Verbosity level")
    @line_magic
    @register_line_magic
    def pipeline(self, line:str)->t.Any:
        """
        A DLT line magic command for pipeline operations.
        """
        args = parse_argstring(self.pipeline, line)
        if args.operation == 'list-pipelines':
            args.operation = 'list'
        try:
            from dlt.cli._dlt import pipeline_command_wrapper, DLT_PIPELINE_COMMAND_DOCS_URL
            pipeline_command_wrapper(
                operation=args.operation,
                pipeline_name=args.pipeline_name,
                pipelines_dir=args.pipelines_dir,
                verbosity=args.verbosity

            )
        except Exception as ex:
            self.on_exception(ex, DLT_PIPELINE_COMMAND_DOCS_URL)
            return -2
    @magic_arguments()
    @argument('--file_path', type=str, help="Schema file name, in yaml or json format")
    @argument('--format', type=str, help="File format")
    @argument('--remove_defaults', type=str, help="Remove defaults")
    @line_magic
    @register_line_magic
    def schema(self, line:str)->t.Any:
        """
        A DLT line magic command for handling schemas.
        """
        args = parse_argstring(self.schema, line)
        try:
            from dlt.cli._dlt import schema_command_wrapper
            schema_command_wrapper(
                file_path=args.file_path,
                format_=args.format,
                remove_defaults=args.remove_defaults
            )
            return self.display(self.success_message({"green-bold": "DLT schema magic ran successfully."}))
        except Exception as ex:
            self.on_exception(ex, "Schema Documentation URL")  # Replace with actual URL
            return -1
    @line_magic
    @register_line_magic
    def dlt_version(self, line:str=None)->t.Any:
        """
        A DLT line magic command to display version information.
        """
        from dlt.version import __version__
        return f"{self.__class__.__name__} version: {__version__}"


def register_magics() -> None:
    from dlt.cli import echo as fmt

    try:
        shell = get_ipython()  # type: ignore
        shell.register_magics(DltMagics)

        # line_magics = shell.magics_manager.magics['line']
        # fmt.echo("Registered Line Magics:" + str(line_magics))
    except NameError:
        pass


def check_notebook_runtime() -> None:
    fmt.echo("Checking if ipython")
    if is_ipython():
        try:
            fmt.echo("Registering magics")
            register_magics()
        except ImportError:
            pass

