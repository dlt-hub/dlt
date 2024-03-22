import os
import typing as t
import textwrap as tw
import dlt

from dlt.cli import echo as fmt
from dlt.common.cli.runner.errors import FriendlyExit, PreflightError, RunnerError
from dlt.common.cli.runner.types import PipelineMembers, RunnerParams
from dlt.sources import DltResource, DltSource


dot_dlt = ".dlt"
select_message = """Please select your %s:
%s
"""


def make_select_options(
    select_what: str, items: t.Dict[str, t.Any]
) -> t.Tuple[str, t.List[str], t.List[str]]:
    options = []
    option_values = []
    choice_message = ""
    for idx, name in enumerate(items.keys()):
        choice_message += f"{idx} - {name}\n"
        options.append(str(idx))
        option_values.append(name)

    choice_message += "n - exit\n"
    options.append("n")
    return select_message % (select_what, choice_message), options, option_values


class Inquirer:
    """This class does pre flight checks required to run the pipeline.
    Also handles user input to allow users to select pipeline, resources and sources.
    """

    def __init__(self, params: RunnerParams, pipeline_members: PipelineMembers) -> None:
        self.params = params
        self.pipelines = pipeline_members.get("pipelines")
        self.sources = pipeline_members.get("sources")
        self.aliases = pipeline_members.get("aliases")

    def maybe_ask(self) -> t.Tuple[dlt.Pipeline, t.Union[DltResource, DltSource]]:
        """Shows prompts to select pipeline, resources and sources

        Returns:
            (DltResource, DltSource): a pair of pipeline and resources and sources
        """
        self.preflight_checks()
        pipeline_name = self.get_pipeline_name()
        pipeline_alias_message = ""
        if pipeline_name in self.pipelines:
            pipeline = self.pipelines[pipeline_name]
        else:
            pipeline = self.aliases[pipeline_name]
            pipeline_alias_message = f" ({pipeline.pipeline_name})"

        source_name = self.get_source_name()
        source_alias_message = ""
        if source_name in self.sources:
            resource = self.sources[source_name]
        else:
            resource = self.aliases[source_name]
            source_alias_message = f" ({resource.name})"

        fmt.echo(
            "Pipeline: "
            + fmt.style(pipeline_name + pipeline_alias_message, fg="blue", underline=True)
        )

        if isinstance(resource, DltResource):
            label = "Resource"
        else:
            label = "Source"

        fmt.echo(
            f"{label}: " + fmt.style(source_name + source_alias_message, fg="blue", underline=True)
        )
        return pipeline, resource

    def get_pipeline_name(self) -> str:
        if self.params.pipeline_name:
            return self.params.pipeline_name

        if len(self.pipelines) > 1:
            message, options, values = make_select_options("pipeline", self.pipelines)
            choice = self.ask(message, options, default="n")
            pipeline_name = values[int(choice)]
            return pipeline_name

        pipeline_name, _ = next(iter(self.pipelines.items()))
        return pipeline_name

    def get_source_name(self) -> str:
        if self.params.source_name:
            return self.params.source_name

        if len(self.sources) > 1:
            message, options, values = make_select_options("resource or source", self.sources)
            choice = self.ask(message, options, default="n")
            source_name = values[int(choice)]
            return source_name

        source_name, _ = next(iter(self.sources.items()))
        return source_name

    def ask(self, message: str, options: t.List[str], default: t.Optional[str] = None) -> str:
        choice = fmt.prompt(message, options, default=default)
        if choice == "n":
            raise FriendlyExit()

        return choice

    def preflight_checks(self) -> None:
        if pipeline_name := self.params.pipeline_name:
            if pipeline_name not in self.pipelines and pipeline_name not in self.aliases:
                fmt.warning(f"Pipeline {pipeline_name} has not been found in pipeline script")
                raise PreflightError()

        if source_name := self.params.source_name:
            if source_name not in self.sources and source_name not in self.aliases:
                fmt.warning(
                    f"Source or resouce with name: {source_name} has not been found in pipeline"
                    " script"
                )
                raise PreflightError()

        if self.params.current_dir != self.params.pipeline_workdir:
            fmt.warning(
                "Current working directory is different from the "
                f"pipeline script {self.params.pipeline_workdir}\n"
            )
            fmt.echo(f"Current workdir: {fmt.style(self.params.current_dir, fg='blue')}")
            fmt.echo(f"Pipeline workdir: {fmt.style(self.params.pipeline_workdir, fg='blue')}")

            has_cwd_config = self.has_dlt_config(self.params.current_dir)
            has_pipeline_config = self.has_dlt_config(self.params.pipeline_workdir)
            if has_cwd_config and has_pipeline_config:
                message = tw.dedent(
                    f"""
                    Found {dot_dlt} in current directory and pipeline directory if you intended to
                    use {self.params.pipeline_workdir}/{dot_dlt}, please change your current directory.

                    Using {dot_dlt} in current directory {self.params.current_dir}/{dot_dlt}.
                    """,
                )
                fmt.echo(fmt.warning_style(message))
            elif not has_cwd_config and has_pipeline_config:
                fmt.error(
                    f"{dot_dlt} is missing in current directory but exists in pipeline script's"
                    " directory"
                )
                fmt.info(
                    f"Please change your current directory to {self.params.pipeline_workdir} and"
                    " try again"
                )
                raise PreflightError()

    def check_if_runnable(self) -> None:
        if not self.pipelines:
            raise RunnerError(
                f"No pipeline instances found in pipeline script {self.params.script_path}"
            )

        if not self.sources:
            raise RunnerError(
                f"No source or resources found in pipeline script {self.params.script_path}"
            )

    def has_dlt_config(self, path: str) -> bool:
        return os.path.exists(os.path.join(path, ".dlt"))
