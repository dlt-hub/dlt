import typing as t

import click
import dlt

from dlt.cli import echo as fmt
from dlt.common.cli.runner.errors import FriendlyExit
from dlt.common.cli.runner.types import PipelineMembers
from dlt.sources import DltResource, DltSource


select_message = """Please select your %s:
%s
"""


def make_select_options(
    select_what: str, items: t.Dict[str, t.Any]
) -> t.Tuple[str, t.List[int], t.List[str]]:
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
    """This class handles user input to allow users to select pipeline and re/sources"""

    def __init__(self, pipeline_members: PipelineMembers) -> None:
        self.pipelines = pipeline_members["pipelines"]
        self.sources = pipeline_members["sources"]

    def ask(self) -> t.Tuple[dlt.Pipeline, t.Union[DltResource, DltSource]]:
        """Shows prompts to select pipeline and re/source

        Returns:
            (DltResource, DltSource): a pair of pipeline and re/source
        """
        # save first pipeline and source
        pipeline_name, _ = next(iter(self.pipelines.items()))
        source_name, _ = next(iter(self.pipelines.items()))
        if len(self.pipelines) > 1:
            message, options, values = make_select_options("pipeline", self.pipelines)
            choice = fmt.prompt(message, options, default="n")
            if choice == "n":
                raise FriendlyExit()
            else:
                pipeline_name = values[int(choice)]

        if len(self.sources) > 1:
            message, options, values = make_select_options("re/source", self.sources)
            choice = fmt.prompt(message, options, default="n")
            if choice == "n":
                raise FriendlyExit()
            else:
                source_name = values[int(choice)]

        pipeline = self.pipelines[pipeline_name]
        resource = self.sources[source_name]
        fmt.echo("Pipeline: " + fmt.style(pipeline_name, fg="blue", underline=True))

        if isinstance(resource, DltResource):
            label = "Resource"
        else:
            label = "Source"
        fmt.echo(f"{label}: " + fmt.style(source_name, fg="blue", underline=True))
        return pipeline, self.sources[source_name]
