import re
import abc
import os
import yaml
from yaml import Dumper
from itertools import chain
from typing import List, Optional, Sequence, Tuple, Any, Dict
from astunparse import unparse

# optional dependencies
import pipdeptree
import cron_descriptor

import dlt

from dlt.common import git
from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.configuration.providers import ConfigTomlProvider, EnvironProvider
from dlt.common.git import get_origin, get_repo, Repo
from dlt.common.configuration.specs.run_configuration import get_default_pipeline_name
from dlt.common.typing import StrAny
from dlt.common.reflection.utils import evaluate_node_literal
from dlt.common.pipeline import LoadInfo, TPipelineState, get_dlt_repos_dir
from dlt.common.storages import FileStorage
from dlt.common.utils import set_working_dir

from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.trace import PipelineTrace
from dlt.reflection import names as n
from dlt.reflection.script_visitor import PipelineScriptVisitor

from dlt.cli import utils
from dlt.cli import echo as fmt
from dlt.cli.exceptions import CliCommandException

GITHUB_URL = "https://github.com/"


def get_schedule_description(schedule: str) -> Optional[Any]:
    try:
        return None if schedule is None else cron_descriptor.get_description(schedule)
    except Exception as ex:
        raise ValueError(f"Error when parsing schedule '{schedule}': {str(ex)}")


class BaseDeployment(abc.ABC):
    def __init__(
        self,
        pipeline_script_path: str,
        location: str,
        branch: Optional[str] = None,
    ):
        self.pipeline_script_path = pipeline_script_path
        self.repo_location = location
        self.branch = branch

        self.pipelines_dir: Optional[str] = None
        self.pipeline_name: Optional[str] = None

        self.deployment_method: str
        self.repo: Repo
        self.repo_storage: FileStorage
        self.origin: str
        self.repo_pipeline_script_path: str
        self.pipeline_script: Any
        self.template_storage: FileStorage
        self.working_directory: str
        self.state: TPipelineState

        self.config_prov = ConfigTomlProvider()
        self.env_prov = EnvironProvider()
        self.envs: List[LookupTrace] = []
        self.secret_envs: List[LookupTrace] = []
        self.artifacts: Dict[str, Any] = {}

    def _prepare_deployment(self) -> None:
        self.repo_storage = FileStorage(str(self.repo.working_dir))
        # make sure the repo has origin
        self.origin = self._get_origin()
        # convert to path relative to repo
        self.repo_pipeline_script_path = self.repo_storage.from_wd_to_relative_path(
            self.pipeline_script_path
        )
        # load a pipeline script and extract full_refresh and pipelines_dir args
        self.pipeline_script = self.repo_storage.load(self.repo_pipeline_script_path)
        fmt.echo(
            "Looking up the deployment template scripts in %s...\n" % fmt.bold(self.repo_location)
        )
        self.template_storage = git.get_fresh_repo_files(
            self.repo_location, get_dlt_repos_dir(), branch=self.branch
        )
        self.working_directory = os.path.split(self.pipeline_script_path)[0]

    def _get_origin(self) -> str:
        try:
            origin = get_origin(self.repo)
            if "github.com" not in origin:
                raise CliCommandException(
                    "deploy",
                    f"Your current repository origin is not set to github but to {origin}.\nYou"
                    " must change it to be able to run the pipelines with github actions:"
                    " https://docs.github.com/en/get-started/getting-started-with-git/managing-remote-repositories",
                )
        except ValueError:
            raise CliCommandException(
                "deploy",
                "Your current repository has no origin set. Please set it up to be able to run the"
                " pipelines with github actions:"
                " https://docs.github.com/en/get-started/importing-your-projects-to-github/importing-source-code-to-github/adding-locally-hosted-code-to-github",
            )

        return origin

    def run_deployment(self) -> None:
        with get_repo(self.pipeline_script_path) as repo:
            self.repo = repo
            self._prepare_deployment()
            # go through all once launched pipelines
            visitors = get_visitors(self.pipeline_script, self.pipeline_script_path)
            possible_pipelines = parse_pipeline_info(visitors)
            pipeline_name: str = None
            pipelines_dir: str = None

            uniq_possible_pipelines = {t[0]: t for t in possible_pipelines}
            if len(uniq_possible_pipelines) == 1:
                pipeline_name, pipelines_dir = possible_pipelines[0]
            elif len(uniq_possible_pipelines) > 1:
                choices = list(uniq_possible_pipelines.keys())
                choices_str = "".join([str(i + 1) for i in range(len(choices))])
                choices_selection = [f"{idx+1}-{name}" for idx, name in enumerate(choices)]
                sel = fmt.prompt(
                    "Several pipelines found in script, please select one: "
                    + ", ".join(choices_selection),
                    choices=choices_str,
                )
                pipeline_name, pipelines_dir = uniq_possible_pipelines[choices[int(sel) - 1]]

            if pipelines_dir:
                self.pipelines_dir = os.path.abspath(pipelines_dir)
            if pipeline_name:
                self.pipeline_name = pipeline_name

            # change the working dir to the script working dir
            with set_working_dir(self.working_directory):
                # use script name to derive pipeline name
                if not self.pipeline_name:
                    self.pipeline_name = dlt.config.get("pipeline_name")
                    if not self.pipeline_name:
                        self.pipeline_name = get_default_pipeline_name(self.pipeline_script_path)
                        fmt.warning(
                            f"Using default pipeline name {self.pipeline_name}. The pipeline name"
                            " is not passed as argument to dlt.pipeline nor configured via config"
                            " provides ie. config.toml"
                        )
                # fmt.echo("Generating deployment for pipeline %s" % fmt.bold(self.pipeline_name))

                # attach to pipeline name, get state and trace
                pipeline = dlt.attach(
                    pipeline_name=self.pipeline_name, pipelines_dir=self.pipelines_dir
                )
                self.state, trace = get_state_and_trace(pipeline)
                self._update_envs(trace)

            self._generate_workflow()
            self._echo_instructions()
            self._make_modification()

    def _update_envs(self, trace: PipelineTrace) -> None:
        # add destination name and dataset name to env
        self.envs = [
            # LookupTrace(self.env_prov.name, (), "destination_name", self.state["destination"]),
            # LookupTrace(self.env_prov.name, (), "dataset_name", self.state["dataset_name"])
        ]

        for resolved_value in trace.resolved_config_values:
            if resolved_value.is_secret_hint:
                # generate special forms for all secrets
                self.secret_envs.append(
                    LookupTrace(
                        self.env_prov.name,
                        tuple(resolved_value.sections),
                        resolved_value.key,
                        resolved_value.value,
                    )
                )
                # fmt.echo(f"{resolved_value.key}:{resolved_value.value}{type(resolved_value.value)} in {resolved_value.sections} is SECRET")
            else:
                # move all config values that are not in config.toml into env
                if resolved_value.provider_name != self.config_prov.name:
                    self.envs.append(
                        LookupTrace(
                            self.env_prov.name,
                            tuple(resolved_value.sections),
                            resolved_value.key,
                            resolved_value.value,
                        )
                    )
                    # fmt.echo(f"{resolved_value.key} in {resolved_value.sections} moved to CONFIG")

    def _echo_secrets(self) -> None:
        for s_v in self.secret_envs:
            fmt.secho("Name:", fg="green")
            fmt.echo(fmt.bold(self.env_prov.get_key_name(s_v.key, *s_v.sections)))
            fmt.secho("Secret:", fg="green")
            fmt.echo(s_v.value)
            fmt.echo()

    def _echo_envs(self) -> None:
        for v in self.envs:
            fmt.secho("Name:", fg="green")
            fmt.echo(fmt.bold(self.env_prov.get_key_name(v.key, *v.sections)))
            fmt.secho("Value:", fg="green")
            fmt.echo(v.value)
            fmt.echo()

    @abc.abstractmethod
    def _echo_instructions(self, *args: Optional[Any]) -> Optional[Any]:
        pass

    @abc.abstractmethod
    def _generate_workflow(self, *args: Optional[Any]) -> Optional[Any]:
        pass

    @abc.abstractmethod
    def _make_modification(self) -> None:
        pass


def get_state_and_trace(pipeline: Pipeline) -> Tuple[TPipelineState, PipelineTrace]:
    # trace must exist and end with a successful loading step
    trace = pipeline.last_trace
    if trace is None or len(trace.steps) == 0:
        raise PipelineWasNotRun(
            "Pipeline run trace could not be found. Please run the pipeline at least once locally."
        )
    last_step = trace.steps[-1]
    if last_step.step_exception is not None:
        raise PipelineWasNotRun(
            "The last pipeline run ended with error. Please make sure that pipeline runs correctly"
            f" before deployment.\n{last_step.step_exception}"
        )
    if not isinstance(last_step.step_info, LoadInfo):
        raise PipelineWasNotRun(
            "The last pipeline run did not reach the load step. Please run the pipeline locally"
            " until it loads data into destination."
        )

    return pipeline.state, trace


def get_visitors(pipeline_script: str, pipeline_script_path: str) -> PipelineScriptVisitor:
    visitor = utils.parse_init_script("deploy", pipeline_script, pipeline_script_path)
    if n.RUN not in visitor.known_calls:
        raise CliCommandException(
            "deploy",
            f"The pipeline script {pipeline_script_path} does not seem to run the pipeline.",
        )
    return visitor


def parse_pipeline_info(visitor: PipelineScriptVisitor) -> List[Tuple[str, Optional[str]]]:
    pipelines: List[Tuple[str, Optional[str]]] = []
    if n.PIPELINE in visitor.known_calls:
        for call_args in visitor.known_calls[n.PIPELINE]:
            pipeline_name, pipelines_dir = None, None
            f_r_node = call_args.arguments.get("full_refresh")
            if f_r_node:
                f_r_value = evaluate_node_literal(f_r_node)
                if f_r_value is None:
                    fmt.warning(
                        "The value of `full_refresh` in call to `dlt.pipeline` cannot be"
                        f" determined from {unparse(f_r_node).strip()}. We assume that you know"
                        " what you are doing :)"
                    )
                if f_r_value is True:
                    if fmt.confirm(
                        "The value of 'full_refresh' is set to True. Do you want to abort to set it"
                        " to False?",
                        default=True,
                    ):
                        raise CliCommandException("deploy", "Please set the full_refresh to False")

            p_d_node = call_args.arguments.get("pipelines_dir")
            if p_d_node:
                pipelines_dir = evaluate_node_literal(p_d_node)
                if pipelines_dir is None:
                    raise CliCommandException(
                        "deploy",
                        "The value of 'pipelines_dir' argument in call to `dlt_pipeline` cannot be"
                        f" determined from {unparse(p_d_node).strip()}. Pipeline working dir will"
                        " be found. Pass it directly with --pipelines-dir option.",
                    )

            p_n_node = call_args.arguments.get("pipeline_name")
            if p_n_node:
                pipeline_name = evaluate_node_literal(p_n_node)
                if pipeline_name is None:
                    raise CliCommandException(
                        "deploy",
                        "The value of 'pipeline_name' argument in call to `dlt_pipeline` cannot be"
                        f" determined from {unparse(p_d_node).strip()}. Pipeline working dir will"
                        " be found. Pass it directly with --pipeline-name option.",
                    )
            pipelines.append((pipeline_name, pipelines_dir))

    return pipelines


def str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    # format multiline strings as blocks with the exception of placeholders
    # that will be expanded as yaml
    if len(data.splitlines()) > 1 and "{{ toYaml" not in data:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def wrap_template_str(s: str) -> str:
    return "${{ %s }}" % s


def serialize_templated_yaml(tree: StrAny) -> str:
    old_representer = Dumper.yaml_representers[str]
    try:
        yaml.add_representer(str, str_representer)
        # pretty serialize yaml
        serialized: str = yaml.dump(
            tree, allow_unicode=True, default_flow_style=False, sort_keys=False
        )
        # removes apostrophes around the template
        serialized = re.sub(r"'([\s\n]*?\${{.+?}})'", r"\1", serialized, flags=re.DOTALL)
        # print(serialized)
        # fix the new lines in templates ending }}
        serialized = re.sub(r"(\${{.+)\n.+(}})", r"\1 \2", serialized)
        return serialized
    finally:
        yaml.add_representer(str, old_representer)


def generate_pip_freeze(requirements_blacklist: List[str], requirements_file_name: str) -> str:
    pkgs = pipdeptree.get_installed_distributions(local_only=True, user_only=False)

    # construct graph with all packages
    tree = pipdeptree.PackageDAG.from_pkgs(pkgs)
    branch_keys = {r.key for r in chain.from_iterable(tree.values())}
    # all the top level packages
    nodes = [p for p in tree.keys() if p.key not in branch_keys]

    # compute excludes to compute includes as set difference
    excludes = set(req.strip() for req in requirements_blacklist if not req.strip().startswith("#"))
    includes = set(node.project_name for node in nodes if node.project_name not in excludes)

    # prepare new filtered DAG
    tree = tree.sort()
    tree = tree.filter_nodes(includes, None)
    branch_keys = {r.key for r in chain.from_iterable(tree.values())}
    nodes = [p for p in tree.keys() if p.key not in branch_keys]

    # detect and warn on conflict
    conflicts = pipdeptree.conflicting_deps(tree)
    cycles = pipdeptree.cyclic_deps(tree)
    if conflicts:
        fmt.warning(
            "Unable to create dependencies for the github action. Please edit"
            f" {requirements_file_name} yourself"
        )
        pipdeptree.render_conflicts_text(conflicts)
        pipdeptree.render_cycles_text(cycles)
        fmt.echo()
        # do not create package because it will most probably fail
        return "# please provide valid dependencies including dlt package"

    lines = [node.render(None, False) for node in nodes]
    return "\n".join(lines)


def github_origin_to_url(origin: str, path: str) -> str:
    # repository origin must end with .git
    if origin.endswith(".git"):
        origin = origin[:-4]
    if origin.startswith("git@github.com:"):
        origin = origin[15:]

    if not origin.startswith(GITHUB_URL):
        origin = GITHUB_URL + origin
    # https://github.com/dlt-hub/data-loading-zoomcamp.git
    # git@github.com:dlt-hub/data-loading-zoomcamp.git

    # https://github.com/dlt-hub/data-loading-zoomcamp/settings/secrets/actions
    return origin + path


def ask_files_overwrite(files: Sequence[str]) -> None:
    existing = [file for file in files if os.path.exists(file)]
    if existing:
        fmt.echo("Following files will be overwritten: %s" % fmt.bold(str(existing)))
        if not fmt.confirm("Do you want to continue?", default=False):
            raise CliCommandException("init", "Aborted")


class PipelineWasNotRun(CliCommandException):
    def __init__(self, msg: str) -> None:
        super().__init__("deploy", msg, None)
