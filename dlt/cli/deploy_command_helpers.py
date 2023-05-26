import abc
import os
from typing import List, Optional, Tuple
from astunparse import unparse
import cron_descriptor

import dlt

from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.configuration.providers import ConfigTomlProvider, EnvironProvider
from dlt.common.git import get_origin, get_repo
from dlt.common.configuration.specs.run_configuration import get_default_pipeline_name
from dlt.common.reflection.utils import evaluate_node_literal
from dlt.common.pipeline import LoadInfo, TPipelineState
from dlt.common.storages import FileStorage
from dlt.common.utils import set_working_dir

from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.trace import PipelineTrace
from dlt.reflection import names as n
from dlt.reflection.script_visitor import PipelineScriptVisitor

from dlt.cli import utils
from dlt.cli import echo as fmt
from dlt.cli.exceptions import CliCommandException


class BaseDeployment(abc.ABC):
    def __init__(
        self,
        pipeline_script_path: str,
        schedule: Optional[str],
        run_on_push: bool,
        run_on_dispatch: bool,
        repo_location: str,
        branch: Optional[str] = None
    ):
        self.pipeline_script_path = pipeline_script_path
        self.schedule = schedule
        self.run_on_push = run_on_push
        self.run_on_dispatch = run_on_dispatch
        self.repo_location = repo_location
        self.branch = branch

        self.deployment_method: Optional[str] = None
        self.repo = None
        self.repo_storage = None
        self.origin = None
        self.repo_pipeline_script_path = None
        self.pipeline_script = None

        self.schedule_description = None
        self.template_storage = None
        self.working_directory = None

        self.config_prov = ConfigTomlProvider()
        self.env_prov = EnvironProvider()

        # full_refresh = False
        self.pipelines_dir: Optional[str] = None
        self.pipeline_name: Optional[str] = None
        self.state = None
        self.envs: List[LookupTrace] = []
        self.secret_envs: List[LookupTrace] = []

    def _prepare_deployment(self):
        self.repo_storage = FileStorage(str(self.repo.working_dir))
        self.origin = self._get_origin()
        # convert to path relative to repo
        self.repo_pipeline_script_path = self.repo_storage.from_wd_to_relative_path(self.pipeline_script_path)
        # load a pipeline script and extract full_refresh and pipelines_dir args
        self.pipeline_script = self.repo_storage.load(self.repo_pipeline_script_path)
        # validate schedule
        self.schedule_description = self._get_schedule_description()
        fmt.echo("Looking up the deployment template scripts in %s...\n" % fmt.bold(self.repo_location))
        self.template_storage = utils.clone_command_repo(self.repo_location, self.branch)
        self.working_directory = os.path.split(self.pipeline_script_path)[0]

    def _get_schedule_description(self) -> Optional[str]:
        return None if self.schedule is None else cron_descriptor.get_description(self.schedule)

    def _get_origin(self) -> str:
        try:
            origin = get_origin(self.repo)
            if "github.com" not in origin:
                raise CliCommandException("deploy", f"Your current repository origin is not set to github but to {origin}.\nYou must change it to be able to run the pipelines with github actions: https://docs.github.com/en/get-started/getting-started-with-git/managing-remote-repositories")
        except ValueError:
            raise CliCommandException("deploy", "Your current repository has no origin set. Please set it up to be able to run the pipelines with github actions: https://docs.github.com/en/get-started/importing-your-projects-to-github/importing-source-code-to-github/adding-locally-hosted-code-to-github")

        return origin

    def run_deployment(self):
        with get_repo(self.pipeline_script_path) as repo:
            self.repo = repo
            self._prepare_deployment()
            # go through all once launched pipelines
            visitors = get_visitors(self.pipeline_script, self.pipeline_script_path)
            pipeline_name, pipelines_dir = parse_pipeline_info(visitors)

            if pipelines_dir:
                self.pipelines_dir = os.path.abspath(self.pipelines_dir)
            if pipeline_name:
                self.pipeline_name = pipeline_name

            # change the working dir to the script working dir
            with set_working_dir(self.working_directory):
                # use script name to derive pipeline name
                if not self.pipeline_name:
                    self.pipeline_name = dlt.config.get("pipeline_name")
                    if not self.pipeline_name:
                        self.pipeline_name = get_default_pipeline_name(self.pipeline_script_path)
                        fmt.warning(f"Using default pipeline name {self.pipeline_name}. The pipeline name is not passed as argument to dlt.pipeline nor configured via config provides ie. config.toml")

                # attach to pipeline name, get state and trace
                pipeline = dlt.attach(pipeline_name=self.pipeline_name, pipelines_dir=self.pipelines_dir)
                self.state, trace = get_state_and_trace(pipeline)
                self._update_envs(trace)

            self._generate_workflow()
            self._echo_instructions()

    def _update_envs(self, trace: PipelineTrace):
        # add destination name and dataset name to env
        self.envs = [
            # LookupTrace(self.env_prov.name, (), "destination_name", self.state["destination"]),
            LookupTrace(self.env_prov.name, (), "dataset_name", self.state["dataset_name"])
        ]

        for resolved_value in trace.resolved_config_values:
            if resolved_value.is_secret_hint:
                # generate special forms for all secrets
                self.secret_envs.append(LookupTrace(self.env_prov.name, tuple(resolved_value.sections), resolved_value.key, resolved_value.value))
                # fmt.echo(f"{resolved_value.key}:{resolved_value.value}{type(resolved_value.value)} in {resolved_value.sections} is SECRET")
            else:
                # move all config values that are not in config.toml into env
                if resolved_value.provider_name != self.config_prov.name:
                    self.envs.append(LookupTrace(self.env_prov.name, tuple(resolved_value.sections), resolved_value.key, resolved_value.value))
                    # fmt.echo(f"{resolved_value.key} in {resolved_value.sections} moved to CONFIG")

    def _echo_secrets(self):
        for s_v in self.secret_envs:
            fmt.secho("Name:", fg="green")
            fmt.echo(fmt.bold(self.env_prov.get_key_name(s_v.key, *s_v.sections)))
            fmt.secho("Secret:", fg="green")
            fmt.echo(s_v.value)
            fmt.echo()

    def _echo_envs(self):
        for v in self.envs:
            fmt.secho("Name:", fg="green")
            fmt.echo(fmt.bold(self.env_prov.get_key_name(v.key, *v.sections)))
            fmt.secho("Value:", fg="green")
            fmt.echo(v.value)
            fmt.echo()

    @abc.abstractmethod
    def _echo_instructions(self, *args):
        pass

    @abc.abstractmethod
    def _generate_workflow(self, *args):
        pass


def get_state_and_trace(pipeline: Pipeline) -> Tuple[TPipelineState, PipelineTrace]:
    # trace must exist and end with a successful loading step
    trace = pipeline.last_trace
    if trace is None or len(trace.steps) == 0:
        raise PipelineWasNotRun("Pipeline run trace could not be found. Please run the pipeline at least once locally.")
    last_step = trace.steps[-1]
    if last_step.step_exception is not None:
        raise PipelineWasNotRun(f"The last pipeline run ended with error. Please make sure that pipeline runs correctly before deployment.\n{last_step.step_exception}")
    if not isinstance(last_step.step_info, LoadInfo):
        raise PipelineWasNotRun("The last pipeline run did not reach the load step. Please run the pipeline locally until it loads data into destination.")

    return pipeline.state, trace


def get_visitors(pipeline_script: str, pipeline_script_path: str) -> PipelineScriptVisitor:
    visitor = utils.parse_init_script("deploy", pipeline_script, pipeline_script_path)
    if n.RUN not in visitor.known_calls:
        raise CliCommandException("deploy", f"The pipeline script {pipeline_script_path} does not seem to run the pipeline.")
    return visitor


def parse_pipeline_info(visitor: PipelineScriptVisitor) -> Tuple[Optional[str], Optional[str]]:
    pipeline_name, pipelines_dir = None, None
    if n.PIPELINE in visitor.known_calls:
        for call_args in visitor.known_calls[n.PIPELINE]:
            f_r_node = call_args.arguments.get("full_refresh")
            if f_r_node:
                f_r_value = evaluate_node_literal(f_r_node)
                if f_r_value is None:
                    fmt.warning(f"The value of `full_refresh` in call to `dlt.pipeline` cannot be determined from {unparse(f_r_node).strip()}. We assume that you know what you are doing :)")
                if f_r_value is True:
                    if fmt.confirm("The value of 'full_refresh' is set to True. Do you want to abort to set it to False?", default=True):
                        return None, None

            p_d_node = call_args.arguments.get("pipelines_dir")
            if p_d_node:
                pipelines_dir = evaluate_node_literal(p_d_node)
                if pipelines_dir is None:
                    raise CliCommandException("deploy", f"The value of 'pipelines_dir' argument in call to `dlt_pipeline` cannot be determined from {unparse(p_d_node).strip()}. Pipeline working dir will be found. Pass it directly with --pipelines-dir option.")

            p_n_node = call_args.arguments.get("pipeline_name")
            if p_n_node:
                pipeline_name = evaluate_node_literal(p_n_node)
                if pipeline_name is None:
                    raise CliCommandException("deploy", f"The value of 'pipeline_name' argument in call to `dlt_pipeline` cannot be determined from {unparse(p_d_node).strip()}. Pipeline working dir will be found. Pass it directly with --pipeline-name option.")

    return pipeline_name, pipelines_dir


class PipelineWasNotRun(CliCommandException):
    def __init__(self, msg: str) -> None:
        super().__init__("deploy", msg, None)
