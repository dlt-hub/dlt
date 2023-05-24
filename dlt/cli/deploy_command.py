from itertools import chain
import os
import re
from typing import List, Optional
from astunparse import unparse
import yaml
from yaml import Dumper
import cron_descriptor
# import pkg_resources
import pipdeptree
from enum import Enum

import dlt

from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.configuration.providers import ConfigTomlProvider, EnvironProvider, SECRETS_TOML
from dlt.common.configuration.paths import make_dlt_settings_path
from dlt.common.configuration.utils import serialize_value
from dlt.common.git import get_origin, get_repo, is_dirty
from dlt.common.configuration.specs.run_configuration import get_default_pipeline_name
from dlt.common.reflection.utils import evaluate_node_literal
from dlt.common.pipeline import LoadInfo
from dlt.common.storages import FileStorage
from dlt.common.typing import StrAny
from dlt.common.utils import set_working_dir

from dlt.reflection import names as n

from dlt.cli import utils
from dlt.cli import echo as fmt
from dlt.cli.exceptions import CliCommandException


REQUIREMENTS_GITHUB_ACTION = "requirements_github_action.txt"
GITHUB_URL = "https://github.com/"
DLT_DEPLOY_DOCS_URL = "https://dlthub.com/docs/walkthroughs/deploy-a-pipeline"
DLT_AIRFLOW_GCP_DOCS_URL = "https://dlthub.com/docs/running-in-production/orchestrators/airflow-gcp-cloud-composer"
AIRFLOW_GETTING_STARTED = "https://airflow.apache.org/docs/apache-airflow/stable/start.html"
AIRFLOW_DAG_TEMPLATE_SCRIPT = "dag_template.py"
AIRFLOW_CLOUDBUILD_YAML = "cloudbuild.yaml"


class DeploymentMethods(Enum):
    github_actions = "github-action"
    airflow = "airflow"




def deploy_command(
    pipeline_script_path: str,
    deployment_method: str,
    schedule: Optional[str],
    run_on_push: bool,
    run_on_dispatch: bool,
    branch: Optional[str] = None
) -> None:
    # get current repo local folder
    with get_repo(pipeline_script_path) as repo:
        deployment_state = DeploymentState(repo, pipeline_script_path, schedule, run_on_push, run_on_dispatch, branch)
        if deployment_method == DeploymentMethods.github_actions.value:
            deployment_state.run_github_actions_deploy()

        elif deployment_method == DeploymentMethods.airflow.value:
            deployment_state.run_airflow_deploy()
        else:
            raise ValueError(f"Deployment method '{deployment_method}' is not supported. Only {', '.join([m.value for m in DeploymentMethods])} are available.'")


class DeploymentState:
    def __init__(
        self,
        repo,
        pipeline_script_path: str,
        schedule: Optional[str],
        run_on_push: bool,
        run_on_dispatch: bool,
        branch: Optional[str] = None
    ):
        self.repo = repo
        self.pipeline_script_path = pipeline_script_path
        self.schedule = schedule
        self.run_on_push = run_on_push
        self.run_on_dispatch = run_on_dispatch
        self.branch = branch

        self.repo_storage = FileStorage(str(repo.working_dir))
        self.origin = self.define_origin()
        # convert to path relative to repo
        self.repo_pipeline_script_path = self.repo_storage.from_wd_to_relative_path(pipeline_script_path)
        # load a pipeline script and extract full_refresh and pipelines_dir args
        self.pipeline_script = self.repo_storage.load(self.repo_pipeline_script_path)
        self.visitor = utils.parse_init_script("deploy", self.pipeline_script, pipeline_script_path)

        if n.RUN not in self.visitor.known_calls:
            raise CliCommandException("deploy", f"The pipeline script {pipeline_script_path} does not seem to run the pipeline.")

        # full_refresh = False
        self.pipelines_dir: str = None
        self.pipeline_name: str = None

        self.state = None
        self.config_prov = ConfigTomlProvider()
        self.env_prov = EnvironProvider()
        self.envs: List[LookupTrace] = []
        self.secret_envs: List[LookupTrace] = []

        # validate schedule
        self.schedule_description = None if schedule is None else cron_descriptor.get_description(schedule)

        # self.template_storage = utils.clone_command_repo("deploy", branch)
        self.template_storage = FileStorage("/home/alenaastrakhantseva/dlthub/python-dlt-deploy-template")

        self.parse_pipeline()
        self.change_working_directory()

    def define_origin(self):
        try:
            origin = get_origin(self.repo)
            if "github.com" not in origin:
                raise CliCommandException("deploy", f"Your current repository origin is not set to github but to {origin}.\nYou must change it to be able to run the pipelines with github actions: https://docs.github.com/en/get-started/getting-started-with-git/managing-remote-repositories")
        except ValueError:
            raise CliCommandException("deploy", "Your current repository has no origin set. Please set it up to be able to run the pipelines with github actions: https://docs.github.com/en/get-started/importing-your-projects-to-github/importing-source-code-to-github/adding-locally-hosted-code-to-github")

        return origin

    def parse_pipeline(self):
        if n.PIPELINE in self.visitor.known_calls:
            for call_args in self.visitor.known_calls[n.PIPELINE]:
                f_r_node = call_args.arguments.get("full_refresh")
                if f_r_node:
                    f_r_value = evaluate_node_literal(f_r_node)
                    if f_r_value is None:
                        fmt.warning(f"The value of `full_refresh` in call to `dlt.pipeline` cannot be determined from {unparse(f_r_node).strip()}. We assume that you know what you are doing :)")
                    if f_r_value is True:
                        if fmt.confirm("The value of 'full_refresh' is set to True. Do you want to abort to set it to False?", default=True):
                            return
                p_d_node = call_args.arguments.get("pipelines_dir")
                if p_d_node:
                    self.pipelines_dir = evaluate_node_literal(p_d_node)
                    if self.pipelines_dir is None:
                        raise CliCommandException("deploy", f"The value of 'pipelines_dir' argument in call to `dlt_pipeline` cannot be determined from {unparse(p_d_node).strip()}. Pipeline working dir will be found. Pass it directly with --pipelines-dir option.")
                p_n_node = call_args.arguments.get("pipeline_name")
                if p_n_node:
                    self.pipeline_name = evaluate_node_literal(p_n_node)
                    if self.pipeline_name is None:
                        raise CliCommandException("deploy", f"The value of 'pipeline_name' argument in call to `dlt_pipeline` cannot be determined from {unparse(p_d_node).strip()}. Pipeline working dir will be found. Pass it directly with --pipeline-name option.")

        if self.pipelines_dir:
            self.pipelines_dir = os.path.abspath(self.pipelines_dir)

    def change_working_directory(self):
        # change the working dir to the script working dir
        with set_working_dir(os.path.split(self.pipeline_script_path)[0]):
            # use script name to derive pipeline name
            if not self.pipeline_name:
                self.pipeline_name = dlt.config.get("pipeline_name")
                if not self.pipeline_name:
                    self.pipeline_name = get_default_pipeline_name(self.pipeline_script_path)
                    fmt.warning(f"Using default pipeline name {self.pipeline_name}. The pipeline name is not passed as argument to dlt.pipeline nor configured via config provides ie. config.toml")
            # attach to pipeline name, get state and trace
            pipeline = dlt.attach(pipeline_name=self.pipeline_name, pipelines_dir=self.pipelines_dir)
            # trace must exist and end with a successful loading step
            trace = pipeline.last_trace
            if trace is None or len(trace.steps) == 0:
                raise PipelineWasNotRun("Pipeline run trace could not be found. Please run the pipeline at least once locally.")
            last_step = trace.steps[-1]
            if last_step.step_exception is not None:
                raise PipelineWasNotRun(f"The last pipeline run ended with error. Please make sure that pipeline runs correctly before deployment.\n{last_step.step_exception}")
            if not isinstance(last_step.step_info, LoadInfo):
                raise PipelineWasNotRun("The last pipeline run did not reach the load step. Please run the pipeline locally until it loads data into destination.")
            # add destination name and dataset name to env
            self.state = pipeline.state
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

    def run_github_actions_deploy(self):
        deployment_method = DeploymentMethods.github_actions.value
        with self.template_storage.open_file(os.path.join(deployment_method, "run_pipeline_workflow.yml")) as f:
            workflow = yaml.safe_load(f)
            # customize the workflow
        workflow["name"] = f"Run {self.state['pipeline_name']} pipeline from {self.pipeline_script_path}"
        if self.run_on_push is False:
            del workflow["on"]["push"]
        if self.run_on_dispatch is False:
            del workflow["on"]["workflow_dispatch"]
        workflow["on"]["schedule"] = [{"cron": self.schedule}]
        workflow["env"] = {}
        for env_var in self.envs:
            env_key = self.env_prov.get_key_name(env_var.key, *env_var.sections)
            # print(serialize_value(env_var.value))
            workflow["env"][env_key] = str(serialize_value(env_var.value))
        for secret_var in self.secret_envs:
            env_key = self.env_prov.get_key_name(secret_var.key, *secret_var.sections)
            workflow["env"][env_key] = wrap_template_str("secrets.%s") % env_key

        # run the correct script at the end
        last_workflow_step = workflow["jobs"]["run_pipeline"]["steps"][-1]
        assert last_workflow_step["run"] == "python pipeline.py"
        # must run in the directory of the script
        wf_run_path, wf_run_name = os.path.split(self.repo_pipeline_script_path)
        if wf_run_path:
            run_cd_cmd = f"cd '{wf_run_path}' && "
        else:
            run_cd_cmd = ""
        last_workflow_step["run"] = f"{run_cd_cmd}python '{wf_run_name}'"
        serialized_workflow = serialize_templated_yaml(workflow)
        serialized_workflow_name = f"run_{self.state['pipeline_name']}_workflow.yml"

        # pip freeze special requirements file
        with self.template_storage.open_file(os.path.join(deployment_method, "requirements_blacklist.txt")) as f:
            requirements_blacklist = f.readlines()
        requirements_txt = generate_pip_freeze(requirements_blacklist)
        requirements_txt_name = REQUIREMENTS_GITHUB_ACTION

        # if repo_storage.has_file(utils.REQUIREMENTS_TXT):
        fmt.echo("Your %s deployment for pipeline %s in script %s is ready!" % (
            fmt.bold(deployment_method), fmt.bold(self.state["pipeline_name"]), fmt.bold(self.pipeline_script_path)
        ))
        #  It contains all relevant configurations and references to credentials that are needed to run the pipeline
        fmt.echo("* A github workflow file %s was created in %s." % (
            fmt.bold(serialized_workflow_name), fmt.bold(utils.GITHUB_WORKFLOWS_DIR)
        ))
        fmt.echo("* The schedule with which the pipeline is run is: %s.%s%s" % (
            fmt.bold(self.schedule_description),
            " You can also run the pipeline manually." if self.run_on_dispatch else "",
            " Pipeline will also run on each push to the repository." if self.run_on_push else "",
        ))
        fmt.echo(
            "* The dependencies that will be used to run the pipeline are stored in %s. If you change add more dependencies, remember to refresh your deployment by running the same 'deploy' command again." % fmt.bold(
                requirements_txt_name))
        fmt.echo()
        fmt.echo("You should now add the secrets to github repository secrets, commit and push the pipeline files to github.")
        fmt.echo("1. Add the following secret values (typically stored in %s): \n%s\nin %s" % (
            fmt.bold(make_dlt_settings_path(SECRETS_TOML)),
            fmt.bold("\n".join(self.env_prov.get_key_name(s_v.key, *s_v.sections) for s_v in self.secret_envs)),
            fmt.bold(github_origin_to_url(self.origin, "/settings/secrets/actions"))
        ))
        fmt.echo()
        # if fmt.confirm("Do you want to list the values of the secrets in the format suitable for github?", default=True):
        for s_v in self.secret_envs:
            fmt.secho("Name:", fg="green")
            fmt.echo(fmt.bold(self.env_prov.get_key_name(s_v.key, *s_v.sections)))
            fmt.secho("Secret:", fg="green")
            fmt.echo(s_v.value)
            fmt.echo()

        fmt.echo("2. Add stage deployment files to commit. Use your Git UI or the following command")
        fmt.echo(fmt.bold(
            f"git add {self.repo_storage.from_relative_path_to_wd(requirements_txt_name)} {self.repo_storage.from_relative_path_to_wd(os.path.join(utils.GITHUB_WORKFLOWS_DIR, serialized_workflow_name))}"))
        fmt.echo()
        fmt.echo("3. Commit the files above. Use your Git UI or the following command")
        fmt.echo(fmt.bold(f"git commit -m 'run {self.state['pipeline_name']} pipeline with github action'"))
        if is_dirty(self.repo):
            fmt.warning("You have modified files in your repository. Do not forget to push changes to your pipeline script as well!")
        fmt.echo()
        fmt.echo("4. Push changes to github. Use your Git UI or the following command")
        fmt.echo(fmt.bold("git push origin"))
        fmt.echo()
        fmt.echo("5. Your pipeline should be running! You can monitor it here:")
        fmt.echo(fmt.bold(github_origin_to_url(self.origin, f"/actions/workflows/{serialized_workflow_name}")))

        if not self.repo_storage.has_folder(utils.GITHUB_WORKFLOWS_DIR):
            self.repo_storage.create_folder(utils.GITHUB_WORKFLOWS_DIR)

        self.repo_storage.save(os.path.join(utils.GITHUB_WORKFLOWS_DIR, serialized_workflow_name), serialized_workflow)
        self.repo_storage.save(requirements_txt_name, requirements_txt)

    def run_airflow_deploy(self):
        deployment_method = DeploymentMethods.airflow.value
        dag_script_name = f"dag_{self.pipeline_name}.py"
        cloudbuild_file = self.template_storage.load(os.path.join(deployment_method, AIRFLOW_CLOUDBUILD_YAML))
        dag_file = self.template_storage.load(os.path.join(deployment_method, AIRFLOW_DAG_TEMPLATE_SCRIPT))

        fmt.echo("Your %s deployment for pipeline %s is ready!" % (
            fmt.bold(deployment_method), fmt.bold(self.state["pipeline_name"]),
        ))
        fmt.echo("* The airflow %s file was created in %s." % (
            fmt.bold(AIRFLOW_CLOUDBUILD_YAML), fmt.bold(utils.AIRFLOW_BUILD_FOLDER)
        ))
        fmt.echo("* The %s script was created in %s." % (
            fmt.bold(AIRFLOW_DAG_TEMPLATE_SCRIPT), fmt.bold(utils.AIRFLOW_DAGS_FOLDER)
        ))
        fmt.echo()

        fmt.echo("You must prepare your repository first:")

        fmt.echo("1. Import you sources in %s, change default_args if necessary." % (fmt.bold(AIRFLOW_DAG_TEMPLATE_SCRIPT)))
        fmt.echo("2. Run airflow pipeline locally.\nSee Airflow getting started: %s" % (fmt.bold(AIRFLOW_GETTING_STARTED)))
        fmt.echo()

        fmt.echo("If you are planning run pipeline with Google Cloud Composer, follow the next instructions:\n")
        fmt.echo("1. Read this doc and set up the Environment: %s" % (
            fmt.bold(DLT_AIRFLOW_GCP_DOCS_URL)
        ))
        fmt.echo("2. Set _BUCKET_NAME up in %s/%s file. " % (
            fmt.bold(utils.AIRFLOW_BUILD_FOLDER), fmt.bold(AIRFLOW_CLOUDBUILD_YAML),
        ))

        fmt.echo("3. Add the following secret values (typically stored in %s): \n%s\nin ENVIRONMENT VARIABLES using Google Composer UI" % (
            fmt.bold(make_dlt_settings_path(SECRETS_TOML)),
            fmt.bold("\n".join(self.env_prov.get_key_name(s_v.key, *s_v.sections) for s_v in self.secret_envs)),
        ))
        fmt.echo()
        for s_v in self.secret_envs:
            fmt.secho("Name:", fg="green")
            fmt.echo(fmt.bold(self.env_prov.get_key_name(s_v.key, *s_v.sections)))
            fmt.secho("Secret:", fg="green")
            fmt.echo(s_v.value)
            fmt.echo()

        fmt.echo("4. Add requirements to PIPY PACKAGES using Google Composer UI.")
        fmt.echo("5. Commit and push the pipeline files to github:")
        fmt.echo("a. Add stage deployment files to commit. Use your Git UI or the following command")
        fmt.echo(fmt.bold(
            f"git add {self.repo_storage.from_relative_path_to_wd(os.path.join(utils.AIRFLOW_DAGS_FOLDER, AIRFLOW_DAG_TEMPLATE_SCRIPT))} {self.repo_storage.from_relative_path_to_wd(os.path.join(utils.AIRFLOW_BUILD_FOLDER, AIRFLOW_CLOUDBUILD_YAML))}"))
        fmt.echo("b. Commit the files above. Use your Git UI or the following command")
        fmt.echo(fmt.bold(f"git commit -m 'initiate {self.state['pipeline_name']} pipeline with Airflow'"))
        if is_dirty(self.repo):
            fmt.warning("You have modified files in your repository. Do not forget to push changes to your pipeline script as well!")
        fmt.echo("c. Push changes to github. Use your Git UI or the following command")
        fmt.echo(fmt.bold("git push origin"))
        fmt.echo("6. You should see your pipeline in Airflow.")

        if not self.repo_storage.has_folder(utils.AIRFLOW_DAGS_FOLDER):
            self.repo_storage.create_folder(utils.AIRFLOW_DAGS_FOLDER)

        if not self.repo_storage.has_folder(utils.AIRFLOW_BUILD_FOLDER):
            self.repo_storage.create_folder(utils.AIRFLOW_BUILD_FOLDER)

        self.repo_storage.save(os.path.join(utils.AIRFLOW_BUILD_FOLDER, AIRFLOW_CLOUDBUILD_YAML), cloudbuild_file)
        self.repo_storage.save(os.path.join(utils.AIRFLOW_DAGS_FOLDER, dag_script_name), dag_file)


class PipelineWasNotRun(CliCommandException):
    def __init__(self, msg: str) -> None:
        super().__init__("deploy", msg, None)


def str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    # format multiline strings as blocks with the exception of placeholders
    # that will be expanded as yaml
    if len(data.splitlines()) > 1 and "{{ toYaml" not in data:  # check for multiline string
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def wrap_template_str(s: str) -> str:
    return "${{ %s }}" % s


def serialize_templated_yaml(tree: StrAny) -> str:
    old_representer = Dumper.yaml_representers[str]
    try:
        yaml.add_representer(str, str_representer)
        # pretty serialize yaml
        serialized: str = yaml.dump(tree, allow_unicode=True, default_flow_style=False, sort_keys=False)
        # removes apostrophes around the template
        serialized = re.sub(r"'([\s\n]*?\${{.+?}})'",
                            r"\1",
                            serialized,
                            flags=re.DOTALL)
        # print(serialized)
        # fix the new lines in templates ending }}
        serialized = re.sub(r"(\${{.+)\n.+(}})",
                            r"\1 \2",
                            serialized)
        return serialized
    finally:
        yaml.add_representer(str, old_representer)


def generate_pip_freeze(requirements_blacklist: List[str]) -> str:

    pkgs = pipdeptree.get_installed_distributions(local_only=True, user_only=False)

    # construct graph with all packages
    tree = pipdeptree.PackageDAG.from_pkgs(pkgs)
    nodes = tree.keys()
    branch_keys = {r.key for r in chain.from_iterable(tree.values())}
    # all the top level packages
    nodes = [p for p in nodes if p.key not in branch_keys]

    # compute excludes to compute includes as set difference
    excludes = set(req.strip() for req in requirements_blacklist if not req.strip().startswith("#"))
    includes = [node.project_name for node in nodes if node.project_name not in excludes]

    # prepare new filtered DAG
    tree = tree.sort()
    tree = tree.filter(includes, None)
    nodes = tree.keys()
    branch_keys = {r.key for r in chain.from_iterable(tree.values())}
    nodes = [p for p in nodes if p.key not in branch_keys]

    # detect and warn on conflict
    conflicts = pipdeptree.conflicting_deps(tree)
    cycles = pipdeptree.cyclic_deps(tree)
    if conflicts:
        fmt.warning(f"Unable to create dependencies for the github action. Please edit {REQUIREMENTS_GITHUB_ACTION} yourself")
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
    #https://github.com/dlt-hub/data-loading-zoomcamp.git
    #git@github.com:dlt-hub/data-loading-zoomcamp.git

    # https://github.com/dlt-hub/data-loading-zoomcamp/settings/secrets/actions
    return origin + path