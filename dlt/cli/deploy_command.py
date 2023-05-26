from itertools import chain
import os
import re
from typing import List, Optional
import yaml
from yaml import Dumper
import pipdeptree
from enum import Enum

from dlt.common.configuration.providers import SECRETS_TOML
from dlt.common.configuration.paths import make_dlt_settings_path
from dlt.common.configuration.utils import serialize_value
from dlt.common.git import is_dirty
from dlt.common.typing import StrAny

from dlt.cli import utils
from dlt.cli import echo as fmt
from dlt.cli.deploy_command_helpers import BaseDeployment, PipelineWasNotRun


REQUIREMENTS_GITHUB_ACTION = "requirements_github_action.txt"
GITHUB_URL = "https://github.com/"
DLT_DEPLOY_DOCS_URL = "https://dlthub.com/docs/walkthroughs/deploy-a-pipeline"
DLT_AIRFLOW_GCP_DOCS_URL = "https://dlthub.com/docs/running-in-production/orchestrators/airflow-gcp-cloud-composer"
AIRFLOW_GETTING_STARTED = "https://airflow.apache.org/docs/apache-airflow/stable/start.html"
AIRFLOW_DAG_TEMPLATE_SCRIPT = "dag_template.py"
AIRFLOW_CLOUDBUILD_YAML = "cloudbuild.yaml"
COMMAND_REPO_LOCATION = "https://github.com/dlt-hub/python-dlt-%s-template.git"


class DeploymentMethods(Enum):
    github_actions = "github-action"
    airflow = "airflow"


def deploy_command(
    pipeline_script_path: str,
    deployment_method: str,
    schedule: Optional[str],
    run_on_push: bool,
    run_on_dispatch: bool,
    repo_location: str,
    branch: Optional[str],
) -> None:
    # get current repo local folder
    if deployment_method == DeploymentMethods.github_actions.value:
        deployment_obj = GithubActionDeployment(
            pipeline_script_path=pipeline_script_path,
            schedule=schedule,
            run_on_push=run_on_push,
            run_on_dispatch=run_on_dispatch,
            repo_location=repo_location,
            branch=branch
        )
    elif deployment_method == DeploymentMethods.airflow.value:
        deployment_obj = AirflowDeployment(
            pipeline_script_path=pipeline_script_path,
            schedule=schedule,
            run_on_push=run_on_push,
            run_on_dispatch=run_on_dispatch,
            repo_location=repo_location,
            branch=branch
        )
    else:
        raise ValueError(f"Deployment method '{deployment_method}' is not supported. Only {', '.join([m.value for m in DeploymentMethods])} are available.'")

    deployment_obj.run_deployment()


class GithubActionDeployment(BaseDeployment):
    def _generate_workflow(self):
        self.deployment_method = DeploymentMethods.github_actions.value
        if self.schedule_description is None:
            raise ValueError(
                f"Setting 'schedule' for '{self.deployment_method}' is required! Use deploy command as 'dlt deploy chess.py {self.deployment_method} --schedule \"*/30 * * * *\"'."
            )
        workflow = self._create_new_workflow()
        serialized_workflow = serialize_templated_yaml(workflow)
        serialized_workflow_name = f"run_{self.state['pipeline_name']}_workflow.yml"
        self.state['serialized_workflow_name'] = serialized_workflow_name

        # pip freeze special requirements file
        with self.template_storage.open_file(os.path.join(self.deployment_method, "requirements_blacklist.txt")) as f:
            requirements_blacklist = f.readlines()
        requirements_txt = generate_pip_freeze(requirements_blacklist)
        requirements_txt_name = REQUIREMENTS_GITHUB_ACTION
        # if repo_storage.has_file(utils.REQUIREMENTS_TXT):
        self.state['requirements_txt_name'] = requirements_txt_name

        if not self.repo_storage.has_folder(utils.GITHUB_WORKFLOWS_DIR):
            self.repo_storage.create_folder(utils.GITHUB_WORKFLOWS_DIR)

        self.repo_storage.save(os.path.join(utils.GITHUB_WORKFLOWS_DIR, serialized_workflow_name), serialized_workflow)
        self.repo_storage.save(requirements_txt_name, requirements_txt)

    def _create_new_workflow(self):
        with self.template_storage.open_file(os.path.join(self.deployment_method, "run_pipeline_workflow.yml")) as f:
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

        return workflow

    def _echo_instructions(self,):
        fmt.echo("Your %s deployment for pipeline %s in script %s is ready!" % (
            fmt.bold(self.deployment_method), fmt.bold(self.state["pipeline_name"]), fmt.bold(self.pipeline_script_path)
        ))
        #  It contains all relevant configurations and references to credentials that are needed to run the pipeline
        fmt.echo("* A github workflow file %s was created in %s." % (
            fmt.bold(self.state["serialized_workflow_name"]), fmt.bold(utils.GITHUB_WORKFLOWS_DIR)
        ))
        fmt.echo("* The schedule with which the pipeline is run is: %s.%s%s" % (
            fmt.bold(self.schedule_description),
            " You can also run the pipeline manually." if self.run_on_dispatch else "",
            " Pipeline will also run on each push to the repository." if self.run_on_push else "",
        ))
        fmt.echo(
            "* The dependencies that will be used to run the pipeline are stored in %s. If you change add more dependencies, remember to refresh your deployment by running the same 'deploy' command again." % fmt.bold(
                self.state['requirements_txt_name'])
        )
        fmt.echo()
        fmt.echo("You should now add the secrets to github repository secrets, commit and push the pipeline files to github.")
        fmt.echo("1. Add the following secret values (typically stored in %s): \n%s\nin %s" % (
            fmt.bold(make_dlt_settings_path(SECRETS_TOML)),
            fmt.bold("\n".join(self.env_prov.get_key_name(s_v.key, *s_v.sections) for s_v in self.secret_envs)),
            fmt.bold(github_origin_to_url(self.origin, "/settings/secrets/actions"))
        ))
        fmt.echo()
        if fmt.confirm("Do you want to list the values of the secrets in the format suitable for github?", default=True):
            self._echo_secrets()

        fmt.echo("2. Add stage deployment files to commit. Use your Git UI or the following command")
        fmt.echo(fmt.bold(
            f"git add {self.repo_storage.from_relative_path_to_wd(self.state['requirements_txt_name'])} {self.repo_storage.from_relative_path_to_wd(os.path.join(utils.GITHUB_WORKFLOWS_DIR, self.state['serialized_workflow_name']))}"))
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
        fmt.echo(fmt.bold(github_origin_to_url(self.origin, f"/actions/workflows/{self.state['serialized_workflow_name']}")))


class AirflowDeployment(BaseDeployment):
    def _generate_workflow(self):
        self.deployment_method = DeploymentMethods.airflow.value

        dag_script_name = f"dag_{self.state['pipeline_name']}.py"
        self.state["dag_script_name"] = dag_script_name

        cloudbuild_file = self.template_storage.load(os.path.join(self.deployment_method, AIRFLOW_CLOUDBUILD_YAML))
        dag_file = self.template_storage.load(os.path.join(self.deployment_method, AIRFLOW_DAG_TEMPLATE_SCRIPT))

        if not self.repo_storage.has_folder(utils.AIRFLOW_DAGS_FOLDER):
            self.repo_storage.create_folder(utils.AIRFLOW_DAGS_FOLDER)

        if not self.repo_storage.has_folder(utils.AIRFLOW_BUILD_FOLDER):
            self.repo_storage.create_folder(utils.AIRFLOW_BUILD_FOLDER)

        self.repo_storage.save(os.path.join(utils.AIRFLOW_BUILD_FOLDER, AIRFLOW_CLOUDBUILD_YAML), cloudbuild_file)
        self.repo_storage.save(os.path.join(utils.AIRFLOW_DAGS_FOLDER, dag_script_name), dag_file)

    def _echo_instructions(self):
        fmt.echo("Your %s deployment for pipeline %s is ready!" % (
            fmt.bold(self.deployment_method), fmt.bold(self.state["pipeline_name"]),
        ))
        fmt.echo("* The airflow %s file was created in %s." % (
            fmt.bold(AIRFLOW_CLOUDBUILD_YAML), fmt.bold(utils.AIRFLOW_BUILD_FOLDER)
        ))
        fmt.echo("* The %s script was created in %s." % (
            fmt.bold(self.state["dag_script_name"]), fmt.bold(utils.AIRFLOW_DAGS_FOLDER)
        ))
        fmt.echo()

        fmt.echo("You must prepare your repository first:")

        fmt.echo("1. Import you sources in %s, change default_args if necessary." % (fmt.bold(self.state["dag_script_name"])))
        fmt.echo("2. Run airflow pipeline locally.\nSee Airflow getting started: %s" % (fmt.bold(AIRFLOW_GETTING_STARTED)))
        fmt.echo()

        fmt.echo("If you are planning run the pipeline with Google Cloud Composer, follow the next instructions:\n")
        fmt.echo("1. Read this doc and set up the Environment: %s" % (
            fmt.bold(DLT_AIRFLOW_GCP_DOCS_URL)
        ))
        fmt.echo("2. Set _BUCKET_NAME up in %s/%s file. " % (
            fmt.bold(utils.AIRFLOW_BUILD_FOLDER), fmt.bold(AIRFLOW_CLOUDBUILD_YAML),
        ))
        fmt.echo("3. Add the following secret values (typically stored in %s): \n%s\n%s\nin ENVIRONMENT VARIABLES using Google Composer UI" % (
            fmt.bold(make_dlt_settings_path(SECRETS_TOML)),
            fmt.bold("\n".join(self.env_prov.get_key_name(s_v.key, *s_v.sections) for s_v in self.secret_envs)),
            fmt.bold("\n".join(self.env_prov.get_key_name(v.key, *v.sections) for v in self.envs)),
        ))
        fmt.echo()
        if fmt.confirm("Do you want to list the environment variables in the format suitable for Airflow?", default=True):
            self._echo_secrets()
            self._echo_envs()

        fmt.echo("4. Add requirements to PIPY PACKAGES using Google Composer UI.")
        fmt.echo("5. Commit and push the pipeline files to github:")
        fmt.echo("a. Add stage deployment files to commit. Use your Git UI or the following command")

        dag_script_path = self.repo_storage.from_relative_path_to_wd(os.path.join(utils.AIRFLOW_DAGS_FOLDER, self.state["dag_script_name"]))
        cloudbuild_path = self.repo_storage.from_relative_path_to_wd(os.path.join(utils.AIRFLOW_BUILD_FOLDER, AIRFLOW_CLOUDBUILD_YAML))
        fmt.echo(fmt.bold(f"git add {dag_script_path} {cloudbuild_path}"))

        fmt.echo("b. Commit the files above. Use your Git UI or the following command")
        fmt.echo(fmt.bold(f"git commit -m 'initiate {self.state['pipeline_name']} pipeline with Airflow'"))
        if is_dirty(self.repo):
            fmt.warning("You have modified files in your repository. Do not forget to push changes to your pipeline script as well!")
        fmt.echo("c. Push changes to github. Use your Git UI or the following command")
        fmt.echo(fmt.bold("git push origin"))
        fmt.echo("6. You should see your pipeline in Airflow.")

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