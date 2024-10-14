import os
from typing import Optional, Any, Type
import yaml
from enum import Enum
from importlib.metadata import version as pkg_version

from dlt.common.configuration.providers import SECRETS_TOML, SECRETS_TOML_KEY
from dlt.common.configuration.utils import serialize_value
from dlt.common.git import is_dirty

from dlt.cli import utils
from dlt.cli import echo as fmt
from dlt.cli.deploy_command_helpers import (
    PipelineWasNotRun,
    BaseDeployment,
    ask_files_overwrite,
    generate_pip_freeze,
    github_origin_to_url,
    serialize_templated_yaml,
    wrap_template_str,
    get_schedule_description,
)

from dlt.version import DLT_PKG_NAME

from dlt.common.destination.reference import Destination

REQUIREMENTS_GITHUB_ACTION = "requirements_github_action.txt"
DLT_AIRFLOW_GCP_DOCS_URL = (
    "https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer"
)
AIRFLOW_GETTING_STARTED = "https://airflow.apache.org/docs/apache-airflow/stable/start.html"
AIRFLOW_DAG_TEMPLATE_SCRIPT = "dag_template.py"
AIRFLOW_CLOUDBUILD_YAML = "cloudbuild.yaml"
COMMAND_REPO_LOCATION = "https://github.com/dlt-hub/dlt-%s-template.git"
COMMAND_DEPLOY_REPO_LOCATION = COMMAND_REPO_LOCATION % "deploy"


class DeploymentMethods(Enum):
    github_actions = "github-action"
    airflow_composer = "airflow-composer"


class SecretFormats(Enum):
    env = "env"
    toml = "toml"


def deploy_command(
    pipeline_script_path: str,
    deployment_method: str,
    repo_location: str,
    branch: Optional[str] = None,
    **kwargs: Any,
) -> None:
    # get current repo local folder
    deployment_class: Type[BaseDeployment] = None
    if deployment_method == DeploymentMethods.github_actions.value:
        deployment_class = GithubActionDeployment
    elif deployment_method == DeploymentMethods.airflow_composer.value:
        deployment_class = AirflowDeployment
    else:
        raise ValueError(
            f"Deployment method '{deployment_method}' is not supported. Only"
            f" {', '.join([m.value for m in DeploymentMethods])} are available.'"
        )
    # command no longer needed
    kwargs.pop("command", None)
    deployment_class(
        pipeline_script_path=pipeline_script_path, location=repo_location, branch=branch, **kwargs
    ).run_deployment()


class GithubActionDeployment(BaseDeployment):
    def __init__(
        self,
        pipeline_script_path: str,
        location: str,
        schedule: Optional[str],
        run_on_push: bool = False,
        run_manually: bool = False,
        branch: Optional[str] = None,
    ):
        super().__init__(pipeline_script_path, location, branch)
        self.schedule = schedule
        self.run_on_push = run_on_push
        self.run_manually = run_manually
        self.schedule_description: Optional[str]

    def _generate_workflow(self, *args: Optional[Any]) -> None:
        self.deployment_method = DeploymentMethods.github_actions.value
        # validate schedule
        self.schedule_description = get_schedule_description(self.schedule)
        if self.schedule_description is None:
            # TODO: move that check to _dlt and some intelligent help message on missing arg
            raise ValueError(
                f"Setting 'schedule' for '{self.deployment_method}' is required! Use deploy command"
                f" as 'dlt deploy chess.py {self.deployment_method} --schedule \"*/30 * * * *\"'."
            )
        workflow = self._create_new_workflow()
        serialized_workflow = serialize_templated_yaml(workflow)
        serialized_workflow_name = f"run_{self.state['pipeline_name']}_workflow.yml"
        self.artifacts["serialized_workflow"] = serialized_workflow
        self.artifacts["serialized_workflow_name"] = serialized_workflow_name

        # pip freeze special requirements file
        with self.template_storage.open_file(
            os.path.join(self.deployment_method, "requirements_blacklist.txt")
        ) as f:
            requirements_blacklist = f.readlines()
        requirements_txt = generate_pip_freeze(requirements_blacklist, REQUIREMENTS_GITHUB_ACTION)
        requirements_txt_name = REQUIREMENTS_GITHUB_ACTION
        # if repo_storage.has_file(utils.REQUIREMENTS_TXT):
        self.artifacts["requirements_txt"] = requirements_txt
        self.artifacts["requirements_txt_name"] = requirements_txt_name

    def _make_modification(self) -> None:
        if not self.repo_storage.has_folder(utils.GITHUB_WORKFLOWS_DIR):
            self.repo_storage.create_folder(utils.GITHUB_WORKFLOWS_DIR)

        self.repo_storage.save(
            os.path.join(utils.GITHUB_WORKFLOWS_DIR, self.artifacts["serialized_workflow_name"]),
            self.artifacts["serialized_workflow"],
        )
        self.repo_storage.save(
            self.artifacts["requirements_txt_name"], self.artifacts["requirements_txt"]
        )

    def _create_new_workflow(self) -> Any:
        with self.template_storage.open_file(
            os.path.join(self.deployment_method, "run_pipeline_workflow.yml")
        ) as f:
            workflow = yaml.safe_load(f)
        # customize the workflow
        workflow["name"] = (
            f"Run {self.state['pipeline_name']} pipeline from {self.pipeline_script_path}"
        )
        if self.run_on_push is False:
            del workflow["on"]["push"]
        if self.run_manually is False:
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

    def _echo_instructions(self, *args: Optional[Any]) -> None:
        fmt.echo(
            "Your %s deployment for pipeline %s in script %s is ready!"
            % (
                fmt.bold(self.deployment_method),
                fmt.bold(self.state["pipeline_name"]),
                fmt.bold(self.pipeline_script_path),
            )
        )
        #  It contains all relevant configurations and references to credentials that are needed to run the pipeline
        fmt.echo(
            "* A github workflow file %s was created in %s."
            % (
                fmt.bold(self.artifacts["serialized_workflow_name"]),
                fmt.bold(utils.GITHUB_WORKFLOWS_DIR),
            )
        )
        fmt.echo(
            "* The schedule with which the pipeline is run is: %s.%s%s"
            % (
                fmt.bold(self.schedule_description),
                " You can also run the pipeline manually." if self.run_manually else "",
                (
                    " Pipeline will also run on each push to the repository."
                    if self.run_on_push
                    else ""
                ),
            )
        )
        fmt.echo(
            "* The dependencies that will be used to run the pipeline are stored in %s. If you"
            " change add more dependencies, remember to refresh your deployment by running the same"
            " 'deploy' command again."
            % fmt.bold(self.artifacts["requirements_txt_name"])
        )
        fmt.echo()
        if len(self.secret_envs) == 0 and len(self.envs) == 0:
            fmt.echo("1. Your pipeline does not seem to need any secrets.")
        else:
            fmt.echo(
                "You should now add the secrets to github repository secrets, commit and push the"
                " pipeline files to github."
            )
            fmt.echo(
                "1. Add the following secret values (typically stored in %s): \n%s\nin %s"
                % (
                    fmt.bold(utils.make_dlt_settings_path(SECRETS_TOML)),
                    fmt.bold(
                        "\n".join(
                            self.env_prov.get_key_name(s_v.key, *s_v.sections)
                            for s_v in self.secret_envs
                        )
                    ),
                    fmt.bold(github_origin_to_url(self.origin, "/settings/secrets/actions")),
                )
            )
            fmt.echo()
            self._echo_secrets()

        fmt.echo(
            "2. Add stage deployment files to commit. Use your Git UI or the following command"
        )
        new_req_path = self.repo_storage.from_relative_path_to_wd(
            self.artifacts["requirements_txt_name"]
        )
        new_workflow_path = self.repo_storage.from_relative_path_to_wd(
            os.path.join(utils.GITHUB_WORKFLOWS_DIR, self.artifacts["serialized_workflow_name"])
        )
        fmt.echo(fmt.bold(f"git add {new_req_path} {new_workflow_path}"))
        fmt.echo()
        fmt.echo("3. Commit the files above. Use your Git UI or the following command")
        fmt.echo(
            fmt.bold(
                f"git commit -m 'run {self.state['pipeline_name']} pipeline with github action'"
            )
        )
        if is_dirty(self.repo):
            fmt.warning(
                "You have modified files in your repository. Do not forget to push changes to your"
                " pipeline script as well!"
            )
        fmt.echo()
        fmt.echo("4. Push changes to github. Use your Git UI or the following command")
        fmt.echo(fmt.bold("git push origin"))
        fmt.echo()
        fmt.echo("5. Your pipeline should be running! You can monitor it here:")
        fmt.echo(
            fmt.bold(
                github_origin_to_url(
                    self.origin, f"/actions/workflows/{self.artifacts['serialized_workflow_name']}"
                )
            )
        )


class AirflowDeployment(BaseDeployment):
    def __init__(
        self,
        pipeline_script_path: str,
        location: str,
        branch: Optional[str] = None,
        secrets_format: Optional[str] = None,
    ):
        super().__init__(pipeline_script_path, location, branch)
        self.secrets_format = secrets_format

    def _generate_workflow(self, *args: Optional[Any]) -> None:
        self.deployment_method = DeploymentMethods.airflow_composer.value

        req_dep = f"{DLT_PKG_NAME}[{Destination.to_name(self.state['destination_type'])}]"
        req_dep_line = f"{req_dep}>={pkg_version(DLT_PKG_NAME)}"

        self.artifacts["requirements_txt"] = req_dep_line

        dag_script_name = f"dag_{self.state['pipeline_name']}.py"
        self.artifacts["dag_script_name"] = dag_script_name

        cloudbuild_file = self.template_storage.load(
            os.path.join(self.deployment_method, AIRFLOW_CLOUDBUILD_YAML)
        )
        self.artifacts["cloudbuild_file"] = cloudbuild_file

        # TODO: rewrite dag file to at least set the schedule
        dag_file = self.template_storage.load(
            os.path.join(self.deployment_method, AIRFLOW_DAG_TEMPLATE_SCRIPT)
        )
        self.artifacts["dag_file"] = dag_file

        # ask user if to overwrite the files
        dest_dag_script = os.path.join(utils.AIRFLOW_DAGS_FOLDER, dag_script_name)
        ask_files_overwrite([dest_dag_script])

    def _make_modification(self) -> None:
        if not self.repo_storage.has_folder(utils.AIRFLOW_DAGS_FOLDER):
            self.repo_storage.create_folder(utils.AIRFLOW_DAGS_FOLDER)

        if not self.repo_storage.has_folder(utils.AIRFLOW_BUILD_FOLDER):
            self.repo_storage.create_folder(utils.AIRFLOW_BUILD_FOLDER)

        # save cloudbuild.yaml only if not exist to allow to run the deploy command for many different pipelines
        dest_cloud_build = os.path.join(utils.AIRFLOW_BUILD_FOLDER, AIRFLOW_CLOUDBUILD_YAML)
        if not self.repo_storage.has_file(dest_cloud_build):
            self.repo_storage.save(dest_cloud_build, self.artifacts["cloudbuild_file"])
        else:
            fmt.warning(
                f"{AIRFLOW_CLOUDBUILD_YAML} already created. Delete the file and run the deploy"
                " command again to re-create."
            )

        dest_dag_script = os.path.join(utils.AIRFLOW_DAGS_FOLDER, self.artifacts["dag_script_name"])
        self.repo_storage.save(dest_dag_script, self.artifacts["dag_file"])

    def _echo_instructions(self, *args: Optional[Any]) -> None:
        fmt.echo(
            "Your %s deployment for pipeline %s is ready!"
            % (
                fmt.bold(self.deployment_method),
                fmt.bold(self.state["pipeline_name"]),
            )
        )
        fmt.echo(
            "* The airflow %s file was created in %s."
            % (fmt.bold(AIRFLOW_CLOUDBUILD_YAML), fmt.bold(utils.AIRFLOW_BUILD_FOLDER))
        )
        fmt.echo(
            "* The %s script was created in %s."
            % (fmt.bold(self.artifacts["dag_script_name"]), fmt.bold(utils.AIRFLOW_DAGS_FOLDER))
        )
        fmt.echo()

        fmt.echo("You must prepare your DAG first:")

        fmt.echo(
            "1. Import your sources in %s, configure the DAG ans tasks as needed."
            % (fmt.bold(self.artifacts["dag_script_name"]))
        )
        fmt.echo(
            "2. Test the DAG with Airflow locally .\nSee Airflow getting started: %s"
            % (fmt.bold(AIRFLOW_GETTING_STARTED))
        )
        fmt.echo()

        fmt.echo(
            "If you are planning run the pipeline with Google Cloud Composer, follow the next"
            " instructions:\n"
        )
        fmt.echo(
            "1. Read this doc and set up the Environment: %s" % (fmt.bold(DLT_AIRFLOW_GCP_DOCS_URL))
        )
        fmt.echo(
            "2. Set _BUCKET_NAME up in %s/%s file. "
            % (
                fmt.bold(utils.AIRFLOW_BUILD_FOLDER),
                fmt.bold(AIRFLOW_CLOUDBUILD_YAML),
            )
        )
        if len(self.secret_envs) == 0 and len(self.envs) == 0:
            fmt.echo("3. Your pipeline does not seem to need any secrets.")
        else:
            if self.secrets_format == SecretFormats.env.value:
                fmt.echo(
                    "3. Add the following secret values (typically stored in %s): \n%s\n%s\nin"
                    " ENVIRONMENT VARIABLES using Google Composer UI"
                    % (
                        fmt.bold(utils.make_dlt_settings_path(SECRETS_TOML)),
                        fmt.bold(
                            "\n".join(
                                self.env_prov.get_key_name(s_v.key, *s_v.sections)
                                for s_v in self.secret_envs
                            )
                        ),
                        fmt.bold(
                            "\n".join(
                                self.env_prov.get_key_name(v.key, *v.sections) for v in self.envs
                            )
                        ),
                    )
                )
                fmt.echo()
                # if fmt.confirm("Do you want to list the environment variables in the format suitable for Airflow?", default=True):
                self._echo_secrets()
                self._echo_envs()
            elif self.secrets_format == SecretFormats.toml.value:
                # build toml
                fmt.echo(
                    "3. Add the following toml-string in the Google Composer UI as the"
                    f" {SECRETS_TOML_KEY} variable."
                )
                fmt.echo()
                self._echo_secrets_toml()
            else:
                raise ValueError(self.secrets_format)

        fmt.echo("4. Add dlt package below using Google Composer UI.")
        fmt.echo(fmt.bold(self.artifacts["requirements_txt"]))
        fmt.note(
            "You may need to add more packages ie. when your source requires additional"
            " dependencies"
        )
        fmt.echo("5. Commit and push the pipeline files to github:")
        fmt.echo(
            "a. Add stage deployment files to commit. Use your Git UI or the following command"
        )

        dag_script_path = self.repo_storage.from_relative_path_to_wd(
            os.path.join(utils.AIRFLOW_DAGS_FOLDER, self.artifacts["dag_script_name"])
        )
        cloudbuild_path = self.repo_storage.from_relative_path_to_wd(
            os.path.join(utils.AIRFLOW_BUILD_FOLDER, AIRFLOW_CLOUDBUILD_YAML)
        )
        fmt.echo(fmt.bold(f"git add {dag_script_path} {cloudbuild_path}"))

        fmt.echo("b. Commit the files above. Use your Git UI or the following command")
        fmt.echo(
            fmt.bold(
                f"git commit -m 'initiate {self.state['pipeline_name']} pipeline with Airflow'"
            )
        )
        if is_dirty(self.repo):
            fmt.warning(
                "You have modified files in your repository. Do not forget to push changes to your"
                " pipeline script as well!"
            )
        fmt.echo("c. Push changes to github. Use your Git UI or the following command")
        fmt.echo(fmt.bold("git push origin"))
        fmt.echo("6. You should see your pipeline in Airflow.")
