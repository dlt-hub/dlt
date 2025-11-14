import time
from io import BytesIO
from pathlib import Path
from typing import Any, Optional, Union
from uuid import UUID

from dlt._workspace._workspace_context import active
from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.exceptions import CliCommandInnerException
from dlt._workspace.deployment.file_selector import ConfigurationFileSelector, WorkspaceFileSelector
from dlt._workspace.deployment.package_builder import PackageBuilder
from dlt._workspace.exceptions import (
    LocalWorkspaceIdNotSet,
    RuntimeNotAuthenticated,
    WorkspaceIdMismatch,
)
from dlt._workspace.runtime import RuntimeAuthService, get_api_client, get_auth_client
from dlt._workspace.runtime_clients.api.api.configurations import (
    create_configuration,
    get_configuration,
    get_latest_configuration,
    list_configurations,
)
from dlt._workspace.runtime_clients.api.api.deployments import (
    create_deployment,
    get_deployment,
    get_latest_deployment,
    list_deployments,
)
from dlt._workspace.runtime_clients.api.api.runs import (
    cancel_run,
    create_run,
    get_run,
    get_run_logs,
    list_runs,
)
from dlt._workspace.runtime_clients.api.api.scripts import (
    create_or_update_script,
    get_script,
    list_scripts,
)
from dlt._workspace.runtime_clients.api.client import Client as ApiClient
from dlt._workspace.runtime_clients.api.models.create_deployment_body import CreateDeploymentBody
from dlt._workspace.runtime_clients.api.models.detailed_run_response import DetailedRunResponse
from dlt._workspace.runtime_clients.api.models.script_type import ScriptType
from dlt._workspace.runtime_clients.api.types import UNSET, File, Response
from dlt._workspace.runtime_clients.auth.api.github import github_oauth_complete, github_oauth_start
from dlt.common.configuration.plugins import SupportsCliCommand
from dlt.common.json import json


def _to_uuid(value: Union[str, UUID]) -> UUID:
    if isinstance(value, UUID):
        return value
    try:
        return UUID(value)
    except ValueError:
        raise RuntimeError(f"Invalid UUID: {value}")


def _exception_from_response(message: str, response: Response[Any]) -> BaseException:
    status = response.status_code
    try:
        details = json.loads(response.content.decode("utf-8"))["detail"]
    except Exception:
        details = response.content.decode("utf-8")

    message += f". {details} (HTTP {status})"
    return CliCommandInnerException(cmd="runtime", msg=message, inner_exc=None)


def login(minimal_logging: bool = True) -> RuntimeAuthService:
    auth_service = RuntimeAuthService(run_context=active())
    try:
        auth_info = auth_service.authenticate()
        if not minimal_logging:
            fmt.echo("Already logged in as %s" % fmt.bold(auth_info.email))
        connect(auth_service=auth_service)
        return auth_service
    except RuntimeNotAuthenticated:
        client = get_auth_client()

        # start device flow
        login_request = github_oauth_start.sync(client=client)
        if not isinstance(login_request, github_oauth_start.GithubDeviceFlowStartResponse):
            raise RuntimeError("Failed to log in with Github OAuth")
        fmt.echo(
            "Logging in with Github OAuth. Please go to %s and enter the code %s"
            % (fmt.bold(login_request.verification_uri), fmt.bold(login_request.user_code))
        )
        fmt.echo("Waiting for response from Github...")

        while True:
            time.sleep(login_request.interval)
            token_response = github_oauth_complete.sync(
                client=client,
                body=github_oauth_complete.GithubDeviceFlowLoginRequest(
                    device_code=login_request.device_code
                ),
            )
            if isinstance(token_response, github_oauth_complete.LoginResponse):
                auth_info = auth_service.login(token_response.jwt)
                fmt.echo("Logged in as %s" % fmt.bold(auth_info.email))
                connect(auth_service=auth_service)
                return auth_service
            elif isinstance(token_response, github_oauth_complete.ErrorResponse400):
                raise RuntimeError("Failed to complete authentication with Github")


def logout() -> None:
    auth_service = RuntimeAuthService(run_context=active())
    auth_service.logout()
    fmt.echo("Logged out")


def connect(
    auth_service: Optional[RuntimeAuthService] = None, minimal_logging: bool = False
) -> None:
    if auth_service is None:
        auth_service = RuntimeAuthService(run_context=active())
        auth_service.authenticate()

    try:
        auth_service.connect()
    except LocalWorkspaceIdNotSet:
        should_overwrite = fmt.confirm(
            "No workspace id found in local config. Do you want to connect local workspace to the"
            " remote one?",
            default=True,
        )
        if should_overwrite:
            auth_service.overwrite_local_workspace_id()
            fmt.echo("Using remote workspace id")
        else:
            raise RuntimeError("Local workspace is not connected to the remote one")
    except WorkspaceIdMismatch as e:
        fmt.warning(
            "Workspace id in local config (%s) is not the same as remote workspace id (%s)"
            % (e.local_workspace_id, e.remote_workspace_id)
        )
        should_overwrite = fmt.confirm(
            "Do you want to overwrite the local workspace id with the remote one?",
            default=True,
        )
        if should_overwrite:
            auth_service.overwrite_local_workspace_id()
            fmt.echo("Local workspace id overwritten with remote workspace id")
        else:
            raise RuntimeError("Unable to synchronise remote and local workspaces")
    if not minimal_logging:
        fmt.echo("Authorized to workspace %s" % fmt.bold(auth_service.workspace_id))


def deploy(
    script_file_name: str, is_interactive: bool = False, profile: Optional[str] = None
) -> None:
    auth_service = login()
    api_client = get_api_client(auth_service)

    script_path = Path(active().run_dir) / script_file_name
    if not script_path.exists():
        raise RuntimeError(f"Script file {script_file_name} not found")

    sync_deployment(auth_service=auth_service, api_client=api_client)
    sync_configuration(auth_service=auth_service, api_client=api_client)
    run_script(
        script_file_name,
        is_interactive,
        profile=profile,
        auth_service=auth_service,
        api_client=api_client,
    )


def sync_deployment(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    content_stream = BytesIO()
    package_builder = PackageBuilder(context=active())
    package_hash = package_builder.write_package_to_stream(
        file_selector=WorkspaceFileSelector(active()), output_stream=content_stream
    )

    latest_deployment = get_latest_deployment.sync_detailed(
        workspace_id=_to_uuid(auth_service.workspace_id),
        client=api_client,
    )
    if isinstance(latest_deployment.parsed, get_latest_deployment.DeploymentResponse):
        if latest_deployment.parsed.content_hash == package_hash:
            fmt.echo("No changes detected in the deployment, skipping file upload")
            content_stream.close()
            return
    elif isinstance(latest_deployment.parsed, get_latest_deployment.ErrorResponse404):
        fmt.echo("No deployment found in this workspace, creating new deployment")
    else:
        content_stream.close()
        raise _exception_from_response("Failed to get latest deployment", latest_deployment)

    create_deployment_result = create_deployment.sync_detailed(
        workspace_id=_to_uuid(auth_service.workspace_id),
        client=api_client,
        body=CreateDeploymentBody(
            file=File(
                payload=content_stream, file_name="workspace.tar.gz", mime_type="application/x-tar"
            )
        ),
    )
    if isinstance(create_deployment_result.parsed, create_deployment.DeploymentResponse):
        fmt.echo(f"Deployment # {create_deployment_result.parsed.version} created successfully")
        fmt.echo(f"Deployment id: {create_deployment_result.parsed.id}")
        fmt.echo(f"File count: {create_deployment_result.parsed.file_count}")
        fmt.echo(f"Content hash: {create_deployment_result.parsed.content_hash}")
    else:
        raise _exception_from_response("Failed to create deployment", create_deployment_result)


def sync_configuration(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    content_stream = BytesIO()
    package_builder = PackageBuilder(context=active())
    package_hash = package_builder.write_package_to_stream(
        file_selector=ConfigurationFileSelector(active()), output_stream=content_stream
    )

    latest_configuration = get_latest_configuration.sync_detailed(
        workspace_id=_to_uuid(auth_service.workspace_id),
        client=api_client,
    )
    if isinstance(latest_configuration.parsed, get_latest_configuration.ConfigurationResponse):
        if latest_configuration.parsed.content_hash == package_hash:
            fmt.echo("No changes detected in the configuration, skipping file upload")
            content_stream.close()
            return
    elif isinstance(latest_configuration.parsed, get_latest_configuration.ErrorResponse404):
        fmt.echo("No configuration found in this workspace, creating new configuration")
    else:
        content_stream.close()
        raise _exception_from_response("Failed to get latest configuration", latest_configuration)

    create_configuration_result = create_configuration.sync_detailed(
        workspace_id=_to_uuid(auth_service.workspace_id),
        client=api_client,
        body=create_configuration.CreateConfigurationBody(
            file=File(
                payload=content_stream,
                file_name="configurations.tar.gz",
                mime_type="application/x-tar",
            )
        ),
    )
    if isinstance(create_configuration_result.parsed, create_configuration.ConfigurationResponse):
        fmt.echo(
            f"configuration # {create_configuration_result.parsed.version} created successfully"
        )
    else:
        raise _exception_from_response(
            "Failed to create configuration", create_configuration_result
        )


def run_script(
    script_file_name: str,
    is_interactive: bool = False,
    profile: Optional[str] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    script_path = Path(active().run_dir) / script_file_name
    if not script_path.exists():
        raise RuntimeError(f"Script file {script_file_name} not found")

    create_script_result = create_or_update_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_or_update_script.CreateScriptRequest(
            name=script_file_name,
            description=f"The {script_file_name} script",
            entry_point=script_file_name,
            script_type=ScriptType.INTERACTIVE if is_interactive else ScriptType.BATCH,
            profile=profile,
            schedule=None,
        ),
    )
    if not isinstance(create_script_result.parsed, create_or_update_script.ScriptResponse):
        raise _exception_from_response("Failed to create script", create_script_result)
    else:
        script_id = create_script_result.parsed.id
        fmt.echo(f"Script {script_file_name} created or updated successfully, id: {script_id}")

    create_run_result = create_run.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_run.CreateRunRequest(
            script_id_or_name=script_file_name,
            profile=None,
        ),
    )
    if isinstance(create_run_result.parsed, create_run.RunResponse):
        fmt.echo("Script %s run successfully" % (fmt.bold(str(script_file_name))))
        if is_interactive:
            url = f"https://{script_id}.apps.tower.dev"
            fmt.echo(f"Script is accessible on {url}")
    else:
        raise _exception_from_response("Failed to run script", create_run_result)


def get_run_info(
    script_name_or_run_id: str,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    try:
        run_id = _to_uuid(script_name_or_run_id)
    except RuntimeError:
        run = _get_latest_run(api_client, auth_service, script_name_or_run_id)
        run_id = run.id

    get_run_result = get_run.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        run_id=_to_uuid(run_id),
    )
    if isinstance(get_run_result.parsed, get_run.DetailedRunResponse):
        fmt.echo("Status of run %s" % fmt.bold(str(run_id)))
        fmt.echo(f"Script: {get_run_result.parsed.script.name}")
        fmt.echo(f"Run #: {get_run_result.parsed.number}")
        fmt.echo("Status: %s" % fmt.bold(get_run_result.parsed.status))
        fmt.echo("Started at: %s" % fmt.bold(get_run_result.parsed.time_started))  # type: ignore[arg-type]
        fmt.echo("Ended at: %s" % fmt.bold(get_run_result.parsed.time_ended))  # type: ignore[arg-type]
        fmt.echo("Duration: %s seconds" % fmt.bold(get_run_result.parsed.duration))  # type: ignore[arg-type]
        fmt.echo("Triggered by: %s" % fmt.bold(get_run_result.parsed.triggered_by))  # type: ignore[arg-type]
        fmt.echo("Deployment id: %s" % fmt.bold(get_run_result.parsed.deployment_id))  # type: ignore[arg-type]
        fmt.echo("Profile: %s" % fmt.bold(get_run_result.parsed.profile))  # type: ignore[arg-type]

    else:
        raise _exception_from_response("Failed to get run status", get_run_result)


def fetch_run_logs(
    script_name_or_run_id: str,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    """Get logs for a run, for the latest run of a script or workspace if script is not provided"""
    try:
        run_id = _to_uuid(script_name_or_run_id)
    except RuntimeError:
        run = _get_latest_run(api_client, auth_service, script_name_or_run_id)
        run_id = run.id

    get_run_logs_result = get_run_logs.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        run_id=run_id,
    )
    if isinstance(get_run_logs_result.parsed, get_run_logs.LogsResponse):
        run = get_run_logs_result.parsed.run
        run_info = (
            f"Run # {run.number} of script {run.script.name}, status: {run.status}, run id:"
            f" {run.id}"
        )
        fmt.echo(f"========== Run logs for {run_info} ==========")
        fmt.echo(get_run_logs_result.parsed.logs)
        fmt.echo(f"========== End of run logs for {run_info} ==========")
    else:
        raise _exception_from_response("Failed to get run logs.", get_run_logs_result)


def get_runs(
    script_name: str = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if script_name:
        try:
            _to_uuid(script_name)
            raise CliCommandInnerException(
                cmd="runtime",
                msg=(
                    "UUID run id provided instead of script name for dlt runtime runs <SCRIPT_NAME>"
                    " list command"
                ),
                inner_exc=None,
            )
        except RuntimeError:
            pass

    script_id = None
    if script_name:
        script = get_script.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            script_id_or_name=script_name,
        )
        if isinstance(script.parsed, get_script.ScriptResponse):
            script_id = script.parsed.id
        else:
            raise _exception_from_response(
                f"Failed to get script with name {script_name} from runtime. Did you create one?",
                script,
            )

    list_runs_result = list_runs.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id=script_id,
    )
    if isinstance(list_runs_result.parsed, list_runs.ListRunsResponse200):
        for run in reversed(list_runs_result.parsed.items or []):
            fmt.echo(
                f"Run # {run.number} of script {run.script.name}, status: {run.status}, profile:"
                f" {run.profile}, started at {run.time_started}, ended at {run.time_ended},"
                f" run id: {run.id}"
            )
    else:
        raise _exception_from_response("Failed to list workspace runs", list_runs_result)


def get_deployments(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    list_deployments_result = list_deployments.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(list_deployments_result.parsed, list_deployments.ListDeploymentsResponse200):
        if not list_deployments_result.parsed.items:
            fmt.echo("No deployments found in this workspace")
            return
        for deployment in reversed(list_deployments_result.parsed.items):
            fmt.echo(
                f"Deployment # {deployment.version}, created at: {deployment.date_added}, id:"
                f" {deployment.id}, file count: {deployment.file_count}, content hash:"
                f" {deployment.content_hash}"
            )
    else:
        raise _exception_from_response("Failed to list deployments", list_deployments_result)


def get_deployment_info(
    deployment_id: str, *, auth_service: RuntimeAuthService, api_client: ApiClient
) -> None:
    get_deployment_result = get_deployment.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        deployment_id_or_version=_to_uuid(deployment_id),
    )
    if isinstance(get_deployment_result.parsed, get_deployment.DeploymentResponse):
        fmt.echo(f"Deployment # {get_deployment_result.parsed.version}")
        fmt.echo(f"Created at: {get_deployment_result.parsed.date_added}")
        fmt.echo(f"Deployment id: {get_deployment_result.parsed.id}")
        fmt.echo(f"File count: {get_deployment_result.parsed.file_count}")
        fmt.echo(f"Content hash: {get_deployment_result.parsed.content_hash}")
    else:
        raise _exception_from_response("Failed to get deployment info", get_deployment_result)


def request_run_cancel(
    script_name_or_run_id: str = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    """Request the cancellation of a run, for a script or workspace if script is not provided"""
    try:
        run_id = _to_uuid(script_name_or_run_id)
    except RuntimeError:
        run = _get_latest_run(api_client, auth_service, script_name_or_run_id)
        run_id = run.id

    cancel_run_result = cancel_run.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        run_id=_to_uuid(run_id),
    )
    if isinstance(cancel_run_result.parsed, cancel_run.DetailedRunResponse):
        fmt.echo(f"Successfully requested cancellation of run {run_id}")
    else:
        raise _exception_from_response("Failed to request cancellation of run", cancel_run_result)


def get_scripts(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    list_scripts_result = list_scripts.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(list_scripts_result.parsed, list_scripts.ListScriptsResponse200) and isinstance(
        list_scripts_result.parsed.items, list
    ):
        for script in reversed(list_scripts_result.parsed.items):
            fmt.echo(
                f"Script {script.name}, created at: {script.date_added}, id: {script.id}, version"
                f" #: {script.version}"
            )
    else:
        raise _exception_from_response("Failed to list scripts", list_scripts_result)


def get_script_info(
    script_id_or_name: str, *, auth_service: RuntimeAuthService, api_client: ApiClient
) -> None:
    get_script_result = get_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id_or_name=script_id_or_name,
    )

    if isinstance(get_script_result.parsed, get_script.ScriptResponse):
        fmt.echo(
            f"Script {get_script_result.parsed.name}, created at:"
            f" {get_script_result.parsed.date_added}, id: {get_script_result.parsed.id}, version #:"
            f" {get_script_result.parsed.version}"
        )
    else:
        raise _exception_from_response("Failed to get script info", get_script_result)


def _get_latest_run(
    api_client: ApiClient, auth_service: RuntimeAuthService, script_id_or_name: str = None
) -> DetailedRunResponse:
    """Get the latest run for a script or workspace if script is not provided"""
    if script_id_or_name:
        script = get_script.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            script_id_or_name=script_id_or_name,
        )
        if isinstance(script.parsed, get_script.ScriptResponse):
            fmt.echo(f"Script {script.parsed.name} found on runtime.")
            runs = list_runs.sync_detailed(
                client=api_client,
                workspace_id=_to_uuid(auth_service.workspace_id),
                script_id=script.parsed.id,
                limit=1,
            )
            if isinstance(runs.parsed, list_runs.ListRunsResponse200):
                if not runs.parsed.items:
                    raise _exception_from_response("No runs executed in for this script", runs)
                else:
                    return runs.parsed.items[0]
            raise _exception_from_response(
                f"Failed to get runs for script with name or id {script_id_or_name}", runs
            )
        else:
            raise _exception_from_response(
                f"Failed to get script with name or id {script_id_or_name}", script
            )

    else:
        runs = list_runs.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            limit=1,
        )
        if isinstance(runs.parsed, list_runs.ListRunsResponse200):
            if not runs.parsed.items:
                raise _exception_from_response("No runs executed in this workspace", runs)
            else:
                return runs.parsed.items[0]
        raise _exception_from_response("Failed to get runs for workspace", runs)


def get_configurations(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    list_configurations_result = list_configurations.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(
        list_configurations_result.parsed, list_configurations.ListConfigurationsResponse200
    ) and isinstance(list_configurations_result.parsed.items, list):
        for configuration in reversed(list_configurations_result.parsed.items):
            fmt.echo(
                f"Configuration # {configuration.version}, created at: {configuration.date_added},"
                f" version id: {configuration.id}"
            )
    else:
        raise _exception_from_response("Failed to list configurations", list_configurations_result)


def get_configuration_info(
    configuration_id: str, *, auth_service: RuntimeAuthService, api_client: ApiClient
) -> None:
    get_configuration_result = get_configuration.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        configuration_id_or_version=_to_uuid(configuration_id),
    )
    if isinstance(get_configuration_result.parsed, get_configuration.ConfigurationResponse):
        fmt.echo(
            f"Configuration # {get_configuration_result.parsed.version}, created at:"
            f" {get_configuration_result.parsed.date_added}, version id:"
            f" {get_configuration_result.parsed.id}"
        )
    else:
        raise _exception_from_response("Failed to get configuration info", get_configuration_result)
