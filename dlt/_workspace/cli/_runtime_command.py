import argparse
import time
import webbrowser
from io import BytesIO
from pathlib import Path
from typing import Any, Optional, Set, Union
from uuid import UUID

from cron_descriptor import FormatException, get_description

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
from dlt._workspace.runtime_clients.api.models.run_status import RunStatus
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


def _check_cron_expression(cron_expression: Optional[str]) -> None:
    if cron_expression:
        try:
            get_description(cron_expression)
        except FormatException as exc:
            raise CliCommandInnerException(
                cmd="runtime",
                msg=f"Invalid cron expression: {cron_expression} ({exc})",
                inner_exc=exc,
            )


def login(minimal_logging: bool = True) -> RuntimeAuthService:
    auth_service = RuntimeAuthService(run_context=active())
    try:
        auth_info = auth_service.authenticate()
        if not minimal_logging:
            fmt.echo("Already logged in as %s" % fmt.bold(auth_info.email))
        connect(auth_service=auth_service, minimal_logging=minimal_logging)
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


def deploy(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    sync_deployment(auth_service=auth_service, api_client=api_client)
    sync_configuration(auth_service=auth_service, api_client=api_client)
    fmt.echo("Deployment and configuration synchronized successfully")


def sync_deployment(
    minimal_logging: bool = True, *, auth_service: RuntimeAuthService, api_client: ApiClient
) -> None:
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
            if not minimal_logging:
                fmt.echo("No changes detected in the deployment, skipping file upload")
            content_stream.close()
            return
    elif isinstance(latest_deployment.parsed, get_latest_deployment.ErrorResponse404):
        if not minimal_logging:
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
        if not minimal_logging:
            fmt.echo(f"Deployment # {create_deployment_result.parsed.version} created successfully")
            fmt.echo(f"File count: {create_deployment_result.parsed.file_count}")
            fmt.echo(f"Content hash: {create_deployment_result.parsed.content_hash}")
    else:
        raise _exception_from_response("Failed to create deployment", create_deployment_result)


def sync_configuration(
    minimal_logging: bool = True, *, auth_service: RuntimeAuthService, api_client: ApiClient
) -> None:
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
            if not minimal_logging:
                fmt.echo("No changes detected in the configuration, skipping file upload")
            content_stream.close()
            return
    elif isinstance(latest_configuration.parsed, get_latest_configuration.ErrorResponse404):
        if not minimal_logging:
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
        if not minimal_logging:
            fmt.echo(
                f"Configuration # {create_configuration_result.parsed.version} created successfully"
            )
            fmt.echo(f"File count: {create_configuration_result.parsed.file_count}")
            fmt.echo(f"Content hash: {create_configuration_result.parsed.content_hash}")
    else:
        raise _exception_from_response(
            "Failed to create configuration", create_configuration_result
        )


def get_job_run_info(
    script_path_or_job_name: Optional[str] = None,
    run_number: Optional[int] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if script_path_or_job_name is None:
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Script path or job name is required",
            inner_exc=None,
        )
    if run_number is None:
        run = _get_latest_run(api_client, auth_service, script_path_or_job_name)
        run_id = run.id
    else:
        run_id = _resolve_run_id_by_number(
            api_client=api_client,
            auth_service=auth_service,
            script_path_or_job_name=script_path_or_job_name,
            run_number=run_number,
        )

    get_run_result = get_run.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        run_id=_to_uuid(run_id),
    )
    if isinstance(get_run_result.parsed, get_run.DetailedRunResponse):
        fmt.echo(f"Job: {get_run_result.parsed.script.name}")
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
    script_path_or_job_name: Optional[str] = None,
    run_number: Optional[int] = None,
    follow: bool = False,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    """Get logs for a run of job (latest if run number not provided)."""
    if script_path_or_job_name is None:
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Script path or job name is required",
            inner_exc=None,
        )
    if run_number is None:
        run = _get_latest_run(api_client, auth_service, script_path_or_job_name)
        run_id = run.id
    else:
        run_id = _resolve_run_id_by_number(
            api_client=api_client,
            auth_service=auth_service,
            script_path_or_job_name=script_path_or_job_name,
            run_number=run_number,
        )

    if follow:
        follow_job_run(
            run_id,
            {RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.COMPLETED},
            None,
            True,
            auth_service=auth_service,
            api_client=api_client,
        )
    else:
        get_run_logs_result = get_run_logs.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            run_id=run_id,
        )
        if isinstance(get_run_logs_result.parsed, get_run_logs.LogsResponse):
            run = get_run_logs_result.parsed.run
            run_info = f"Run # {run.number} of job {run.script.name}"
            fmt.echo(f"========== Run logs for {run_info} ==========")
            fmt.echo(get_run_logs_result.parsed.logs)
            fmt.echo(f"========== End of run logs for {run_info} ==========")
        else:
            raise _exception_from_response("Failed to get run logs.", get_run_logs_result)


def get_runs(
    script_path_or_job_name: Optional[str] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    script_id: Optional[UUID] = None
    if script_path_or_job_name:
        script = get_script.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            script_id_or_name=script_path_or_job_name,
        )
        if isinstance(script.parsed, get_script.DetailedScriptResponse):
            script_id = script.parsed.id
        else:
            raise _exception_from_response(
                f"Failed to get script with name {script_path_or_job_name} from runtime. Did you"
                " create one?",
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
                f"Run # {run.number} of job {run.script.name}, status: {run.status}, profile:"
                f" {run.profile}, started at {run.time_started}, ended at {run.time_ended}"
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
                f"Deployment # {deployment.version}, created at: {deployment.date_added}, "
                f"file count: {deployment.file_count}, content hash: {deployment.content_hash}"
            )
    else:
        raise _exception_from_response("Failed to list deployments", list_deployments_result)


def get_deployment_info(
    deployment_version_no: Optional[int] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if deployment_version_no is None:
        get_deployment_result = get_latest_deployment.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
        )
    else:
        get_deployment_result = get_deployment.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            deployment_id_or_version=deployment_version_no,
        )
    if isinstance(get_deployment_result.parsed, get_deployment.DeploymentResponse):
        fmt.echo(f"Deployment # {get_deployment_result.parsed.version}")
        fmt.echo(f"Created at: {get_deployment_result.parsed.date_added}")
        fmt.echo(f"File count: {get_deployment_result.parsed.file_count}")
        fmt.echo(f"Content hash: {get_deployment_result.parsed.content_hash}")
    else:
        raise _exception_from_response("Failed to get deployment info", get_deployment_result)


def request_run_cancel(
    script_path_or_job_name: Optional[str] = None,
    run_number: Optional[int] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    """Request the cancellation of a run, for a script or workspace if script is not provided"""
    if script_path_or_job_name is None:
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Script path or job name is required",
            inner_exc=None,
        )
    if run_number is None:
        run = _get_latest_run(api_client, auth_service, script_path_or_job_name)
        run_id = run.id
    else:
        run_id = _resolve_run_id_by_number(
            api_client=api_client,
            auth_service=auth_service,
            script_path_or_job_name=script_path_or_job_name,
            run_number=run_number,
        )

    cancel_run_result = cancel_run.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        run_id=_to_uuid(run_id),
    )
    if isinstance(cancel_run_result.parsed, cancel_run.DetailedRunResponse):
        fmt.echo(f"Successfully requested cancellation of run # {run.number}")
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
                f"Script {script.name}, created at: {script.date_added}, version"
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

    if isinstance(get_script_result.parsed, get_script.DetailedScriptResponse):
        fmt.echo(
            f"Script {get_script_result.parsed.name}, created at:"
            f" {get_script_result.parsed.date_added}, version #:"
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
        if isinstance(script.parsed, get_script.DetailedScriptResponse):
            fmt.echo(f"Job {script.parsed.name} found on runtime.")
            runs = list_runs.sync_detailed(
                client=api_client,
                workspace_id=_to_uuid(auth_service.workspace_id),
                script_id=script.parsed.id,
                limit=1,
            )
            if isinstance(runs.parsed, list_runs.ListRunsResponse200):
                if not runs.parsed.items:
                    raise _exception_from_response("No runs executed in for this job", runs)
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
                f" file count: {configuration.file_count}, content hash:"
                f" {configuration.content_hash}"
            )
    else:
        raise _exception_from_response("Failed to list configurations", list_configurations_result)


def get_configuration_info(
    configuration_version_no: Optional[int] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if configuration_version_no is None:
        get_configuration_result = get_latest_configuration.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
        )
    else:
        get_configuration_result = get_configuration.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            configuration_id_or_version=configuration_version_no,
        )
    if isinstance(get_configuration_result.parsed, get_configuration.ConfigurationResponse):
        fmt.echo(f"Configuration # {get_configuration_result.parsed.version}")
        fmt.echo(f"Created at: {get_configuration_result.parsed.date_added}")
        fmt.echo(f"File count: {get_configuration_result.parsed.file_count}")
        fmt.echo(f"Content hash: {get_configuration_result.parsed.content_hash}")
    else:
        raise _exception_from_response("Failed to get configuration info", get_configuration_result)


def _ensure_profile_warning(required_profile: str) -> bool:
    """Warn if recommended profile is not set up."""
    try:
        ctx = active()
        available = set(ctx.available_profiles())
        if required_profile not in available:
            if required_profile == "access":
                fmt.warning(
                    "No 'access' profile detected. Only default config/secrets will be used. "
                    "Dashboard/notebook sharing may be limited."
                )
            elif required_profile == "prod":
                fmt.warning("No 'prod' profile detected. Only default config/secrets will be used.")
            return False
        return True
    except Exception:
        # Fallback silent; lack of profiles is non-fatal
        return False


def _resolve_run_id_by_number(
    *,
    api_client: ApiClient,
    auth_service: RuntimeAuthService,
    script_path_or_job_name: str,
    run_number: int,
) -> UUID:
    script = get_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id_or_name=script_path_or_job_name,
    )
    if not isinstance(script.parsed, get_script.DetailedScriptResponse):
        raise _exception_from_response(
            f"Failed to get script with name or id {script_path_or_job_name}", script
        )
    runs = list_runs.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id=script.parsed.id,
    )
    if not isinstance(runs.parsed, list_runs.ListRunsResponse200) or not runs.parsed.items:
        raise _exception_from_response("Failed to get runs for script", runs)
    for r in runs.parsed.items:
        if r.number == run_number:
            return r.id
    raise CliCommandInnerException(
        cmd="runtime",
        msg=f"Run number {run_number} not found for script/job {script_path_or_job_name}",
        inner_exc=None,
    )


# Convenience commands


def launch(
    script_path: str,
    detach: bool = False,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    _ensure_profile_warning("prod")
    # Sync and run
    sync_deployment(auth_service=auth_service, api_client=api_client)
    sync_configuration(auth_service=auth_service, api_client=api_client)
    run_id = run_script(
        script_path,
        is_interactive=False,
        auth_service=auth_service,
        api_client=api_client,
    )
    if not detach:
        # Show status and then logs for latest run
        follow_run_status(run_id, True, auth_service=auth_service, api_client=api_client)
        follow_run_logs(run_id, auth_service=auth_service, api_client=api_client)


def serve(script_path: str, *, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    _ensure_profile_warning("access")
    # Try to detect marimo notebook
    try:
        script_path_obj = Path(active().run_dir) / script_path
        if script_path_obj.exists() and script_path_obj.is_file():
            content = script_path_obj.read_text(encoding="utf-8", errors="ignore")
            if "import marimo" not in content and "from marimo" not in content:
                fmt.warning(
                    "Could not detect a marimo notebook in the provided script. "
                    "Proceeding to serve as an interactive app."
                )
        else:
            raise CliCommandInnerException(
                cmd="runtime",
                msg="Provided script path does not exist locally",
                inner_exc=None,
            )
    except Exception as e:
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Failed to read script file",
            inner_exc=e,
        )

    # Sync and run interactive
    sync_deployment(auth_service=auth_service, api_client=api_client)
    sync_configuration(auth_service=auth_service, api_client=api_client)
    run_id = run_script(
        script_path,
        is_interactive=True,
        auth_service=auth_service,
        api_client=api_client,
    )
    # Follow until ready: show status
    follow_run_status(run_id, False, auth_service=auth_service, api_client=api_client)

    # Open the application URL
    try:
        res = get_script.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            script_id_or_name=script_path,
        )
        if isinstance(res.parsed, get_script.DetailedScriptResponse):
            url = res.parsed.script_url
            fmt.echo(f"Opening {url}")
            import webbrowser

            webbrowser.open(url, new=2, autoraise=True)
    except Exception:
        # Non-fatal if we cannot resolve or open URL
        fmt.warning(f"Failed to open application URL for script {script_path}")


def run_script(
    script_file_name: str,
    is_interactive: bool = False,
    profile: Optional[str] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> UUID:
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
        fmt.echo(
            f"Job {script_file_name} created or updated successfully, version #:"
            f" {create_script_result.parsed.version}"
        )

    create_run_result = create_run.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_run.CreateRunRequest(
            script_id_or_name=script_file_name,
            profile=None,
        ),
    )
    if isinstance(create_run_result.parsed, create_run.RunResponse):
        fmt.echo("Job %s run successfully" % (fmt.bold(str(script_file_name))))
        if is_interactive:
            url = create_script_result.parsed.script_url
            fmt.echo(f"Job is accessible on {url}")
        return create_run_result.parsed.id
    else:
        raise _exception_from_response("Failed to run script", create_run_result)


def follow_run_status(
    run_id: UUID, is_batch: bool, *, auth_service: RuntimeAuthService, api_client: ApiClient
) -> None:
    final_states = {RunStatus.FAILED, RunStatus.CANCELLED}
    if is_batch:
        final_states.add(RunStatus.STARTING)
    else:
        final_states.add(RunStatus.RUNNING)
    return follow_job_run(run_id, final_states, auth_service=auth_service, api_client=api_client)


def follow_run_logs(
    run_id: UUID, *, auth_service: RuntimeAuthService, api_client: ApiClient
) -> None:
    final_states = {RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.COMPLETED}
    return follow_job_run(
        run_id,
        final_states,
        RunStatus.STARTING,
        True,
        auth_service=auth_service,
        api_client=api_client,
    )


def follow_job_run(
    run_id: UUID,
    final_states: Set[RunStatus],
    start_status: Optional[RunStatus] = None,
    follow_logs: bool = False,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    status = start_status
    print_from_line_idx = 0

    if follow_logs:
        fmt.echo("========== Run logs ==========")
    while True:
        get_run_result = get_run.sync_detailed(
            client=api_client,
            workspace_id=_to_uuid(auth_service.workspace_id),
            run_id=run_id,
        )
        if not isinstance(get_run_result.parsed, get_run.DetailedRunResponse):
            raise _exception_from_response("Failed to get run info", get_run_result)
        new_status = get_run_result.parsed.status
        if new_status != status:
            if not follow_logs:
                fmt.echo(f"Run status: {new_status}")
            status = new_status

        if follow_logs:
            get_run_logs_result = get_run_logs.sync_detailed(
                client=api_client,
                workspace_id=_to_uuid(auth_service.workspace_id),
                run_id=run_id,
            )
            if not isinstance(get_run_logs_result.parsed, get_run_logs.LogsResponse):
                raise _exception_from_response("Failed to get run logs", get_run_logs_result)
            if isinstance(get_run_logs_result.parsed.logs, str):
                log_lines = get_run_logs_result.parsed.logs.split("\n")
                for line in log_lines[print_from_line_idx:]:
                    fmt.echo(line)
                print_from_line_idx = len(log_lines)

        if status in final_states:
            if follow_logs:
                fmt.echo("========== End of run logs ==========")
                fmt.echo(f"Run status: {new_status}")
            break
        time.sleep(2)


def schedule(
    script_path: str,
    cron: Optional[str],
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if not cron:
        raise CliCommandInnerException(
            cmd="runtime",
            msg=(
                "Cron schedule must be provided: dlt runtime schedule <SCRIPT_PATH> <SCHEDULE_CRON>"
            ),
            inner_exc=None,
        )
    _check_cron_expression(cron)
    _ensure_profile_warning("prod")

    # Ensure deployment/configuration in place
    sync_deployment(auth_service=auth_service, api_client=api_client)
    sync_configuration(auth_service=auth_service, api_client=api_client)

    # Upsert script with schedule
    upsert = create_or_update_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_or_update_script.CreateScriptRequest(
            name=script_path,
            description=f"The {script_path} scheduled job",
            entry_point=script_path,
            script_type=ScriptType.BATCH,
            schedule=cron,
        ),
    )
    if isinstance(upsert.parsed, create_or_update_script.ScriptResponse):
        fmt.echo(
            f"Scheduled {fmt.bold(script_path)} with cron {fmt.bold(cron)}. Job version #:"
            f" {upsert.parsed.version}"
        )
    else:
        raise _exception_from_response("Failed to schedule script", upsert)


def schedule_cancel(
    script_path: str,
    cancel_current: bool = False,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    existing_script = get_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id_or_name=script_path,
    )
    if isinstance(existing_script.parsed, get_script.DetailedScriptResponse):
        if not isinstance(existing_script.parsed.schedule, str):
            fmt.error(f"{script_path} is not a scheduled job")
            return
    else:
        raise _exception_from_response("Failed to get job", existing_script)

    # Unset schedule
    upsert = create_or_update_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_or_update_script.CreateScriptRequest(
            name=script_path,
            description=f"The {script_path} job",
            entry_point=script_path,
            script_type=ScriptType.BATCH,
            schedule=None,
        ),
    )
    if isinstance(upsert.parsed, create_or_update_script.ScriptResponse):
        fmt.echo(f"Cancelled schedule for {fmt.bold(script_path)}")
    else:
        raise _exception_from_response("Failed to cancel schedule", upsert)
    if cancel_current:
        try:
            request_run_cancel(script_path, auth_service=auth_service, api_client=api_client)
        except CliCommandInnerException as e:
            if "terminal state" not in e.args[0]:
                raise e


def open_dashboard(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    job = get_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id_or_name="dashboard",
    )
    if isinstance(job.parsed, get_script.DetailedScriptResponse):
        if not job.parsed.script_url:
            fmt.error("Failed to get the URL for the dashboard")
            return
        fmt.echo(f"Dashboard is available at {job.parsed.script_url}")
        webbrowser.open(job.parsed.script_url)
    else:
        raise _exception_from_response("Failed to get dashboard job", job)


def runtime_info(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    # jobs
    scr = list_scripts.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    job_count = (
        len(scr.parsed.items)
        if isinstance(scr.parsed, list_scripts.ListScriptsResponse200) and scr.parsed.items
        else 0
    )
    fmt.echo(f"# registered jobs: {job_count}. Run `dlt runtime job list` to see all")

    # last job run

    latest_run = _get_latest_run(api_client, auth_service)
    if isinstance(latest_run, DetailedRunResponse):
        fmt.echo(
            f"Latest job run: {latest_run.script.name} ({latest_run.status}), started at"
            f" {latest_run.time_started}, ended at {latest_run.time_ended}"
        )
    else:
        raise _exception_from_response("Failed to get latest run", latest_run)

    # deployments
    latest_deployment = get_latest_deployment.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(latest_deployment.parsed, get_latest_deployment.DeploymentResponse):
        fmt.echo(
            f"Current deployment version: {latest_deployment.parsed.version}, last updated at"
            f" {latest_deployment.parsed.date_added}. Run `dlt runtime deployment info` to see"
            " detailed deployment information"
        )
    else:
        raise _exception_from_response("Failed to get latest deployment", latest_deployment)

    # configurations
    latest_configuration = get_latest_configuration.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(latest_configuration.parsed, get_latest_configuration.ConfigurationResponse):
        fmt.echo(
            f"Current configuration version: {latest_configuration.parsed.version}, last updated at"
            f" {latest_configuration.parsed.date_added}. Run `dlt runtime configuration info` to"
            " see detailed configuration information"
        )
    else:
        raise _exception_from_response("Failed to get latest configuration", latest_configuration)


# Power user: jobs and job-runs


def jobs_list(*, auth_service: RuntimeAuthService, api_client: ApiClient) -> None:
    res = list_scripts.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
    )
    if isinstance(res.parsed, list_scripts.ListScriptsResponse200) and isinstance(
        res.parsed.items, list
    ):
        for s in reversed(res.parsed.items):
            fmt.echo(f"Job {s.name}, created at: {s.date_added}, version #: {s.version}")
    else:
        raise _exception_from_response("Failed to list jobs", res)


def job_info(
    script_path_or_job_name: Optional[str] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if not script_path_or_job_name:
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Script path or job name is required",
            inner_exc=None,
        )
    res = get_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id_or_name=script_path_or_job_name,
    )
    if isinstance(res.parsed, get_script.DetailedScriptResponse):
        fmt.echo(
            f"Job {res.parsed.name}, created at: {res.parsed.date_added},"
            f" version #: {res.parsed.version}"
        )
    else:
        raise _exception_from_response("Failed to get job info", res)


def job_create(
    script_path: Optional[str],
    args: argparse.Namespace,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if not script_path:
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Script path is required to be a first argument",
            inner_exc=None,
        )
    script_path_obj = Path(active().run_dir) / script_path
    if not script_path_obj.exists():
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Script path is required to be a first argument. Provided path does not exist",
            inner_exc=None,
        )
    if args.schedule:
        _check_cron_expression(args.schedule)
    job_name = args.name or script_path
    job_type = ScriptType.INTERACTIVE if args.interactive else ScriptType.BATCH
    job_description = args.description or f"The {job_name} job"

    # warn if the job exists already with different parameters
    res = get_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        script_id_or_name=job_name,
    )
    if isinstance(res.parsed, get_script.DetailedScriptResponse):
        if args.name and res.parsed.entry_point != script_path:
            fmt.warning(
                f"Warning: Job {job_name} already exists for different script path"
                f" ({res.parsed.entry_point} -> {script_path}). Overwriting..."
            )
        elif res.parsed.schedule != args.schedule:
            fmt.warning(
                f"Warning: Job {job_name} already exists with different schedule"
                f" ({res.parsed.schedule} -> {args.schedule}). Overwriting..."
            )
        elif res.parsed.script_type != job_type:
            fmt.warning(
                f"Warning: Job {job_name} already exists with different interactive mode"
                f" ({res.parsed.script_type} -> {job_type}). Overwriting..."
            )

    upsert = create_or_update_script.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_or_update_script.CreateScriptRequest(
            name=job_name,
            description=job_description,
            entry_point=script_path,
            script_type=job_type,
            profile=None,
            schedule=args.schedule,
        ),
    )
    if isinstance(upsert.parsed, create_or_update_script.ScriptResponse):
        fmt.echo(f"Job {fmt.bold(script_path)} created, version #: {upsert.parsed.version}")
    else:
        raise _exception_from_response("Failed to create job", upsert)


def create_job_run(
    script_path_or_job_name: Optional[str] = None,
    *,
    auth_service: RuntimeAuthService,
    api_client: ApiClient,
) -> None:
    if script_path_or_job_name is None:
        raise CliCommandInnerException(
            cmd="runtime",
            msg="Script path or job name is required",
            inner_exc=None,
        )
    res = create_run.sync_detailed(
        client=api_client,
        workspace_id=_to_uuid(auth_service.workspace_id),
        body=create_run.CreateRunRequest(
            script_id_or_name=script_path_or_job_name,
            profile=None,
        ),
    )
    if isinstance(res.parsed, create_run.RunResponse):
        fmt.echo(f"Run started for {fmt.bold(script_path_or_job_name)}")
    else:
        raise _exception_from_response("Failed to start run", res)
