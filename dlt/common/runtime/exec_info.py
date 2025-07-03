import io
import os
import contextlib
import sys
import multiprocessing
import platform

from dlt.common.runtime.typing import TExecutionContext, TVersion, TExecInfoNames
from dlt.common.typing import StrStr, StrAny, List
from dlt.common.utils import filter_env_vars
from dlt.version import __version__, DLT_PKG_NAME


# if one of these environment variables is set, we assume to be running in CI env
CI_ENVIRONMENT_TELL = [
    "bamboo.buildKey",
    "BUILD_ID",
    "BUILD_NUMBER",
    "BUILDKITE",
    "CI",
    "CIRCLECI",
    "CONTINUOUS_INTEGRATION",
    "GITHUB_ACTIONS",
    "HUDSON_URL",
    "JENKINS_URL",
    "TEAMCITY_VERSION",
    "TRAVIS",
    "CODEBUILD_BUILD_ARN",
    "CODEBUILD_BUILD_ID",
    "CODEBUILD_BATCH_BUILD_IDENTIFIER",
]


def exec_info_names() -> List[TExecInfoNames]:
    """Get names of execution environments"""
    names: List[TExecInfoNames] = []
    if kube_pod_info():
        names.append("kubernetes")
    if is_docker():
        names.append("docker")
    if is_codespaces():
        names.append("codespaces")
    if is_github_actions():
        names.append("github_actions")
    if is_notebook():
        names.append("notebook")
    if is_colab():
        names.append("colab")
    if is_running_in_airflow_task():
        names.append("airflow")
    if is_running_in_dagster_task():
        names.append("dagster")
    if is_running_in_prefect_flow():
        names.append("prefect")
    if is_marimo():
        names.append("marimo")
    if is_aws_lambda():
        names.append("aws_lambda")
    if is_gcp_cloud_function():
        names.append("gcp_cloud_function")
    if is_streamlit():
        names.append("streamlit")
    return names


def is_codespaces() -> bool:
    return "CODESPACES" in os.environ


def is_github_actions() -> bool:
    return "GITHUB_ACTIONS" in os.environ


def is_streamlit() -> bool:
    return "STREAMLIT_SERVER_PORT" in os.environ


def is_notebook() -> bool:
    try:
        return bool(str(get_ipython()))  # type: ignore
    except NameError:
        return False


def is_pyodide() -> bool:
    return sys.platform == "emscripten"


def platform_supports_threading() -> bool:
    return not is_pyodide()


def is_colab() -> bool:
    try:
        return "COLAB_RELEASE_TAG" in os.environ or "google.colab" in str(get_ipython())  # type: ignore
    except NameError:
        return False


def airflow_info() -> StrAny:
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            from airflow.operators.python import get_current_context

            get_current_context()
            return {"AIRFLOW_TASK": True}
    except Exception:
        return None


def is_airflow_installed() -> bool:
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            import airflow
        return True
    except Exception:
        return False


def is_running_in_airflow_task() -> bool:
    return "AIRFLOW_CTX_TASK_ID" in os.environ


def is_running_in_dagster_task() -> bool:
    # module must be imported in dagster task
    if dagster_ := sys.modules.get("dagster._core.execution.context.compute"):
        try:
            return dagster_.current_execution_context.get() is not None
        except Exception:
            pass
    return False


def is_running_in_prefect_flow() -> bool:
    # check if prefect module is imported must be the case if running in flow
    if pf := sys.modules.get("prefect"):
        get_ctx = getattr(getattr(pf, "context", None), "get_run_context", None)
        if callable(get_ctx):
            try:
                get_ctx()
                return True
            except Exception:
                pass
    return False


def is_marimo() -> bool:
    if marimo_ := sys.modules.get("marimo"):
        get_ctx = getattr(marimo_, "running_in_notebook", None)
        if callable(get_ctx):
            try:
                return get_ctx()  # type: ignore[no-any-return]
            except Exception:
                pass
    return False


def dlt_version_info(pipeline_name: str) -> StrStr:
    """Gets dlt version info including commit and image version available in docker"""
    version_info = {"dlt_version": __version__, "pipeline_name": pipeline_name}
    # extract envs with build info
    version_info.update(filter_env_vars(["COMMIT_SHA", "IMAGE_VERSION"]))

    return version_info


def kube_pod_info() -> StrStr:
    """Extracts information on pod name, namespace and node name if running on Kubernetes"""
    return filter_env_vars(["KUBE_NODE_NAME", "KUBE_POD_NAME", "KUBE_POD_NAMESPACE"])


def github_info() -> StrStr:
    """Extracts github info"""
    info = filter_env_vars(["GITHUB_USER", "GITHUB_REPOSITORY", "GITHUB_REPOSITORY_OWNER"])
    # set GITHUB_REPOSITORY_OWNER as github user if not present. GITHUB_REPOSITORY_OWNER is available in github action context
    if "github_user" not in info and "github_repository_owner" in info:
        info["github_user"] = info["github_repository_owner"]  # type: ignore
    return info


def in_continuous_integration() -> bool:
    """Returns `True` if currently running inside a continuous integration context."""
    return any(env in os.environ for env in CI_ENVIRONMENT_TELL)


def is_docker() -> bool:
    """Guess if we are running in docker environment.

    https://stackoverflow.com/questions/20010199/how-to-determine-if-a-process-runs-inside-lxc-docker

    Returns:
        `True` if we are running inside docker, `False` otherwise.
    """
    # first we try to use the env
    try:
        os.stat("/.dockerenv")
        return True
    except Exception:
        pass

    # if that didn't work, try to use proc information
    try:
        with open("/proc/self/cgroup", mode="r", encoding="utf-8") as f:
            return "docker" in f.read()
    except Exception:
        return False


def is_aws_lambda() -> bool:
    "Return True if the process is running in the serverless platform AWS Lambda"
    return os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None


def is_gcp_cloud_function() -> bool:
    "Return True if the process is running in the serverless platform GCP Cloud Functions"
    return os.environ.get("FUNCTION_NAME") is not None


def get_plus_version() -> TVersion:
    "Gets dlt+ library version"
    try:
        from dlt_plus.version import __version__, PKG_NAME

        return TVersion(name=PKG_NAME, version=__version__)
    except Exception:
        return None


def run_context_name() -> str:
    try:
        from dlt.common.configuration.container import Container
        from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext

        container = Container()
        if PluggableRunContext in container:
            return container[PluggableRunContext].context.name

    except Exception:
        pass

    return "dlt"


def get_execution_context() -> TExecutionContext:
    "Get execution context information"
    context = TExecutionContext(
        ci_run=in_continuous_integration(),
        python=sys.version.split(" ")[0],
        cpu=multiprocessing.cpu_count(),
        exec_info=exec_info_names(),
        os=TVersion(name=platform.system(), version=platform.release()),
        library=TVersion(name=DLT_PKG_NAME, version=__version__),
        run_context=run_context_name(),
    )
    if plus_version := get_plus_version():
        context["plus"] = plus_version

    return context
