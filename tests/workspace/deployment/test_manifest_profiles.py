"""Manifests generated for an environment. Every job gets the profile of the environment with the
data access it declares. `__deployment__.py` is imported with the default write profile. The
isolated worker gets the environment from the caller and reports its failures."""

from contextlib import contextmanager
import importlib
import os
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import pytest

from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.deployment.exceptions import (
    JobProfileAccessMismatch,
    JobProfileNotInEnvironment,
    ProfileDataAccessNotAvailable,
)
from dlt._workspace.deployment.manifest import manifest_from_module
from dlt._workspace.deployment.typing import DASHBOARD_JOB_REF, TJobsDeploymentManifest
from dlt._workspace.environment import activate_environment, add_environments, new_environment
from dlt._workspace.profile import add_profiles
from dlt._workspace.settings_files import create_profile_settings
from dlt._workspace.typing import TWorkspaceAccess
from dlt._workspace.workspace_definition import default_profile, load_workspace_definition

from tests.workspace.manifest_utils import job_profiles
from tests.workspace.utils import (
    empty_workspace_file,
    importable_workspace,
    save_definition,
    touch_settings,
)

WORKSPACE = "job_profiles"
MODULES = ("__deployment__", "jobs")
"""Modules of the case workspace, unloaded after each copy so the next profile imports afresh."""

TJobProfiles = Dict[str, Tuple[Optional[str], TWorkspaceAccess]]
"""The profile and the access of each job of a manifest, by job ref."""

READ: TWorkspaceAccess = {"data": ["read"]}
READ_WRITE: TWorkspaceAccess = {"data": ["read", "write"]}
JOBS: Dict[str, TJobProfiles] = {
    "prod": {
        "jobs.jobs.nightly": ("prod", READ_WRITE),
        "jobs.jobs.api": ("access", READ),
        "jobs.jobs.freshness": ("access", READ),
        "jobs.jobs.editor": ("prod", READ_WRITE),
        # `prod` declares no profile without data access
        "jobs.jobs.triage": ("access", {}),
        "jobs.jobs.writer": ("prod", {"data": ["write"]}),
        "jobs.jobs.declared": ("prod", READ_WRITE),
        "jobs.jobs.reporting": ("analytics", READ_WRITE),
        # the workspace dashboard goes to the default environment only
        DASHBOARD_JOB_REF: ("access", READ),
    },
    "ci": {
        "jobs.jobs.nightly": ("ci-write", READ_WRITE),
        "jobs.jobs.api": ("ci-read", READ),
        "jobs.jobs.freshness": ("ci-read", READ),
        "jobs.jobs.editor": ("ci-write", READ_WRITE),
        "jobs.jobs.triage": ("ci-none", {}),
        "jobs.jobs.writer": ("ci-write", {"data": ["write"]}),
        "jobs.jobs.ci_only": ("ci-write", READ_WRITE),
    },
}
"""What `__deployment__.py` of the case workspace deploys to each of its environments."""


def _profiles_and_access(manifest: TJobsDeploymentManifest) -> TJobProfiles:
    profiles = job_profiles(manifest)
    return {j["job_ref"]: (profiles[j["job_ref"]], j["access"]) for j in manifest["jobs"]}


@pytest.mark.parametrize("isolated", [False, True], ids=["inprocess", "isolated"])
@pytest.mark.parametrize(
    "env,fallbacks",
    [("prod", ["jobs.jobs.triage"]), ("ci", [])],
    ids=["not-strict-environment", "strict-environment"],
)
def test_manifest_for_environment(env: str, fallbacks: List[str], isolated: bool) -> None:
    """`prod` is not strict: a job without data access gets `access`, with a warning. `ci` has a
    profile for every data access and deploys `ci_only` too. The caller activates the
    environment, so `__deployment__.py` is imported with the default write profile."""
    with importable_workspace(WORKSPACE, *MODULES):
        environment = activate_environment(env)
        manifest, warnings = manifest_from_module("__deployment__", environment, isolated=isolated)
    assert manifest["environment"] == env
    assert _profiles_and_access(manifest) == JOBS[env]
    assert len(warnings) == len(fallbacks)
    for job_ref, warning in zip(fallbacks, warnings):
        assert f"job `{job_ref}`" in warning


def test_manifest_without_environment() -> None:
    """A local run generates without an environment: the jobs keep the profiles they declare and
    the manifest records no environment."""
    with importable_workspace(WORKSPACE, *MODULES, profile="prod"):
        manifest, warnings = manifest_from_module("__deployment__", None)
    assert "environment" not in manifest
    assert warnings == []
    declared = {"jobs.jobs.declared": "prod", "jobs.jobs.reporting": "analytics"}
    assert _profiles_and_access(manifest) == {
        job_ref: (declared.get(job_ref), access) for job_ref, (_, access) in JOBS["prod"].items()
    }


@pytest.mark.parametrize("isolated", [False, True], ids=["inprocess", "isolated"])
def test_manifest_strict_environment_without_profiles(isolated: bool) -> None:
    with importable_workspace(WORKSPACE, *MODULES) as ctx:
        # a strict environment takes only profiles with settings files
        create_profile_settings(ctx, ["solo-write"])
        add_environments(ctx, {"solo": new_environment("solo")})
        environment = activate_environment("solo")
        with pytest.raises(ProfileDataAccessNotAvailable) as exc:
            manifest_from_module("__deployment__", environment, isolated=isolated)
        # the message names the job and the fixes
        message = str(exc.value)
        assert "Job `jobs.jobs.api` declares read data access." in message
        assert "dlthub profile add solo-read --access read --env solo" in message
        assert "strict_access: false" in message


def test_new_environment_deploys_batch_jobs() -> None:
    """A new environment is strict and has only its default write profile. It is not the default
    environment and gets no workspace dashboard, so a deployment of batch jobs needs no other
    profile."""
    with importable_workspace("deployment_mixed", "__deployment__", "plain") as ctx:
        # `add-environment` creates the settings files of `ci-write`, `access` gets its own
        create_profile_settings(ctx, ["ci-write", "access"])
        add_environments(ctx, {"ci": new_environment("ci")})
        manifest, warnings = manifest_from_module("__deployment__", activate_environment("ci"))
        assert warnings == []
        assert job_profiles(manifest) == {
            "jobs.__deployment__.batch_one": "ci-write",
            "jobs.__deployment__.load_fruitshop": "ci-write",
            "jobs.plain": "ci-write",
        }
        # the default environment gets the dashboard, with the profile that has read data access
        manifest, _ = manifest_from_module("__deployment__", activate_environment("prod"))
        assert job_profiles(manifest)[DASHBOARD_JOB_REF] == "access"


@pytest.mark.parametrize("isolated", [False, True], ids=["inprocess", "isolated"])
def test_job_profile_not_in_environment(isolated: bool) -> None:
    """A job that declares a profile of `prod` cannot be deployed to `ci`."""
    with importable_workspace(WORKSPACE, *MODULES):
        environment = activate_environment("ci")
        with pytest.raises(JobProfileNotInEnvironment) as exc:
            manifest_from_module("jobs", environment, isolated=isolated, use_all=False)
        message = str(exc.value)
        assert "not a profile of environment `ci`" in message
        assert "ci-write, ci-read, ci-none" in message and "--environment" in message


DEPLOYMENT_BEFORE_PROFILES = '''"""Deployment of a workspace created before profiles were declared."""

from dlt.hub.run import job

__all__ = ["load", "report"]


@job
def load() -> int:
    return 1


@job(require={"profile": "%s"})
def report() -> int:
    return 2
'''
LOAD_REF = "jobs.__deployment__.load"
REPORT_REF = "jobs.__deployment__.report"


@contextmanager
def _workspace_before_profiles(
    job_profile: str, configured: List[str]
) -> Iterator[WorkspaceRunContext]:
    """A workspace created before profiles were declared: `.dlt/.workspace` is empty, the
    `configured` profiles have settings files and the `report` job of `__deployment__.py` declares
    `job_profile`."""
    with importable_workspace("empty", "__deployment__") as ctx:
        touch_settings(ctx, *(f"{name}.config.toml" for name in configured))
        with open(os.path.join(ctx.run_dir, "__deployment__.py"), "w", encoding="utf-8") as f:
            f.write(DEPLOYMENT_BEFORE_PROFILES % job_profile)
        importlib.invalidate_caches()
        # the copy migrated when it loaded: emptied, it migrates again with the files above
        empty_workspace_file(ctx)
        yield ctx


@pytest.mark.parametrize("isolated", [False, True], ids=["inprocess", "isolated"])
def test_job_profile_access_mismatch(isolated: bool) -> None:
    """A strict environment refuses a batch job that declares a profile with read data access."""
    with _workspace_before_profiles("access", ["prod", "access"]) as ctx:
        doc = load_workspace_definition(ctx)
        doc["environments"]["prod"]["strict_access"] = True
        save_definition(ctx, doc)
        environment = activate_environment("prod")
        with pytest.raises(JobProfileAccessMismatch) as exc:
            manifest_from_module("__deployment__", environment, isolated=isolated)
        message = str(exc.value)
        assert f"Job `{REPORT_REF}` declares profile `access` with read data access" in message
        assert "Environment `prod` is strict about access" in message


def _add_to_prod(ctx: WorkspaceRunContext, profile: str) -> str:
    """Declares `profile` like `dlthub profile add <profile> --access write --env prod`, with
    its settings files."""
    create_profile_settings(ctx, [profile])
    add_profiles(
        ctx, {profile: {"location": "remote", "data_access": "write", "env": "prod"}}, "prod"
    )
    return "prod"


def _move_to_prod(ctx: WorkspaceRunContext, profile: str) -> str:
    """Edits `.dlt/.workspace`: the declaration of `profile` moves from the environment of its
    own to the profiles of `prod`."""
    doc = load_workspace_definition(ctx)
    moved = doc["environments"].pop(profile)["profiles"][profile]
    moved["env"] = "prod"
    # `prod` stays the default profile with write data access
    del moved["default"]
    doc["environments"]["prod"]["profiles"][profile] = moved
    save_definition(ctx, doc)
    return "prod"


def _own_environment(ctx: WorkspaceRunContext, profile: str) -> str:
    """Changes nothing: the deployment goes to the environment of `profile`."""
    return ctx.get_profile_definition(profile)["env"]


@pytest.mark.parametrize(
    "job_profile,configured,fixes",
    [
        ("reports", [], [_add_to_prod]),
        # the first fix changes nothing, so the second one starts from the same workspace
        ("analytics", ["analytics"], [_own_environment, _move_to_prod]),
    ],
    ids=["undeclared-profile", "migrated-profile"],
)
def test_job_profile_after_migration(
    job_profile: str,
    configured: List[str],
    fixes: List[Callable[[WorkspaceRunContext, str], str]],
) -> None:
    """A workspace created before profiles were declared has a job that declares a profile. The
    migration makes no such profile a profile of `prod`: the deployment to `prod` fails. Each
    fix that the message names deploys the job with its profile."""
    with _workspace_before_profiles(job_profile, configured) as ctx:
        # the migration declares only a profile with settings files, as an environment of its own
        assert ctx.profile == "dev"
        assert list(ctx.environment_definitions) == ["prod", *configured]
        with pytest.raises(JobProfileNotInEnvironment) as exc:
            manifest_from_module("__deployment__", activate_environment("prod"))
        message = str(exc.value)
        assert f"Job `{REPORT_REF}` declares profile `{job_profile}`" in message
        assert "not a profile of environment `prod` (prod, access, no-access)" in message

        for fix in fixes:
            environment = activate_environment(fix(active(), job_profile))
            manifest, warnings = manifest_from_module("__deployment__", environment)
            assert manifest["environment"] == environment["name"]
            profiles = job_profiles(manifest)
            # `access` has no settings files: the dashboard of `prod` gets `prod`, with a warning
            in_prod = environment["name"] == "prod"
            assert profiles.pop(DASHBOARD_JOB_REF, None) == ("prod" if in_prod else None)
            assert [DASHBOARD_JOB_REF in warning for warning in warnings] == (
                [True] if in_prod else []
            )
            # a job that declares no profile gets the default write profile
            assert profiles == {
                LOAD_REF: default_profile(environment, "write"),
                REPORT_REF: job_profile,
            }
