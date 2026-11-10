"""The CI/CD pattern of a workspace with no shared `config.toml`: `prod.config.toml` and
`stg.config.toml` each hold the organization, the id and the name of one remote workspace. CI
names the remote workspace with `RUNTIME__WORKSPACE_ID` and deploys to the default environment:
`__deployment__.py` to the production workspace, or, with `stg.config.toml` renamed to
`prod.config.toml`, `__staging__.py` to the staging one. `__staging__.py` proxies
`__deployment__.py` with copies of its jobs. Locally both config files exist."""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence

import pytest

import dlt
from dlt.common.configuration.providers import CONFIG_TOML
from dlt.common.utils import custom_environ

from dlt._workspace._workspace_context import WorkspaceRunContext, switch_context
from dlt._workspace.deployment.manifest import manifest_from_module
from dlt._workspace.deployment.typing import DASHBOARD_JOB_REF
from dlt._workspace.environment import activate_environment
from dlt._workspace.settings_files import save_profile_pin

from tests.workspace.manifest_utils import job_profiles, shipped_settings
from tests.workspace.utils import empty_workspace_file, importable_workspace, isolated_workspace

WORKSPACE = "cicd_staging"
MODULES = ("__deployment__", "__staging__", "notebooks")
BATCH_REFS = ("jobs.__deployment__.load_items", "jobs.__deployment__.transform_items")
STAGING_BATCH_REFS = ("jobs.__staging__.load_items", "jobs.__staging__.transform_items")
REVIEW_NOTEBOOK = "jobs.notebooks.review_notebook"
MIGRATION_NOTEBOOK = "jobs.notebooks.migration_notebook"
CONNECTED_IN_PROD = (
    '[workspace.environment.prod]\nworkspace_id = "prod-ws-id"\nworkspace_name = "dlt-de-prd"\n'
)
"""Shared `config.toml` once `dlthub workspace connect` wrote the section of `prod`."""


@contextmanager
def _ci_job(
    workspace_id: Optional[str],
    staging: bool = False,
    migrated: bool = False,
    shared_config: Optional[str] = None,
) -> Iterator[WorkspaceRunContext]:
    """The workspace as a CI job sets it up before `dlthub deploy`. The checkout holds the empty
    `.workspace` unless `migrated`. A staging job renames `stg.config.toml`."""
    environ = {"RUNTIME__ORGANIZATION_ID": "org-id", "RUNTIME__API_KEY": "ci-api-key"}
    if workspace_id:
        environ["RUNTIME__WORKSPACE_ID"] = workspace_id
    with importable_workspace(WORKSPACE, *MODULES) as ctx, custom_environ(environ):
        settings = Path(ctx.settings_dir)
        if not migrated:
            empty_workspace_file(ctx)
        if shared_config:
            (settings / CONFIG_TOML).write_text(shared_config, encoding="utf-8")
        if staging:
            os.replace(settings / "stg.config.toml", settings / "prod.config.toml")
        yield switch_context(ctx.run_dir)


def _assert_fallbacks(warnings: List[str], job_refs: Sequence[str]) -> None:
    """One warning for each job in `job_refs`: it gets a profile with more data access."""
    assert len(warnings) == len(job_refs)
    for job_ref, warning in zip(job_refs, warnings):
        assert f"job `{job_ref}`" in warning


def test_ci_deploys_production() -> None:
    with _ci_job("ci-prod-id"):
        environment = activate_environment()
        ctx = dlt.current.workspace()
        assert environment["name"] == "prod" and ctx.profile == "prod"
        assert ctx.name == "dlt-de-prd"
        # the id that CI sets wins over the one in `prod.config.toml`
        assert ctx.runtime_config.workspace_id == "ci-prod-id"
        manifest, warnings = manifest_from_module("__deployment__", environment)

    assert manifest["environment"] == "prod"
    # `access` has no settings files: the notebook and the dashboard get `prod`
    interactive_refs = (REVIEW_NOTEBOOK, DASHBOARD_JOB_REF)
    assert job_profiles(manifest) == dict.fromkeys([*BATCH_REFS, *interactive_refs], "prod")
    _assert_fallbacks(warnings, interactive_refs)


@pytest.mark.parametrize(
    "migrated,shared_config,ci_workspace_id,workspace_id",
    [
        (False, None, "ci-stg-id", "ci-stg-id"),
        # a declared `stg` stays valid without its files
        (True, None, "ci-stg-id", "ci-stg-id"),
        # the id that CI sets wins over the section of `prod` too
        (False, CONNECTED_IN_PROD, "ci-stg-id", "ci-stg-id"),
        # the renamed file gives the id when CI sets none
        (False, None, None, "stg-ws-id"),
    ],
    ids=["empty-workspace-file", "stg-declared", "connected-in-prod", "id-from-renamed-file"],
)
def test_ci_deploys_staging_with_the_default_environment(
    migrated: bool,
    shared_config: Optional[str],
    ci_workspace_id: Optional[str],
    workspace_id: str,
) -> None:
    with _ci_job(ci_workspace_id, staging=True, migrated=migrated, shared_config=shared_config):
        environment = activate_environment()
        ctx = dlt.current.workspace()
        assert ctx.configured_profiles() == ["prod"]
        assert environment["name"] == "prod" and ctx.profile == "prod"
        assert ctx.name == "dlt-de-stage"
        assert ctx.runtime_config.workspace_id == workspace_id
        manifest, warnings = manifest_from_module("__staging__", environment)

    assert manifest["deployment_module"] == "__staging__"
    assert manifest["environment"] == "prod"
    # `access` has no settings files: the notebooks get `prod`
    notebooks = (REVIEW_NOTEBOOK, MIGRATION_NOTEBOOK)
    assert job_profiles(manifest) == dict.fromkeys([*STAGING_BATCH_REFS, *notebooks], "prod")
    _assert_fallbacks(warnings, notebooks)
    for job in manifest["jobs"]:
        if job["job_ref"] in STAGING_BATCH_REFS:
            # copies of the production jobs: they belong to `__staging__` and run on demand only
            assert job["entry_point"]["module"] == "__staging__"
            assert job["triggers"] == []


@pytest.mark.parametrize("recorded", [False, True], ids=["named", "recorded"])
def test_developer_deploys_to_the_stg_environment(recorded: bool) -> None:
    """With both config files `stg` is migrated to an environment of its own. A developer
    deploys to it with `--environment stg`, or with `stg` recorded as the active profile."""
    with importable_workspace(WORKSPACE, *MODULES) as ctx:
        assert ctx.configured_profiles() == ["prod", "stg"]
        if recorded:
            save_profile_pin(ctx, "stg")
            switch_context(ctx.run_dir)
            environment = activate_environment()
        else:
            environment = activate_environment("stg")
        ctx = dlt.current.workspace()
        assert environment["name"] == "stg" and ctx.profile == "stg"
        # `stg.config.toml` belongs to `stg` alone: its name gets no `-stg` suffix
        assert ctx.name == "dlt-de-stage"
        assert ctx.runtime_config.workspace_id == "stg-ws-id"
        assert ctx.runtime_config.organization_id == "org-id"
        manifest, warnings = manifest_from_module("__deployment__", environment)
        # the settings of `prod` stay on the machine
        assert shipped_settings(ctx, environment) == ["stg.config.toml"]

    assert manifest["environment"] == "stg"
    # the dashboard goes to the default environment only. no profile of `stg` has read data
    # access: the notebook gets the default write profile
    assert job_profiles(manifest) == dict.fromkeys([*BATCH_REFS, REVIEW_NOTEBOOK], "stg")
    _assert_fallbacks(warnings, [REVIEW_NOTEBOOK])


@pytest.mark.parametrize(
    "environ,profile,name,prod_workspace_id",
    [
        (
            {"WORKSPACE__PROFILE": "prod", "RUNTIME__WORKSPACE_ID": "run-ws-id"},
            "prod",
            "dlt-de-prd",
            "run-ws-id",
        ),
        # `access` has no settings file: the name is the folder name
        (
            {"WORKSPACE__PROFILE": "access", "RUNTIME__WORKSPACE_ID": "run-ws-id"},
            "access",
            WORKSPACE,
            "run-ws-id",
        ),
        ({}, "dev", WORKSPACE, "prod-ws-id"),
    ],
    ids=["platform-batch-job", "platform-notebook", "developer"],
)
def test_job_runs_with_its_profile_and_switches_to_prod(
    environ: Dict[str, str], profile: str, name: str, prod_workspace_id: str
) -> None:
    """The platform gives a job its profile with `WORKSPACE__PROFILE` and names the remote
    workspace of the run with `RUNTIME__WORKSPACE_ID`. The notebooks then call
    `switch_profile("prod")`, also when a developer runs them."""
    with isolated_workspace(WORKSPACE) as ctx, custom_environ(environ):
        ctx = switch_context(ctx.run_dir)
        assert ctx.profile == profile
        assert ctx.name == name
        assert ctx.runtime_config.workspace_id == environ.get("RUNTIME__WORKSPACE_ID")
        ctx = dlt.current.workspace().switch_profile("prod")
        assert ctx.profile == "prod" and ctx.environment == "prod"
        assert ctx.name == "dlt-de-prd"
        assert ctx.runtime_config.workspace_id == prod_workspace_id
