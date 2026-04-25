"""Tests for manifest generation from deployment modules."""

import sys
from importlib import import_module
from typing import List

import pytest

from dlt._workspace.deployment.exceptions import InvalidManifest
from dlt._workspace.deployment.manifest import (
    DASHBOARD_JOB_REF,
    generate_manifest,
    manifest_from_module,
    validate_manifest,
)
from dlt._workspace.deployment.typing import (
    MANIFEST_ENGINE_VERSION,
    TJobDefinition,
    TJobRef,
    TTrigger,
)

WORKSPACE = "tests.workspace.cases.runtime_workspace"


def _user_jobs(jobs: List[TJobDefinition]) -> List[TJobDefinition]:
    """Filter out auto-included workspace jobs (dashboard)."""
    return [j for j in jobs if j["job_ref"] != DASHBOARD_JOB_REF]


def test_full_deployment() -> None:
    """Full deployment module discovers all job types."""
    mod = import_module(f"{WORKSPACE}.deployment_full")
    manifest, warnings = generate_manifest(mod)

    assert manifest["engine_version"] == MANIFEST_ENGINE_VERSION
    assert manifest["deployment_module"] == f"{WORKSPACE}.deployment_full"
    # only the first line of the multi-line docstring is used as the manifest description
    assert manifest["description"] == "Full workspace deployment with all job types."
    assert "second line" not in manifest["description"]
    assert "\n" not in manifest["description"]
    assert manifest["tags"] == ["production", "team:data"]

    job_refs = {j["job_ref"] for j in manifest["jobs"]}

    # batch jobs
    assert "jobs.batch_jobs.backfill" in job_refs
    assert "jobs.batch_jobs.daily_ingest" in job_refs
    assert "jobs.batch_jobs.transform" in job_refs
    assert "jobs.batch_jobs.maintenance" in job_refs

    # interactive jobs
    assert "jobs.interactive_jobs.api_server" in job_refs
    assert "jobs.interactive_jobs.mcp_tools" in job_refs

    # module-level framework jobs
    assert "jobs.marimo_notebook" in job_refs
    assert "jobs.mcp_server" in job_refs
    assert "jobs.streamlit_app" in job_refs

    assert len(_user_jobs(manifest["jobs"])) == 9
    # dashboard is only auto-included for the canonical `__deployment__` module
    assert DASHBOARD_JOB_REF not in {j["job_ref"] for j in manifest["jobs"]}
    assert warnings == []


def test_full_deployment_job_details() -> None:
    """Check individual job properties in full deployment."""
    mod = import_module(f"{WORKSPACE}.deployment_full")
    manifest, _ = generate_manifest(mod)

    jobs_by_ref = {j["job_ref"]: j for j in manifest["jobs"]}

    # batch job with timeout — schedule trigger is default_trigger
    daily = jobs_by_ref[TJobRef("jobs.batch_jobs.daily_ingest")]
    assert daily["entry_point"]["job_type"] == "batch"
    assert daily["execute"]["timeout"]["timeout"] == 14400.0
    assert daily["default_trigger"] == TTrigger("schedule:0 8 * * *")

    # batch job with chained triggers — first trigger is default
    transform = jobs_by_ref[TJobRef("jobs.batch_jobs.transform")]
    assert len(transform["triggers"]) == 2
    assert all("job.success:" in t for t in transform["triggers"])
    assert transform["default_trigger"] == transform["triggers"][0]
    assert transform["description"] == "Transform ingested data."

    # starred job with config keys
    maintenance = jobs_by_ref[TJobRef("jobs.batch_jobs.maintenance")]
    assert maintenance["expose"]["starred"] is True
    assert maintenance["expose"]["tags"] == ["ops"]
    assert "cleanup_days" in maintenance.get("config_keys", [])

    # interactive rest api
    api = jobs_by_ref[TJobRef("jobs.interactive_jobs.api_server")]
    assert api["entry_point"]["job_type"] == "interactive"
    assert api["expose"]["interface"] == "rest_api"
    assert api["expose"]["manual"] is True
    assert "http:" in api["triggers"]

    # marimo notebook (detected from module)
    notebook = jobs_by_ref[TJobRef("jobs.marimo_notebook")]
    assert notebook["entry_point"]["job_type"] == "interactive"
    assert notebook["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.marimo"
    assert "Test Notebook" in notebook.get("description", "")
    assert "notebook" in notebook.get("expose", {}).get("tags", [])

    # mcp server (detected from module)
    mcp = jobs_by_ref[TJobRef("jobs.mcp_server")]
    assert mcp["expose"]["interface"] == "mcp"
    assert "test-tools" in mcp.get("description", "")

    # streamlit (detected from module)
    st_app = jobs_by_ref[TJobRef("jobs.streamlit_app")]
    assert st_app["expose"]["interface"] == "gui"
    assert st_app["description"] == "Test dashboard."
    assert st_app.get("expose", {}).get("category") == "dashboard"


def test_batch_only_deployment() -> None:
    """Deployment with only batch jobs."""
    mod = import_module(f"{WORKSPACE}.deployment_batch_only")
    manifest, warnings = generate_manifest(mod)

    user_jobs = _user_jobs(manifest["jobs"])
    assert len(user_jobs) == 3
    assert warnings == []

    for j in user_jobs:
        assert j["entry_point"]["job_type"] == "batch"


def test_deployment_with_unknown_warns() -> None:
    """Unknown items in __all__ produce warnings."""
    mod = import_module(f"{WORKSPACE}.deployment_with_unknown")
    manifest, warnings = generate_manifest(mod)

    assert len(_user_jobs(manifest["jobs"])) == 1
    assert any("helper" in w and "not a recognized" in w for w in warnings)


def test_deployment_with_local_module() -> None:
    """Plain local module detected as batch job via __all__."""
    mod = import_module(f"{WORKSPACE}.deployment_with_local_module")
    manifest, warnings = generate_manifest(mod)

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.batch_jobs.backfill" in job_refs
    assert "jobs.etl_script" in job_refs
    assert warnings == []

    etl = next(j for j in manifest["jobs"] if j["job_ref"] == "jobs.etl_script")
    assert etl["entry_point"]["job_type"] == "batch"
    assert etl["entry_point"]["function"] is None
    assert etl["description"] == "ETL script that runs as __main__."


def test_chained_triggers_resolved() -> None:
    """Triggers referencing jobs in same deployment are resolved."""
    mod = import_module(f"{WORKSPACE}.deployment_batch_only")
    manifest, _ = generate_manifest(mod)

    result = validate_manifest(manifest)
    assert result.unresolved_triggers == {}


def test_missing_all_falls_back_to_dict() -> None:
    """Module without __all__ falls back to __dict__ scan with warning."""
    mod = import_module(f"{WORKSPACE}.batch_jobs")
    manifest, warnings = generate_manifest(mod, use_all=True)

    assert any("no __all__" in w for w in warnings)
    assert len(manifest["jobs"]) > 0


def test_use_all_false_with_all_present() -> None:
    """`use_all=False` on a module that has `__all__` scans `__dict__` ad-hoc.

    Third-party imports must be filtered by `is_local_module`, helpers/constants
    must not produce "not a recognized" warnings, and the no-`__all__` advisory
    must not fire (the module does have `__all__`).
    """
    from unittest.mock import patch

    import dlt._workspace.deployment.manifest as M

    mod = import_module(f"{WORKSPACE}.deployment_with_extras")

    detect_calls: List[str] = []
    original_detect = M.detect_module_job

    def spy_detect_module_job(m: object) -> object:
        detect_calls.append(getattr(m, "__name__", repr(m)))
        return original_detect(m)  # type: ignore[arg-type]

    with patch.object(M, "detect_module_job", side_effect=spy_detect_module_job):
        manifest, warnings = generate_manifest(mod, use_all=False)

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.batch_jobs.backfill" in job_refs

    # third-party / stdlib imports must be filtered out before framework detection
    assert "pendulum" not in detect_calls
    assert "os" not in detect_calls

    # no spurious "not a recognized" warnings for helpers/constants in __dict__
    assert not any("not a recognized" in w for w in warnings)

    # no advisory about missing __all__ — the module does have one
    assert not any("no __all__" in w for w in warnings)


def test_ad_hoc_batch_jobs() -> None:
    """Ad-hoc deployment scans __dict__ and finds all JobFactory instances."""
    mod = import_module(f"{WORKSPACE}.batch_jobs")
    manifest, warnings = generate_manifest(mod, use_all=False)

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.batch_jobs.backfill" in job_refs
    assert "jobs.batch_jobs.daily_ingest" in job_refs
    assert "jobs.batch_jobs.transform" in job_refs
    assert "jobs.batch_jobs.maintenance" in job_refs

    # ad-hoc scan (no __all__) does not warn about non-job names
    assert not any("not a recognized" in w for w in warnings)

    # all user-defined are batch
    for j in _user_jobs(manifest["jobs"]):
        assert j["entry_point"]["job_type"] == "batch"


def test_ad_hoc_interactive_jobs() -> None:
    """Ad-hoc deployment from a module with interactive jobs."""
    mod = import_module(f"{WORKSPACE}.interactive_jobs")
    manifest, warnings = generate_manifest(mod, use_all=False)

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.interactive_jobs.api_server" in job_refs
    assert "jobs.interactive_jobs.mcp_tools" in job_refs

    for j in manifest["jobs"]:
        assert j["entry_point"]["job_type"] == "interactive"


def test_ad_hoc_framework_mcp() -> None:
    """Ad-hoc deployment detects FastMCP singleton in the module itself."""
    mod = import_module(f"{WORKSPACE}.mcp_server")
    manifest, _ = generate_manifest(mod, use_all=False)

    jobs_by_ref = {j["job_ref"]: j for j in manifest["jobs"]}
    assert "jobs.mcp_server" in jobs_by_ref

    mcp_job = jobs_by_ref[TJobRef("jobs.mcp_server")]
    assert mcp_job["expose"]["interface"] == "mcp"
    assert "test-tools" in mcp_job.get("description", "")


def test_ad_hoc_framework_marimo() -> None:
    """Ad-hoc deployment detects marimo.App in the module itself."""
    mod = import_module(f"{WORKSPACE}.marimo_notebook")
    manifest, _ = generate_manifest(mod, use_all=False)

    jobs_by_ref = {j["job_ref"]: j for j in manifest["jobs"]}
    assert "jobs.marimo_notebook" in jobs_by_ref

    nb_job = jobs_by_ref[TJobRef("jobs.marimo_notebook")]
    assert nb_job["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.marimo"
    assert "Test Notebook" in nb_job.get("description", "")


def test_ad_hoc_framework_streamlit() -> None:
    """Ad-hoc deployment detects streamlit import in the module itself."""
    mod = import_module(f"{WORKSPACE}.streamlit_app")
    manifest, _ = generate_manifest(mod, use_all=False)

    jobs_by_ref = {j["job_ref"]: j for j in manifest["jobs"]}
    assert "jobs.streamlit_app" in jobs_by_ref

    st_job = jobs_by_ref[TJobRef("jobs.streamlit_app")]
    assert st_job["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.streamlit"
    assert st_job["description"] == "Test dashboard."


def test_ad_hoc_plain_module() -> None:
    """Ad-hoc deployment promotes a plain local Python module to a batch job."""
    mod = import_module(f"{WORKSPACE}.plain_module")
    manifest, _ = generate_manifest(mod, use_all=False)

    user_jobs = _user_jobs(manifest["jobs"])
    assert len(user_jobs) == 1
    job = user_jobs[0]
    assert job["job_ref"] == "jobs.plain_module"
    assert job["entry_point"]["job_type"] == "batch"
    assert job["entry_point"]["function"] is None
    assert job["entry_point"]["module"] == f"{WORKSPACE}.plain_module"
    assert job["description"] == "A plain module with no jobs."


def test_ad_hoc_etl_script() -> None:
    """Ad-hoc deployment promotes a plain Python script (no JobFactory, no framework)."""
    mod = import_module(f"{WORKSPACE}.etl_script")
    manifest, _ = generate_manifest(mod, use_all=False)

    user_jobs = _user_jobs(manifest["jobs"])
    assert len(user_jobs) == 1
    job = user_jobs[0]
    assert job["job_ref"] == "jobs.etl_script"
    assert job["entry_point"]["job_type"] == "batch"
    assert job["entry_point"]["function"] is None
    assert job["entry_point"]["module"] == f"{WORKSPACE}.etl_script"
    assert job["description"] == "ETL script that runs as __main__."


def test_ad_hoc_plain_module_not_promoted_with_use_all() -> None:
    """The self-promotion fallback only fires in ad-hoc (`use_all=False`) mode.

    A `__deployment__.py` that legitimately declares no jobs in `__all__` must
    not be accidentally promoted to a job.
    """
    mod = import_module(f"{WORKSPACE}.plain_module")
    manifest, _ = generate_manifest(mod, use_all=True)
    assert _user_jobs(manifest["jobs"]) == []


def test_ad_hoc_uses_fully_qualified_module_name() -> None:
    """Ad-hoc deployment stores fully qualified module name."""
    mod = import_module(f"{WORKSPACE}.batch_jobs")
    manifest, _ = generate_manifest(mod, use_all=False)

    assert manifest["deployment_module"] == f"{WORKSPACE}.batch_jobs"


def test_all_framework_triggers_use_portless_http() -> None:
    """All framework-detected jobs use http: trigger without port."""
    mod = import_module(f"{WORKSPACE}.deployment_full")
    manifest, _ = generate_manifest(mod)

    framework_refs = {
        "jobs.marimo_notebook",
        "jobs.mcp_server",
        "jobs.streamlit_app",
    }
    for j in manifest["jobs"]:
        if j["job_ref"] in framework_refs:
            assert "http:" in j["triggers"], f"{j['job_ref']} should use http: trigger"


def test_tags_in_expose() -> None:
    """Job tags are stored in expose spec, not as triggers."""
    mod = import_module(f"{WORKSPACE}.deployment_full")
    manifest, _ = generate_manifest(mod)

    jobs_by_ref = {j["job_ref"]: j for j in manifest["jobs"]}

    # maintenance has tags=["ops"] in expose
    maintenance = jobs_by_ref[TJobRef("jobs.batch_jobs.maintenance")]
    assert "ops" in maintenance["expose"]["tags"]
    assert "tag:ops" not in maintenance["triggers"]

    # streamlit has category="dashboard" in expose (not a tag)
    st_app = jobs_by_ref[TJobRef("jobs.streamlit_app")]
    assert st_app["expose"]["category"] == "dashboard"


def test_self_detection_before_dict_scan() -> None:
    """Framework module run ad-hoc is self-detected, not scanned for sub-modules."""
    mod = import_module(f"{WORKSPACE}.marimo_notebook")
    manifest, _ = generate_manifest(mod, use_all=False)

    user_jobs = _user_jobs(manifest["jobs"])
    assert len(user_jobs) == 1
    job = user_jobs[0]
    assert job["job_ref"] == "jobs.marimo_notebook"
    assert job["entry_point"]["job_type"] == "interactive"
    assert job["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.marimo"

    # no spurious batch job for the imported `marimo` package
    refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.marimo" not in refs


def test_ad_hoc_does_not_pick_up_venv_modules() -> None:
    """Ad-hoc __dict__ scan skips installed packages even if venv is inside project."""
    mod = import_module(f"{WORKSPACE}.batch_jobs")
    manifest, _ = generate_manifest(mod, use_all=False)

    refs = {j["job_ref"] for j in _user_jobs(manifest["jobs"])}
    # batch_jobs imports dlt — should not appear as a job
    assert all(not ref.startswith("jobs.dlt") for ref in refs)
    # only actual JobFactory jobs from this module
    assert "jobs.batch_jobs.backfill" in refs


# -- dashboard auto-include and validation --


def test_dashboard_auto_included_for_canonical_deployment() -> None:
    """The canonical `__deployment__` module gets a dashboard auto-included."""
    mod = import_module(f"{WORKSPACE}.__deployment__")
    manifest, _ = generate_manifest(mod)

    jobs_by_ref = {j["job_ref"]: j for j in manifest["jobs"]}
    dashboard = jobs_by_ref[DASHBOARD_JOB_REF]
    assert dashboard["entry_point"]["job_type"] == "interactive"
    assert dashboard["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.dashboard"
    assert dashboard["expose"]["interface"] == "gui"
    assert dashboard["expose"]["category"] == "dashboard"
    assert "http:" in dashboard["triggers"]


def test_dashboard_not_auto_included_for_non_canonical_module() -> None:
    """Non-`__deployment__` modules do not get a dashboard — ad-hoc deployments
    (`--file foo.py`, framework apps imported by name, etc.) should never have
    a workspace dashboard tagged on by the manifest generator.
    """
    mod = import_module(f"{WORKSPACE}.deployment_batch_only")
    manifest, _ = generate_manifest(mod)

    assert DASHBOARD_JOB_REF not in {j["job_ref"] for j in manifest["jobs"]}


@pytest.mark.parametrize(
    "mutation,error_frag",
    [
        (lambda j: j["entry_point"].__setitem__("job_type", "batch"), "must be interactive"),
        (lambda j: j["expose"].__setitem__("interface", "mcp"), "must have gui interface"),
        (lambda j: j["expose"].__setitem__("category", "notebook"), "must have dashboard category"),
    ],
    ids=["wrong-type", "wrong-interface", "wrong-category"],
)
def test_dashboard_validation_rejects_invalid(mutation: object, error_frag: str) -> None:
    """Dashboard job must be interactive with gui interface and dashboard category."""
    from dlt._workspace.deployment.manifest import default_dashboard_job, validate_job_definition

    job = default_dashboard_job()
    mutation(job)  # type: ignore[operator]
    result = validate_job_definition(job)
    assert any(error_frag in e for e in result.errors), f"expected {error_frag!r} in {result}"


# -- manifest_from_module (isolated and inprocess) --


@pytest.mark.parametrize("isolated", [False, True], ids=["inprocess", "isolated"])
def test_manifest_from_module_full_deployment(isolated: bool) -> None:
    """manifest_from_module discovers all jobs and returns valid manifest."""
    manifest, warnings = manifest_from_module(f"{WORKSPACE}.deployment_full", isolated=isolated)

    assert manifest["engine_version"] == MANIFEST_ENGINE_VERSION
    assert manifest["deployment_module"] == f"{WORKSPACE}.deployment_full"
    assert len(_user_jobs(manifest["jobs"])) == 9
    assert warnings == []

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.batch_jobs.daily_ingest" in job_refs
    assert "jobs.marimo_notebook" in job_refs


@pytest.mark.parametrize("isolated", [False, True], ids=["inprocess", "isolated"])
def test_manifest_from_module_use_all_false(isolated: bool) -> None:
    """manifest_from_module with use_all=False scans __dict__ ad-hoc."""
    manifest, warnings = manifest_from_module(
        f"{WORKSPACE}.batch_jobs", use_all=False, isolated=isolated
    )

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.batch_jobs.backfill" in job_refs
    assert "jobs.batch_jobs.daily_ingest" in job_refs


@pytest.mark.parametrize("isolated", [False, True], ids=["inprocess", "isolated"])
def test_manifest_from_module_invalid_module(isolated: bool) -> None:
    """Both modes raise ModuleNotFoundError for a bad module name."""
    with pytest.raises(ModuleNotFoundError, match="nonexistent_module_xyz_123"):
        manifest_from_module("nonexistent_module_xyz_123", isolated=isolated)


def test_manifest_from_module_isolated_no_pollution() -> None:
    """Isolated mode does not leak the deployment module into the parent process."""
    module_path = f"{WORKSPACE}.deployment_full"
    marker = "deployment_full"

    # clean up any prior import of this module
    leaked_before = [k for k in sys.modules if marker in k]
    for k in leaked_before:
        del sys.modules[k]

    manifest_from_module(module_path, isolated=True)

    leaked = [k for k in sys.modules if marker in k]
    assert not leaked, f"isolated mode leaked modules: {leaked}"
