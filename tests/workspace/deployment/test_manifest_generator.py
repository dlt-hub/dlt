"""Tests for manifest generation from deployment modules."""

from importlib import import_module

import pytest

from dlt._workspace.deployment.manifest import generate_manifest, validate_manifest
from dlt._workspace.deployment.typing import MANIFEST_ENGINE_VERSION, TJobRef

WORKSPACE = "tests.workspace.cases.runtime_workspace"


def test_full_deployment() -> None:
    """Full deployment module discovers all job types."""
    mod = import_module(f"{WORKSPACE}.deployment_full")
    manifest, warnings = generate_manifest(mod)

    assert manifest["engine_version"] == MANIFEST_ENGINE_VERSION
    assert manifest["deployment_module"] == f"{WORKSPACE}.deployment_full"
    assert manifest["description"] == "Full workspace deployment with all job types."
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

    assert len(manifest["jobs"]) == 9
    assert warnings == []


def test_full_deployment_job_details() -> None:
    """Check individual job properties in full deployment."""
    mod = import_module(f"{WORKSPACE}.deployment_full")
    manifest, _ = generate_manifest(mod)

    jobs_by_ref = {j["job_ref"]: j for j in manifest["jobs"]}

    # batch job with timeout
    daily = jobs_by_ref[TJobRef("jobs.batch_jobs.daily_ingest")]
    assert daily["entry_point"]["job_type"] == "batch"
    assert daily["execution"]["timeout"]["timeout"] == 14400.0

    # batch job with chained triggers + auto manual
    transform = jobs_by_ref[TJobRef("jobs.batch_jobs.transform")]
    non_manual = [t for t in transform["triggers"] if not t.startswith("manual:")]
    assert len(non_manual) == 2
    assert all("job.success:" in t for t in non_manual)
    assert any(t == "manual:jobs.batch_jobs.transform" for t in transform["triggers"])
    assert transform["description"] == "Transform ingested data."

    # starred job with config keys
    maintenance = jobs_by_ref[TJobRef("jobs.batch_jobs.maintenance")]
    assert maintenance["starred"] is True
    assert maintenance["tags"] == ["ops"]
    assert "cleanup_days" in maintenance.get("config_keys", [])

    # interactive rest api
    api = jobs_by_ref[TJobRef("jobs.interactive_jobs.api_server")]
    assert api["entry_point"]["job_type"] == "interactive"
    assert api["expose"] == {"interface": "rest_api"}
    assert "http:" in api["triggers"]

    # marimo notebook (detected from module)
    notebook = jobs_by_ref[TJobRef("jobs.marimo_notebook")]
    assert notebook["entry_point"]["job_type"] == "interactive"
    assert notebook["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.marimo"
    assert notebook["display_name"] == "Test Notebook"
    assert "notebook" in notebook.get("tags", [])

    # mcp server (detected from module)
    mcp = jobs_by_ref[TJobRef("jobs.mcp_server")]
    assert mcp["expose"] == {"interface": "mcp"}
    assert mcp["display_name"] == "test-tools"

    # streamlit (detected from module)
    st_app = jobs_by_ref[TJobRef("jobs.streamlit_app")]
    assert st_app["expose"] == {"interface": "gui"}
    assert st_app["description"] == "Test dashboard."
    assert "dashboard" in st_app.get("tags", [])


def test_batch_only_deployment() -> None:
    """Deployment with only batch jobs."""
    mod = import_module(f"{WORKSPACE}.deployment_batch_only")
    manifest, warnings = generate_manifest(mod)

    assert len(manifest["jobs"]) == 3
    assert warnings == []

    for j in manifest["jobs"]:
        assert j["entry_point"]["job_type"] == "batch"


def test_deployment_with_unknown_warns() -> None:
    """Unknown items in __all__ produce warnings."""
    mod = import_module(f"{WORKSPACE}.deployment_with_unknown")
    manifest, warnings = generate_manifest(mod)

    assert len(manifest["jobs"]) == 1
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

    # all are batch
    for j in manifest["jobs"]:
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

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.mcp_server" in job_refs

    mcp_job = manifest["jobs"][0]
    assert mcp_job["expose"]["interface"] == "mcp"
    assert mcp_job["display_name"] == "test-tools"


def test_ad_hoc_framework_marimo() -> None:
    """Ad-hoc deployment detects marimo.App in the module itself."""
    mod = import_module(f"{WORKSPACE}.marimo_notebook")
    manifest, _ = generate_manifest(mod, use_all=False)

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.marimo_notebook" in job_refs

    nb_job = manifest["jobs"][0]
    assert nb_job["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.marimo"
    assert nb_job["display_name"] == "Test Notebook"


def test_ad_hoc_framework_streamlit() -> None:
    """Ad-hoc deployment detects streamlit import in the module itself."""
    mod = import_module(f"{WORKSPACE}.streamlit_app")
    manifest, _ = generate_manifest(mod, use_all=False)

    job_refs = {j["job_ref"] for j in manifest["jobs"]}
    assert "jobs.streamlit_app" in job_refs

    st_job = manifest["jobs"][0]
    assert st_job["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.streamlit"
    assert st_job["description"] == "Test dashboard."


def test_ad_hoc_plain_module() -> None:
    """Ad-hoc deployment of a plain Python module with no jobs or frameworks.
    Module itself is not detected — no jobs produced.
    """
    mod = import_module(f"{WORKSPACE}.plain_module")
    manifest, _ = generate_manifest(mod, use_all=False)

    assert manifest["jobs"] == []


def test_ad_hoc_etl_script() -> None:
    """Ad-hoc deployment of a plain Python script — not detected as job
    since it has no JobFactory, no framework, and self-detection as local
    module requires a parent.
    """
    mod = import_module(f"{WORKSPACE}.etl_script")
    manifest, _ = generate_manifest(mod, use_all=False)

    assert manifest["jobs"] == []


def test_ad_hoc_uses_fully_qualified_module_name() -> None:
    """Ad-hoc deployment stores fully qualified module name."""
    mod = import_module(f"{WORKSPACE}.batch_jobs")
    manifest, _ = generate_manifest(mod, use_all=False)

    assert manifest["deployment_module"] == f"{WORKSPACE}.batch_jobs"


def test_all_framework_triggers_use_portless_http() -> None:
    """All framework-detected jobs use http: trigger without port."""
    mod = import_module(f"{WORKSPACE}.deployment_full")
    manifest, _ = generate_manifest(mod)

    framework_refs = {"jobs.marimo_notebook", "jobs.mcp_server", "jobs.streamlit_app"}
    for j in manifest["jobs"]:
        if j["job_ref"] in framework_refs:
            assert "http:" in j["triggers"], f"{j['job_ref']} should use http: trigger"
