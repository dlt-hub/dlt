"""Tests for module-level framework detectors."""

from importlib import import_module

import pytest

from dlt._workspace.deployment.detectors import (
    detect_local_module,
    detect_module_job,
    is_local_module,
)

CASES = "tests.workspace.deployment.cases.detectors"
HTTP_TRIGGER = "http:"


@pytest.mark.parametrize(
    "module_name,expected_display_name",
    [
        ("marimo_standard", None),
        ("marimo_titled", "Sales Dashboard"),
        ("marimo_custom_var", None),
    ],
    ids=["standard", "titled", "custom-var"],
)
def test_detect_marimo(module_name: str, expected_display_name: str) -> None:
    """Detects marimo.App regardless of variable name, extracts app_title."""
    mod = import_module(f"{CASES}.{module_name}")
    job_def = detect_module_job(mod)

    assert job_def is not None
    assert job_def["entry_point"]["job_type"] == "interactive"
    assert job_def["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.marimo"
    assert job_def["expose"]["interface"] == "gui"
    assert job_def["triggers"] == [HTTP_TRIGGER]
    assert "notebook" in job_def.get("expose", {}).get("tags", [])

    if expected_display_name:
        assert expected_display_name in job_def.get("description", "")

    # standard has module docstring
    if module_name == "marimo_standard":
        assert job_def["description"] == "A basic marimo notebook."


@pytest.mark.parametrize(
    "module_name,expected_display_name,expected_description",
    [
        ("mcp_standard", "simple-mcp", None),
        ("mcp_with_instructions", "data-tools", None),
        ("mcp_aliased", "aliased-server", None),
        ("mcp_app_var", "app-named-server", "MCP server using app variable name."),
    ],
    ids=["standard", "with-instructions", "aliased-import", "app-var-name"],
)
def test_detect_mcp(
    module_name: str, expected_display_name: str, expected_description: str
) -> None:
    """Detects FastMCP with preferred names, aliased imports, and full scan."""
    mod = import_module(f"{CASES}.{module_name}")
    job_def = detect_module_job(mod)

    assert job_def is not None
    assert job_def["entry_point"]["job_type"] == "interactive"
    assert job_def["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.mcp"
    assert job_def["expose"]["interface"] == "mcp"
    assert job_def["triggers"] == [HTTP_TRIGGER]
    # display_name merged into description
    desc = job_def.get("description", "")
    assert expected_display_name in desc
    if expected_description:
        assert expected_description in desc


@pytest.mark.parametrize(
    "module_name",
    ["streamlit_standard", "streamlit_full_import", "streamlit_aliased"],
    ids=["as-st", "full-import", "aliased"],
)
def test_detect_streamlit(module_name: str) -> None:
    """Detects streamlit via module object in namespace, any alias."""
    mod = import_module(f"{CASES}.{module_name}")
    job_def = detect_module_job(mod)

    assert job_def is not None
    assert job_def["entry_point"]["job_type"] == "interactive"
    assert job_def["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.streamlit"
    assert job_def["expose"]["interface"] == "gui"
    assert job_def["triggers"] == [HTTP_TRIGGER]
    assert job_def.get("expose", {}).get("category") == "dashboard"

    # full_import has module docstring
    if module_name == "streamlit_full_import":
        assert job_def["description"] == "Analytics dashboard."


def test_no_framework() -> None:
    """Returns None for modules without any recognized framework."""
    mod = import_module(f"{CASES}.no_framework")
    assert detect_module_job(mod) is None


def test_mcp_wins_over_streamlit() -> None:
    """When both FastMCP and streamlit are present, MCP takes priority."""
    mod = import_module(f"{CASES}.mixed_mcp_and_streamlit")
    job_def = detect_module_job(mod)

    assert job_def is not None
    assert job_def["entry_point"]["launcher"] == "dlt._workspace.deployment.launchers.mcp"
    assert "mixed-server" in job_def.get("description", "")


def test_job_ref_uses_module_name() -> None:
    """Job ref is jobs.<module_name> for module-level jobs."""
    mod = import_module(f"{CASES}.mcp_standard")
    job_def = detect_module_job(mod)
    assert job_def["job_ref"] == "jobs.mcp_standard"

    mod = import_module(f"{CASES}.marimo_standard")
    job_def = detect_module_job(mod)
    assert job_def["job_ref"] == "jobs.marimo_standard"


WORKSPACE = "tests.workspace.cases.runtime_workspace"


def test_is_local_module() -> None:
    """Local modules are below the parent module's directory."""
    parent = import_module(f"{WORKSPACE}.deployment_with_local_module")
    local = import_module(f"{WORKSPACE}.etl_script")
    assert is_local_module(local, parent)

    import os

    assert not is_local_module(os, parent)


def test_detect_local_module() -> None:
    """Plain local module detected as batch job."""
    parent = import_module(f"{WORKSPACE}.deployment_with_local_module")
    local = import_module(f"{WORKSPACE}.etl_script")
    job_def = detect_local_module(local, parent)

    assert job_def is not None
    assert job_def["entry_point"]["job_type"] == "batch"
    assert job_def["entry_point"]["function"] is None
    assert job_def["job_ref"] == "jobs.etl_script"
    assert job_def["description"] == "ETL script that runs as __main__."


def test_detect_local_module_skips_framework() -> None:
    """Framework modules are skipped by local module detector."""
    parent = import_module(f"{WORKSPACE}.deployment_full")
    mcp_mod = import_module(f"{WORKSPACE}.mcp_server")
    assert detect_local_module(mcp_mod, parent) is None


def test_detect_local_module_rejects_external() -> None:
    """External packages are rejected."""
    parent = import_module(f"{WORKSPACE}.deployment_full")
    import os

    assert detect_local_module(os, parent) is None


def test_is_local_module_rejects_venv_packages() -> None:
    """Installed packages in .venv/site-packages are not local."""
    parent = import_module(f"{WORKSPACE}.deployment_full")
    import marimo

    assert not is_local_module(marimo, parent)


def test_is_local_module_rejects_site_packages() -> None:
    """Any module from site-packages is not local."""
    parent = import_module(f"{WORKSPACE}.deployment_full")
    import fastmcp

    assert not is_local_module(fastmcp, parent)


# ---- module dunders ----


def test_module_trigger_appended() -> None:
    """__trigger__ on module appends to detector triggers."""
    mod = import_module(f"{CASES}.marimo_with_trigger")
    job_def = detect_module_job(mod)

    assert job_def is not None
    assert HTTP_TRIGGER in job_def["triggers"]
    assert "tag:refresh" in job_def["triggers"]
    assert len(job_def["triggers"]) == 2


def test_module_trigger_list() -> None:
    """__trigger__ as list appends all triggers."""
    mod = import_module(f"{CASES}.mcp_with_trigger")
    job_def = detect_module_job(mod)

    assert job_def is not None
    assert HTTP_TRIGGER in job_def["triggers"]
    assert "tag:tools" in job_def["triggers"]
    assert "deployment:" in job_def["triggers"]


def test_module_expose_overrides() -> None:
    """__expose__ overrides detector expose."""
    mod = import_module(f"{CASES}.marimo_with_expose")
    job_def = detect_module_job(mod)

    assert job_def is not None
    assert job_def["expose"] == {"interface": "rest_api"}
