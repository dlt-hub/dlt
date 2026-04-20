"""Tests for `dlt workspace info` — fetch_deployment_info + view."""

from pathlib import Path
from typing import Any

import pytest

from dlt._workspace.cli._workspace_command import _print_deployment_info
from dlt._workspace.cli.utils import fetch_deployment_info
from dlt._workspace.typing import TDeploymentManifestInfo

from tests.workspace.utils import isolated_workspace


def test_fetch_deployment_info_not_found(auto_isolated_workspace: Any) -> None:
    info = fetch_deployment_info()
    assert info["status"] == "not_found"


def test_fetch_deployment_info_generation_failed(auto_isolated_workspace: Any) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    (Path(ws_dir) / "__deployment__.py").write_text("import definitely_not_installed_xyz\n")
    info = fetch_deployment_info()
    assert info["status"] == "generation_failed"
    assert "error" in info
    assert "definitely_not_installed_xyz" in info["error"]


def test_fetch_deployment_info_ok_categorizes_and_lists_triggers() -> None:
    # workspace case at tests/workspace/cases/workspaces/deployment_mixed/ has:
    #  - @pipeline_run load_fruitshop → category=pipeline (via deliver.pipeline_name)
    #  - @job batch_one → category=batch (no expose/deliver)
    #  - plain.py → category=batch (module-level job, no expose/deliver)
    with isolated_workspace("deployment_mixed", profile="dev"):
        info = fetch_deployment_info()

    assert info["status"] == "ok"
    assert info["total_jobs"] >= 3

    counts = info["counts_by_category"]
    assert counts.get("pipeline", 0) >= 1
    assert counts.get("batch", 0) >= 2

    by_ref = {j["short_name"]: j for j in info["jobs"]}
    assert by_ref["batch_one"]["default_trigger"] == "schedule: 0 2 * * *"
    assert by_ref["batch_one"]["category"] == "batch"
    assert by_ref["load_fruitshop"]["default_trigger"] == "schedule: 0 3 * * *"
    assert by_ref["load_fruitshop"]["category"] == "pipeline"


def test_print_deployment_info_not_found_renders_single_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {"status": "not_found"}
    _print_deployment_info(info, verbosity=0)
    out = capsys.readouterr().out
    assert "no manifest found" in out
    assert "__deployment__.py" in out


def test_print_deployment_info_generation_failed_non_verbose_hides_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "generation_failed",
        "error": "ImportError: bad",
    }
    _print_deployment_info(info, verbosity=0)
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "manifest generation failed" in combined
    assert "ImportError: bad" not in combined


def test_print_deployment_info_generation_failed_verbose_shows_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "generation_failed",
        "error": "ImportError: bad",
    }
    _print_deployment_info(info, verbosity=1)
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "manifest generation failed" in combined
    assert "ImportError: bad" in combined


def test_print_deployment_info_ok_non_verbose_shows_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "ok",
        "total_jobs": 3,
        "counts_by_category": {"pipeline": 2, "notebook": 1},
        "jobs": [],
    }
    _print_deployment_info(info, verbosity=0)
    out = capsys.readouterr().out
    assert "3" in out and "job(s)" in out
    assert "2 pipeline(s)" in out
    assert "1 notebook(s)" in out
    # no per-job lines in non-verbose
    lines = [line for line in out.splitlines() if line.startswith("  ")]
    assert lines == []


def test_print_deployment_info_ok_verbose_lists_jobs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "ok",
        "total_jobs": 2,
        "counts_by_category": {"pipeline": 1, "notebook": 1},
        "jobs": [
            {
                "job_ref": "jobs.backfill",
                "short_name": "backfill",
                "category": "pipeline",
                "default_trigger": "schedule: 0 2 * * *",
                "triggers": ["tag:nightly"],
            },
            {
                "job_ref": "jobs.dashboard",
                "short_name": "dashboard",
                "category": "notebook",
                "triggers": [],
            },
        ],
    }
    _print_deployment_info(info, verbosity=1)
    out = capsys.readouterr().out
    assert "🎯 schedule: 0 2 * * *" in out
    assert "tag:nightly" in out
    assert "(interactive)" in out  # no triggers on notebook
    assert "backfill" in out
    assert "dashboard" in out
