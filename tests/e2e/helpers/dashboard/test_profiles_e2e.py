import contextlib
from typing import Any, cast
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path
import urllib.request

import dlt
import pytest
from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
from dlt.sources.rest_api import rest_api_source
from playwright.sync_api import Page, expect
from tests.utils import (
    auto_test_run_context,
    preserve_environ,
    deactivate_pipeline,
    TEST_STORAGE_ROOT,
)

# .. helpers .. #


def _wait_http_up(url: str, timeout_s: float = 5.0) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            with urllib.request.urlopen(url, timeout=0.2):
                return
        except Exception:
            time.sleep(0.1)
    raise TimeoutError(f"Server did not become ready: {url}")


def _start_dashboard_server(workspace_dir: Path, port: int = 2718) -> subprocess.Popen[bytes]:
    """Start Marimo dashboard in given workspace and return the process."""
    cmd = [
        "uv",
        "run",
        "marimo",
        "run",
        "--headless",
        "--port",
        str(port),
        str(
            Path(__file__).resolve().parents[4]
            / "dlt/_workspace/helpers/dashboard/dlt_dashboard.py"
        ),
    ]
    workspace_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(cmd, cwd=str(Path(__file__).resolve().parents[4] / workspace_dir))
    _wait_http_up(f"http://localhost:{port}", timeout_s=15.0)
    return proc


# .. Fixtures .. #


@pytest.fixture(scope="module", autouse=True)
def prepare_workspaces() -> None:
    """Create two isolated workspaces: custom-dev-workspace and custom-tests-workspace."""
    # Workspace custom-dev-workspace with one profile: custom-dev-profile
    ws_dev = Path(TEST_STORAGE_ROOT) / "server_ws_dev"
    ws_dev.mkdir(parents=True, exist_ok=True)
    dlt_dev = ws_dev / ".dlt"
    dlt_dev.mkdir(parents=True, exist_ok=True)
    (dlt_dev / ".workspace").touch(exist_ok=True)
    (dlt_dev / "custom-dev-profile.config.toml").write_text(
        '[workspace.settings]\nname = "custom-dev-workspace"\n', encoding="utf-8"
    )
    # pin profile
    (dlt_dev / "profile-name").write_text("custom-dev-profile", encoding="utf-8")

    # Workspace custom-tests-workspace with one profile: custom-tests-profile
    ws_tests = Path(TEST_STORAGE_ROOT) / "server_ws_tests"
    ws_tests.mkdir(parents=True, exist_ok=True)
    dlt_tests = ws_tests / ".dlt"
    dlt_tests.mkdir(parents=True, exist_ok=True)
    (dlt_tests / ".workspace").touch(exist_ok=True)
    (dlt_tests / "custom-tests-profile.config.toml").write_text(
        '[workspace.settings]\nname = "custom-tests-workspace"\n', encoding="utf-8"
    )
    # pin profile
    (dlt_tests / "profile-name").write_text("custom-tests-profile", encoding="utf-8")


@pytest.fixture(scope="module", autouse=True)
def seed_pipelines(prepare_workspaces):
    # Seed into custom-dev-workspace
    dev_pipelines = (
        Path(TEST_STORAGE_ROOT)
        / "server_ws_dev"
        / ".dlt"
        / ".var"
        / "custom-dev-profile"
        / "pipelines"
    )
    dev_pipelines.mkdir(parents=True, exist_ok=True)
    load = dlt.pipeline(
        pipeline_name="fruitshop",
        pipelines_dir=str(dev_pipelines),
        destination="duckdb",
        dataset_name="fruitshop_data",
    ).run(fruitshop_source())
    assert load is not None
    # Seed into custom-tests-workspace
    tests_pipelines = (
        Path(TEST_STORAGE_ROOT)
        / "server_ws_tests"
        / ".dlt"
        / ".var"
        / "custom-tests-profile"
        / "pipelines"
    )
    tests_pipelines.mkdir(parents=True, exist_ok=True)
    pokemon_cfg = {
        "client": {"base_url": "https://pokeapi.co/api/v2/"},
        "resource_defaults": {"endpoint": {"params": {"limit": 1000}}},
        "resources": ["pokemon", "berry", "location"],
    }
    pokemon_src = rest_api_source(cast(Any, pokemon_cfg), name="pokemon")
    load = dlt.pipeline(
        pipeline_name="rest_api_pokemon",
        pipelines_dir=str(tests_pipelines),
        destination="duckdb",
        dataset_name="rest_api_data",
    ).run(pokemon_src)
    assert load is not None


@pytest.fixture()
def kill_server_after():
    """Register processes to be terminated when the test ends."""
    procs: list[subprocess.Popen[bytes]] = []
    yield procs.append
    for p in procs:
        with contextlib.suppress(Exception):
            p.send_signal(signal.SIGTERM)
            p.wait(timeout=5)
        if p.poll() is None:
            with contextlib.suppress(Exception):
                p.kill()


@pytest.fixture(scope="module", autouse=True)
def cleanup_profiles_and_pipelines():
    yield
    for ws_name in ["server_ws_dev", "server_ws_tests"]:
        ws_dir = Path(TEST_STORAGE_ROOT) / ws_name
        if ws_dir.exists():
            shutil.rmtree(ws_dir, ignore_errors=True)


# .. Tests .. #


def test_dev_workspace(page: Page, kill_server_after, prepare_workspaces, seed_pipelines):
    proc = _start_dashboard_server(Path(TEST_STORAGE_ROOT) / "server_ws_dev", port=2718)
    kill_server_after(proc)

    page.goto("http://localhost:2718/?profile=tests")
    expect(page.get_by_role("link", name="fruitshop")).to_have_count(0)
    expect(page.get_by_role("link", name="rest_api_pokemon")).to_have_count(0)

    page.goto("http://localhost:2718/?profile=custom-dev-profile")
    expect(page.get_by_text("Workspace:")).to_be_visible()
    expect(page.get_by_text("custom-dev-workspace")).to_be_visible()
    expect(page.get_by_role("link", name="fruitshop")).to_be_visible()
    expect(page.get_by_role("link", name="rest_api_pokemon")).to_have_count(0)


def test_tests_workspace(page: Page, kill_server_after, prepare_workspaces, seed_pipelines):
    proc = _start_dashboard_server(Path(TEST_STORAGE_ROOT) / "server_ws_tests", port=2718)
    kill_server_after(proc)

    page.goto("http://localhost:2718/?profile=dev")
    expect(page.get_by_role("link", name="fruitshop")).to_have_count(0)
    expect(page.get_by_role("link", name="rest_api_pokemon")).to_have_count(0)

    page.goto("http://localhost:2718/?profile=custom-tests-profile")
    expect(page.get_by_text("Workspace:")).to_be_visible()
    expect(page.get_by_text("custom-tests-workspace")).to_be_visible()
    expect(page.get_by_role("link", name="rest_api_pokemon")).to_be_visible()
    expect(page.get_by_role("link", name="fruitshop")).to_have_count(0)
