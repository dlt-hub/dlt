import os
import subprocess
import sys
from pathlib import Path

import pytest

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.runtime.run_context import DOT_DLT
from dlt._workspace._workspace_context import WorkspaceRunContext

from tests.utils import capture_dlt_logger


def _write(path: str, text: str = "") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _reload(run_dir: str) -> None:
    Container()[PluggableRunContext].reload(run_dir)


def test_profile_files_without_workspace_warn_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "dev.config.toml"), 'bucket_url = "from-profile"\n')
    _write(os.path.join(settings, "prod.secrets.toml"), 'secret = "x"\n')
    _write(os.path.join(settings, "config.toml"), 'bucket_url = "from-base"\n')
    os.makedirs(os.path.join(settings, "notes.config.toml"))

    with capture_dlt_logger(caplog):
        _reload(str(tmp_path))
    assert "dev.config.toml" in caplog.text
    assert "prod.secrets.toml" in caplog.text
    assert "notes.config.toml" not in caplog.text
    assert "will not be loaded" in caplog.text
    assert ".workspace" in caplog.text

    caplog.clear()
    with capture_dlt_logger(caplog):
        _reload(str(tmp_path))
    assert "will not be loaded" not in caplog.text


def test_base_toml_files_do_not_warn(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "config.toml"))
    _write(os.path.join(settings, "secrets.toml"))

    with capture_dlt_logger(caplog):
        _reload(str(tmp_path))
    assert "will not be loaded" not in caplog.text


def test_workspace_marker_suppresses_profile_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "dev.config.toml"), 'bucket_url = "from-profile"\n')
    _write(os.path.join(settings, ".workspace"))

    with capture_dlt_logger(caplog):
        _reload(str(tmp_path))
    assert isinstance(Container()[PluggableRunContext].context, WorkspaceRunContext)
    assert "will not be loaded" not in caplog.text


def test_cold_start_warns_after_runtime_init(tmp_path: Path) -> None:
    # a fresh interpreter has no logger when the run-context hook first runs
    settings = tmp_path / DOT_DLT
    settings.mkdir()
    (settings / "dev.config.toml").write_text('bucket_url = "from-profile"\n', encoding="utf-8")
    script = (
        "from dlt.common.configuration.container import Container\n"
        "from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext\n"
        "Container()[PluggableRunContext]\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "dev.config.toml" in proc.stderr
    assert "will not be loaded" in proc.stderr
