import os
from pathlib import Path

import pytest

from dlt._workspace._plugins import plug_workspace_context_impl
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt.common.runtime.run_context import DOT_DLT

from tests.utils import capture_dlt_logger


def _write(path: str, text: str = "") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def test_profile_files_without_workspace_warn_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "dev.config.toml"), 'bucket_url = "from-profile"\n')
    _write(os.path.join(settings, "prod.secrets.toml"), 'secret = "x"\n')
    _write(os.path.join(settings, "config.toml"), 'bucket_url = "from-base"\n')

    with capture_dlt_logger(caplog):
        assert plug_workspace_context_impl(str(tmp_path), None) is None
    assert "dev.config.toml" in caplog.text
    assert "prod.secrets.toml" in caplog.text
    assert "will not be loaded" in caplog.text
    assert ".workspace" in caplog.text

    caplog.clear()
    with capture_dlt_logger(caplog):
        plug_workspace_context_impl(str(tmp_path), None)
    assert "will not be loaded" not in caplog.text


def test_base_toml_files_do_not_warn(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "config.toml"))
    _write(os.path.join(settings, "secrets.toml"))

    with capture_dlt_logger(caplog):
        assert plug_workspace_context_impl(str(tmp_path), None) is None
    assert "will not be loaded" not in caplog.text


def test_workspace_marker_suppresses_profile_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "dev.config.toml"), 'bucket_url = "from-profile"\n')
    _write(os.path.join(settings, ".workspace"))

    with capture_dlt_logger(caplog):
        ctx = plug_workspace_context_impl(str(tmp_path), None)
    assert isinstance(ctx, WorkspaceRunContext)
    assert "will not be loaded" not in caplog.text
