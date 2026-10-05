import logging
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.runtime.run_context import DOT_DLT, switch_context
from dlt.common.utils import custom_environ
from dlt._workspace._workspace_context import WorkspaceRunContext


def _write(path: str, text: str = "") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _reload(run_dir: str) -> None:
    Container()[PluggableRunContext].reload(run_dir)


@contextmanager
def _dlt_records() -> Iterator[List[logging.LogRecord]]:
    # runtime init sets propagate=False, so caplog on the root logger misses the warning
    records: List[logging.LogRecord] = []
    dlt_logger = logging.getLogger("dlt")
    handler = logging.Handler()
    handler.setLevel(logging.DEBUG)
    handler.emit = lambda record: records.append(record)  # type: ignore[method-assign]
    dlt_logger.addHandler(handler)
    try:
        yield records
    finally:
        dlt_logger.removeHandler(handler)


def _messages(records: List[logging.LogRecord]) -> str:
    return " ".join(record.getMessage() for record in records)


def test_profile_files_without_workspace_warn_once(tmp_path: Path) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "dev.config.toml"), 'bucket_url = "from-profile"\n')
    _write(os.path.join(settings, "prod.secrets.toml"), 'secret = "x"\n')
    _write(os.path.join(settings, "config.toml"), 'bucket_url = "from-base"\n')
    os.makedirs(os.path.join(settings, "notes.config.toml"))

    with _dlt_records() as records:
        _reload(str(tmp_path))
    text = _messages(records)
    assert "dev.config.toml" in text
    assert "prod.secrets.toml" in text
    assert "notes.config.toml" not in text
    assert "will not be loaded" in text
    assert ".workspace" in text
    assert str(tmp_path) in text

    with _dlt_records() as records:
        _reload(str(tmp_path))
    assert "will not be loaded" not in _messages(records)


def test_base_toml_files_do_not_warn(tmp_path: Path) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "config.toml"))
    _write(os.path.join(settings, "secrets.toml"))

    with _dlt_records() as records:
        _reload(str(tmp_path))
    assert "will not be loaded" not in _messages(records)


def test_workspace_marker_suppresses_profile_warning(tmp_path: Path) -> None:
    settings = os.path.join(str(tmp_path), DOT_DLT)
    _write(os.path.join(settings, "dev.config.toml"), 'bucket_url = "from-profile"\n')
    _write(os.path.join(settings, ".workspace"))

    with _dlt_records() as records:
        _reload(str(tmp_path))
    assert isinstance(Container()[PluggableRunContext].context, WorkspaceRunContext)
    assert "will not be loaded" not in _messages(records)


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
    assert str(tmp_path) in proc.stderr


def test_reload_warns_with_the_new_log_level(tmp_path: Path) -> None:
    # previous context at ERROR must not swallow the warning for the next project
    quiet = tmp_path / "quiet"
    loud = tmp_path / "loud"
    _write(os.path.join(str(quiet), DOT_DLT, "config.toml"), '[runtime]\nlog_level = "ERROR"\n')
    _write(os.path.join(str(loud), DOT_DLT, "config.toml"), '[runtime]\nlog_level = "INFO"\n')
    _write(
        os.path.join(str(loud), DOT_DLT, "dev.config.toml"),
        'bucket_url = "from-profile"\n',
    )

    with _dlt_records() as records:
        with custom_environ({"RUNTIME__LOG_LEVEL": "ERROR"}):
            switch_context(str(quiet))
        records.clear()
        with custom_environ({"RUNTIME__LOG_LEVEL": "INFO"}):
            switch_context(str(loud))

    messages = _messages(records)
    assert "dev.config.toml" in messages
    assert "will not be loaded" in messages
    assert str(loud) in messages
