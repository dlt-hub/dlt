import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import tomlkit

from dlt._workspace.cli.dlthub import utils as dlthub_utils
from dlt._workspace.cli.dlthub._init_command import (
    _render_pyproject,
    init_dlthub_workspace,
)
from dlt._workspace.cli.dlthub.typing import TInitPlan
from dlt._workspace.cli.dlthub.utils import (
    WORKSPACE_DEPS,
    fetch_init_plan,
)
from dlt._workspace.cli.exceptions import CliCommandException


def _set_uv(monkeypatch: pytest.MonkeyPatch, available: bool) -> None:
    monkeypatch.setattr(dlthub_utils, "is_uv_available", lambda: available)


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _statuses(plan: TInitPlan) -> Dict[str, str]:
    return {f["path"]: f["status"] for f in plan["files"]}


def _paths(plan: TInitPlan) -> List[str]:
    return [f["path"] for f in plan["files"]]


def _assert_seeded_deps(deps_file: Path) -> None:
    """Assert the deps file pins `dlt[hub]` (or `-e <path>` to dlt) and contains every WORKSPACE_DEPS entry."""
    if deps_file.suffix == ".toml":
        parsed = tomlkit.parse(_read(deps_file))
        listed = [str(d) for d in parsed["project"]["dependencies"]]  # type: ignore[index, union-attr]
        # pyproject always uses the `dlt[hub]` extra; a uv source override may carry the path
        assert listed[0].startswith("dlt[hub]")
    else:
        listed = _read(deps_file).splitlines()
        # requirements.txt path: editable installs render as `-e <path>` instead of `dlt[hub]==…`
        assert listed[0].startswith("dlt[hub]") or listed[0].startswith("-e ")
    for d in WORKSPACE_DEPS:
        assert d in listed


def _run_init(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    name: Optional[str] = None,
    force: bool = False,
    dry_run: bool = False,
    verbosity: int = 0,
) -> None:
    """Run `_execute_init` from `tmp_path` with sensible defaults."""
    from dlt._workspace.cli.dlthub.commands import _execute_init

    monkeypatch.chdir(str(tmp_path))
    _set_uv(monkeypatch, True)
    _execute_init(name=name, force=force, dry_run=dry_run, verbosity=verbosity)


def test_workspace_deps_match_pyproject_group() -> None:
    """`WORKSPACE_DEPS` constant mirrors `[dependency-groups] workspace-deps`."""
    pyproject = tomlkit.parse(
        Path(__file__).parents[4].joinpath("pyproject.toml").read_text(encoding="utf-8")
    )
    group = list(pyproject["dependency-groups"]["workspace-deps"])  # type: ignore[index, arg-type]
    assert list(WORKSPACE_DEPS) == [str(d) for d in group]


@pytest.mark.parametrize(
    "uv_available, deps_file, other_file",
    [
        pytest.param(True, "pyproject.toml", "requirements.txt", id="uv-on"),
        pytest.param(False, "requirements.txt", "pyproject.toml", id="uv-off"),
    ],
)
def test_fetch_init_plan_dependency_system_matches_uv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    uv_available: bool,
    deps_file: str,
    other_file: str,
) -> None:
    _set_uv(monkeypatch, uv_available)
    plan = fetch_init_plan(str(tmp_path), name="myproj")
    assert plan["uv_available"] is uv_available
    assert plan["dependency_system"] == deps_file
    assert plan["project_name"] == "myproj"
    paths = _paths(plan)
    assert os.path.join(str(tmp_path), deps_file) in paths
    assert os.path.join(str(tmp_path), other_file) not in paths


def test_fetch_init_plan_default_name_is_dirname(tmp_path: Path) -> None:
    target = tmp_path / "alpha-beta"
    target.mkdir()
    plan = fetch_init_plan(str(target))
    assert plan["project_name"] == "alpha-beta"


def test_fetch_init_plan_workspace_marker_existing_sets_flag(tmp_path: Path) -> None:
    """`workspace_exists=True` only when `.dlt/.workspace` is present."""
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (settings / ".workspace").write_text("", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path))

    assert plan["workspace_exists"] is True
    assert _statuses(plan)[os.path.join(str(tmp_path), ".dlt", ".workspace")] == "skip"


def test_fetch_init_plan_existing_files_are_skip_not_conflict(tmp_path: Path) -> None:
    """Without --force, existing scaffold files mark as `skip`, not `conflict`."""
    (tmp_path / "pyproject.toml").write_text("# theirs\n", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path))

    assert _statuses(plan)[os.path.join(str(tmp_path), "pyproject.toml")] == "skip"
    # only .workspace can ever conflict, and it doesn't exist here
    assert plan["workspace_exists"] is False


def test_fetch_init_plan_force_flips_existing_to_create(tmp_path: Path) -> None:
    """`force=True` flips `accept_existing` so existing files are reported as `create`."""
    (tmp_path / "pyproject.toml").write_text("# theirs\n", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path), force=True)

    assert _statuses(plan)[os.path.join(str(tmp_path), "pyproject.toml")] == "create"


def test_fetch_init_plan_secrets_toml_skipped_when_present(tmp_path: Path) -> None:
    """`secrets.toml` is always `accept_existing=True`, so it's `skip` when present."""
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (settings / "secrets.toml").write_text("# user creds\n", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path))

    statuses = _statuses(plan)
    assert statuses[os.path.join(str(tmp_path), ".dlt", "secrets.toml")] == "skip"
    # workspace marker would be created (doesn't exist yet)
    assert statuses[os.path.join(str(tmp_path), ".dlt", ".workspace")] == "create"


@pytest.mark.parametrize("user_owned", ["secrets.toml", "config.toml"])
def test_fetch_init_plan_force_keeps_user_owned_accept_existing(
    tmp_path: Path, user_owned: str
) -> None:
    """`secrets.toml` AND `config.toml` stay accept_existing=True even with --force."""
    plan = fetch_init_plan(str(tmp_path), force=True)
    entry = next(f for f in plan["files"] if f["path"].endswith(user_owned))
    assert entry["accept_existing"] is True


@pytest.mark.parametrize(
    "uv_available, deps_file",
    [
        pytest.param(True, "pyproject.toml", id="uv-on"),
        pytest.param(False, "requirements.txt", id="uv-off"),
    ],
)
def test_init_dlthub_workspace_writes_full_scaffold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    uv_available: bool,
    deps_file: str,
) -> None:
    _set_uv(monkeypatch, uv_available)
    plan = fetch_init_plan(str(tmp_path), name="my-fancy-proj")
    init_dlthub_workspace(plan)

    assert (tmp_path / deps_file).is_file()
    other = "requirements.txt" if deps_file == "pyproject.toml" else "pyproject.toml"
    assert not (tmp_path / other).exists()

    assert (tmp_path / ".gitignore").is_file()
    assert (tmp_path / ".dlt" / "config.toml").is_file()
    assert (tmp_path / ".dlt" / "secrets.toml").is_file()
    # marker enables workspace detection by `is_workspace_dir`
    assert (tmp_path / ".dlt" / ".workspace").is_file()

    _assert_seeded_deps(tmp_path / deps_file)

    # config.toml carries [workspace.settings] name + the [runtime] template content
    config = tomlkit.parse(_read(tmp_path / ".dlt" / "config.toml"))
    assert config["workspace"]["settings"]["name"] == "my-fancy-proj"  # type: ignore[index]
    assert "runtime" in config
    # workspaces always emit _dlt_load_id, incl. for arrow/parquet data
    assert config["normalize"]["parquet_normalizer"]["add_dlt_load_id"] is True  # type: ignore[index]

    # no pipelines, no deployment module
    assert not (tmp_path / "__deployment__.py").exists()
    assert not list(tmp_path.glob("*_pipeline.py"))


def test_init_dlthub_workspace_dry_run_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_uv(monkeypatch, True)
    plan = fetch_init_plan(str(tmp_path))
    preview = init_dlthub_workspace(plan, dry_run=True)

    # nothing on disk
    assert not (tmp_path / "pyproject.toml").exists()
    assert not (tmp_path / ".dlt").exists()
    # but preview lists every target
    paths = list(preview.keys())
    for relative in (
        "pyproject.toml",
        ".gitignore",
        os.path.join(".dlt", "config.toml"),
        os.path.join(".dlt", "secrets.toml"),
        os.path.join(".dlt", ".workspace"),
    ):
        assert os.path.join(str(tmp_path), relative) in paths


def test_init_dlthub_workspace_force_overwrites_but_keeps_user_owned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`--force` overwrites the dlthub-managed scaffold but never touches
    `secrets.toml` or `config.toml` (user-owned files)."""
    _set_uv(monkeypatch, True)
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (tmp_path / "pyproject.toml").write_text("# theirs\n", encoding="utf-8")
    (settings / "secrets.toml").write_text("# user creds\n", encoding="utf-8")
    (settings / "config.toml").write_text("# user-edited config\n", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path), force=True)
    init_dlthub_workspace(plan, force=True)

    # pyproject overwritten
    body = _read(tmp_path / "pyproject.toml")
    assert "# theirs" not in body
    assert "[project]" in body
    # user secrets preserved
    assert _read(settings / "secrets.toml") == "# user creds\n"
    # user config preserved (no [workspace.settings] injection over an existing file)
    assert _read(settings / "config.toml") == "# user-edited config\n"


def test_init_dlthub_workspace_default_keeps_existing_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without --force, the writer leaves existing user files alone (accept_existing=True)."""
    _set_uv(monkeypatch, True)
    (tmp_path / "pyproject.toml").write_text("# user-edited\n", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path))
    init_dlthub_workspace(plan)  # force=False by default

    # user content preserved
    assert _read(tmp_path / "pyproject.toml") == "# user-edited\n"
    # but the workspace marker DID get created (no force needed when absent)
    assert (tmp_path / ".dlt" / ".workspace").is_file()


@pytest.mark.parametrize(
    "uv_sources,expect_tool_section",
    [
        ({}, False),
        ({"dlt": {"path": "/home/me/dlt", "editable": True}}, True),
        ({"dlt": {"git": "https://example.com/dlt.git", "rev": "abc123"}}, True),
    ],
    ids=["empty-omits-tool", "editable-path", "git-with-rev"],
)
def test_render_pyproject_emits_uv_sources_when_present(
    uv_sources: Dict[str, Dict[str, Any]], expect_tool_section: bool
) -> None:
    """Valid TOML with `[project]` always; `[tool.uv.sources]` iff the map is non-empty."""
    deps: List[str] = ["dlt[hub]==1.26.0", "duckdb>=0.9"]
    body = _render_pyproject("hello", deps, uv_sources)
    parsed = tomlkit.parse(body)
    # baseline: project table is well-formed and dependencies round-trip
    assert parsed["project"]["name"] == "hello"  # type: ignore[index]
    assert list(parsed["project"]["dependencies"]) == deps  # type: ignore[index, arg-type]
    if not expect_tool_section:
        assert "tool" not in parsed
        return
    rendered = parsed["tool"]["uv"]["sources"]  # type: ignore[index]
    for pkg, src in uv_sources.items():
        assert dict(rendered[pkg]) == src  # type: ignore[index, arg-type]


_FAKE_DLT_PYPI: Dict[str, Any] = {
    "name": "dlt",
    "extras": ["hub"],
    "version": "1.26.0",
    "mode": "pypi",
}
_FAKE_DLT_EDITABLE: Dict[str, Any] = {
    "name": "dlt",
    "extras": ["hub"],
    "version": "1.26.0",
    "mode": "editable",
    "path": "/home/me/dlt",
}


@pytest.mark.parametrize(
    "uv_available,fake_spec,deps_filename,expected_first_line,expect_uv_sources",
    [
        (True, _FAKE_DLT_PYPI, "pyproject.toml", "dlt[hub]==1.26.0", False),
        (True, _FAKE_DLT_EDITABLE, "pyproject.toml", "dlt[hub]==1.26.0", True),
        (False, _FAKE_DLT_EDITABLE, "requirements.txt", "-e /home/me/dlt", False),
        (False, _FAKE_DLT_PYPI, "requirements.txt", "dlt[hub]==1.26.0", False),
    ],
    ids=[
        "pyproject-pypi-no-tool-section",
        "pyproject-editable-uv-sources",
        "requirements-editable-dash-e",
        "requirements-pypi-version-pin",
    ],
)
def test_init_renders_dlt_hub_with_correct_source_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    uv_available: bool,
    fake_spec: Dict[str, Any],
    deps_filename: str,
    expected_first_line: str,
    expect_uv_sources: bool,
) -> None:
    """End-to-end: scaffold reproduces the install mode of `dlt[hub]` in the right file."""
    _set_uv(monkeypatch, uv_available)
    monkeypatch.setattr(dlthub_utils, "get_workspace_install_specs", lambda: [fake_spec])

    plan = fetch_init_plan(str(tmp_path))
    init_dlthub_workspace(plan)

    if deps_filename == "pyproject.toml":
        parsed = tomlkit.parse(_read(tmp_path / "pyproject.toml"))
        deps = [str(d) for d in parsed["project"]["dependencies"]]  # type: ignore[index, union-attr]
        assert deps[0] == expected_first_line
        if expect_uv_sources:
            entry = parsed["tool"]["uv"]["sources"]["dlt"]  # type: ignore[index]
            assert dict(entry) == {"path": "/home/me/dlt", "editable": True}  # type: ignore[arg-type]
        else:
            assert "tool" not in parsed
    else:
        lines = _read(tmp_path / "requirements.txt").splitlines()
        assert lines[0] == expected_first_line
        # workspace tooling deps still follow
        assert "duckdb>=0.9" in lines


@pytest.mark.parametrize(
    "dependencies_choice,uv_available,expected_filename",
    [
        ("auto", True, "pyproject.toml"),
        ("auto", False, "requirements.txt"),
        ("pyproject", True, "pyproject.toml"),
        ("pyproject", False, "pyproject.toml"),
        ("requirements", True, "requirements.txt"),
        ("requirements", False, "requirements.txt"),
    ],
    ids=[
        "auto-uv-on-uses-pyproject",
        "auto-uv-off-uses-requirements",
        "pyproject-forces-pyproject-with-uv",
        "pyproject-forces-pyproject-without-uv",
        "requirements-forces-requirements-with-uv",
        "requirements-forces-requirements-without-uv",
    ],
)
def test_fetch_init_plan_dependencies_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dependencies_choice: str,
    uv_available: bool,
    expected_filename: str,
) -> None:
    """`--dependencies` override flips the file regardless of `uv` availability."""
    _set_uv(monkeypatch, uv_available)
    plan = fetch_init_plan(str(tmp_path), dependencies=dependencies_choice)  # type: ignore[arg-type]
    assert plan["dependency_system"] == expected_filename


def test_dlthub_init_default_output_is_quiet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_init(monkeypatch, tmp_path)
    out = capsys.readouterr().out

    # the "  files:" header is part of the verbose plan, must NOT appear in default output
    assert "files:" not in out
    # the welcome banner IS shown
    assert "Workspace ready at" in out
    # scaffold actually written
    assert (tmp_path / ".dlt" / ".workspace").is_file()


def test_dlthub_init_verbose_shows_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_init(monkeypatch, tmp_path, verbosity=1)
    out = capsys.readouterr().out

    assert "files:" in out
    assert "[CREATE]" in out
    assert "Workspace ready at" in out


def test_dlthub_init_dry_run_implies_verbose_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_init(monkeypatch, tmp_path, dry_run=True)
    out = capsys.readouterr().out

    # plan is shown without -v because --dry-run forces verbose
    assert "files:" in out
    # no welcome banner — we didn't actually init
    assert "Workspace ready at" not in out
    # nothing on disk
    assert not (tmp_path / "pyproject.toml").exists()
    assert not (tmp_path / ".dlt").exists()


def test_dlthub_init_bails_when_workspace_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Existing `.dlt/.workspace` triggers a friendly error before any write."""
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (settings / ".workspace").write_text("", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("# user\n", encoding="utf-8")
    pyproject_mtime = (tmp_path / "pyproject.toml").stat().st_mtime

    with pytest.raises(CliCommandException):
        _run_init(monkeypatch, tmp_path)

    out = capsys.readouterr().out
    assert "Workspace already exists" in out
    # nothing written
    assert (tmp_path / "pyproject.toml").stat().st_mtime == pyproject_mtime


def test_dlthub_init_force_re_inits_existing_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--force overrides the .workspace gate and overwrites scaffold files; secrets preserved."""
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (settings / ".workspace").write_text("", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("# old\n", encoding="utf-8")
    (settings / "secrets.toml").write_text("# user creds\n", encoding="utf-8")

    _run_init(monkeypatch, tmp_path, force=True)

    body = _read(tmp_path / "pyproject.toml")
    assert "# old" not in body
    assert "[project]" in body
    # secrets always preserved
    assert _read(settings / "secrets.toml") == "# user creds\n"
