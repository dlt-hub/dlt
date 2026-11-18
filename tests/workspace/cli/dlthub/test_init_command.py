import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import tomlkit
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli._pipeline_files import get_single_file_templates_storage
from dlt._workspace.cli.dlthub import utils as dlthub_utils
from dlt._workspace.cli.dlthub._init_command import MERGE_MARKER, init_dlthub_workspace
from dlt._workspace.cli.dlthub.typing import TInitPlan
from dlt._workspace.cli.dlthub.utils import (
    get_missing_gitignore_entries,
    workspace_deps,
    fetch_init_plan,
)
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.deployment.requirements import is_uv_available, python_version
from dlt._workspace.workspace_definition import (
    WORKSPACE_ENGINE_VERSION,
    create_built_in_workspace_definition,
)
from dlt._workspace.workspace_file import build_stored_workspace_definition
from dlt._workspace.settings_files import load_yaml_file
from dlt.version import get_requires_python

from tests.workspace.cli.dlthub.ai.utils import AI_CASES_DIR


def _set_uv(monkeypatch: pytest.MonkeyPatch, available: bool) -> None:
    monkeypatch.setattr(dlthub_utils, "is_uv_available", lambda: available)


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _statuses(plan: TInitPlan) -> Dict[str, str]:
    return {f["path"]: f["status"] for f in plan["files"]}


def _paths(plan: TInitPlan) -> List[str]:
    return [f["path"] for f in plan["files"]]


def _assert_seeded_deps(deps_file: Path) -> None:
    """Assert the deps file pins `dlt[hub]` (or `-e <path>` to dlt) and contains every `workspace_deps()` entry."""
    if deps_file.suffix == ".toml":
        parsed = tomlkit.parse(_read(deps_file))
        listed = [str(d) for d in parsed["project"]["dependencies"]]  # type: ignore[index, union-attr]
        # pyproject always uses the `dlt[hub]` extra; a uv source override may carry the path
        assert listed[0].startswith("dlt[hub]")
    else:
        listed = _read(deps_file).splitlines()
        # requirements.txt path: editable installs render as `-e <path>` instead of `dlt[hub]==…`
        assert listed[0].startswith("dlt[hub]") or listed[0].startswith("-e ")
    for d in workspace_deps():
        assert d in listed


def _run_init(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    name: Optional[str] = None,
    overwrite: bool = False,
    skip: bool = False,
    dry_run: bool = False,
    verbosity: int = 0,
    agent: Optional[str] = None,
    answer: bool = False,
    uv: bool = True,
) -> None:
    """Run `_execute_init` from `tmp_path` with `answer` given to its confirmation."""
    from dlt._workspace.cli.dlthub.commands import _execute_init

    monkeypatch.chdir(str(tmp_path))
    _set_uv(monkeypatch, uv)
    with fmt.always_choose(False, answer):
        _execute_init(
            name=name,
            overwrite=overwrite,
            skip=skip,
            dry_run=dry_run,
            verbosity=verbosity,
            agent=agent,
        )


def _mock_harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Copies the mock AI harness and makes it the default one; returns the workspace dir."""
    harness = tmp_path / "harness"
    shutil.copytree(os.path.join(AI_CASES_DIR, "mock_workbench"), harness / "workbench")
    monkeypatch.setattr("dlt._workspace.cli._urls.DEFAULT_AI_WORKBENCH_REPO", str(harness))
    # cortex writes its user MCP config to SNOWFLAKE_HOME
    monkeypatch.setenv("SNOWFLAKE_HOME", str(tmp_path / "snowflake_home"))
    workspace_dir = tmp_path / "ws"
    workspace_dir.mkdir()
    return workspace_dir


def test_workspace_deps_match_pyproject_group() -> None:
    """`workspace_deps()` mirrors `[dependency-groups] workspace-deps`."""
    pyproject = tomlkit.parse(
        Path(__file__).parents[4].joinpath("pyproject.toml").read_text(encoding="utf-8")
    )
    group = list(pyproject["dependency-groups"]["workspace-deps"])  # type: ignore[index, arg-type]
    assert workspace_deps() == [str(d) for d in group]


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


def test_fetch_init_plan_existing_pyproject(tmp_path: Path) -> None:
    pyproject = os.path.join(str(tmp_path), "pyproject.toml")
    (tmp_path / "pyproject.toml").write_text("# theirs\n", encoding="utf-8")

    # skip keeps it
    plan = fetch_init_plan(str(tmp_path), skip=True)
    assert _statuses(plan)[pyproject] == "skip"
    assert "merge_error" not in plan

    # a merge needs a [project] table to add the dependencies to
    plan = fetch_init_plan(str(tmp_path))
    assert _statuses(plan)[pyproject] == "skip"
    assert plan["merge_error"] == "no_project_table"

    plan = fetch_init_plan(str(tmp_path), overwrite=True)
    assert _statuses(plan)[pyproject] == "overwrite"
    assert "merge_error" not in plan


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
def test_fetch_init_plan_overwrite_keeps_user_owned_accept_existing(
    tmp_path: Path, user_owned: str
) -> None:
    """`secrets.toml` AND `config.toml` stay accept_existing=True even with --overwrite."""
    plan = fetch_init_plan(str(tmp_path), overwrite=True)
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
    # marker enables workspace detection by `is_workspace_dir` and declares the built-in profiles
    workspace_path = str(tmp_path / ".dlt" / ".workspace")
    workspace_file = load_yaml_file(workspace_path)
    assert workspace_file["engine_version"] == WORKSPACE_ENGINE_VERSION
    assert workspace_file == build_stored_workspace_definition(
        create_built_in_workspace_definition(), workspace_path
    )
    assert "name" not in workspace_file["environments"]["prod"]
    assert "env" not in workspace_file["environments"]["prod"]["profiles"]["access"]

    _assert_seeded_deps(tmp_path / deps_file)
    if deps_file == "pyproject.toml":
        project = tomlkit.parse(_read(tmp_path / deps_file))["project"]
        assert project["requires-python"] == plan["requires_python"]  # type: ignore[index]
        # the running Python and newer ones, up to the newest Python dlt supports
        specifiers = {str(s) for s in SpecifierSet(plan["requires_python"])}
        assert ">=" + python_version() in specifiers
        dlt_upper_bounds = {
            str(s) for s in SpecifierSet(get_requires_python()) if s.operator.startswith("<")
        }
        assert dlt_upper_bounds <= specifiers

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


def test_init_dlthub_workspace_overwrite_keeps_user_owned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`--overwrite` replaces the dlthub-managed scaffold but never touches
    `secrets.toml` or `config.toml` (user-owned files)."""
    _set_uv(monkeypatch, True)
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (tmp_path / "pyproject.toml").write_text("# theirs\n", encoding="utf-8")
    (settings / "secrets.toml").write_text("# user creds\n", encoding="utf-8")
    (settings / "config.toml").write_text("# user-edited config\n", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path), overwrite=True)
    init_dlthub_workspace(plan, overwrite=True)

    # pyproject overwritten
    body = _read(tmp_path / "pyproject.toml")
    assert "# theirs" not in body
    assert "[project]" in body
    # user secrets preserved
    assert _read(settings / "secrets.toml") == "# user creds\n"
    # user config preserved (no [workspace.settings] injection over an existing file)
    assert _read(settings / "config.toml") == "# user-edited config\n"


def test_init_dlthub_workspace_skip_keeps_existing_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With --skip, the writer leaves existing user files alone (accept_existing=True)."""
    _set_uv(monkeypatch, True)
    (tmp_path / "pyproject.toml").write_text("# user-edited\n", encoding="utf-8")

    plan = fetch_init_plan(str(tmp_path), skip=True)
    init_dlthub_workspace(plan)

    # user content preserved
    assert _read(tmp_path / "pyproject.toml") == "# user-edited\n"
    # but the missing workspace marker is created
    assert (tmp_path / ".dlt" / ".workspace").is_file()


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
        assert "duckdb>=0.9,<2" in lines


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


def test_dlthub_init_new_workspace_asks_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A new workspace is written without showing the plan or asking, a re-run writes nothing."""
    # `_run_init` declines when asked
    _run_init(monkeypatch, tmp_path)
    out = capsys.readouterr().out
    assert "[CREATE]" not in out
    assert "Workspace ready at" in out
    assert (tmp_path / ".dlt" / ".workspace").is_file()

    _run_init(monkeypatch, tmp_path)
    out = capsys.readouterr().out
    assert "Workspace exists. Nothing to do." in out
    assert "Workspace ready at" not in out


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


def test_dlthub_init_without_agent_sets_up_no_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace_dir = _mock_harness(tmp_path, monkeypatch)
    _run_init(monkeypatch, workspace_dir)
    assert not (workspace_dir / ".dlt" / ".toolkits").exists()
    assert not (workspace_dir / ".claude").exists()
    assert "Configure your AI coding agent" in capsys.readouterr().out


def test_dlthub_init_with_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The agent's rules are installed into the new workspace right after the scaffold."""
    workspace_dir = _mock_harness(tmp_path, monkeypatch)
    _run_init(monkeypatch, workspace_dir, agent="claude")
    out = capsys.readouterr().out
    assert (workspace_dir / ".claude" / "rules" / "init-base.md").is_file()
    index = load_yaml_file(str(workspace_dir / ".dlt" / ".toolkits"))
    assert index["init"]["agent"] == "claude"
    assert "Workspace ready at" in out
    assert "Configure your AI coding agent" not in out
    # claude gets the default dependencies
    _assert_seeded_deps(workspace_dir / "pyproject.toml")

    # the agent is already set up
    _run_init(monkeypatch, workspace_dir, agent="claude")
    out = capsys.readouterr().out
    assert "Workspace exists. Nothing to do." in out
    assert "install your first toolkit" not in out


def test_dlthub_init_agent_into_existing_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Setting up an agent in an up-to-date workspace is a change, not an existing workspace."""
    workspace_dir = _mock_harness(tmp_path, monkeypatch)
    _run_init(monkeypatch, workspace_dir)
    capsys.readouterr()

    _run_init(monkeypatch, workspace_dir, agent="claude")
    out = capsys.readouterr().out
    assert "Workspace ready at" in out
    assert "Workspace exists." not in out
    assert (workspace_dir / ".claude" / "rules" / "init-base.md").is_file()


def test_dlthub_init_with_cortex_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A cortex workspace depends on dlthub without dlthub-client and without marimo."""
    workspace_dir = _mock_harness(tmp_path, monkeypatch)
    _run_init(monkeypatch, workspace_dir, agent="cortex", verbosity=1)
    out = capsys.readouterr().out
    assert "AI coding agent: cortex" in out

    parsed = tomlkit.parse(_read(workspace_dir / "pyproject.toml"))
    listed = [str(d) for d in parsed["project"]["dependencies"]]  # type: ignore[index, union-attr]
    names = {Requirement(d).name for d in listed if not d.startswith("-e ")}
    assert "dlthub-client" not in names
    assert "marimo" not in names
    assert "mowidgets" not in names
    assert "dlthub" in names
    assert not any(d.startswith("dlt[hub]") for d in listed)
    assert (workspace_dir / "CORTEX.md").is_file()
    assert (workspace_dir / ".cortex" / "rules" / "init-base.md").is_file()


def test_dlthub_init_ambiguous_agent_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """When no single agent is detected, init stops before writing anything."""
    workspace_dir = _mock_harness(tmp_path, monkeypatch)
    monkeypatch.setenv("CLAUDECODE", "1")
    monkeypatch.setenv("CORTEX_CODE_CLIENT_SURFACE", "coco_cli")
    with pytest.raises(CliCommandException):
        _run_init(monkeypatch, workspace_dir, agent="auto")
    assert "Multiple AI coding agents detected" in capsys.readouterr().out
    assert not (workspace_dir / ".dlt").exists()
    assert not (workspace_dir / "pyproject.toml").exists()


def test_dlthub_init_dry_run_with_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace_dir = _mock_harness(tmp_path, monkeypatch)
    _run_init(monkeypatch, workspace_dir, agent="cortex", dry_run=True)
    out = capsys.readouterr().out
    assert "AI coding agent: cortex" in out
    assert "dependencies:" in out
    assert not (workspace_dir / ".dlt").exists()
    assert not (workspace_dir / ".cortex").exists()


def test_dlthub_init_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """--skip keeps an existing workspace and its files without asking and adds what is missing."""
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (settings / ".workspace").write_text("", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("# user\n", encoding="utf-8")
    (tmp_path / ".gitignore").write_text(".venv\n", encoding="utf-8")

    _run_init(monkeypatch, tmp_path, skip=True)

    assert "Workspace ready at" in capsys.readouterr().out
    assert _read(tmp_path / "pyproject.toml") == "# user\n"
    assert _read(tmp_path / ".gitignore") == ".venv\n"
    assert _read(settings / ".workspace") == ""
    assert (settings / "config.toml").is_file()


def test_dlthub_init_overwrite_existing_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """--overwrite shows the files it overwrites and asks, secrets are preserved."""
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (settings / ".workspace").write_text("", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("# old\n", encoding="utf-8")
    (settings / "secrets.toml").write_text("# user creds\n", encoding="utf-8")

    with pytest.raises(CliCommandException):
        _run_init(monkeypatch, tmp_path, overwrite=True)
    out = capsys.readouterr().out
    assert re.search(r"\[OVERWRITE\] +pyproject\.toml", out)
    assert "secrets.toml" not in out
    assert "dlt changed nothing." in out
    assert _read(tmp_path / "pyproject.toml") == "# old\n"

    _run_init(monkeypatch, tmp_path, overwrite=True, answer=True)

    body = _read(tmp_path / "pyproject.toml")
    assert "# old" not in body
    assert "[project]" in body
    # secrets always preserved
    assert _read(settings / "secrets.toml") == "# user creds\n"


def test_gitignore_template_required_block() -> None:
    """The top block of the `.gitignore` template is what a merge adds to an existing one."""
    template = get_single_file_templates_storage().load(".gitignore")
    required = [
        "secrets.toml",
        "*.secrets.toml",
        ".dlt/profile-name",
        ".dlt/state",
        ".dlt/data",
        "*.duckdb",
        "*.wal",
        "__marimo__/",
        ".streamlit/secrets.toml",
    ]
    assert get_missing_gitignore_entries("", template) == required
    assert get_missing_gitignore_entries(".venv\n  secrets.toml \n", template) == required[1:]


def test_dlthub_init_merge_into_requirements_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A merge adds the missing .gitignore entries and the missing or outdated dependencies and
    keeps everything else. It shows the changes and asks first, a second run changes nothing."""
    (tmp_path / ".gitignore").write_text(".venv\nsecrets.toml\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text(
        "# my deps\nrequests>=2\ndlt==1.20.0  # old\n", encoding="utf-8"
    )
    settings = tmp_path / ".dlt"
    settings.mkdir()
    (settings / "secrets.toml").write_text("# user creds\n", encoding="utf-8")

    # --dry-run shows the full plan
    _run_init(monkeypatch, tmp_path, dry_run=True, uv=False)
    out = capsys.readouterr().out
    assert "[CREATE]" in out
    assert "[MERGE]" in out
    assert "+ __marimo__/" in out
    assert "~ dlt==1.20.0 -> " in out

    # the question shows only the changes to existing files
    with pytest.raises(CliCommandException):
        _run_init(monkeypatch, tmp_path, uv=False)
    out = capsys.readouterr().out
    assert re.search(r"\[MERGE\] +\.gitignore", out)
    assert re.search(r"\[MERGE\] +requirements\.txt", out)
    assert "~ dlt==1.20.0 -> " in out
    assert "[CREATE]" not in out
    assert _read(tmp_path / ".gitignore") == ".venv\nsecrets.toml\n"
    assert not (settings / ".workspace").exists()

    _run_init(monkeypatch, tmp_path, uv=False, answer=True)
    gitignore = _read(tmp_path / ".gitignore").splitlines()
    assert gitignore[:4] == [".venv", "secrets.toml", "", MERGE_MARKER]
    assert gitignore.count("secrets.toml") == 1
    assert "__marimo__/" in gitignore
    requirements = _read(tmp_path / "requirements.txt").splitlines()
    assert requirements[:2] == ["# my deps", "requests>=2"]
    # the outdated dlt is replaced in place
    assert requirements[2].startswith(("dlt[hub]==", "-e "))
    for dep in workspace_deps():
        assert dep in requirements
    assert _read(settings / "secrets.toml") == "# user creds\n"
    assert (settings / ".workspace").is_file()

    merged = {name: _read(tmp_path / name) for name in (".gitignore", "requirements.txt")}
    capsys.readouterr()
    _run_init(monkeypatch, tmp_path, uv=False)
    out = capsys.readouterr().out
    assert "[MERGE]" not in out
    assert "Workspace exists. Nothing to do." in out
    assert {name: _read(tmp_path / name) for name in merged} == merged


@pytest.mark.skipif(not is_uv_available(), reason="uv merges into pyproject.toml")
@pytest.mark.parametrize("with_lock", [False, True], ids=["no-lock", "lock"])
def test_dlthub_init_merge_into_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, with_lock: bool
) -> None:
    """uv adds the missing and upgrades the outdated dependencies, keeps the others and never
    touches the lock."""
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "mine"\nversion = "0.1.0"\nrequires-python = ">=3.10"\n'
        'dependencies = ["requests>=2", "dlt==1.20.0"]\n',
        encoding="utf-8",
    )
    lock = tmp_path / "uv.lock"
    if with_lock:
        lock.write_text("version = 1\n", encoding="utf-8")

    _run_init(monkeypatch, tmp_path, answer=True)

    parsed = tomlkit.parse(_read(tmp_path / "pyproject.toml"))
    assert parsed["project"]["name"] == "mine"  # type: ignore[index]
    declared: Dict[str, Requirement] = {
        canonicalize_name(Requirement(str(d)).name): Requirement(str(d))
        for d in parsed["project"]["dependencies"]  # type: ignore[index, union-attr]
    }
    assert str(declared["requests"].specifier) == ">=2"
    assert "hub" in declared["dlt"].extras
    assert "1.20.0" not in str(declared["dlt"].specifier)
    assert "fastmcp" in declared
    if with_lock:
        assert _read(lock) == "version = 1\n"
    else:
        assert not lock.exists()


@pytest.mark.parametrize(
    "pyproject, uv, error",
    [
        (
            '[project]\nname = "mine"\nversion = "0.1.0"\ndependencies = []\n',
            False,
            "uv is needed",
        ),
        ("[tool.ruff]\nline-length = 100\n", True, "has no [project] table"),
    ],
    ids=["no-uv", "no-project-table"],
)
def test_dlthub_init_cannot_merge_pyproject(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    pyproject: str,
    uv: bool,
    error: str,
) -> None:
    (tmp_path / "pyproject.toml").write_text(pyproject, encoding="utf-8")
    with pytest.raises(CliCommandException):
        _run_init(monkeypatch, tmp_path, uv=uv, answer=True)
    assert error in capsys.readouterr().out
    assert not (tmp_path / ".dlt").exists()
    assert _read(tmp_path / "pyproject.toml") == pyproject


def test_dlthub_init_merge_existing_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, main_dlthub: Any
) -> None:
    """`dlthub init -y` adds what an existing workspace lacks and keeps its settings."""
    _run_init(monkeypatch, tmp_path, uv=False)
    (tmp_path / ".gitignore").write_text(".venv\n", encoding="utf-8")
    settings = {
        name: _read(tmp_path / ".dlt" / name)
        for name in ("config.toml", "secrets.toml", ".workspace")
    }

    code, err = main_dlthub("init", "-y")
    assert code == 0, err
    assert "secrets.toml" in _read(tmp_path / ".gitignore").splitlines()
    assert {name: _read(tmp_path / ".dlt" / name) for name in settings} == settings

    # --force is the old name of --overwrite
    code, err = main_dlthub("init", "--force", "-y")
    assert code == 0, err
    assert MERGE_MARKER not in _read(tmp_path / ".gitignore")
    assert _read(tmp_path / ".dlt" / "secrets.toml") == settings["secrets.toml"]
