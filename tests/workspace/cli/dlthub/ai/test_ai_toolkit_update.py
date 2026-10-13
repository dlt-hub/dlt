import argparse
import json
import shutil
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest
import yaml

from dlt._workspace.cli.dlthub.commands import AiCommand
from dlt._workspace.cli.dlthub.ai import commands
from dlt._workspace.cli.dlthub.ai.agents import AI_AGENTS, _AIAgent
from dlt._workspace.cli.dlthub.ai.utils import (
    compute_file_hash,
    fetch_ai_status,
    load_toolkits_index,
    save_toolkits_index,
)
from dlt._workspace.cli.exceptions import CliCommandException


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> Path:
    base = Path("workbench")
    base.mkdir()
    monkeypatch.setattr(commands, "fetch_workbench_base", lambda *_: base)
    return base


def ship(base: Path, name: str = "example", files: Optional[Dict[str, str]] = None) -> Path:
    toolkit = base / name
    meta = toolkit / ".claude-plugin" / "plugin.json"
    meta.parent.mkdir(parents=True, exist_ok=True)
    meta.write_text(json.dumps({"name": name, "version": "1", "description": "Original"}))
    for rel, content in (files if files is not None else {"commands/a.md": "original"}).items():
        path = toolkit / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return toolkit


def new_version(toolkit: Path, **fields: str) -> None:
    meta = toolkit / ".claude-plugin" / "plugin.json"
    data = json.loads(meta.read_text())
    data.update(version="2", description="New description", **fields)
    meta.write_text(json.dumps(data))


def install(name: str = "example", agent: str = "claude", overwrite: bool = False) -> None:
    commands.ai_toolkit_install_command(name, agent, "test-source", overwrite=overwrite)


def update(name: Optional[str] = None, overwrite: bool = False, strict: bool = False) -> None:
    commands.ai_toolkit_update_command(name, "test-source", overwrite=overwrite, strict=strict)


@pytest.mark.parametrize("name", [None, "example"])
def test_update_cli_dispatch(name: Optional[str]) -> None:
    parser = argparse.ArgumentParser()
    command = AiCommand()
    command.configure_parser(parser)
    argv = ["toolkit", "update"] + ([name] if name else [])
    args = parser.parse_args(
        argv + ["--overwrite", "--strict", "--location", "local", "--branch", "dev"]
    )
    with patch("dlt._workspace.cli.dlthub.ai.ai_toolkit_update_command") as run:
        command.execute(args)
    run.assert_called_once_with(
        name=name, location="local", branch="dev", overwrite=True, strict=True
    )


@pytest.fixture
def installed_file(source: Path, state: str) -> Path:
    ship(source)
    install()
    path = Path(".claude/commands/a.md")
    if state == "edited":
        path.write_text("user edit")
    elif state == "unknown":
        records = load_toolkits_index()
        records["example"]["files"][str(path)] = {}
        save_toolkits_index(records)
    elif state == "absent":
        path.unlink()
    return path


@pytest.mark.parametrize(
    "state,overwrite,expected",
    [
        ("unchanged", False, "new version"),
        ("unchanged", True, "new version"),
        ("edited", False, "user edit"),
        ("edited", True, "new version"),
        ("unknown", False, "original"),
        ("unknown", True, "new version"),
        ("absent", False, "new version"),
        ("absent", True, "new version"),
    ],
)
def test_update_desired_file(
    source: Path, installed_file: Path, overwrite: bool, expected: str
) -> None:
    before = load_toolkits_index()["example"]["files"][str(installed_file)]
    (source / "example/commands/a.md").write_text("new version")
    new_version(source / "example")
    update(overwrite=overwrite)
    entry = load_toolkits_index()["example"]
    assert installed_file.read_text() == expected
    assert entry["version"] == "2" and entry["description"] == "New description"
    expected_record = (
        {"sha3_256": compute_file_hash(installed_file)} if expected == "new version" else before
    )
    assert entry["files"][str(installed_file)] == expected_record


@pytest.mark.parametrize(
    "state,overwrite,expected",
    [
        ("unchanged", False, None),
        ("unchanged", True, None),
        ("edited", False, "user edit"),
        ("edited", True, None),
        ("unknown", False, "original"),
        ("unknown", True, None),
        ("absent", False, None),
        ("absent", True, None),
    ],
)
def test_update_orphan(
    source: Path, installed_file: Path, overwrite: bool, expected: Optional[str]
) -> None:
    before = load_toolkits_index()["example"]["files"][str(installed_file)]
    (source / "example/commands/a.md").unlink()
    new_version(source / "example")
    update(overwrite=overwrite)
    entry = load_toolkits_index()["example"]
    assert entry["version"] == "2" and entry["description"] == "New description"
    if expected is None:
        assert not installed_file.exists()
        assert str(installed_file) not in entry["files"]
    else:
        assert installed_file.read_text() == expected
        assert entry["files"][str(installed_file)] == before


def test_update_selects_installed_only_and_checks_same_version(source: Path) -> None:
    for name in ("init", "a", "b", "not-installed"):
        ship(source, name, {"commands/%s.md" % name: "original"})
    for name in ("init", "a", "b"):
        install(name)
        (source / name / ("commands/%s.md" % name)).write_text("changed without version bump")
    with (
        patch.object(commands, "fetch_workbench_base", return_value=source) as fetch,
        patch.object(commands, "load_toolkits_index", wraps=load_toolkits_index) as load,
        patch.object(
            commands, "fetch_workbench_toolkits", side_effect=AssertionError("catalogue scan")
        ),
    ):
        update("a")
        assert Path(".claude/commands/a.md").read_text() == "changed without version bump"
        assert Path(".claude/commands/b.md").read_text() == "original"
        update()
        assert load.call_count == 2
        assert fetch.call_count == 2
    assert Path(".claude/commands/init.md").read_text() == "changed without version bump"
    assert Path(".claude/commands/b.md").read_text() == "changed without version bump"
    assert not Path(".claude/commands/not-installed.md").exists()


def test_empty_and_unknown_selection_do_not_fetch(source: Path) -> None:
    with patch.object(commands, "fetch_workbench_base", side_effect=AssertionError("fetch")):
        update()
        with pytest.raises(CliCommandException):
            update("not-installed")


def test_update_uses_recorded_host(source: Path) -> None:
    ship(source, "a", {"commands/a.md": "a"})
    ship(source, "b", {"commands/b.md": "b"})
    install("a", "claude")
    install("b", "cursor")
    (source / "b/commands/b.md").write_text("new b")
    update()
    assert Path(".cursor/commands/b.md").read_text() == "new b"
    assert not Path(".claude/commands/b.md").exists()


def test_update_does_not_install_new_dependencies(
    source: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    toolkit = ship(source)
    ship(source, "dep", {"commands/dep.md": "dependency"})
    install()
    (toolkit / ".claude-plugin/toolkit.json").write_text(json.dumps({"dependencies": ["dep"]}))
    update()
    assert "missing dependencies: dep" in capsys.readouterr().out
    assert "dep" not in load_toolkits_index()
    assert not Path(".claude/commands/dep.md").exists()


@pytest.mark.parametrize("overwrite", [False, True])
def test_update_preserves_untracked_skill_folder(source: Path, overwrite: bool) -> None:
    toolkit = ship(source)
    install()
    folder = Path(".claude/skills/mine")
    folder.mkdir(parents=True)
    (folder / "SKILL.md").write_text("my skill")
    shipped = toolkit / "skills/mine"
    shipped.mkdir(parents=True)
    (shipped / "SKILL.md").write_text("shipped skill")
    (shipped / "helper.py").write_text("helper")
    update(overwrite=overwrite)
    assert (folder / "SKILL.md").read_text() == ("shipped skill" if overwrite else "my skill")
    assert (folder / "helper.py").exists() == overwrite
    assert (str(folder / "helper.py") in load_toolkits_index()["example"]["files"]) == overwrite


def test_incomplete_inventory_suppresses_orphans(source: Path) -> None:
    toolkit = ship(source, files={"skills/mine/SKILL.md": "skill", "commands/a.md": "original"})
    install()
    marker = toolkit / "skills/mine/SKILL.md"
    marker.write_text("---\nname: [broken\n---\n")
    (toolkit / "commands/a.md").unlink()
    new_version(toolkit)
    with pytest.raises(CliCommandException):
        update(strict=True)
    update()
    assert Path(".claude/commands/a.md").read_text() == "original"
    assert Path(".claude/skills/mine/SKILL.md").read_text() == "skill"
    assert load_toolkits_index()["example"]["version"] == "1"


@pytest.fixture(params=["claude", "cursor", "codex"])
def shared_agent(source: Path, request: pytest.FixtureRequest) -> _AIAgent:
    """Two installed toolkits sharing configuration with a user's own additions."""
    init = source / "init"
    init.mkdir()
    (init / "AGENTS.md").write_text("## ALWAYS ACTIVATE those skills\n")
    ship(
        source, files={"rules/a.md": "rule", "mcp.json": json.dumps({"first": {"command": "old"}})}
    )
    agent = AI_AGENTS[request.param]()
    install(agent=agent.name)
    config = agent.mcp_config_path(Path.cwd())
    config.write_text(agent.merge_mcp_servers(config.read_text(), {"user": {"command": "mine"}}))
    if agent.name == "codex":
        with Path("AGENTS.md").open("a") as f:
            f.write("\nUser notes\n")
    ship(
        source,
        "other",
        {"rules/b.md": "other rule", "mcp.json": json.dumps({"second": {"command": "two"}})},
    )
    install("other", agent.name)
    return agent


def test_shared_merges_preserve_user_content_and_advance_versions(
    source: Path, shared_agent: _AIAgent
) -> None:
    toolkit = source / "example"
    (toolkit / "mcp.json").write_text(
        json.dumps({"first": {"command": "new"}, "third": {"command": "three"}})
    )
    new_version(toolkit)
    new_version(source / "other")
    update()
    config = shared_agent.mcp_config_path(Path.cwd())
    servers = shared_agent.parse_mcp_servers(config.read_text())
    assert servers["first"]["command"] == "old"
    assert servers["user"]["command"] == "mine"
    assert set(servers) == {"first", "second", "third", "user"}
    records = load_toolkits_index()
    assert records["example"]["version"] == records["other"]["version"] == "2"
    for entry in records.values():
        assert str(config.relative_to(Path.cwd())) not in entry["files"]
        assert "AGENTS.md" not in entry["files"]
        assert str(config.relative_to(Path.cwd())) in entry["shared_files"]
    if shared_agent.name == "codex":
        content = Path("AGENTS.md").read_text()
        assert "User notes" in content and "example-a" in content and "other-b" in content


def test_shared_overwrite_replaces_supplied_server_and_then_is_noop(
    source: Path, shared_agent: _AIAgent
) -> None:
    (source / "example/mcp.json").write_text(json.dumps({"first": {"command": "new"}}))
    update("example", overwrite=True)
    config = shared_agent.mcp_config_path(Path.cwd())
    servers = shared_agent.parse_mcp_servers(config.read_text())
    assert servers["first"]["command"] == "new"
    assert servers["user"]["command"] == "mine"
    assert servers["second"]["command"] == "two"
    with patch.object(commands, "safe_write_bytes", wraps=commands.safe_write_bytes) as write:
        update("example", overwrite=True)
        write.assert_not_called()


def test_removed_shared_definitions_leave_documents_intact(
    source: Path, shared_agent: _AIAgent
) -> None:
    toolkit = source / "example"
    config = shared_agent.mcp_config_path(Path.cwd())
    before = config.read_bytes()
    agents_before = Path("AGENTS.md").read_bytes() if shared_agent.name == "codex" else None
    (toolkit / "mcp.json").unlink()
    shutil.rmtree(toolkit / "rules")
    update("example")
    assert config.read_bytes() == before
    if agents_before is not None:
        assert Path("AGENTS.md").read_bytes() == agents_before


def test_other_claim_protects_deletion_but_not_overwrite(source: Path) -> None:
    a = ship(source, "a", {"commands/shared.md": "a"})
    ship(source, "b", {"commands/shared.md": "b"})
    install("a")
    install("b", overwrite=True)
    assert Path(".claude/commands/shared.md").read_text() == "b"
    (a / "commands/shared.md").unlink()
    update("a", overwrite=True)
    assert Path(".claude/commands/shared.md").read_text() == "b"
    assert ".claude/commands/shared.md" in load_toolkits_index()["a"]["files"]


@pytest.mark.parametrize("failure", ["write", "hash"])
def test_partial_failure_saves_completed_hashes(source: Path, failure: str) -> None:
    toolkit = ship(source, files={"commands/a.md": "old a", "commands/b.md": "old b"})
    install()
    before = load_toolkits_index()["example"]
    for name in ("a", "b"):
        (toolkit / ("commands/%s.md" % name)).write_text("new " + name)
    new_version(toolkit)
    target = "safe_write_bytes" if failure == "write" else "compute_file_hash"
    real = getattr(commands, target)

    def fail_second(path: Path, *args):
        if path.name == "b.md":
            raise OSError("injected " + failure)
        return real(path, *args)

    # overwrite avoids baseline-comparison reads so the hash fault is a read-back fault.
    with (
        patch.object(commands, target, side_effect=fail_second),
        patch.object(commands, "save_toolkits_index", wraps=save_toolkits_index) as save,
    ):
        with pytest.raises(OSError, match="injected"):
            update(overwrite=True)
        save.assert_called_once()
    after = load_toolkits_index()["example"]
    assert after["version"] == "1" and after["description"] == "Original"
    assert after["files"][".claude/commands/a.md"]["sha3_256"] == compute_file_hash(
        Path(".claude/commands/a.md")
    )
    assert after["files"][".claude/commands/b.md"] == before["files"][".claude/commands/b.md"]
    update()
    assert Path(".claude/commands/a.md").read_text() == "new a"
    assert load_toolkits_index()["example"]["version"] == "2"
    assert load_toolkits_index()["example"]["files"][".claude/commands/b.md"][
        "sha3_256"
    ] == compute_file_hash(Path(".claude/commands/b.md"))


def test_hash_is_read_from_written_disk_bytes(source: Path) -> None:
    ship(source)
    real_write = commands.safe_write_bytes
    with patch.object(
        commands, "safe_write_bytes", side_effect=lambda path, data: real_write(path, data + b"\n")
    ):
        install()
    path = Path(".claude/commands/a.md")
    assert path.read_bytes() == b"original\n"
    assert load_toolkits_index()["example"]["files"][str(path)]["sha3_256"] == compute_file_hash(
        path
    )
    update()
    assert path.read_bytes() == b"original"


@pytest.mark.parametrize("save_error", [OSError("save failed"), yaml.YAMLError("save failed")])
def test_save_failure_keeps_execution_error_primary(
    source: Path, capsys: pytest.CaptureFixture[str], save_error: Exception
) -> None:
    toolkit = ship(source, files={"commands/a.md": "a", "commands/b.md": "b"})
    install()
    (toolkit / "commands/a.md").write_text("new a")
    (toolkit / "commands/b.md").write_text("new b")
    real_write = commands.safe_write_bytes

    def write(path: Path, data: bytes) -> None:
        if path.name == "b.md":
            raise OSError("write failed")
        real_write(path, data)

    with (
        patch.object(commands, "safe_write_bytes", side_effect=write),
        patch.object(commands, "save_toolkits_index", side_effect=save_error),
    ):
        with pytest.raises(OSError, match="write failed") as exc:
            update()
    assert str(exc.value.__cause__) == "save failed"
    assert "save failed" in capsys.readouterr().out


def test_removal_pruning_failure_records_confirmed_absence(source: Path) -> None:
    toolkit = ship(source)
    install()
    (toolkit / "commands/a.md").unlink()

    def remove_then_fail(path: Path, boundary: Path) -> None:
        path.unlink()
        raise OSError("pruning failed")

    with patch.object(commands, "_remove_installed_file", side_effect=remove_then_fail):
        with pytest.raises(OSError, match="pruning failed"):
            update()
    assert load_toolkits_index()["example"]["files"] == {}


def test_source_failure_precedes_all_mutation(source: Path) -> None:
    ship(source, files={"skills/mine/SKILL.md": "skill", "skills/mine/helper.py": "helper"})
    real_read = commands._action_bytes

    def read(action):
        if action.dest_path.name == "helper.py":
            raise FileNotFoundError("source vanished")
        return real_read(action)

    with patch.object(commands, "_action_bytes", side_effect=read):
        with pytest.raises(FileNotFoundError, match="source vanished"):
            install()
    assert not Path(".claude").exists()
    assert load_toolkits_index() == {}


@pytest.mark.parametrize("invalid", ["missing", "metadata", "identity"])
def test_update_warning_skips_continue(source: Path, invalid: str) -> None:
    first = ship(source, "first")
    second = ship(source, "second", {"commands/b.md": "b"})
    install("first")
    install("second")
    if invalid == "missing":
        shutil.rmtree(first)
    elif invalid == "metadata":
        (first / ".claude-plugin/plugin.json").write_text(json.dumps({"name": "first"}))
    else:
        new_version(first, name="someone-else")
    new_version(second)
    update()
    records = load_toolkits_index()
    assert records["first"]["version"] == "1"
    assert records["second"]["version"] == "2"


def test_update_failure_stops_batch(source: Path) -> None:
    first = ship(source, "first", {"mcp.json": json.dumps({"one": {"command": "one"}})})
    second = ship(source, "second", {"commands/b.md": "b"})
    install("first")
    install("second")
    new_version(first)
    new_version(second)
    Path(".mcp.json").write_text("malformed json")
    with pytest.raises(ValueError):
        update()
    assert load_toolkits_index()["second"]["version"] == "1"


@pytest.mark.parametrize("kind", ["symlink", "directory", "outside", "absolute"])
def test_unsafe_ordinary_paths_preserved_even_with_overwrite(source: Path, kind: str) -> None:
    toolkit = ship(source)
    install()
    path = Path(".claude/commands/a.md")
    if kind in ("symlink", "directory"):
        path.unlink()
        if kind == "directory":
            path.mkdir()
        else:
            Path("user-file").write_text("mine")
            path.symlink_to(Path("user-file").absolute())
    else:
        records = load_toolkits_index()
        key = "../outside" if kind == "outside" else str(Path("user-file").absolute())
        Path(key).write_text("mine")
        records["example"]["files"][key] = {"sha3_256": compute_file_hash(Path(key))}
        save_toolkits_index(records)
    new_version(toolkit)
    update(overwrite=True)
    assert load_toolkits_index()["example"]["version"] == "2"
    if kind == "symlink":
        assert path.is_symlink() and path.read_text() == "mine"
    elif kind == "directory":
        assert path.is_dir()
    else:
        assert Path(key).read_text() == "mine"


def test_symlinked_ancestor_is_supported(source: Path) -> None:
    ship(source)
    Path("host-skills").mkdir()
    Path(".claude").symlink_to(Path("host-skills").absolute(), target_is_directory=True)
    install()
    update()
    assert Path("host-skills/commands/a.md").read_text() == "original"


def test_duplicate_destination_is_not_written(source: Path) -> None:
    ship(source, files={"skills/a/SKILL.md": "skill", "commands/a.md": "command"})
    install(agent="codex")
    assert not Path(".agents/skills/a/SKILL.md").exists()


def test_install_carries_records_through_dependency_chain(source: Path) -> None:
    for name, deps in (("first", []), ("second", ["first"]), ("root", ["first", "second"])):
        toolkit = ship(source, name, {"commands/%s.md" % name: name})
        (toolkit / ".claude-plugin/toolkit.json").write_text(json.dumps({"dependencies": deps}))
    install("first")
    first = load_toolkits_index()["first"]
    (source / "first/commands/first.md").write_text("upstream change")
    with patch.object(commands, "load_toolkits_index", wraps=load_toolkits_index) as load:
        install("root", overwrite=True)
        load.assert_called_once()
    records = load_toolkits_index()
    assert set(records) == {"first", "second", "root"}
    assert records["first"] == first  # root overwrite does not reinstall an installed dependency
    for name in records:
        assert Path(".claude/commands/%s.md" % name).read_text() == name


def test_init_and_status_reuse_supplied_records(source: Path) -> None:
    ship(source, "init")
    with (
        patch.object(commands, "load_toolkits_index", wraps=load_toolkits_index) as load,
        patch(
            "dlt._workspace.cli.dlthub.ai.utils.load_toolkits_index",
            side_effect=AssertionError("hidden load"),
        ),
    ):
        commands.ai_init_command("claude", "test-source")
        load.assert_called_once()
        status = fetch_ai_status(Path.cwd(), {})
        assert not status["has_init_toolkit"]


def test_equality_does_not_normalize_disk_line_endings(source: Path) -> None:
    toolkit = ship(source)
    install()
    path = Path(".claude/commands/a.md")
    path.write_bytes(b"new bytes\r\n")
    (toolkit / "commands/a.md").write_bytes(b"new bytes\n")
    before = load_toolkits_index()["example"]["files"][str(path)]
    update()
    assert path.read_bytes() == b"new bytes\r\n"
    assert load_toolkits_index()["example"]["files"][str(path)] == before


@pytest.mark.parametrize("baseline", ["missing", "stale"])
def test_equal_content_recovers_baseline_without_write(source: Path, baseline: str) -> None:
    toolkit = ship(source)
    install()
    path = Path(".claude/commands/a.md")
    path.write_bytes(b"new bytes\n")
    (toolkit / "commands/a.md").write_bytes(b"new bytes\n")
    if baseline == "missing":
        records = load_toolkits_index()
        records["example"]["files"] = {}
        save_toolkits_index(records)
    with patch.object(commands, "safe_write_bytes", wraps=commands.safe_write_bytes) as write:
        update()
        write.assert_not_called()
    assert load_toolkits_index()["example"]["files"][str(path)]["sha3_256"] == compute_file_hash(
        path
    )


def test_noop_update_preserves_record_and_timestamp(source: Path) -> None:
    ship(source)
    install()
    before = load_toolkits_index()
    with (
        patch.object(commands, "safe_write_bytes", wraps=commands.safe_write_bytes) as write,
        patch.object(commands, "save_toolkits_index", wraps=save_toolkits_index) as save,
    ):
        update()
        write.assert_not_called()
        save.assert_not_called()
    assert load_toolkits_index() == before


@pytest.mark.parametrize("missing_helper", [False, True])
def test_legacy_folder_adoption_requires_all_shipped_files(
    source: Path, missing_helper: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    ship(source, files={"skills/mine/SKILL.md": "skill", "skills/mine/helper.py": "helper"})
    install()
    records = load_toolkits_index()
    records["example"].pop("files")
    save_toolkits_index(records)
    folder = Path(".claude/skills/mine")
    if missing_helper:
        (folder / "helper.py").unlink()
    capsys.readouterr()
    with patch.object(commands, "safe_write_bytes") as write:
        update()
        update()  # partial adoption must not authorize additions on the next run
        write.assert_not_called()
    out = capsys.readouterr().out
    if missing_helper:
        assert not (folder / "helper.py").exists()
        assert out.count("component folder exists") == 2  # once per component/run
        assert not load_toolkits_index()["example"].get("files")
    else:
        assert len(load_toolkits_index()["example"]["files"]) == 2
        assert "Keeping" not in out


@pytest.mark.parametrize("folder", ["skills/_shared", "dlthub/agents/_shared"])
def test_helper_folders_do_not_block_strict_or_orphan_cleanup(source: Path, folder: str) -> None:
    toolkit = ship(source)
    install()
    helper = toolkit / folder
    helper.mkdir(parents=True)
    (helper / "helper.py").write_text("helper")
    (toolkit / "commands/a.md").unlink()
    update(strict=True)
    assert not Path(".claude/commands/a.md").exists()


@pytest.mark.parametrize("helper_contents", ["new helper", None], ids=["write", "remove"])
def test_partial_component_update_is_reported(
    source: Path, capsys: pytest.CaptureFixture[str], helper_contents: Optional[str]
) -> None:
    toolkit = ship(
        source, files={"skills/mine/SKILL.md": "skill", "skills/mine/helper.py": "helper"}
    )
    install()
    folder = Path(".claude/skills/mine")
    (folder / "SKILL.md").write_text("user edit")
    shipped_helper = toolkit / "skills/mine/helper.py"
    if helper_contents is None:
        shipped_helper.unlink()
    else:
        shipped_helper.write_text(helper_contents)
    new_version(toolkit)
    capsys.readouterr()
    update()
    out = capsys.readouterr().out
    assert "Partially updated" in out
    assert (folder / "SKILL.md").read_text() == "user edit"
    assert load_toolkits_index()["example"]["version"] == "2"
    if helper_contents is None:
        assert not (folder / "helper.py").exists()
        assert "  - " in out and "  + " not in out
    else:
        assert (folder / "helper.py").read_text() == helper_contents
        assert "  + " in out and "  - " not in out


@pytest.mark.parametrize("name", ["example", "init"])
def test_mcp_only_noop_install_records_presence_and_metadata(
    source: Path, name: str, capsys: pytest.CaptureFixture[str]
) -> None:
    toolkit = ship(source, name, files={})
    servers = {"example-server": {"command": "example"}}
    (toolkit / ".mcp.json").write_text(json.dumps({"mcpServers": servers}))
    # The server can already exist from a user's configuration or another toolkit.
    Path(".mcp.json").write_text(AI_AGENTS["claude"]().merge_mcp_servers("", servers))

    def run(overwrite: bool = False) -> None:
        if name == "init":
            with patch.object(commands, "fetch_ai_status", return_value={"warnings": []}):
                commands.ai_init_command("claude", "test-source", overwrite=overwrite)
        else:
            install(name, overwrite=overwrite)

    with patch.object(commands, "safe_write_bytes") as write:
        run()
        assert load_toolkits_index()[name]["version"] == "1"
        assert load_toolkits_index()[name]["mcp_servers"] == ["example-server"]
        new_version(toolkit)
        run(overwrite=True)
        assert load_toolkits_index()[name]["version"] == "2"
        before = load_toolkits_index()
        with patch.object(commands, "save_toolkits_index") as save:
            run(overwrite=True)
            save.assert_not_called()
        assert load_toolkits_index() == before
        write.assert_not_called()
    assert "No components found" not in capsys.readouterr().out


def test_protected_component_noop_does_not_report_changes(
    source: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ship(source, files={"skills/mine/SKILL.md": "skill", "skills/mine/helper.py": "helper"})
    install()
    Path(".claude/skills/mine/SKILL.md").write_text("local skill")
    capsys.readouterr()
    update()
    out = capsys.readouterr().out
    assert "Keeping" in out
    assert "Partially" not in out
    assert "  + " not in out
    assert "0 item(s) installed" in out


def test_overwrite_can_replace_unreadable_old_file(source: Path) -> None:
    toolkit = ship(source)
    install()
    (toolkit / "commands/a.md").write_text("new contents")
    dest = Path(".claude/commands/a.md").absolute()
    original_read = Path.read_bytes
    reads: List[Path] = []

    def read(path: Path) -> bytes:
        if path == dest and not reads:
            reads.append(path)
            raise PermissionError("old file cannot be read")
        return original_read(path)

    with patch.object(Path, "read_bytes", read):
        update(overwrite=True)
    assert reads
    assert dest.read_text() == "new contents"
    assert load_toolkits_index()["example"]["files"][".claude/commands/a.md"][
        "sha3_256"
    ] == compute_file_hash(dest)


def test_mcp_only_failed_install_does_not_record_presence(source: Path) -> None:
    toolkit = ship(source, files={})
    (toolkit / ".mcp.json").write_text(json.dumps({"mcpServers": {"server": {"command": "cmd"}}}))
    with patch.object(commands, "safe_write_bytes", side_effect=OSError("write failed")):
        with pytest.raises(OSError, match="write failed"):
            install()
    assert "example" not in load_toolkits_index()


def test_empty_toolkit_install_still_skips(
    source: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ship(source, files={})
    install()
    assert "No components found" in capsys.readouterr().out
    assert "example" not in load_toolkits_index()


@pytest.mark.parametrize("unreadable", ["destination", "source"])
def test_untracked_component_read_failure(
    source: Path, unreadable: str, capsys: pytest.CaptureFixture[str]
) -> None:
    toolkit = ship(source)
    install()
    (toolkit / "commands/a.md").write_text("new command")
    incoming = toolkit / "skills/mine"
    incoming.mkdir(parents=True)
    (incoming / "SKILL.md").write_text("skill")
    (incoming / "helper.py").write_text("helper")
    folder = Path(".claude/skills/mine")
    folder.mkdir(parents=True)
    (folder / "SKILL.md").write_text("skill")
    broken = ((folder if unreadable == "destination" else incoming) / "SKILL.md").absolute()
    original_read = Path.read_bytes

    def read(path: Path) -> bytes:
        if path.absolute() == broken:
            raise PermissionError("injected read failure")
        return original_read(path)

    capsys.readouterr()
    with patch.object(Path, "read_bytes", read):
        if unreadable == "source":
            with pytest.raises(PermissionError, match="injected"):
                update()
        else:
            update()
            update()  # no partial adoption that could allow helpers on the next run
    assert (folder / "SKILL.md").read_text() == "skill"
    assert not (folder / "helper.py").exists()
    assert not any("skills/mine" in p for p in load_toolkits_index()["example"]["files"])
    assert Path(".claude/commands/a.md").read_text() == (
        "original" if unreadable == "source" else "new command"
    )
    if unreadable == "destination":
        assert capsys.readouterr().out.count("component folder exists") == 2
