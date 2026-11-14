import contextlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Dict, Iterator
from unittest.mock import patch

import pytest
import tomlkit
import yaml

from dlt.common.configuration.const import TYPE_EXAMPLES
from dlt._workspace.cli.dlthub.ai.utils import (
    ensure_cursor_rule_frontmatter,
    extract_toolkit_info,
    fetch_ai_status,
    is_component_visible,
    is_toolkit_visible,
    load_toolkits_index,
    resolve_toolkit_dependencies,
    save_toolkit_entry,
    merge_json_mcp_servers,
    merge_toml_mcp_servers,
    parse_json_mcp,
    parse_toml_mcp,
    redact_toml_document,
    redact_value,
    safe_write_bytes,
    fetch_workbench_toolkits,
    strip_rule_frontmatter,
    wrap_as_skill,
    MIN_REDACT_STARS,
)
from dlt._workspace.cli.markdown import MarkdownDocument

from tests.workspace.cli.dlthub.ai.utils import AI_CASES_DIR


@pytest.mark.parametrize(
    ("content", "top_key", "expected_servers"),
    [
        ("", "mcpServers", {}),
        ("  \n  ", "mcpServers", {}),
        ('{"other": "value"}', "mcpServers", {}),
        ('{"mcpServers": {"srv": {"command": "uv"}}}', "mcpServers", {"srv": {"command": "uv"}}),
    ],
    ids=["empty", "whitespace", "missing-key", "with-servers"],
)
def test_parse_json_mcp(content: str, top_key: str, expected_servers: Dict[str, Any]) -> None:
    _, servers = parse_json_mcp(content, top_key)
    assert servers == expected_servers


def test_merge_json_mcp_servers() -> None:
    new = {"srv": {"type": "stdio", "command": "uv", "args": ["run"]}}

    # into empty, keep type
    data = json.loads(merge_json_mcp_servers("", new, "mcpServers", strip_type=False))
    assert data["mcpServers"]["srv"]["type"] == "stdio"
    assert data["mcpServers"]["srv"]["command"] == "uv"

    # strip type
    data = json.loads(merge_json_mcp_servers("", new, "mcpServers", strip_type=True))
    assert "type" not in data["mcpServers"]["srv"]
    assert data["mcpServers"]["srv"]["command"] == "uv"

    # preserves existing servers
    existing = json.dumps({"mcpServers": {"old": {"command": "old-cmd"}}})
    data = json.loads(merge_json_mcp_servers(existing, new, "mcpServers", strip_type=False))
    assert "old" in data["mcpServers"]
    assert "srv" in data["mcpServers"]


@pytest.mark.parametrize(
    ("content", "top_key", "expected_servers"),
    [
        ("", "mcp_servers", {}),
        ('[mcp_servers.srv]\ncommand = "uv"\n', "mcp_servers", {"srv": {"command": "uv"}}),
    ],
    ids=["empty", "with-servers"],
)
def test_parse_toml_mcp(content: str, top_key: str, expected_servers: Dict[str, Any]) -> None:
    _, servers = parse_toml_mcp(content, top_key)
    assert servers == expected_servers


def test_merge_toml_mcp_servers() -> None:
    new = {"srv": {"type": "stdio", "command": "uv"}}

    # into empty — always strips type
    doc: Dict[str, Any] = tomlkit.parse(merge_toml_mcp_servers("", new, "mcp_servers"))
    assert "type" not in doc["mcp_servers"]["srv"]
    assert doc["mcp_servers"]["srv"]["command"] == "uv"

    # preserves other sections
    existing = '[other]\nkey = "value"\n'
    doc = tomlkit.parse(merge_toml_mcp_servers(existing, new, "mcp_servers"))
    assert doc["other"]["key"] == "value"  # type: ignore[index]
    assert doc["mcp_servers"]["srv"]["command"] == "uv"  # type: ignore[index]

    # defaults land at the top level, a value the user set stays
    defaults = {"grace": 5000}
    doc = tomlkit.parse(merge_toml_mcp_servers(existing, new, "mcp_servers", defaults=defaults))
    assert doc["grace"] == 5000
    assert doc["other"]["key"] == "value"  # type: ignore[index]
    user_set = "grace = 100\n\n" + existing
    doc = tomlkit.parse(merge_toml_mcp_servers(user_set, new, "mcp_servers", defaults=defaults))
    assert doc["grace"] == 100

    # an existing server is updated in place, so comments stay and dotted keys stay valid
    user_servers = (
        'mcp_servers.dotted.command = "uv"\n'
        'mcp_servers.dotted.args = ["run"]\n'
        "\n"
        "[mcp_servers.srv]\n"
        'command = "uv"  # user note\n'
        'args = ["run"]\n'
        "startup_timeout_sec = 20\n"
    )
    updated = {
        "dotted": {"command": "uv", "args": ["run", "--features=x"]},
        "srv": {"command": "uv", "args": ["run", "--features=x"], "startup_timeout_sec": 20},
    }
    merged = merge_toml_mcp_servers(user_servers, updated, "mcp_servers")
    assert 'command = "uv"  # user note' in merged
    assert tomlkit.parse(merged).unwrap()["mcp_servers"] == updated
    # keys the new config does not have are dropped, as when the table is replaced
    replaced = merge_toml_mcp_servers(user_servers, {"srv": {"command": "dlt"}}, "mcp_servers")
    assert tomlkit.parse(replaced).unwrap()["mcp_servers"]["srv"] == {"command": "dlt"}


def test_strip_rule_frontmatter() -> None:
    content = "---\nalwaysApply: true\nname: keep\ndescription: also keep\n---\n# Body"
    result = strip_rule_frontmatter(content)
    assert "name: keep" in result
    assert "description: also keep" in result
    assert "alwaysApply" not in result

    # no frontmatter passes through
    plain = "# Just a heading\nBody text"
    assert strip_rule_frontmatter(plain) == plain

    # empty frontmatter passes through
    empty_fm = "---\n---\n# Body"
    assert strip_rule_frontmatter(empty_fm) == empty_fm

    # frontmatter with only unknown keys is stripped entirely
    unknown = "---\nalwaysApply: true\nglobs: '*.py'\n---\n# Body"
    assert strip_rule_frontmatter(unknown) == "# Body"


def test_ensure_cursor_rule_frontmatter() -> None:
    def _fm(content: str) -> Dict[str, Any]:
        return MarkdownDocument(ensure_cursor_rule_frontmatter(content)).split_frontmatter()[0]

    fm = _fm("---\nname: test\n---\n# Heading\nBody")
    assert fm["alwaysApply"] is True
    assert fm["description"] == "Heading"

    # preserves existing description
    fm2 = _fm("---\ndescription: Custom\n---\n# Heading\nBody")
    assert fm2["description"] == "Custom"
    assert fm2["alwaysApply"] is True

    # no heading → no description key
    fm3 = _fm("Just text, no heading")
    assert fm3["alwaysApply"] is True
    assert "description" not in fm3


def test_wrap_as_skill() -> None:
    # with frontmatter: preserves name/description
    content = "---\nname: Custom\ndescription: A desc\n---\n# Body\nText"
    fm, body = MarkdownDocument(wrap_as_skill(content, "fallback-name")).split_frontmatter()
    assert fm["name"] == "Custom"
    assert fm["description"] == "A desc"
    assert "# Body" in body

    # without frontmatter: derives from args and heading
    fm2, _ = MarkdownDocument(wrap_as_skill("# Heading\nBody text", "my-skill")).split_frontmatter()
    assert fm2["name"] == "my-skill"
    assert fm2["description"] == "Heading"

    # no heading: falls back to skill name for description
    fm3, _ = MarkdownDocument(wrap_as_skill("Just text", "my-skill")).split_frontmatter()
    assert fm3["name"] == "my-skill"
    assert fm3["description"] == "my-skill"

    # always_apply prefixes description
    always = wrap_as_skill("# Heading\nBody", "s", always_apply=True)
    fm4, _ = MarkdownDocument(always).split_frontmatter()
    assert fm4["description"].startswith("ALWAYS read and follow")
    assert fm4["description"].endswith("Heading")


def test_redact_value_length() -> None:
    N = MIN_REDACT_STARS
    # short values always produce N stars (hides length)
    assert redact_value("abc") == "*" * N
    assert redact_value(42) == "*" * N
    assert redact_value(True) == "*" * N
    # exactly N chars
    assert redact_value("x" * N) == "*" * N
    # longer values produce stars matching their length
    long_val = "sk-" + "a" * N
    assert redact_value(long_val) == "*" * len(long_val)


def test_redact_value_newlines() -> None:
    N = MIN_REDACT_STARS
    assert redact_value("line1\nline2") == "*" * N + "\n" + "*" * N
    # long lines keep their length
    long = "a" * 20 + "\n" + "b" * 5
    assert redact_value(long) == "*" * 20 + "\n" + "*" * N
    # empty lines stay empty
    assert redact_value("a\n\nb") == "*" * N + "\n\n" + "*" * N


@pytest.mark.parametrize(
    ("toml_str", "check"),
    [
        (
            '[section]\nkey = "value"\nnum = 42\nflag = true\n',
            lambda doc: (
                doc["section"]["key"] == "*" * MIN_REDACT_STARS
                and doc["section"]["num"] == "*" * MIN_REDACT_STARS
                and doc["section"]["flag"] == "*" * MIN_REDACT_STARS
            ),
        ),
        (
            '[a]\n[a.b]\ndeep = "secret"\n',
            lambda doc: doc["a"]["b"]["deep"] == "*" * MIN_REDACT_STARS,
        ),
        (
            "[section]\narr = [1, 2, 3]\n",
            lambda doc: set(doc["section"]["arr"]) == {"*"},
        ),
        (
            "",
            lambda doc: len(doc) == 0,
        ),
    ],
    ids=["scalar-types", "nested-tables", "arrays", "empty-doc"],
)
def test_redact_toml_document(toml_str: str, check: Callable[..., bool]) -> None:
    doc = tomlkit.parse(toml_str)
    redacted = redact_toml_document(doc)
    assert check(redacted)


def test_redact_toml_document_does_not_mutate_original() -> None:
    doc = tomlkit.parse('[section]\nkey = "original"\n')
    redact_toml_document(doc)
    assert doc["section"]["key"] == "original"  # type: ignore[index]


def test_redact_toml_document_preserves_placeholders() -> None:
    doc = tomlkit.parse(
        '[destination.credentials]\nhost = "<configure me>"\npassword = "real-secret"\n'
    )
    redacted = redact_toml_document(doc)
    assert redacted["destination"]["credentials"]["host"] == "<configure me>"  # type: ignore[index]
    assert redacted["destination"]["credentials"]["password"] == "*" * 11  # type: ignore[index]
    for placeholder_val in TYPE_EXAMPLES.values():
        single = tomlkit.parse('[t]\nk = "%s"\n' % placeholder_val)
        r = redact_toml_document(single)
        assert r["t"]["k"] == placeholder_val  # type: ignore[index]


def test_scan_workbench_toolkits() -> None:
    """Skips dirs without plugin.json; listed_only filters unlisted toolkits."""
    base = Path("workbench")
    base.mkdir()
    _valid = {"version": "0.1.0", "description": "A toolkit"}

    # valid toolkit (no "listed" key — defaults to listed)
    tk_meta = base / "my-toolkit" / ".claude-plugin"
    tk_meta.mkdir(parents=True)
    (tk_meta / "plugin.json").write_text(
        json.dumps({"name": "my-toolkit", **_valid}), encoding="utf-8"
    )

    # explicitly listed toolkit (via toolkit.json)
    tk2_meta = base / "also-visible" / ".claude-plugin"
    tk2_meta.mkdir(parents=True)
    (tk2_meta / "plugin.json").write_text(
        json.dumps({"name": "also-visible", **_valid}), encoding="utf-8"
    )
    (tk2_meta / "toolkit.json").write_text(json.dumps({"listed": True}), encoding="utf-8")

    # unlisted toolkit (via toolkit.json)
    unlisted_meta = base / "hidden" / ".claude-plugin"
    unlisted_meta.mkdir(parents=True)
    (unlisted_meta / "plugin.json").write_text(
        json.dumps({"name": "hidden", **_valid}), encoding="utf-8"
    )
    (unlisted_meta / "toolkit.json").write_text(json.dumps({"listed": False}), encoding="utf-8")

    # dir without plugin.json — should be skipped
    (base / "no-meta").mkdir()

    # default: returns all valid toolkits including unlisted
    all_toolkits, warnings = fetch_workbench_toolkits(base)
    all_names = sorted(all_toolkits.keys())
    assert all_names == ["also-visible", "hidden", "my-toolkit"]
    assert warnings == []

    # listed_only=True filters out unlisted
    listed, warnings = fetch_workbench_toolkits(base, listed_only=True)
    names = sorted(listed.keys())
    assert names == ["also-visible", "my-toolkit"]
    assert warnings == []


def test_extract_toolkit_info() -> None:
    meta: Dict[str, Any] = {
        "name": "tk",
        "description": "A toolkit",
        "version": "1.0.0",
        "keywords": ["tag1"],
    }
    info = extract_toolkit_info(meta, "fallback")
    assert info["name"] == "tk"
    assert info["version"] == "1.0.0"
    assert info["description"] == "A toolkit"
    assert info["tags"] == ["tag1"]


def test_extract_toolkit_info_visibility() -> None:
    """`agents` and `components` from toolkit.json say which agents see what."""
    meta: Dict[str, Any] = {
        "name": "tk",
        "description": "A toolkit",
        "version": "1.0.0",
        "agents": {"exclude": ["cortex"]},
        "components": {"skills/deploy": {"agents": {"include": ["claude"]}}},
    }
    info = extract_toolkit_info(meta, "tk")
    assert not is_toolkit_visible(info, "cortex")
    assert is_toolkit_visible(info, "claude")
    # without an agent everything is visible
    assert is_toolkit_visible(info, None)
    assert is_component_visible(info, "skills/deploy", "claude")
    assert not is_component_visible(info, "skills/deploy", "codex")
    assert is_component_visible(info, "skills/other", "codex")

    # include or exclude, not both, and always a list
    for bad in ({"include": ["a"], "exclude": ["b"]}, {"include": "claude"}, {"only": ["x"]}):
        with pytest.raises(ValueError, match="agent visibility"):
            extract_toolkit_info({**meta, "agents": bad}, "tk")
    with pytest.raises(ValueError, match="component skills/deploy"):
        extract_toolkit_info({**meta, "components": {"skills/deploy": {"agents": []}}}, "tk")


def test_extract_toolkit_info_mcp_features() -> None:
    """`mcp_features` from toolkit.json maps server names to the features the toolkit adds."""
    meta: Dict[str, Any] = {"name": "tk", "description": "A toolkit", "version": "1.0.0"}
    features = {"dlt-workspace-mcp": ["dlthub.jobs", "dlthub.logs"]}
    assert (
        extract_toolkit_info({**meta, "mcp_features": features}, "tk")["mcp_features"] == features
    )
    assert "mcp_features" not in extract_toolkit_info(meta, "tk")
    bad: Any
    for bad in (
        ["dlthub.jobs"],
        {"dlt-workspace-mcp": "dlthub.jobs"},
        {"s": [1]},
        {"s": []},
        {"s": ["a,b"]},
        {"s": ["-secrets"]},
        {"s": ["+dlthub.jobs"]},
        {"s": [""]},
    ):
        with pytest.raises(ValueError, match="mcp_features"):
            extract_toolkit_info({**meta, "mcp_features": bad}, "tk")


def test_resolve_toolkit_dependencies_skip() -> None:
    """A skipped toolkit is left out together with what only it depends on."""
    dep_map = {"app": ["base", "platform"], "platform": ["runtime"], "runtime": [], "base": []}
    assert resolve_toolkit_dependencies("app", dep_map) == ["base", "runtime", "platform"]
    assert resolve_toolkit_dependencies("app", dep_map, skip={"platform"}) == ["base"]


def test_extract_toolkit_info_missing_fields() -> None:
    with pytest.raises(ValueError, match="name, description, version"):
        extract_toolkit_info({}, "bad-toolkit")

    with pytest.raises(ValueError, match="description"):
        extract_toolkit_info({"name": "tk", "version": "1.0.0"}, "tk")

    # empty string counts as missing
    with pytest.raises(ValueError, match="version"):
        extract_toolkit_info({"name": "tk", "description": "ok", "version": ""}, "tk")


def test_safe_write_bytes_creates_file() -> None:
    dest = Path("output") / "test.txt"
    dest.parent.mkdir(parents=True, exist_ok=True)
    # newlines are written as given, so the bytes match a hash taken before writing
    safe_write_bytes(dest, b"hello\nworld\n")
    assert dest.read_bytes() == b"hello\nworld\n"


def test_safe_write_bytes_overwrites_existing() -> None:
    dest = Path("output") / "test2.txt"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("old", encoding="utf-8")
    safe_write_bytes(dest, b"new")
    assert dest.read_bytes() == b"new"


def test_safe_write_bytes_no_tmp_file_left() -> None:
    dest = Path("output") / "test3.txt"
    dest.parent.mkdir(parents=True, exist_ok=True)
    safe_write_bytes(dest, b"content")
    assert not list(dest.parent.glob("*.tmp"))


def test_safe_write_bytes_cleanup_on_failure() -> None:
    """Temp file is cleaned up if os.replace fails."""
    dest = Path("output") / "sub" / "test4.txt"
    dest.parent.mkdir(parents=True, exist_ok=True)
    safe_write_bytes(dest, b"ok")
    assert dest.read_bytes() == b"ok"


def _make_settings(project_root: Path, with_config: bool = False) -> str:
    settings_dir = str(project_root / ".dlt")
    os.makedirs(settings_dir, exist_ok=True)
    if with_config:
        Path(settings_dir, "config.toml").write_text("[runtime]\n", encoding="utf-8")
    return settings_dir


@contextlib.contextmanager
def _patch_settings_paths(project_root: Path) -> Iterator[None]:
    """Redirect toolkits index and settings path lookups to project_root/.dlt/."""
    with (
        patch(
            "dlt._workspace.cli.dlthub.ai.utils._toolkits_index_path",
            return_value=str(project_root / ".dlt" / ".toolkits"),
        ),
        patch(
            "dlt._workspace.cli.dlthub.ai.utils.make_dlt_settings_path",
            return_value=str(project_root / ".dlt" / "config.toml"),
        ),
    ):
        yield


def _write_toolkits_index(project_root: Path, data: Dict[str, Any]) -> None:
    index_path = project_root / ".dlt" / ".toolkits"
    os.makedirs(index_path.parent, exist_ok=True)
    with open(index_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f)


def test_toolkits_index_written_by_older_dlt() -> None:
    """An index without `see_all` loads as is; a new entry adds it, the old ones stay."""
    project_root = Path("project")
    index_path = project_root / ".dlt" / ".toolkits"
    index_path.parent.mkdir(parents=True)
    shutil.copy(os.path.join(AI_CASES_DIR, "toolkits_index_dlt_1_32.yaml"), index_path)
    original = yaml.safe_load(index_path.read_text(encoding="utf-8"))

    with _patch_settings_paths(project_root):
        index = load_toolkits_index()
        assert set(index) == {"init", "rest-api-pipeline"}
        assert index["init"]["agent"] == "claude"
        assert "see_all" not in index["init"]

        save_toolkit_entry(
            extract_toolkit_info(
                {
                    "name": "sql",
                    "description": "SQL",
                    "version": "1.0.0",
                    "agents": {"include": ["claude"]},
                },
                "sql",
            ),
            agent="claude",
            see_all=True,
        )
        index = load_toolkits_index()

    saved = yaml.safe_load(index_path.read_text(encoding="utf-8"))
    # the old entries are written back without the `name` key, the new one adds see_all
    assert saved["init"] == {k: v for k, v in original["init"].items() if k != "name"}
    assert saved["rest-api-pipeline"] == original["rest-api-pipeline"]
    assert saved["sql"]["see_all"] is True
    # visibility is metadata of the harness, it is not recorded
    assert "agent_visibility" not in saved["sql"]
    assert index["sql"]["name"] == "sql"


def test_fetch_ai_status_empty_workspace(environment: Any) -> None:
    """Empty workspace: all warnings present, no agent, no toolkits."""
    project_root = Path("project")
    project_root.mkdir()
    _make_settings(project_root)

    with (
        _patch_settings_paths(project_root),
        patch("dlt._workspace.cli.dlthub.ai.agents.home_dir", return_value=None),
    ):
        status = fetch_ai_status(project_root)

    assert status["dlt_version"]
    assert status["agent_name"] is None
    assert status["initialized"] is False
    assert status["has_init_toolkit"] is False
    assert status["toolkits"] == {}
    assert "not_initialized" in status["warnings"]
    assert "no_init_toolkit" in status["warnings"]
    assert "no_toolkits" in status["warnings"]


def test_fetch_ai_status_initialized_with_init_toolkit() -> None:
    """Workspace with config.toml and init toolkit: only no_toolkits warning."""
    project_root = Path("project")
    project_root.mkdir()
    _make_settings(project_root, with_config=True)
    _write_toolkits_index(
        project_root,
        {"init": {"version": "1.0.0", "agent": "claude", "description": "Init"}},
    )

    with _patch_settings_paths(project_root):
        status = fetch_ai_status(project_root)

    assert status["initialized"] is True
    assert status["has_init_toolkit"] is True
    assert status["agent_name"] == "claude"
    assert "not_initialized" not in status["warnings"]
    assert "no_init_toolkit" not in status["warnings"]
    assert "no_toolkits" in status["warnings"]


def test_fetch_ai_status_with_toolkits() -> None:
    """Workspace with init and user toolkits: no init/toolkit warnings."""
    project_root = Path("project")
    project_root.mkdir()
    _make_settings(project_root, with_config=True)
    _write_toolkits_index(
        project_root,
        {
            "init": {"version": "1.0.0", "agent": "cursor", "description": "Init"},
            "rest-api": {"version": "0.1.0", "description": "REST API toolkit"},
        },
    )

    with _patch_settings_paths(project_root):
        status = fetch_ai_status(project_root)

    assert status["agent_name"] == "cursor"
    assert "rest-api" in status["toolkits"]
    assert "init" not in status["toolkits"]
    assert "no_toolkits" not in status["warnings"]
    assert "no_init_toolkit" not in status["warnings"]
