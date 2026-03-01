import json
from pathlib import Path
from typing import Any, Callable, Dict

import pytest
import tomlkit

from dlt.common.configuration.const import TYPE_EXAMPLES
from dlt._workspace.cli.ai.utils import (
    ensure_cursor_rule_frontmatter,
    extract_toolkit_info,
    merge_json_mcp_servers,
    merge_toml_mcp_servers,
    parse_json_mcp,
    parse_toml_mcp,
    redact_toml_document,
    redact_value,
    scan_workbench_toolkits,
    strip_rule_frontmatter,
    wrap_as_skill,
    MIN_REDACT_STARS,
)
from dlt._workspace.cli.formatters import parse_frontmatter


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
    content = "---\nname: test\n---\n# Heading\nBody"
    fm, _ = parse_frontmatter(ensure_cursor_rule_frontmatter(content))
    assert fm["alwaysApply"] is True
    assert fm["description"] == "Heading"

    # preserves existing description
    content2 = "---\ndescription: Custom\n---\n# Heading\nBody"
    fm2, _ = parse_frontmatter(ensure_cursor_rule_frontmatter(content2))
    assert fm2["description"] == "Custom"
    assert fm2["alwaysApply"] is True

    # no heading → no description key
    fm3, _ = parse_frontmatter(ensure_cursor_rule_frontmatter("Just text, no heading"))
    assert fm3["alwaysApply"] is True
    assert "description" not in fm3


def test_wrap_as_skill() -> None:
    # with frontmatter: preserves name/description
    content = "---\nname: Custom\ndescription: A desc\n---\n# Body\nText"
    fm, body = parse_frontmatter(wrap_as_skill(content, "fallback-name"))
    assert fm["name"] == "Custom"
    assert fm["description"] == "A desc"
    assert "# Body" in body

    # without frontmatter: derives from args and heading
    fm2, _ = parse_frontmatter(wrap_as_skill("# Heading\nBody text", "my-skill"))
    assert fm2["name"] == "my-skill"
    assert fm2["description"] == "Heading"

    # no heading: falls back to skill name for description
    fm3, _ = parse_frontmatter(wrap_as_skill("Just text", "my-skill"))
    assert fm3["name"] == "my-skill"
    assert fm3["description"] == "my-skill"

    # always_apply prefixes description
    fm4, _ = parse_frontmatter(wrap_as_skill("# Heading\nBody", "s", always_apply=True))
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
    all_names = sorted(scan_workbench_toolkits(base).keys())
    assert all_names == ["also-visible", "hidden", "my-toolkit"]

    # listed_only=True filters out unlisted
    names = sorted(scan_workbench_toolkits(base, listed_only=True).keys())
    assert names == ["also-visible", "my-toolkit"]


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


def test_extract_toolkit_info_missing_fields() -> None:
    with pytest.raises(ValueError, match="name, description, version"):
        extract_toolkit_info({}, "bad-toolkit")

    with pytest.raises(ValueError, match="description"):
        extract_toolkit_info({"name": "tk", "version": "1.0.0"}, "tk")

    # empty string counts as missing
    with pytest.raises(ValueError, match="version"):
        extract_toolkit_info({"name": "tk", "description": "ok", "version": ""}, "tk")
