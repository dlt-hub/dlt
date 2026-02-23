from pathlib import Path
from typing import Any, Dict

import pytest

from dlt._workspace.cli.formatters import (
    extract_first_heading,
    parse_frontmatter,
    read_md_name_desc,
    render_frontmatter,
)


@pytest.mark.parametrize(
    ("text", "expected_fm", "expected_body"),
    [
        (
            "---\nname: test\ndescription: A test\n---\n# Body\nHello",
            {"name": "test", "description": "A test"},
            "# Body\nHello",
        ),
        ("# Just a heading\nSome content", {}, "# Just a heading\nSome content"),
        ("", {}, ""),
        ("---\nname: test\nno closing delimiter", {}, "---\nname: test\nno closing delimiter"),
        ("---\n---\nBody here", {}, "Body here"),
        (
            '---\nname: test\ndesc: "has --- inside"\n---\n# Body',
            {"name": "test", "desc": "has --- inside"},
            "# Body",
        ),
        (
            "---\nname: skill\nargument-hint: [pipeline-name] [-- <hints>]\n---\n# Body",
            {},
            "---\nname: skill\nargument-hint: [pipeline-name] [-- <hints>]\n---\n# Body",
        ),
        (
            "---\n: invalid\n{broken yaml\n---\nBody",
            {},
            "---\n: invalid\n{broken yaml\n---\nBody",
        ),
        (
            "---\n- list\n- not a dict\n---\nBody",
            {},
            "---\n- list\n- not a dict\n---\nBody",
        ),
        (
            "---\njust a scalar string\n---\nBody",
            {},
            "---\njust a scalar string\n---\nBody",
        ),
        (
            "---\nname: colon: in value\n---\n# Body",
            {},
            "---\nname: colon: in value\n---\n# Body",
        ),
        (
            "---\nname: ok\ntags: {invalid: [nested}\n---\nBody",
            {},
            "---\nname: ok\ntags: {invalid: [nested}\n---\nBody",
        ),
        (
            "---\r\nname: crlf\r\n---\r\n# Body",
            {"name": "crlf"},
            "# Body",
        ),
    ],
    ids=[
        "with-fm",
        "no-fm",
        "empty",
        "no-closing",
        "empty-fm",
        "yaml-with-dashes",
        "yaml-flow-seq-in-value",
        "broken-yaml",
        "yaml-list-not-dict",
        "yaml-scalar-not-dict",
        "yaml-ambiguous-colon",
        "yaml-mismatched-brackets",
        "crlf-line-endings",
    ],
)
def test_parse_frontmatter(text: str, expected_fm: Dict[str, Any], expected_body: str) -> None:
    fm, body = parse_frontmatter(text)
    assert fm == expected_fm
    assert body == expected_body


def test_render_frontmatter() -> None:
    result = render_frontmatter({"name": "test"}, "# Body")
    assert result.startswith("---\n")
    assert "name: test" in result
    assert result.endswith("---\n# Body")

    assert render_frontmatter({}, "# Body") == "# Body"


def test_frontmatter_roundtrip() -> None:
    original = {"name": "test", "description": "A test"}
    body = "# Content\nHello"
    rendered = render_frontmatter(original, body)
    fm, parsed_body = parse_frontmatter(rendered)
    assert fm == original
    assert parsed_body == body


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("# First Heading\nContent", "First Heading"),
        ("## Second Level\nContent", "Second Level"),
        ("No heading here", None),
        ("", None),
        ("  # Indented heading\nContent", "Indented heading"),
    ],
    ids=["h1", "h2", "no-heading", "empty", "indented"],
)
def test_extract_first_heading(body: str, expected: str) -> None:
    assert extract_first_heading(body) == expected


def test_read_md_name_desc_with_frontmatter(tmp_path: Path) -> None:
    md = tmp_path / "my-skill.md"
    md.write_text(
        "---\nname: Custom Name\ndescription: A description\n---\n# Heading\nBody",
        encoding="utf-8",
    )
    name, desc = read_md_name_desc(md)
    assert name == "Custom Name"
    assert desc == "A description"


def test_read_md_name_desc_invalid_yaml(tmp_path: Path) -> None:
    """Falls back to stem/heading when frontmatter is invalid YAML."""
    md = tmp_path / "my-skill.md"
    md.write_text(
        "---\nname: my-skill\nargument-hint: [pipeline] [-- <hints>]\n---\n"
        "# Skill Heading\nBody text",
        encoding="utf-8",
    )
    name, desc = read_md_name_desc(md)
    assert name == "my-skill"
    assert desc == "Skill Heading"


def test_read_md_name_desc_no_frontmatter(tmp_path: Path) -> None:
    md = tmp_path / "plain.md"
    md.write_text("# First Heading\nSome content", encoding="utf-8")
    name, desc = read_md_name_desc(md)
    assert name == "plain"
    assert desc == "First Heading"


def test_read_md_name_desc_no_heading(tmp_path: Path) -> None:
    md = tmp_path / "bare.md"
    md.write_text("Just some text with no heading", encoding="utf-8")
    name, desc = read_md_name_desc(md)
    assert name == "bare"
    assert desc == ""
