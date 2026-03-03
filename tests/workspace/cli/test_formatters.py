from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from dlt._workspace.cli.formatters import (
    extract_first_heading,
    merge_agents_md_skills,
    parse_frontmatter,
    read_md_name_desc,
    render_frontmatter,
)
from tests.utils import get_test_storage_root


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
        "yaml-list-not-dict",
        "yaml-scalar-not-dict",
        "crlf-line-endings",
    ],
)
def test_parse_frontmatter(text: str, expected_fm: Dict[str, Any], expected_body: str) -> None:
    fm, body = parse_frontmatter(text)
    assert fm == expected_fm
    assert body == expected_body


@pytest.mark.parametrize(
    "text",
    [
        "---\nname: skill\nargument-hint: [pipeline-name] [-- <hints>]\n---\n# Body",
        "---\n: invalid\n{broken yaml\n---\nBody",
        "---\nname: colon: in value\n---\n# Body",
        "---\nname: ok\ntags: {invalid: [nested}\n---\nBody",
    ],
    ids=[
        "yaml-flow-seq-in-value",
        "broken-yaml",
        "yaml-ambiguous-colon",
        "yaml-mismatched-brackets",
    ],
)
def test_parse_frontmatter_raises_on_invalid_yaml(text: str) -> None:
    with pytest.raises(yaml.YAMLError):
        parse_frontmatter(text)


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


def test_read_md_name_desc_with_frontmatter() -> None:
    md = Path("my-skill.md")
    md.write_text(
        "---\nname: Custom Name\ndescription: A description\n---\n# Heading\nBody",
        encoding="utf-8",
    )
    name, desc = read_md_name_desc(md)
    assert name == "Custom Name"
    assert desc == "A description"


def test_read_md_name_desc_invalid_yaml() -> None:
    """Falls back to stem/heading when frontmatter is invalid YAML."""
    md = Path("my-skill.md")
    md.write_text(
        "---\nname: my-skill\nargument-hint: [pipeline] [-- <hints>]\n---\n"
        "# Skill Heading\nBody text",
        encoding="utf-8",
    )
    name, desc = read_md_name_desc(md)
    assert name == "my-skill"
    assert desc == "Skill Heading"


def test_read_md_name_desc_no_frontmatter() -> None:
    md = Path("plain.md")
    md.write_text("# First Heading\nSome content", encoding="utf-8")
    name, desc = read_md_name_desc(md)
    assert name == "plain"
    assert desc == "First Heading"


def test_read_md_name_desc_no_heading() -> None:
    md = Path("bare.md")
    md.write_text("Just some text with no heading", encoding="utf-8")
    name, desc = read_md_name_desc(md)
    assert name == "bare"
    assert desc == ""


_HEADING = "# ALWAYS ACTIVATE those skills"
_SECTION_WITH_A = "%s\n- `skill-a`\n" % _HEADING


@pytest.mark.parametrize(
    ("existing", "skills", "check"),
    [
        # creates section from scratch
        (
            "",
            ["skill-a"],
            lambda r: _HEADING in r and "- `skill-a`" in r,
        ),
        # appends to existing section, single heading
        (
            _SECTION_WITH_A,
            ["skill-b"],
            lambda r: ("- `skill-a`" in r and "- `skill-b`" in r and r.count(_HEADING) == 1),
        ),
        # duplicate in existing is skipped (identity)
        (_SECTION_WITH_A, ["skill-a"], lambda r: r == _SECTION_WITH_A),
        # mixed new + existing
        (
            _SECTION_WITH_A,
            ["skill-a", "skill-b"],
            lambda r: r.count("- `skill-a`") == 1 and "- `skill-b`" in r,
        ),
        # deduplicates within the input list
        ("", ["skill-a", "skill-a"], lambda r: r.count("- `skill-a`") == 1),
        # empty skill list is identity
        ("some content", [], lambda r: r == "some content"),
        # backtick mention anywhere counts as present
        (
            "Text mentioning `skill-a` inline.\n",
            ["skill-a"],
            lambda r: r == "Text mentioning `skill-a` inline.\n",
        ),
    ],
    ids=[
        "empty-file",
        "append-to-section",
        "skip-duplicate",
        "mixed-new-and-existing",
        "dedup-input",
        "no-skills",
        "backtick-in-body",
    ],
)
def test_merge_agents_md_skills(existing: str, skills: List[str], check: Any) -> None:
    assert check(merge_agents_md_skills(existing, skills))


def test_merge_agents_md_skills_preserves_surrounding_content() -> None:
    """User content before and after the skills section is kept intact."""
    existing = (
        "# My Project\n\nSome user notes.\n\n%s\n- `skill-a`\n\n# Other section\nMore content.\n"
        % _HEADING
    )
    result = merge_agents_md_skills(existing, ["skill-b"])
    assert "# My Project" in result
    assert "Some user notes." in result
    assert "# Other section" in result
    assert "More content." in result
    assert "- `skill-b`" in result
