from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
import yaml

from dlt._workspace.cli.formatters import (
    MarkdownDocument,
    extract_first_heading,
    merge_agents_md_skills,
    parse_frontmatter,
    read_md_name_desc,
    render_frontmatter,
)
from tests.utils import get_test_storage_root
from tests.workspace.cli.ai.utils import MOCK_AGENTS_MD_TEMPLATE


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


_TEMPLATE = MOCK_AGENTS_MD_TEMPLATE
_HEADING = "## ALWAYS ACTIVATE those skills"
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
    assert check(merge_agents_md_skills(existing, skills, template=_TEMPLATE))


def test_merge_agents_md_skills_preserves_surrounding_content() -> None:
    """User content before and after the skills section is kept intact."""
    existing = (
        "# My Project\n\nSome user notes.\n\n%s\n- `skill-a`\n\n# Other section\nMore content.\n"
        % _HEADING
    )
    result = merge_agents_md_skills(existing, ["skill-b"], template=_TEMPLATE)
    assert "# My Project" in result
    assert "Some user notes." in result
    assert "# Other section" in result
    assert "More content." in result
    assert "- `skill-b`" in result


def test_markdown_document_empty() -> None:
    doc = MarkdownDocument("")
    assert doc.is_empty
    assert doc.lines == [""]
    assert doc.frontmatter() == ({}, 0)
    assert str(doc) == ""

    # inserting a line makes it non-empty
    doc.insert_lines(0, ["hello"])
    assert not doc.is_empty

    assert not MarkdownDocument("x").is_empty
    assert not MarkdownDocument("\n").is_empty
    assert not MarkdownDocument("\n\n").is_empty


def test_markdown_document_roundtrip() -> None:
    for text in [
        "",
        "hello",
        "# H1\n\nBody\n## H2\nMore",
        "---\nk: v\n---\nbody",
        "trailing newline\n",
        "two trailing\n\n",
        "\nleading newline",
    ]:
        assert str(MarkdownDocument(text)) == text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("# One\n## Two\n# Three", [("One", 1), ("Two", 2), ("Three", 1)]),
        ("no headings here", []),
        ("  ## Indented", [("Indented", 2)]),
        ("##No space", []),
    ],
    ids=["mixed", "none", "indented", "no-space"],
)
def test_markdown_document_find_headings(
    text: str,
    expected: List[Tuple[str, int]],
) -> None:
    headings = MarkdownDocument(text).find_headings()
    assert [(h.text, h.depth) for h in headings] == expected


def test_markdown_document_find_headings_by_depth() -> None:
    doc = MarkdownDocument("# A\n## B\n# C\n### D")
    assert [h.text for h in doc.find_headings(depth=1)] == ["A", "C"]
    assert [h.text for h in doc.find_headings(depth=2)] == ["B"]
    assert [h.text for h in doc.find_headings(depth=3)] == ["D"]
    assert doc.find_headings(depth=4) == []


def test_markdown_document_find_first_heading() -> None:
    assert MarkdownDocument("text\n## Second\n# First").find_first_heading().text == "Second"
    assert MarkdownDocument("no heading").find_first_heading() is None


def test_markdown_document_find_line() -> None:
    doc = MarkdownDocument("alpha\n  beta  \ngamma")
    assert doc.find_line("beta") == 1
    assert doc.find_line("alpha") == 0
    assert doc.find_line("missing") is None


def test_markdown_document_search() -> None:
    doc = MarkdownDocument("foo bar\nbaz\nfoo baz")
    matches = doc.search(r"foo\s+(\w+)")
    assert len(matches) == 2
    assert matches[0][0] == 0
    assert matches[0][1].group(1) == "bar"
    assert matches[1][0] == 2
    assert matches[1][1].group(1) == "baz"


def test_markdown_document_insert_lines() -> None:
    doc = MarkdownDocument("a\nb\nc")
    doc.insert_lines(0, ["x"])
    assert str(doc) == "x\na\nb\nc"

    doc2 = MarkdownDocument("a\nb")
    doc2.insert_lines(1, ["x", "y"])
    assert str(doc2) == "a\nx\ny\nb"

    doc3 = MarkdownDocument("a")
    doc3.insert_lines(len(doc3.lines), ["z"])
    assert str(doc3) == "a\nz"


def test_markdown_document_frontmatter() -> None:
    doc = MarkdownDocument("---\nname: test\n---\n# Body")
    data, start = doc.frontmatter()
    assert data == {"name": "test"}
    assert start == 3
    assert doc.body_text == "# Body"

    doc2 = MarkdownDocument("no frontmatter")
    data2, start2 = doc2.frontmatter()
    assert data2 == {}
    assert start2 == 0
    assert doc2.body_text == "no frontmatter"

    doc3 = MarkdownDocument("---\n: {bad\n---\nbody")
    with pytest.raises(yaml.YAMLError):
        doc3.frontmatter()


def test_markdown_document_from_frontmatter() -> None:
    original = {"name": "test", "key": "val"}
    body = "# Content\nHello"
    doc = MarkdownDocument.from_frontmatter(original, body)
    data, _ = doc.frontmatter()
    assert data == original
    assert doc.body_text == body

    assert str(MarkdownDocument.from_frontmatter({}, "bare")) == "bare"


def test_markdown_document_insert_md() -> None:
    doc = MarkdownDocument("a\nb\nc")
    other = MarkdownDocument("x\ny")

    # insert at index
    doc.insert_md(1, other)
    assert str(doc) == "a\nx\ny\nb\nc"

    # None appends at end
    doc2 = MarkdownDocument("a\nb")
    doc2.insert_md(None, MarkdownDocument("z"))
    assert str(doc2) == "a\nb\nz"

    # insert into empty doc
    doc3 = MarkdownDocument("")
    doc3.insert_md(None, MarkdownDocument("hello"))
    assert not doc3.is_empty


def test_merge_agents_md_skills_with_template() -> None:
    """Template heading/subheading override the hardcoded fallback."""
    template = "## Custom Skills\nCustom description\n"
    result = merge_agents_md_skills("", ["sk-a"], template=template)
    assert "## Custom Skills" in result
    assert "Custom description" in result
    assert "# ALWAYS ACTIVATE" not in result
    assert "- `sk-a`" in result


def test_merge_agents_md_skills_template_no_subheading() -> None:
    """Template with heading only (no subheading line)."""
    template = "# Skills"
    result = merge_agents_md_skills("", ["sk-a"], template=template)
    assert "# Skills" in result
    assert "- `sk-a`" in result

    # appends to existing section with heading-only template
    existing = "# Skills\n- `sk-a`\n"
    result = merge_agents_md_skills(existing, ["sk-b"], template=template)
    assert result.count("# Skills") == 1
    assert "- `sk-a`" in result
    assert "- `sk-b`" in result
