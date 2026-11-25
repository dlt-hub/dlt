from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest
import yaml

from dlt._workspace.cli.markdown import MarkdownDocument


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
        ("﻿---\nname: bom\n---\n# Body", {"name": "bom"}, "# Body"),
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
        "byte-order-mark",
    ],
)
def test_split_frontmatter(text: str, expected_fm: Dict[str, Any], expected_body: str) -> None:
    fm, body = MarkdownDocument(text).split_frontmatter()
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
def test_split_frontmatter_raises_on_invalid_yaml(text: str) -> None:
    with pytest.raises(yaml.YAMLError):
        MarkdownDocument(text).split_frontmatter()


def test_from_frontmatter() -> None:
    original = {"name": "test", "description": "A test"}
    body = "# Content\nHello"
    text = str(MarkdownDocument.from_frontmatter(original, body))
    assert text.startswith("---\n")
    assert text.endswith("---\n" + body)
    # what is written reads back the same
    assert MarkdownDocument(text).split_frontmatter() == (original, body)
    assert str(MarkdownDocument.from_frontmatter({}, "bare")) == "bare"


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("# First Heading\nContent", "First Heading"),
        ("## Second Level\nContent", "Second Level"),
        ("text\n## Second\n# First", "Second"),
        ("No heading here", None),
        ("", None),
        ("  # Indented heading\nContent", "Indented heading"),
    ],
    ids=["h1", "h2", "first-of-two", "no-heading", "empty", "indented"],
)
def test_find_first_heading(body: str, expected: Optional[str]) -> None:
    heading = MarkdownDocument(body).find_first_heading()
    assert (heading.text if heading else None) == expected


@pytest.mark.parametrize(
    ("filename", "content", "expected_name", "expected_desc"),
    [
        (
            "my-skill.md",
            "---\nname: Custom Name\ndescription: A description\n---\n# Heading\nBody",
            "Custom Name",
            "A description",
        ),
        (
            "my-skill.md",
            (
                "---\nname: my-skill\nargument-hint: [pipeline] [-- <hints>]\n---\n"
                "# Skill Heading\nBody text"
            ),
            "my-skill",
            "Skill Heading",
        ),
        ("plain.md", "# First Heading\nSome content", "plain", "First Heading"),
        ("bare.md", "Just some text with no heading", "bare", ""),
    ],
    ids=["with-frontmatter", "invalid-yaml-fallback", "no-frontmatter", "no-heading"],
)
def test_read_name_desc(
    filename: str, content: str, expected_name: str, expected_desc: str
) -> None:
    md = Path(filename)
    md.write_text(content, encoding="utf-8")
    assert MarkdownDocument.read_name_desc(md) == (expected_name, expected_desc)


def test_read_name_desc_default_name() -> None:
    """`default_name` beats the file stem, and a frontmatter `name` beats both."""
    md = Path("SKILL.md")
    md.write_text("---\ndescription: A description\n---\n", encoding="utf-8")
    assert MarkdownDocument.read_name_desc(md, "my-skill") == ("my-skill", "A description")
    assert MarkdownDocument.read_name_desc(md) == ("SKILL", "A description")
    md.write_text("---\nname: named\n---\n", encoding="utf-8")
    assert MarkdownDocument.read_name_desc(md, "my-skill")[0] == "named"


_SECTION = "# dltHub rules\nnotice\n\n## rule\nbody"
_FENCED = "```md\n# not a heading\n```"


@pytest.mark.parametrize(
    ("existing", "expected"),
    [
        ("", _SECTION + "\n"),
        ("# Mine\nnotes\n", "# Mine\nnotes\n\n" + _SECTION + "\n"),
        # the old section is replaced up to the next top-level heading, which is kept
        (
            "# Mine\n\n# dltHub rules\nold\n## old rule\n\n# After\nkeep\n",
            "# Mine\n\n" + _SECTION + "\n\n# After\nkeep\n",
        ),
        # a `#` line in a code fence does not end the section
        (
            "# dltHub rules\n" + _FENCED + "\nold\n# After\nkeep\n",
            _SECTION + "\n\n# After\nkeep\n",
        ),
        # a section heading inside a code fence is not the section
        (
            "```\n# dltHub rules\n```\n",
            "```\n# dltHub rules\n```\n\n" + _SECTION + "\n",
        ),
        # a setext heading of the same depth ends the section
        (
            "# dltHub rules\nold\n\nAfter\n=====\nkeep\n",
            _SECTION + "\n\nAfter\n=====\nkeep\n",
        ),
        # headings in quotes and lists do not end the section
        (
            "# dltHub rules\n> # quoted\n\n- # listed\n\n# After\nkeep\n",
            _SECTION + "\n\n# After\nkeep\n",
        ),
        # a section heading in a quote is not the section
        (
            "> # dltHub rules\n",
            "> # dltHub rules\n\n" + _SECTION + "\n",
        ),
        # indented code is not a heading
        (
            "# dltHub rules\n\n    # code\n\nold\n",
            _SECTION + "\n",
        ),
    ],
    ids=[
        "new-file",
        "append",
        "replace-keep-after",
        "fence-in-section",
        "fenced-heading",
        "setext-ends-section",
        "nested-headings-in-section",
        "quoted-heading",
        "indented-code",
    ],
)
def test_replace_section(existing: str, expected: str) -> None:
    doc = MarkdownDocument(existing)
    doc.replace_section("# dltHub rules", _SECTION)
    assert str(doc) == expected
    # a second run changes nothing
    doc.replace_section("# dltHub rules", _SECTION)
    assert str(doc) == expected


def _demoted(text: str) -> str:
    doc = MarkdownDocument(text)
    doc.demote_headings()
    return str(doc)


def test_demote_headings() -> None:
    text = "# Title\ntext\n## Sub\n```\n# code\n```\n~~~\n## tilde code\n~~~\n###### deepest"
    assert (
        _demoted(text)
        == "## Title\ntext\n### Sub\n```\n# code\n```\n~~~\n## tilde code\n~~~\n###### deepest"
    )
    # setext headings become ATX headings one step deeper, and multi-line text is joined
    assert _demoted("Title\n=====\n\nTwo\nlines\n---\ntext") == "## Title\n\n### Two lines\ntext"
    # headings in quotes, lists and indented code stay as they are
    nested = "> # quoted\n\n- # listed\n\n    # code"
    assert _demoted(nested) == nested


@pytest.mark.parametrize(
    ("text", "closer"),
    [
        ("text\n```py\ncode", "```"),
        ("~~~~\ncode\n~~~", "~~~~"),
        ("<!-- open\nstuff", "-->"),
        ("<pre>\ncode", "</pre>"),
        ("<?php\ncode", "?>"),
        ("<![CDATA[\ndata", "]]>"),
    ],
    ids=["backtick-fence", "longer-tilde-fence", "comment", "pre", "processing", "cdata"],
)
def test_close_open_blocks(text: str, closer: str) -> None:
    doc = MarkdownDocument(text)
    doc.close_open_blocks()
    assert str(doc) == text + "\n" + closer
    # a heading after the closed text is a top-level heading again
    headings = MarkdownDocument(str(doc) + "\n\n# next").find_headings()
    assert headings[-1].text == "next" and headings[-1].level == 0


@pytest.mark.parametrize(
    "text",
    ["# T\n\n```\nok\n```", "- a\n- b", "> q\n> ```\n> x", "    code", "<div>\nx", ""],
    ids=["closed-fence", "list", "fence-in-quote", "indented-code", "html-block", "empty"],
)
def test_close_open_blocks_keeps_closed_text(text: str) -> None:
    doc = MarkdownDocument(text)
    doc.close_open_blocks()
    assert str(doc) == text


def test_markdown_document_structure() -> None:
    """Headings come from the CommonMark structure, with document line numbers."""
    text = (  # frontmatter is not a thematic break with a setext heading
        "---\nname: x\n---\n"
        "# Head\n"
        "```\n# fenced\n```\n"
        "<!-- comment -->\n"
        "Setext\n------\n"
        "\n    # indented\n"
        "> ## Quoted\n"
        "\n"
        "## Last ##"
    )
    doc = MarkdownDocument(text)
    headings = doc.find_headings()
    assert [(h.line_index, h.depth, h.text, h.level, h.end_index) for h in headings] == [
        (3, 1, "Head", 0, 4),
        (8, 2, "Setext", 0, 10),
        (12, 2, "Quoted", 1, 13),
        (14, 2, "Last", 0, 15),
    ]
    # a section runs to the next top-level heading of the same or a smaller depth
    assert doc.find_section_end(headings[0]) == 15
    assert doc.find_section_end(headings[1]) == 14
    assert doc.find_top_heading("## Last ##") == headings[3]
    assert doc.find_top_heading("> ## Quoted") is None

    # edits refresh the structure
    doc.replace_lines(3, 3, ["# New"])
    assert doc.find_first_heading() == (3, 1, "New", 0, 4)

    # a byte order mark does not hide the first heading
    assert MarkdownDocument("﻿# Bom\ntext").find_top_heading("# Bom") is not None


def test_markdown_document_empty() -> None:
    doc = MarkdownDocument("")
    assert doc.split_frontmatter() == ({}, "")
    assert str(doc) == ""
    doc.replace_lines(0, 0, ["hello"])
    assert str(doc) == "hello\n"


def test_markdown_document_body_text() -> None:
    """The delimited block at the top is never body, even when its YAML does not parse."""
    assert MarkdownDocument("---\nname: x\n---\nbody").body_text == "body"
    assert MarkdownDocument("---\n: broken [\n---\nbody").body_text == "body"
    assert MarkdownDocument("---\nno closing\nbody").body_text == "---\nno closing\nbody"
    assert MarkdownDocument("plain").body_text == "plain"


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


def test_markdown_document_replace_lines() -> None:
    doc = MarkdownDocument("a\nb\nc")
    doc.replace_lines(0, 0, ["x"])
    assert str(doc) == "x\na\nb\nc"
    doc.replace_lines(1, 3, ["y"])
    assert str(doc) == "x\ny\nc"
    doc.replace_lines(3, 3, ["z"])
    assert str(doc) == "x\ny\nc\nz"
