import pytest
from pathlib import Path

from dlt._workspace.cli import _ai_command, DEFAULT_VERIFIED_SOURCES_REPO
from dlt._workspace.cli._ai_command import TSupportedIde


@pytest.mark.parametrize(
    ("ide", "expected_relative_path", "is_single_file"),
    (
        ("amp", "AGENT.md", True),
        ("codex", "AGENT.md", True),
        ("claude", "CLAUDE.md", True),
        ("cody", ".sourcegraph", False),
        ("cline", ".clinerules", False),
        ("cursor", ".cursor/rules/", False),
        ("continue", ".continue/rules/", False),
        ("windsurf", ".windsurf/rules/", False),
        ("copilot", ".github/instructions/", False),
    ),
)
def test_ai_setup_command(
    ide: TSupportedIde, expected_relative_path: str, is_single_file: bool
) -> None:
    _ai_command.ai_setup_command(ide=ide, location=DEFAULT_VERIFIED_SOURCES_REPO)

    base_path = Path(".").resolve()
    expected_location = base_path / expected_relative_path

    if is_single_file:
        assert expected_location.is_file()
        return

    assert expected_location.is_dir()
    all_files = list(expected_location.rglob("*"))
    if ide == "cursor":
        assert all(file.suffix == ".mdc" for file in all_files)
        assert (base_path / ".cursorignore").is_file()
    else:
        assert all(file.suffix == ".md" for file in all_files)
