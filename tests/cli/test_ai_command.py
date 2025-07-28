import pytest
from pathlib import Path

from dlt.common.utils import set_working_dir
from dlt.cli import ai_command
from dlt.cli.ai_command import TSupportedIde, LOCATION_IN_BASE_DIR, SUPPORTED_IDES
from dlt.cli.plugins import DEFAULT_VERIFIED_SOURCES_REPO
from tests.utils import TEST_STORAGE_ROOT


@pytest.mark.parametrize(
    "ide",
    SUPPORTED_IDES,
)
def test_ai_setup_command(ide: TSupportedIde) -> None:
    with set_working_dir(TEST_STORAGE_ROOT):
        ai_command.ai_setup_command(
            ide=ide, location=DEFAULT_VERIFIED_SOURCES_REPO, branch="feat/continue-rules"
        )

    expected_location = Path(TEST_STORAGE_ROOT).resolve() / LOCATION_IN_BASE_DIR[ide]

    if ide in {"amp", "codex", "claude"}:
        assert expected_location.is_file()
        return

    if ide == "copilot":
        expected_location /= "instructions"
    elif ide in {"cursor", "continue", "windsurf"}:
        expected_location /= "rules"

    assert expected_location.is_dir()
    md_files = list(expected_location.glob("*.mdc" if ide == "cursor" else "*.md"))
    assert md_files
