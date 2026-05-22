"""Provider-aware behavior of `dlthub ai secrets` commands."""

import os
from pathlib import Path

import pytest

from dlt._workspace.cli.dlthub.ai import (
    ai_secrets_list_command,
    ai_secrets_update_fragment_command,
    ai_secrets_view_redacted_command,
)

from tests.workspace.utils import isolated_workspace


def test_ai_secrets_list_oss(
    legacy_workspace_context,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """OSS context lists secrets.toml without profiles."""
    # note: test conftest patches RunContext.initial_providers to use tests/.dlt
    # so providers point to tests/.dlt, not the workspace's .dlt — that's expected
    ai_secrets_list_command()
    output = capsys.readouterr().out

    assert "Secret file locations:" in output
    # no profile tags in OSS context
    assert "profile:" not in output
    # no "not found"
    assert "not found" not in output.lower()


def test_ai_secrets_list_workspace_with_profile(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Workspace context lists profile-scoped path first, no global."""
    with isolated_workspace("default", profile="dev"):
        ai_secrets_list_command()
        output = capsys.readouterr().out

        assert "Secret file locations:" in output
        # profile path listed
        assert "dev.secrets.toml" in output
        assert "profile: dev" in output
        # base path listed
        assert "secrets.toml" in output
        # no global
        assert "global" not in output.lower()
        # no "not found"
        assert "not found" not in output.lower()
        # profile-scoped appears before base
        lines = [line.strip() for line in output.splitlines() if "secrets.toml" in line]
        assert len(lines) >= 2
        assert "dev.secrets.toml" in lines[0]


def test_ai_secrets_view_redacted_missing_creates_nothing(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """view-redacted on a missing file warns but does not create the file."""
    with isolated_workspace("empty", profile="dev"):
        secrets_path = os.path.join(".dlt", "secrets.toml")
        assert not os.path.exists(secrets_path)

        ai_secrets_view_redacted_command(path=secrets_path)
        output = capsys.readouterr().out

        assert "not found" in output.lower()
        # file should NOT be created
        assert not os.path.exists(secrets_path)


def test_ai_secrets_update_fragment_creates_file(
    autouse_test_storage: None,
    preserve_run_context: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """update-fragment creates secrets file if it doesn't exist."""
    with isolated_workspace("empty", profile="dev"):
        secrets_path = os.path.join(".dlt", "secrets.toml")
        assert not os.path.exists(secrets_path)

        ai_secrets_update_fragment_command(
            fragment='[sources.my_api]\napi_key = "sk-test-xxx"\n',
            path=secrets_path,
        )
        output = capsys.readouterr().out

        # file was created
        assert os.path.isfile(secrets_path)
        # output shows redacted content
        assert "api_key" in output
        assert "sk-test-xxx" not in output
        assert "**" in output
        # actual file has the real value
        content = Path(secrets_path).read_text(encoding="utf-8")
        assert "sk-test-xxx" in content
