import pytest

from dlt.common.runtime.exec_info import (
    exec_info_names,
    is_aws_lambda,
    is_claude_code,
    is_claude_code_cli,
    is_codespaces,
    is_codex,
    is_cursor,
    is_gcp_cloud_function,
    is_github_actions,
    is_running_in_airflow_task,
    is_streamlit,
)
from dlt.common.typing import DictStrStr

from tests.common.configuration.utils import environment


@pytest.mark.parametrize(
    "env_var,detect_fn",
    [
        ("CODESPACES", is_codespaces),
        ("GITHUB_ACTIONS", is_github_actions),
        ("STREAMLIT_SERVER_PORT", is_streamlit),
        ("AWS_LAMBDA_FUNCTION_NAME", is_aws_lambda),
        ("FUNCTION_NAME", is_gcp_cloud_function),
        ("AIRFLOW_CTX_TASK_ID", is_running_in_airflow_task),
    ],
    ids=[
        "codespaces",
        "github-actions",
        "streamlit",
        "aws-lambda",
        "gcp-cloud-function",
        "airflow-task",
    ],
)
def test_env_presence_detection(env_var: str, detect_fn, environment: DictStrStr) -> None:
    assert detect_fn() is False
    environment[env_var] = "1"
    assert detect_fn() is True


def test_is_claude_code(environment: DictStrStr) -> None:
    assert is_claude_code() is False

    # wrong value does not trigger
    environment["CLAUDECODE"] = "0"
    assert is_claude_code() is False

    environment["CLAUDECODE"] = "1"
    assert is_claude_code() is True


def test_is_claude_code_cli(environment: DictStrStr) -> None:
    assert is_claude_code_cli() is False

    # only CLAUDECODE — not enough
    environment["CLAUDECODE"] = "1"
    assert is_claude_code_cli() is False

    # only entrypoint — not enough
    del environment["CLAUDECODE"]
    environment["CLAUDE_CODE_ENTRYPOINT"] = "cli"
    assert is_claude_code_cli() is False

    # both set
    environment["CLAUDECODE"] = "1"
    assert is_claude_code_cli() is True


def test_is_cursor(environment: DictStrStr) -> None:
    assert is_cursor() is False

    # empty or "0" must not trigger
    environment["CURSOR_AGENT"] = ""
    assert is_cursor() is False
    environment["CURSOR_AGENT"] = "0"
    assert is_cursor() is False

    environment["CURSOR_AGENT"] = "1"
    assert is_cursor() is True


def test_is_codex(environment: DictStrStr) -> None:
    assert is_codex() is False

    # wrong CODEX_CI value alone
    environment["CODEX_CI"] = "0"
    assert is_codex() is False

    # each trigger independently
    environment["CODEX_CI"] = "1"
    assert is_codex() is True

    del environment["CODEX_CI"]
    environment["CODEX_THREAD_ID"] = "thread-abc"
    assert is_codex() is True

    del environment["CODEX_THREAD_ID"]
    environment["CODEX_SANDBOX"] = "/tmp/sandbox"
    assert is_codex() is True


@pytest.mark.parametrize(
    "env_vars,expected_names",
    [
        ({"CLAUDECODE": "1"}, ["claude_code"]),
        (
            {"CLAUDECODE": "1", "CLAUDE_CODE_ENTRYPOINT": "cli"},
            ["claude_code", "claude_code_cli"],
        ),
        ({"CURSOR_AGENT": "1"}, ["cursor"]),
        ({"CODEX_CI": "1"}, ["codex"]),
        ({"CODEX_THREAD_ID": "t-1"}, ["codex"]),
        (
            {"CLAUDECODE": "1", "CURSOR_AGENT": "1", "CODEX_CI": "1"},
            ["claude_code", "cursor", "codex"],
        ),
    ],
    ids=[
        "claude-code-only",
        "claude-code-cli",
        "cursor",
        "codex-ci",
        "codex-thread-id",
        "all-agents",
    ],
)
def test_exec_info_names_includes_agents(env_vars, expected_names, environment: DictStrStr) -> None:
    environment.update(env_vars)
    names = exec_info_names()
    for expected in expected_names:
        assert expected in names


def test_exec_info_names_excludes_agents_when_absent(environment: DictStrStr) -> None:
    names = exec_info_names()
    agent_names = {"claude_code", "claude_code_cli", "cursor", "codex"}
    assert agent_names.isdisjoint(set(names))
