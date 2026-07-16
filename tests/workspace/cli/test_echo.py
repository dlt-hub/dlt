import io
from unittest.mock import patch

import pytest

from dlt._workspace.cli import echo as fmt


def test_style_and_echo_color_handling() -> None:
    styled = fmt.style("message", fg="yellow", bold=True)
    assert styled == "\033[33;1mmessage\033[0m"

    plain_output = io.StringIO()
    fmt.echo(styled, file=plain_output)
    assert plain_output.getvalue() == "message\n"

    color_output = io.StringIO()
    fmt.echo(styled, file=color_output, color=True)
    assert "\033[" in color_output.getvalue()
    assert "message" in color_output.getvalue()


def test_secho_can_write_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    fmt.secho("problem", fg="red", err=True)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "problem\n"


@pytest.mark.parametrize(
    ("answer", "default", "expected"),
    [("y", False, True), ("NO", True, False), ("", True, True), ("", False, False)],
)
def test_confirm(answer: str, default: bool, expected: bool) -> None:
    with patch("builtins.input", return_value=answer):
        assert fmt.confirm("Continue?", default=default) is expected


def test_confirm_retries_invalid_input(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("builtins.input", side_effect=["maybe", "yes"]):
        assert fmt.confirm("Continue?") is True
    assert capsys.readouterr().out == "Error: invalid input\n"


def test_prompt_retries_until_choice(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("builtins.input", side_effect=["x", "b"]):
        assert fmt.prompt("Pick", ("a", "b"), default="a") == "b"
    assert "invalid choice" in capsys.readouterr().out


def test_prompt_and_text_input_use_defaults() -> None:
    with patch("builtins.input", return_value=""):
        assert fmt.prompt("Pick", ("a", "b"), default="a") == "a"
        assert fmt.text_input("Name", default="Jane") == "Jane"
