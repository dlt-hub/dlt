"""CLI prompting and output helpers."""

import io
import sys
import contextlib
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple, ContextManager
import click


ALWAYS_CHOOSE_DEFAULT = False
ALWAYS_CHOOSE_VALUE: Any = None
ALWAYS_CONFIRM = False

_CLI_HOST: str = "dlt"

DLT_TO_DLTHUB_COMMANDS: Dict[str, str] = {
    "init": "pipeline init",
    "pipeline": "local pipeline",
    "schema": "local schema",
    "telemetry": "local telemetry",
    "dashboard": "local show",
    "ai": "ai",
}
"""Maps `dlt` commands to their `dlthub` replacements suggested in an active workspace."""

_pipeline_parser: Any = None


def get_cli_host_name() -> str:
    """Returns the active CLI host name (e.g. `"dlt"` or `"dlthub"`)."""
    return _CLI_HOST


def set_cli_host_name(host: str) -> None:
    """Sets the active CLI host name. Called by `_dlt.main()` at startup."""
    global _CLI_HOST
    _CLI_HOST = host


def _parse_pipeline_cmd(tokens: Iterable[str]) -> Tuple[Optional[str], Optional[str]]:
    """Extracts (pipeline_name, operation) from OSS `pipeline` args using the real parser.
    Returns (None, None) when the args cannot be parsed."""
    global _pipeline_parser
    if _pipeline_parser is None:
        import argparse
        from dlt._workspace.cli.commands import PipelineCommand

        _pipeline_parser = argparse.ArgumentParser(prog="pipeline", add_help=False)
        PipelineCommand().configure_parser(_pipeline_parser)
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            ns, _ = _pipeline_parser.parse_known_args(list(tokens))
    except SystemExit:
        return None, None
    return getattr(ns, "pipeline_name", None), getattr(ns, "operation", None)


def cli_cmd(rest: str = "") -> str:
    """Formats an example command line for the active CLI host.

    For the `dlthub` host the command word is mapped to its dlthub group and `pipeline`
    commands are reordered to the verb-first `local pipeline <verb> <name> ...` form.

    Args:
        rest: The command line as written for the `dlt` (OSS) host, e.g.
            `"pipeline my_pipe load-package 123 row-counts"`.

    Returns:
        str: The full example prefixed and reordered for the active host.
    """
    tokens = rest.split()
    if _CLI_HOST != "dlthub" or not tokens:
        return f"{_CLI_HOST} {rest}".rstrip()
    group = DLT_TO_DLTHUB_COMMANDS.get(tokens[0])
    if group is None:
        return f"dlthub {rest}".rstrip()
    if tokens[0] != "pipeline":
        return " ".join(["dlthub", group, *tokens[1:]])
    name, verb = _parse_pipeline_cmd(tokens[1:])
    if verb is None:
        return " ".join(["dlthub", group, *tokens[1:]])
    verb_idx = tokens.index(verb)
    # tokens before the verb are global flags (e.g. -v); the name moves after the verb
    flags = [t for t in tokens[1:verb_idx] if t != name]
    suffix = tokens[verb_idx + 1 :]
    return " ".join(
        ["dlthub", "local", "pipeline", *flags, verb, *([name] if name else []), *suffix]
    )


def is_interactive() -> bool:
    """True when the CLI may prompt the user for input."""
    return not (ALWAYS_CHOOSE_DEFAULT or ALWAYS_CONFIRM) and ALWAYS_CHOOSE_VALUE is None


def set_non_interactive(value: bool = True) -> None:
    """Toggle `--non-interactive`."""
    global ALWAYS_CHOOSE_DEFAULT
    ALWAYS_CHOOSE_DEFAULT = value


def set_auto_yes(value: bool = True) -> None:
    """Toggle `-y`/`--yes`."""
    global ALWAYS_CONFIRM
    ALWAYS_CONFIRM = value


@contextlib.contextmanager
def always_choose(
    always_choose_default: bool,
    always_choose_value: Any,
    always_confirm: bool = False,
) -> Iterator[None]:
    """Temporarily answer all confirmations and prompts with preset values.

    Args:
        always_choose_default: When True, confirm/prompt calls return their default.
        always_choose_value: When set, confirm/prompt calls return this value instead.
        always_confirm: When True, confirm calls always return True, regardless of
            `always_choose_default` and `always_choose_value`.
    """
    global ALWAYS_CHOOSE_DEFAULT, ALWAYS_CHOOSE_VALUE, ALWAYS_CONFIRM
    _always_choose_default = ALWAYS_CHOOSE_DEFAULT
    _always_choose_value = ALWAYS_CHOOSE_VALUE
    _always_confirm = ALWAYS_CONFIRM
    ALWAYS_CHOOSE_DEFAULT = always_choose_default
    ALWAYS_CHOOSE_VALUE = always_choose_value
    ALWAYS_CONFIRM = always_confirm
    try:
        yield
    finally:
        ALWAYS_CHOOSE_DEFAULT = _always_choose_default
        ALWAYS_CHOOSE_VALUE = _always_choose_value
        ALWAYS_CONFIRM = _always_confirm


@contextlib.contextmanager
def suppress_echo() -> Iterator[None]:
    """Temporarily suppress all fmt output."""
    global echo, secho, error, warning, note
    original_echo, original_secho = echo, secho
    original_error, original_warning, original_note = error, warning, note

    def noop(*args: Any, **kwargs: Any) -> None:
        pass

    echo = secho = error = warning = note = noop
    try:
        yield
    finally:
        echo, secho = original_echo, original_secho
        error, warning, note = original_error, original_warning, original_note


def maybe_no_stdin() -> ContextManager[None]:
    """Switch to non-interactive mode for the duration of the block if stdin is not at tty."""
    return always_choose(
        True if not sys.stdin.isatty() else ALWAYS_CHOOSE_DEFAULT,
        ALWAYS_CHOOSE_VALUE,
        ALWAYS_CONFIRM,
    )


echo = click.echo
secho = click.secho
style = click.style


def bold(msg: str) -> str:
    return click.style(msg, bold=True, reset=False) + click.style("", bold=False, reset=False)


def warning_style(msg: str) -> str:
    return click.style(msg, fg="yellow", reset=True)


def error(msg: str) -> None:
    click.secho("ERROR: " + msg, fg="red")


def warning(msg: str) -> None:
    click.secho("WARNING: " + msg, fg="yellow")


def note(msg: str) -> None:
    click.secho("NOTE: " + msg, fg="green")


def _raise_no_default(text: str) -> None:
    """Raise `CliCommandException` when a prompt has no default in non-interactive mode."""
    error(
        "Cannot read `%s` in non-interactive mode (no default provided). Pass the value via a"
        " CLI option, or run interactively." % text
    )
    # do not import at the top
    from dlt._workspace.cli.exceptions import CliCommandException

    raise CliCommandException()


def confirm(text: str, default: Optional[bool] = None) -> bool:
    if ALWAYS_CONFIRM:
        return True
    if ALWAYS_CHOOSE_VALUE is not None:
        return bool(ALWAYS_CHOOSE_VALUE)
    if ALWAYS_CHOOSE_DEFAULT:
        if default is None:
            _raise_no_default(text)
        return default
    return click.confirm(text, default=default)


def prompt(
    text: str,
    choices: Iterable[str],
    default: Optional[Any] = None,
    show_choices: bool = True,
    show_default: bool = True,
) -> Any:
    if ALWAYS_CHOOSE_VALUE is not None:
        assert ALWAYS_CHOOSE_VALUE in choices
        return ALWAYS_CHOOSE_VALUE
    if ALWAYS_CHOOSE_DEFAULT or ALWAYS_CONFIRM:
        if default is None:
            _raise_no_default(text)
        return default
    click_choices = click.Choice(choices)
    return click.prompt(
        text,
        type=click_choices,
        default=default,
        show_choices=show_choices,
        show_default=show_default,
    )


def text_input(text: str, default: str = None) -> str:
    if ALWAYS_CHOOSE_VALUE is not None:
        return str(ALWAYS_CHOOSE_VALUE)
    if ALWAYS_CHOOSE_DEFAULT or ALWAYS_CONFIRM:
        if default is None:
            _raise_no_default(text)
        return default
    return click.prompt(text, default=default)  # type: ignore[no-any-return]
