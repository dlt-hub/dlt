"""CLI prompting and output helpers."""

import contextlib
import sys
from typing import IO, Any, ContextManager, Iterable, Iterator, Optional

from rich.console import Console
from rich.text import Text

_ANSI_COLORS = {
    "black": 30,
    "red": 31,
    "green": 32,
    "yellow": 33,
    "blue": 34,
    "magenta": 35,
    "cyan": 36,
    "white": 37,
    "bright_black": 90,
    "bright_red": 91,
    "bright_green": 92,
    "bright_yellow": 93,
    "bright_blue": 94,
    "bright_magenta": 95,
    "bright_cyan": 96,
    "bright_white": 97,
}


ALWAYS_CHOOSE_DEFAULT = False
ALWAYS_CHOOSE_VALUE: Any = None
ALWAYS_CONFIRM = False

_CLI_HOST: str = "dlt"


def get_cli_host_name() -> str:
    """Returns the active CLI host name (e.g. `"dlt"` or `"dlthub"`)."""
    return _CLI_HOST


def set_cli_host_name(host: str) -> None:
    """Sets the active CLI host name. Called by `_dlt.main()` at startup."""
    global _CLI_HOST
    _CLI_HOST = host


def cli_cmd(rest: str = "") -> str:
    """Formats an example command line prefixed with the active CLI host name.

    Args:
        rest (str): Argument string to append after the host name.

    Returns:
        str: The full example, e.g. `"dlt pipeline my_pipe info"` or
        `"dlthub pipeline my_pipe info"` when the dlthub host is active.
    """
    return f"{_CLI_HOST} {rest}".rstrip()


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
        always_choose_default (bool): When True, confirm/prompt calls return their default.
        always_choose_value (Any): When set, confirm/prompt calls return this value instead.
        always_confirm (bool): When True, confirm calls always return True, regardless of
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


def style(
    text: Any,
    fg: Optional[str] = None,
    bold: Optional[bool] = None,
    reset: bool = True,
) -> str:
    """Apply ANSI foreground color and bold styling to ``text``."""
    codes = []
    if fg is not None:
        try:
            codes.append(str(_ANSI_COLORS[fg]))
        except KeyError as ex:
            raise TypeError(f"Unknown color {fg!r}") from ex
    if bold is not None:
        codes.append("1" if bold else "22")

    value = str(text)
    if not codes:
        return value
    value = f"\033[{';'.join(codes)}m{value}"
    return value + "\033[0m" if reset else value


def echo(
    message: Any = None,
    file: Optional[IO[Any]] = None,
    nl: bool = True,
    err: bool = False,
    color: Optional[bool] = None,
) -> None:
    """Write a message to stdout or stderr."""
    output = file or (sys.stderr if err else sys.stdout)
    value = "" if message is None else str(message)
    console = Console(file=output, force_terminal=color)
    console.print(Text.from_ansi(value), end="\n" if nl else "", soft_wrap=True)


def secho(
    message: Any = None,
    file: Optional[IO[Any]] = None,
    nl: bool = True,
    err: bool = False,
    color: Optional[bool] = None,
    fg: Optional[str] = None,
    bold: Optional[bool] = None,
) -> None:
    """Style a message and write it with :func:`echo`."""
    echo(style(message if message is not None else "", fg=fg, bold=bold), file, nl, err, color)


def bold(msg: str) -> str:
    return style(msg, bold=True, reset=False) + style("", bold=False, reset=False)


def warning_style(msg: str) -> str:
    return style(msg, fg="yellow", reset=True)


def error(msg: str) -> None:
    secho("ERROR: " + msg, fg="red")


def warning(msg: str) -> None:
    secho("WARNING: " + msg, fg="yellow")


def note(msg: str) -> None:
    secho("NOTE: " + msg, fg="green")


def _read_input(label: str) -> str:
    try:
        return input(label)
    except (EOFError, KeyboardInterrupt):
        echo(err=True)
        raise


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
    choices = "Y/n" if default is True else "y/N" if default is False else "y/n"
    while True:
        value = _read_input(f"{text} [{choices}]: ").strip().lower()
        if not value and default is not None:
            return default
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False
        echo("Error: invalid input")


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
    available = tuple(choices)
    label = text
    if show_choices:
        label += f" ({', '.join(available)})"
    if show_default and default is not None:
        label += f" [{default}]"
    label += ": "

    while True:
        value = _read_input(label)
        if not value and default is not None:
            return default
        if value in available:
            return value
        echo(f"Error: invalid choice: {value}. (choose from {', '.join(available)})")


def text_input(text: str, default: Optional[str] = None) -> str:
    if ALWAYS_CHOOSE_VALUE is not None:
        return str(ALWAYS_CHOOSE_VALUE)
    if ALWAYS_CHOOSE_DEFAULT or ALWAYS_CONFIRM:
        if default is None:
            _raise_no_default(text)
        return default
    label = f"{text} [{default}]: " if default is not None else f"{text}: "
    value = _read_input(label)
    return default if not value and default is not None else value
