import contextlib
from typing import Any, Iterable, Iterator, Optional
import click


ALWAYS_CHOOSE_DEFAULT = False
ALWAYS_CHOOSE_VALUE: Any = None


@contextlib.contextmanager
def always_choose(always_choose_default: bool, always_choose_value: Any) -> Iterator[None]:
    """Temporarily answer all confirmations and prompts with the values specified in arguments"""
    global ALWAYS_CHOOSE_DEFAULT, ALWAYS_CHOOSE_VALUE
    _always_choose_default = ALWAYS_CHOOSE_DEFAULT
    _always_choose_value = ALWAYS_CHOOSE_VALUE
    ALWAYS_CHOOSE_DEFAULT = always_choose_default
    ALWAYS_CHOOSE_VALUE = always_choose_value
    try:
        yield
    finally:
        ALWAYS_CHOOSE_DEFAULT = _always_choose_default
        ALWAYS_CHOOSE_VALUE = _always_choose_value


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


echo = click.echo
secho = click.secho
style = click.style


def bold(msg: str) -> str:
    return click.style(msg, bold=True, reset=True)


def warning_style(msg: str) -> str:
    return click.style(msg, fg="yellow", reset=True)


def error(msg: str) -> None:
    click.secho("ERROR: " + msg, fg="red")


def warning(msg: str) -> None:
    click.secho("WARNING: " + msg, fg="yellow")


def note(msg: str) -> None:
    click.secho("NOTE: " + msg, fg="green")


def confirm(text: str, default: Optional[bool] = None) -> bool:
    if ALWAYS_CHOOSE_VALUE:
        return bool(ALWAYS_CHOOSE_VALUE)
    if ALWAYS_CHOOSE_DEFAULT:
        assert default is not None
        return default
    return click.confirm(text, default=default)


def prompt(
    text: str,
    choices: Iterable[str],
    default: Optional[Any] = None,
    show_choices: bool = True,
    show_default: bool = True,
) -> Any:
    if ALWAYS_CHOOSE_VALUE:
        assert ALWAYS_CHOOSE_VALUE in choices
        return ALWAYS_CHOOSE_VALUE
    if ALWAYS_CHOOSE_DEFAULT:
        assert default is not None
        return default
    click_choices = click.Choice(choices)
    return click.prompt(
        text,
        type=click_choices,
        default=default,
        show_choices=show_choices,
        show_default=show_default,
    )


def text_input(text: str) -> str:
    return click.prompt(text)  # type: ignore[no-any-return]
