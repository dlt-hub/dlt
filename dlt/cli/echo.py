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
    yield
    ALWAYS_CHOOSE_DEFAULT = _always_choose_default
    ALWAYS_CHOOSE_VALUE = _always_choose_value



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


def prompt(text: str, choices: Iterable[str], default: Optional[Any] = None) -> Any:
    if ALWAYS_CHOOSE_VALUE:
        assert ALWAYS_CHOOSE_VALUE in choices
        return ALWAYS_CHOOSE_VALUE
    if ALWAYS_CHOOSE_DEFAULT:
        assert default is not None
        return default
    click_choices = click.Choice(choices)
    return click.prompt(text, type=click_choices, default=default)

def text_input(text: str) -> str:
    return click.prompt(text)  # type: ignore[no-any-return]
