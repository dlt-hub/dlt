import click


def bold(msg: str) -> str:
    return click.style(msg, bold=True, reset=True)


def warning(msg: str) -> None:
    click.secho("WARNING: " + msg, fg="yellow")


def note(msg: str) -> None:
    click.secho("NOTE: " + msg, fg="green")
