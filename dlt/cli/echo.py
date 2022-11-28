import click


def bold(msg: str) -> str:
    return click.style(msg, bold=True)


def warning(msg: str) -> None:
    click.secho("WARNING: " + msg, fg="yellow")
