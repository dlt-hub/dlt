import click


def bold(msg: str) -> str:
    return click.style(msg, bold=True)
