from typing import Any

import marimo as mo
import traceback


def build_error_callout(message: str, code: str = None) -> Any:
    """Build a callout with a message and a exposable stacktrace.

    Args:
        message (str): The message to display in the callout.

    Returns:
        mo.ui.Callout: The callout with the message and the stacktrace.
    """
    if code:
        code = code.replace("\x1b[4m", ">>>").replace("\x1b[0m", "<<<")
    return mo.callout(
        mo.vstack(
            [
                mo.md(message),
                mo.ui.code_editor(code, language="shell") if code else None,
                mo.accordion(
                    {"Show stacktrace": mo.ui.code_editor(traceback.format_exc(), language="shell")}
                ),
            ]
        ),
        kind="warn",
    )
