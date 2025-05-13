from typing import Any

import marimo as mo
import traceback


def build_error_callout(message: str) -> Any:
    """Build a callout with a message and a exposable stacktrace.

    Args:
        message (str): The message to display in the callout.

    Returns:
        mo.ui.Callout: The callout with the message and the stacktrace.
    """
    return mo.callout(
        mo.vstack(
            [
                mo.md(message),
                mo.accordion(
                    {"Show stacktrace": mo.ui.code_editor(traceback.format_exc(), language="shell")}
                ),
            ]
        ),
        kind="warn",
    )
