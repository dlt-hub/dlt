from typing import Any
from dlt.common.configuration.specs.pluggable_run_context import ProfilesRunContext

import dlt

import marimo as mo
import traceback


def build_error_callout(message: str, code: str = None, traceback_string: str = None) -> Any:
    """Build a callout with a message and a exposable stacktrace.

    Args:
        message (str): The message to display in the callout.
        code (str): The code to display in the callout.
        traceback_string (str): The traceback to display in the callout.

    Returns:
        mo.ui.Callout: The callout with the message and the stacktrace.
    """
    if code:
        code = code.replace("\x1b[4m", ">>>").replace("\x1b[0m", "<<<")

    traceback_string = traceback_string or traceback.format_exc()
    if traceback_string.startswith("NoneType: None"):
        traceback_string = None

    stack_items = [mo.md(message)]
    if code:
        stack_items.append(
            mo.ui.code_editor(code, language="python", disabled=True, show_copy_button=True)
        )
    if traceback_string:
        stack_items.append(
            mo.accordion(
                {
                    "Show stacktrace for more information or debugging": mo.ui.code_editor(
                        traceback_string, language="python", disabled=True, show_copy_button=True
                    )
                }
            )
        )

    return mo.callout(
        mo.vstack(stack_items),
        kind="warn",
    )


def build_title_and_subtitle(title: str, subtitle: str = None, title_level: int = 2) -> Any:
    """Build a title and a subtitle block"""
    _result = []
    if title:
        _result.append(mo.md(f"{title_level * '#'} {title}"))
    if subtitle:
        _result.append(mo.md(f"<small>{subtitle}</small>"))
    return mo.vstack(_result)


def build_run_context_inline_label() -> Any:
    """Inline, small-font label with profile/workspace info to place near controls."""
    run_context = dlt.current.run_context()
    if isinstance(run_context, ProfilesRunContext):
        label = (
            run_context.profile
            if run_context.default_profile != run_context.profile
            else run_context.default_profile
        )
        return mo.md(f"<small>Profile: {label}</small>")
    # Non-profile-aware context
    return mo.md("<small>Profile: _</small>")


def build_workspace_label() -> Any:
    """Show workspace name, or '_' if not set/available."""
    run_context = dlt.current.run_context()
    text = getattr(run_context, "name", None)
    return mo.md(f"<small>{text}</small>")


def build_labeled_inline(label_text: str, content: Any) -> Any:
    """Inline label + content, matching selector header design."""
    return mo.hstack([mo.md(f"<small>{label_text}:</small>"), content], align="center").style(
        gap="0.5rem"
    )


def build_tabs_spacer(num_tabs: int = 2) -> Any:
    """Creates an inline spacer approximating tabs using non-breaking spaces."""
    try:
        count = int(num_tabs)
    except Exception:
        count = 2
    spaces = "&nbsp;" * (count * 4)  # approx 4 spaces per tab
    return mo.md(spaces)


def build_page_header(
    dlt_pipeline: dlt.Pipeline,
    title: str,
    subtitle: str,
    subtitle_long: str,
    button: Any = None,
) -> Any:
    """Build a page header with a title, a subtitle, button and conditional longer subtitle"""
    if not dlt_pipeline:
        return []
    return [
        mo.hstack(
            [
                build_title_and_subtitle(title, subtitle if not button.value else subtitle_long),
                button,
            ],
            align="center",
        )
    ]
