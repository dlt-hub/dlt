from typing import Any, Dict, List

import dlt

import marimo as mo
import traceback

from dlt.common.pendulum import pendulum

from dlt.helpers.dashboard.config import DashboardConfiguration


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


def build_pipeline_link_list(
    config: DashboardConfiguration, pipelines: List[Dict[str, Any]]
) -> str:
    """Build a list of links to the pipeline."""
    if not pipelines:
        return "No local pipelines found."

    count = 0
    link_list: str = ""
    for _p in pipelines:
        link = f"* [{_p['name']}](?pipeline={_p['name']})"
        if _p["timestamp"] == 0:
            link = link + " - never used"
        else:
            link = (
                link
                + " - last executed"
                f" {pendulum.from_timestamp(_p['timestamp']).format(config.datetime_format)}"
            )

        link_list += f"{link}\n"
        count += 1
        if count == 5:
            break

    return link_list


def build_title_and_subtitle(title: str, subtitle: str = None, title_level: int = 2) -> Any:
    """Build a title and a subtitle block"""
    _result = []
    if title:
        _result.append(mo.md(f"{title_level * '#'} {title}"))
    if subtitle:
        _result.append(mo.md(f"<small>{subtitle}</small>"))
    return mo.vstack(_result)


def build_page_header(
    dlt_pipeline: dlt.Pipeline, title: str, subtitle: str, subtitle_long: str, button: Any = None
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
