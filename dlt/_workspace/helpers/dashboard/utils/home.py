"""Home page rendering helpers: workspace and pipeline home views."""

from typing import Any, List, Optional

import marimo as mo

import dlt
from dlt.common.configuration.specs.pluggable_run_context import ProfilesRunContext

from dlt._workspace.cli.utils import open_local_folder
from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard import utils
from dlt._workspace.helpers.dashboard.utils import ui


def header_controls(dlt_profile_select: mo.ui.dropdown) -> Optional[List[mo.Html]]:
    """Build profile-related header controls if profiles are enabled."""
    if isinstance(dlt.current.run_context(), ProfilesRunContext):
        return [
            dlt_profile_select,
            mo.md(
                ui.small(
                    strings.home_workspace_label.format(
                        getattr(dlt.current.run_context(), "name", None)
                    )
                )
            ),
        ]
    return None


def detect_dlt_hub() -> bool:
    """Check whether dlt.hub is available."""
    return dlt.hub.__found__


def home_header_row(
    dlt_profile_select: mo.ui.dropdown,
    right_control: Any = None,
) -> mo.Html:
    """Shared header row with logo, profile/workspace info and an optional right-hand control."""
    _header_controls = header_controls(dlt_profile_select)
    return mo.hstack(
        [
            mo.hstack(
                [
                    mo.image(
                        "https://dlthub.com/docs/img/dlthub-logo.png",
                        width=100,
                        alt="dltHub logo",
                    ),
                    _header_controls[0] if _header_controls else "",
                ],
                justify="start",
                gap=2,
            ),
            mo.hstack(
                [
                    _header_controls[1] if _header_controls else "",
                ],
                justify="center",
            ),
            mo.hstack(
                [right_control] if right_control is not None else [],
                justify="end",
            ),
        ],
        justify="space-between",
    )


def render_no_pipelines_home(
    dlt_profile_select: mo.ui.dropdown,
) -> List[mo.Html]:
    """Render a minimal landing shown when no pipelines are available to inspect.

    The pipeline dropdown is omitted because there is nothing to select.
    """
    return [
        utils.ui.section_marker(strings.app_section_name, has_content=True),
        home_header_row(dlt_profile_select),
        mo.callout(
            mo.md(strings.home_no_pipelines),
            kind="info",
        ),
    ]


def render_pipeline_header_row(
    dlt_pipeline_name: str,
    dlt_profile_select: mo.ui.dropdown,
    dlt_pipeline_select: mo.ui.multiselect,
    buttons: List[mo.Html],
) -> List[mo.Html]:
    """Render the pipeline header row with logo, title, and action buttons."""
    header_row = home_header_row(dlt_profile_select, dlt_pipeline_select)
    pipeline_title = mo.center(
        mo.hstack(
            [
                mo.md(strings.app_title_pipeline.format(dlt_pipeline_name)),
            ],
            align="center",
        ),
    )

    return [
        mo.vstack(
            [
                mo.hstack(
                    [
                        mo.vstack(
                            [
                                header_row,
                                pipeline_title,
                            ]
                        ),
                    ],
                    justify="space-between",
                ),
            ]
        ),
        mo.hstack(buttons, justify="start"),
    ]


def render_pipeline_home(
    dlt_profile_select: mo.ui.dropdown,
    dlt_pipeline: dlt.Pipeline,
    dlt_pipeline_select: mo.ui.multiselect,
    dlt_refresh_button: mo.ui.run_button,
    dlt_pipeline_name: str,
) -> List[mo.Html]:
    """Render the pipeline-level home view (pipeline selected or requested)."""
    _buttons: List[mo.Html] = [dlt_refresh_button]
    _pipeline_execution_exception: List[mo.Html] = []
    _pipeline_execution_summary: Optional[mo.Html] = None
    _last_load_packages_info: Optional[mo.Html] = None

    _buttons.append(
        mo.ui.button(
            label=ui.small(strings.home_open_working_dir_button),
            on_click=lambda _: open_local_folder(dlt_pipeline.working_dir),
        )
    )
    if local_dir := utils.pipeline.get_local_data_path(dlt_pipeline):
        _buttons.append(
            mo.ui.button(
                label=ui.small(strings.home_open_local_data_button),
                on_click=lambda _: open_local_folder(local_dir),
            )
        )

    _stack: List[mo.Html] = [
        utils.ui.section_marker(strings.home_section_name, has_content=dlt_pipeline is not None)
    ]
    _stack.extend(
        render_pipeline_header_row(
            dlt_pipeline_name, dlt_profile_select, dlt_pipeline_select, _buttons
        )
    )

    # NOTE: last_trace does not raise on broken traces
    if trace := dlt_pipeline.last_trace:
        if _pipeline_execution_summary := utils.visualization.pipeline_execution_visualization(
            trace
        ):
            _stack.append(_pipeline_execution_summary)
        if _last_load_packages_info := mo.vstack(
            [
                mo.md(ui.small(strings.view_load_packages_text)),
                utils.visualization.load_package_status_labels(trace),
            ]
        ):
            _stack.append(_last_load_packages_info)
        if _pipeline_execution_exception := utils.pipeline.exception_section(dlt_pipeline):
            _stack.extend(_pipeline_execution_exception)
    else:
        _stack.append(
            mo.callout(
                mo.md(strings.app_pipeline_no_trace.format(dlt_pipeline_name)),
                kind="info",
            )
        )

    return _stack
