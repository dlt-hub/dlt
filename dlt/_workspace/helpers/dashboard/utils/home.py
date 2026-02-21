"""Home page rendering helpers: workspace and pipeline home views."""

from typing import Any, Dict, List, Union

import marimo as mo

import dlt
from dlt.common.configuration.specs.pluggable_run_context import ProfilesRunContext

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard import utils
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import ui


def header_controls(dlt_profile_select: mo.ui.dropdown) -> Union[List[Any], None]:
    """Build profile-related header controls if profiles are enabled."""
    if isinstance(dlt.current.run_context(), ProfilesRunContext):
        return [
            dlt_profile_select,
            mo.md(ui.small(f" Workspace: {getattr(dlt.current.run_context(), 'name', None)}")),
        ]
    return None


def detect_dlt_hub() -> bool:
    """Check whether dlt.hub is available."""
    try:
        return dlt.hub.__found__
    except ImportError:
        return False


def home_header_row(
    dlt_profile_select: mo.ui.dropdown,
    dlt_pipeline_select: mo.ui.multiselect,
) -> Any:
    """Shared header row with logo, profile/workspace info and pipeline select."""
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
                [
                    dlt_pipeline_select,
                ],
                justify="end",
            ),
        ],
        justify="space-between",
    )


def render_workspace_home(
    dlt_profile_select: mo.ui.dropdown,
    dlt_all_pipelines: List[Dict[str, Any]],
    dlt_pipeline_select: mo.ui.multiselect,
    dlt_pipelines_dir: str,
    dlt_config: DashboardConfiguration,
) -> List[Any]:
    """Render the workspace-level home view (no pipeline selected)."""
    return [
        utils.ui.section_marker(strings.app_section_name, has_content=True),
        home_header_row(dlt_profile_select, dlt_pipeline_select),
        mo.md(strings.app_title).center(),
        mo.md(strings.app_intro).center(),
        mo.callout(
            mo.vstack(
                [
                    mo.md(
                        strings.home_quick_start_title.format(
                            utils.pipeline.pipeline_link_list(dlt_config, dlt_all_pipelines)
                        )
                    ),
                    dlt_pipeline_select,
                ]
            ),
            kind="info",
        ),
        mo.md(strings.home_basics_text.format(len(dlt_all_pipelines), dlt_pipelines_dir)),
    ]


def render_pipeline_header_row(
    dlt_pipeline_name: str,
    dlt_profile_select: mo.ui.dropdown,
    dlt_pipeline_select: mo.ui.multiselect,
    buttons: List[Any],
) -> List[Any]:
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
    dlt_pipelines_dir: str,
    dlt_refresh_button: mo.ui.run_button,
    dlt_pipeline_name: str,
) -> List[Any]:
    """Render the pipeline-level home view (pipeline selected or requested)."""
    _buttons: List[Any] = [dlt_refresh_button]
    _pipeline_execution_exception: List[Any] = []
    _pipeline_execution_summary: Any = None
    _last_load_packages_info: Any = None
    _errors: List[Any] = []

    _buttons.append(
        mo.ui.button(
            label=ui.small("Open pipeline working dir"),
            on_click=lambda _: utils.pipeline.open_local_folder(dlt_pipeline.working_dir),
        )
    )
    if local_dir := utils.pipeline.get_local_data_path(dlt_pipeline):
        _buttons.append(
            mo.ui.button(
                label=ui.small("Open local data location"),
                on_click=lambda _: utils.pipeline.open_local_folder(local_dir),
            )
        )

    # NOTE: last_trace does not raise on broken traces
    if trace := dlt_pipeline.last_trace:
        _pipeline_execution_summary = utils.visualization.pipeline_execution_visualization(trace)
        _last_load_packages_info = mo.vstack(
            [
                mo.md(ui.small(strings.view_load_packages_text)),
                utils.visualization.load_package_status_labels(trace),
            ]
        )
        _pipeline_execution_exception = utils.pipeline.exception_section(dlt_pipeline)

    _stack = [
        utils.ui.section_marker(strings.home_section_name, has_content=dlt_pipeline is not None)
    ]
    _stack.extend(
        render_pipeline_header_row(
            dlt_pipeline_name, dlt_profile_select, dlt_pipeline_select, _buttons
        )
    )

    if _pipeline_execution_summary:
        _stack.append(_pipeline_execution_summary)
    if _last_load_packages_info:
        _stack.append(_last_load_packages_info)
    if _pipeline_execution_exception:
        _stack.extend(_pipeline_execution_exception)
    if _errors:
        _stack.extend(_errors)

    if not dlt_pipeline and dlt_pipeline_name:
        _stack.append(
            mo.callout(
                mo.md(strings.app_pipeline_not_found.format(dlt_pipeline_name, dlt_pipelines_dir)),
                kind="warn",
            )
        )

    return _stack
