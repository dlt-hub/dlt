"""Data quality dashboard helpers: controls, section widget, and raw table display."""

import traceback
from typing import Callable, List, Optional, Tuple

import marimo as mo

import dlt
import pyarrow

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.utils import queries, ui


def create_dq_controls(
    pipeline: dlt.Pipeline,
) -> Tuple[
    Optional[mo.ui.checkbox],
    Optional[mo.ui.dropdown],
    Optional[mo.ui.slider],
    Optional[pyarrow.Table],
]:
    """Create data quality filter controls by importing from dlthub.

    Returns (show_failed_filter, table_filter, rate_filter, checks_arrow).
    All None on failure or if dlthub is not available.
    """
    try:
        from dlthub.data_quality._dashboard import create_data_quality_controls

        result: Tuple[
            Optional[mo.ui.checkbox],
            Optional[mo.ui.dropdown],
            Optional[mo.ui.slider],
            Optional[pyarrow.Table],
        ] = create_data_quality_controls(pipeline)
        return result
    except Exception:
        return None, None, None, None


def build_dq_section(
    pipeline: dlt.Pipeline,
    show_failed_filter: Optional[mo.ui.checkbox],
    table_filter: Optional[mo.ui.dropdown],
    rate_filter: Optional[mo.ui.slider],
    checks_arrow: Optional[pyarrow.Table],
) -> Tuple[List[mo.Html], Optional[mo.ui.switch]]:
    """Build the data quality widget section.

    Returns (result_widgets, raw_table_switch). raw_table_switch is None when
    there is no data or on error.
    """
    result: List[mo.Html] = []
    raw_table_switch: Optional[mo.ui.switch] = None

    try:
        from dlthub.data_quality._dashboard import data_quality_widget

        # extract values from controls
        show_failed_value = show_failed_filter.value if show_failed_filter is not None else False
        table_value = None
        if table_filter is not None and table_filter.value != "All":
            table_value = table_filter.value
        rate_value = rate_filter.value if rate_filter is not None else None

        widget_output = data_quality_widget(
            dlt_pipeline=pipeline,
            failure_rate_slider=rate_filter,
            failure_rate_filter_value=rate_value,
            show_only_failed_checkbox=show_failed_filter,
            show_only_failed_value=show_failed_value,
            table_dropdown=table_filter,
            table_name_filter_value=table_value,
            checks_arrow=checks_arrow,
        )
        if widget_output is not None:
            result.append(widget_output)

        # only show raw table switch if there is data
        if checks_arrow is not None and checks_arrow.num_rows > 0:
            raw_table_switch = mo.ui.switch(
                value=False,
                label=ui.small(strings.data_quality_show_raw_table),
            )
            result.append(mo.hstack([raw_table_switch], justify="start"))
    except ImportError:
        result.append(mo.md(strings.data_quality_not_available))
    except Exception as exc:
        result.append(
            ui.error_callout(
                strings.data_quality_error_loading.format(exc),
                traceback_string=traceback.format_exc(),
            )
        )

    return result, raw_table_switch


def build_dq_raw_table(
    pipeline: dlt.Pipeline,
    get_result: Callable[[], pyarrow.Table],
    set_result: Callable[[pyarrow.Table], None],
) -> List[mo.Html]:
    """Build the raw data quality checks table.

    Returns list of widgets to display.
    """
    result: List[mo.Html] = []

    try:
        from dlthub import data_quality as dq

        _error_message: str = None
        with mo.status.spinner(title=strings.data_quality_loading_raw_table_spinner):
            try:
                _raw_sql_query = dq.read_check(pipeline.dataset())
                _raw_query_result, _error_message, _traceback_string = queries.get_query_result(
                    pipeline, _raw_sql_query.to_sql()
                )
                set_result(_raw_query_result)
            except Exception as exc:
                _error_message = str(exc)
                _traceback_string = traceback.format_exc()

        if _error_message:
            result.append(
                ui.error_callout(
                    strings.data_quality_raw_table_error.format(_error_message),
                    traceback_string=_traceback_string,
                )
            )

        _last_result = get_result()
        if _last_result is not None:
            result.append(ui.dlt_table(_last_result, freeze_column=None))
    except ImportError:
        result.append(
            mo.callout(
                mo.md(strings.data_quality_raw_table_not_available),
                kind="warn",
            )
        )
    except Exception as exc:
        result.append(
            ui.error_callout(
                strings.data_quality_raw_table_error.format(exc),
                traceback_string=traceback.format_exc(),
            )
        )

    return result
