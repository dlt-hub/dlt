"""Dashboard UI helpers: tables, callouts, section boilerplate, and shared utilities."""

from typing import Any, Dict, List, Optional, Tuple, Union


class BoundedDict(Dict[Any, Any]):
    """Dict that evicts the oldest entry when *maxsize* is exceeded."""

    def __init__(self, maxsize: int = 64) -> None:
        super().__init__()
        self._maxsize = maxsize

    def __setitem__(self, key: Any, value: Any) -> None:
        if key not in self and len(self) >= self._maxsize:
            del self[next(iter(self))]
        super().__setitem__(key, value)


import marimo as mo
import pyarrow
import traceback


import dlt

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.strings import TSectionStrings
from dlt._workspace.helpers.dashboard.utils.formatters import align_dict_keys


def _style_cell(row_id: str, name: str, __: Any) -> Dict[str, str]:
    """Apply alternating row colors and bold the name column."""
    style: Dict[str, str] = {"background-color": "white" if (int(row_id) % 2 == 0) else "#f4f4f9"}
    if name.lower() == "name":
        style["font-weight"] = "bold"
    return style


def dlt_table(
    data: Union[List[Dict[str, Any]], pyarrow.Table],
    *,
    selection: Optional[str] = None,
    style: bool = True,
    freeze_column: Optional[str] = "name",
    initial_selection: Optional[List[int]] = None,
    pagination: Optional[bool] = None,
    show_download: Optional[bool] = None,
    **kwargs: Any,
) -> mo.ui.table:
    """Create a styled mo.ui.table with common dashboard defaults.

    Applies alternating-row styling, freezes the given column, aligns dict keys,
    and sets initial_selection to None when data is empty.

    Args:
        data: Table data as a list of dicts or a pyarrow Table.
        selection: Selection mode ("single", "multi") or None to disable.
        style: Whether to apply alternating-row cell styling.
        freeze_column: Column name to freeze on the left. Pass None to disable.
        initial_selection: Row indices to pre-select. Forced to None when data is empty.
        pagination: Passed through to mo.ui.table.
        show_download: Passed through to mo.ui.table.
    """
    is_list = isinstance(data, list)
    has_data = (len(data) > 0) if is_list else (data.num_rows > 0)  # type: ignore[union-attr]

    # align dict keys for consistent column widths
    if is_list and has_data:
        data = align_dict_keys(data)

    # build kwargs for mo.ui.table
    table_kwargs: Dict[str, Any] = {"selection": selection, **kwargs}

    if style:
        table_kwargs["style_cell"] = _style_cell

    if freeze_column is not None and has_data and freeze_column in data[0]:
        table_kwargs["freeze_columns_left"] = [freeze_column]

    if initial_selection is not None and has_data:
        table_kwargs["initial_selection"] = initial_selection

    if pagination is not None:
        table_kwargs["pagination"] = pagination

    if show_download is not None:
        table_kwargs["show_download"] = show_download

    return mo.ui.table(data, **table_kwargs)


def small(text: str) -> str:
    """Wrap text in <small> HTML tags for consistent dashboard styling."""
    return f"<small>{text}</small>"


def error_callout(message: str, code: str = None, traceback_string: str = None) -> mo.Html:
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
                    strings.error_show_stacktrace: mo.ui.code_editor(
                        traceback_string, language="python", disabled=True, show_copy_button=True
                    )
                }
            )
        )

    return mo.callout(
        mo.vstack(stack_items),
        kind="warn",
    )


def title_and_subtitle(title: str, subtitle: str = None, title_level: int = 3) -> mo.Html:
    """Build a title and a subtitle block"""
    _result = []
    if title:
        _result.append(mo.md(f"{title_level * '#'} {title}"))
    if subtitle:
        _result.append(mo.md(small(subtitle)))
    return mo.vstack(_result)


def page_header(
    dlt_pipeline: dlt.Pipeline,
    section_strings: TSectionStrings,
    button: mo.ui.switch = None,
) -> List[mo.Html]:
    """Build a page header with a title, a subtitle, button and conditional longer subtitle.

    When collapsed, the short subtitle is shown inline after the title to save
    vertical space.  When expanded, the longer subtitle appears on its own line.
    """
    if not dlt_pipeline:
        return []
    if button.value:
        title_block = title_and_subtitle(
            section_strings.title, section_strings.subtitle_long, title_level=2
        )
        header = mo.hstack([title_block, button], align="center")
    else:
        # title left, subtitle + switch right-aligned
        right = mo.hstack([mo.md(small(section_strings.subtitle)), button], align="center", gap=1)
        header = mo.hstack(
            [mo.md(f"## {section_strings.title}"), right],
            align="center",
            justify="space-between",
        )
    return [mo.Html(f'<div class="section-header">{header.text}</div>')]


def section_marker(section_name: str, has_content: bool = False) -> mo.Html:
    """Create an invisible marker element to identify sections for CSS styling.

    Args:
        section_name: Name identifier for the section (e.g., "home_section", "schema_section")
        has_content: If True, adds 'has-content' class to enable CSS styling (borders, backgrounds).
                     Should be True only when the section has actual content and is displayed.

    Returns:
        Hidden HTML div element with section marker classes for CSS targeting.
    """
    content_class = "has-content" if has_content else ""
    return mo.Html(
        f'<div class="section-marker {content_class}" data-section="{section_name}"'
        ' hidden="hidden"></div>'
    )


def section(
    section_strings: TSectionStrings,
    dlt_pipeline: dlt.Pipeline,
    switch: mo.ui.switch,
) -> Tuple[List[mo.Html], bool]:
    """Build standard section boilerplate: marker + page header.

    Returns:
        Tuple of (result list, should_render_content). The list already
        contains the section marker and header.  Append content only when
        should_render_content is True, then call ``mo.vstack(result)``.
    """
    result = [
        section_marker(section_strings.section_name, has_content=dlt_pipeline is not None),
    ]
    result.extend(page_header(dlt_pipeline, section_strings, switch))
    return result, bool(dlt_pipeline and switch.value)
