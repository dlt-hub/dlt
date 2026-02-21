import marimo as mo
import dlt


from dlt._workspace.helpers.dashboard.utils.ui import (
    title_and_subtitle,
    page_header,
    error_callout,
    small,
    section_marker,
    section,
    dlt_table,
)


def test_title_and_subtitle():
    title = "Test Title"
    subtitle = "Test Subtitle"
    html = title_and_subtitle(title, subtitle).text
    assert title in html
    assert subtitle in html


def test_page_header(success_pipeline_duckdb: dlt.Pipeline):
    title = "Test Title"
    subtitle = "Test Subtitle"
    subtitle_long = "Test Alternative Subtitle Long"

    # not opened
    html = page_header(success_pipeline_duckdb, title, subtitle, subtitle_long, mo.ui.button())[
        0
    ].text
    assert title in html
    assert subtitle in html
    assert subtitle_long not in html

    # opened
    html = page_header(
        success_pipeline_duckdb, title, subtitle, subtitle_long, mo.ui.button(value=True)
    )[0].text
    assert title in html
    assert subtitle not in html
    assert subtitle_long in html


def test_error_callout():
    message = "Test Message"
    code = "Test Code"
    traceback_string = "Test Traceback String"
    html = error_callout(message, code, traceback_string).text
    assert message in html
    assert code in html
    assert traceback_string in html


def test_small():
    assert small("hello") == "<small>hello</small>"
    assert small("") == "<small></small>"
    assert small("a <b>bold</b> word") == "<small>a <b>bold</b> word</small>"


def test_section_marker():
    marker = section_marker("test_section")
    assert 'data-section="test_section"' in marker.text
    assert "has-content" not in marker.text
    assert "hidden" in marker.text

    marker_with_content = section_marker("test_section", has_content=True)
    assert "has-content" in marker_with_content.text


def test_section(success_pipeline_duckdb: dlt.Pipeline):
    result, show = section(
        "test_section",
        success_pipeline_duckdb,
        "Title",
        "Subtitle",
        "Long subtitle",
        mo.ui.switch(value=True),
    )
    assert show is True
    assert len(result) >= 1

    # switch off: should not show content
    result, show = section(
        "test_section",
        success_pipeline_duckdb,
        "Title",
        "Subtitle",
        "Long subtitle",
        mo.ui.switch(value=False),
    )
    assert show is False

    # no pipeline: should not show content
    result, show = section(
        "test_section",
        None,
        "Title",
        "Subtitle",
        "Long subtitle",
        mo.ui.switch(value=True),
    )
    assert show is False


def test_dlt_table_with_list():
    data = [{"name": "a", "value": 1}, {"name": "b", "value": 2}]
    table = dlt_table(data)
    assert table.text is not None


def test_dlt_table_with_empty_list():
    table = dlt_table([])
    assert table.text is not None


def test_dlt_table_with_selection():
    data = [{"name": "a"}, {"name": "b"}]
    table = dlt_table(data, selection="single", initial_selection=[0])
    assert table.text is not None


def test_dlt_table_no_freeze():
    data = [{"col1": "a", "col2": "b"}]
    table = dlt_table(data, freeze_column=None)
    assert table.text is not None
