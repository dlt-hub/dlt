import marimo as mo
import dlt


from dlt._workspace.helpers.dashboard.ui_elements import (
    build_title_and_subtitle,
    build_page_header,
    build_error_callout,
)


def test_build_title_and_subtitle():
    title = "Test Title"
    subtitle = "Test Subtitle"
    html = build_title_and_subtitle(title, subtitle).text
    assert title in html
    assert subtitle in html


def test_build_page_header(success_pipeline_duckdb: dlt.Pipeline):
    title = "Test Title"
    subtitle = "Test Subtitle"
    subtitle_long = "Test Alternative Subtitle Long"

    # not opened
    html = build_page_header(
        success_pipeline_duckdb, title, subtitle, subtitle_long, mo.ui.button()
    )[0].text
    assert title in html
    assert subtitle in html
    assert subtitle_long not in html

    # opened
    html = build_page_header(
        success_pipeline_duckdb, title, subtitle, subtitle_long, mo.ui.button(value=True)
    )[0].text
    assert title in html
    assert subtitle not in html
    assert subtitle_long in html


def test_build_error_callout():
    message = "Test Message"
    code = "Test Code"
    traceback_string = "Test Traceback String"
    html = build_error_callout(message, code, traceback_string).text
    assert message in html
    assert code in html
    assert traceback_string in html
