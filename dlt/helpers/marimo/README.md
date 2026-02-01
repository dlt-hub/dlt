# How to add a marimo widget

This guide shows how to add the hypothetical `pipeline_browser` widget.

## 1. Create a new widget notebook under `dlt/helpers/marimo/_widgets`

A marimo widget **is** a marimo notebook. We'll call this the *widget notebook*. 
Create the marimo notebook with: `marimo edit dlt/helpers/marimo/_widgets/_pipeline_browser.py`.
The widget notebook file name is prefixed with `_` to avoid name collisions.

## 2. edit your widget notebook

Edit the `_pipeline_browser.py` notebook until you're satisfied. Use a [setup cell](https://docs.marimo.io/guides/reusing_functions/#1-create-a-setup-cell) for your import and constants. Set a unique name to be able to [run cells directly](https://docs.marimo.io/api/cell/) in a unit test.


## 3. "register" the widget

To make the widget available, you need to modify `dlt/helpers/marimo/_widgets/__init__.py` and `dlt/helpers/marimo/__init__.py`

```python
# `dlt/helpers/marimo/_widgets/__init__.py`
# import the `app` variable from the notebook file
# the alias for `app` will be the widget name imported by users
from dlt.helpers.marimo._widgets._pipeline_browser import app as pipeline_browser
```

```python
# `dlt/helpers/marimo/__init__.py`
# import the `pipeline_browser` (the aliased `app` variable from the notebook)
# and add it to the `__all__` public interface
from dlt.helpers.marimo._widgets import render, pipeline_browser

__all__ = (
    "render",
    "pipeline_browser",
)
```

## 4. unit tests

Add tests for cells with complex operations. You can retrieve a cell by name and give it inputs.

Source code for the widget
```python
# dlt/helpers/marimo/_widgets/_pipeline_browser.py
@app.cell
def pipeline_selector(base_path):
    pipelines = [p.name for p in pathlib.Path(base_path).expanduser().iterdir()]
    select_pipeline = mo.ui.dropdown(pipelines, value="pokemon")
    select_pipeline
    return (select_pipeline,)
```

Unit test
```python
# tests/helpers/marimo/test_pipeline_browser.py
# import the named cell directly from the widget notebook
from dlt.helpers.marimo._widgets._pipeline_browser import pipeline_selector

def test_pipeline_selector():
    base_path = ...

    # output is the HTML to be displayed
    # definitions is a dictionary of values returned
    output, definitions = pipeline_selector.run(base_path=base_path)

    assert definitions["select_pipeline"] == ... 
```

