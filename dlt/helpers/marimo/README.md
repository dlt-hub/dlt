# How to add a marimo widget

This guide shows how to add the hypothetical `pipeline_browser` widget.
A marimo widget **is** a marimo notebook. We'll call this the *widget notebook*. 

## 1. Create a new marimo notebook under `dlt/helpers`

Create the notebook with 

```shell
marimo edit dlt/helpers/_pipeline_browser.py
``` 

The widget notebook file name is prefixed with `_` for convention 
and to avoid name collisions.

## 2. Edit your widget notebook

Edit the `_pipeline_browser.py` notebook via the marimo GUI until you're satisfied. 

Tips:
- Use a [setup cell](https://docs.marimo.io/guides/reusing_functions/#1-create-a-setup-cell) for your import and constants.
- Set a cell names to be able to [run cells directly](https://docs.marimo.io/api/cell/) in a unit test. This is done by setting a function name instead of the anonymous `_`
- If you want your widget to take input arguments, define these variables inside the setup
cell and set them to `None`. In downstream cells, you can use `mo.stop(VAR is None)` to hide
cells if the argument is missing

The next snippet shows the setup cell of a widget that takes the argument `pipeline_name`.
The cell that depends on it to instantiate a pipeline via `dlt.attach(pipeline_name)` starts
with `mo.stop(pipeline_name is None)`

```python
# dlt/helpers/marimo/_pipeline_browser.py

with app.setup:
    from typing import Any, cast
    from itertools import chain

    import marimo as mo

    import dlt
    from dlt.common.utils import without_none

    pipeline_name = None


@app.cell
def _():
    mo.stop(pipeline_name is None)
    pipeline = dlt.attach(pipeline_name)
    return (pipeline,)

# ...
```

## 3. Register the widget

To make the widget publicly available via `dlt`, you need to modify `dlt/helpers/marimo/__init__.py`.

The following is required:
1. import the `app` variable from the widget notebook `.py` file and give it an alias
2. add this alias to the `__all__` clause
3. create a function that instantiates the widget from the `app` argument
   and has the input parameters
4. add an `if/else` condition inside `render()` to render the widget

The following snippet shows what's required to render the `_pipeline_browser.py` widget

```python
import marimo
import mowidgets

from dlt.helpers.marimo._pipeline_browser import app as pipeline_browser

# pre-existing function
def render(app: marimo.App, *args, **kwargs):
    if not isinstance(app, marimo.App):
        raise ValueError("app must be an instance of marimo.App")

    if ...:
        ...
    # add new condition
    elif app is pipeline_browser:
        return pipeline_browser_widget(app, *args, **kwargs)
    else:
        raise ValueError("app must be either load_package_viewer or schema_viewer")

# ...

def pipeline_browser_widget(app, pipeline_name, *args, **kwargs):
    return mowidgets.widgetize(
        app,
        data_access=True,
        # this must match the input name in the notebook
        inputs={"pipeline_name": pipeline_name}
    )

__all__ = (
    "render",
    ...,
    # add the aliased `app` variable; not the newly created `_widget` function
    "pipeline_browser",
)
```

# 4. Trying the widget

Open a new marimo notebook with `marimo edit dev.py`. To use the widget, import
`render` and the app object from the `dlt.helpers.marimo`. Calling `render(app)`
will return a widget object. Calling `await` on it will render it.

Example snippet

```python
# dev.py
@app.cell
def _():
    import marimo as mo
    from dlt.helpers.marimo import render, pipeline_browser
    return pipeline_browser, render


@app.cell
async def _(pipeline_browser, render):
    # to display directly
    await render(pipeline_browser)
    return

# call `await` on `render()` to display the widget directly
@app.cell
async def _(pipeline_browser, render):
    await render(pipeline_browser)
    return

# or assign a variable to display elsewhere and access the widget's data
@app.cell
async def _(pipeline_browser, render):
    w = render(pipeline_browser)
    await w
    return (w,)


@app.cell
def _(w):
    w.data
    return
```

Alternatively, you can skip the render function and call the `_widget()` function
on the app object directly

```python
@app.cell
def _():
    import marimo as mo
    from dlt.helpers.marimo import pipeline_browser_widget, pipeline_browser
    return pipeline_browser, pipeline_browser_widget


@app.cell
async def _(pipeline_browser, pipeline_browser_widget):
    # to display directly
    await pipeline_browser_widget(pipeline_browser)
    return
```



## 5. unit tests

Add tests for cells with complex operations. You can retrieve a cell by name and give it inputs.

Source code for the widget
```python
# dlt/helpers/marimo/_pipeline_browser.py
@app.cell
def selector(base_path):
    pipelines = [p.name for p in pathlib.Path(base_path).expanduser().iterdir()]
    select_pipeline = mo.ui.dropdown(pipelines, value="pokemon")
    select_pipeline
    return (select_pipeline,)
```

Unit test
```python
# tests/helpers/marimo/test_pipeline_browser.py
# import the named cell directly from the widget notebook
from dlt.helpers.marimo._pipeline_browser import selector

def test_cell_selector():
    base_path = ...

    # outputs is the HTML to be displayed
    # definitions is a dictionary of values returned
    outputs, definitions = selector.run(base_path=base_path)

    assert definitions["select_pipeline"] == ... 
```

## 6. Integration tests
You can run the full widget notebook to see if it can run "end-to-end"
for a set of definitions. These input definitions can override nodes of the
DAG, which allows to test various states of the application.

```python
# tests/helpers/marimo/test_pipeline_browser.py
import pytest
# import the aliased app
from dlt.helpers.marimo import pipeline_browser

@pytest.mark.parametrize("input1", [...])
@pytest.mark.parametrize("input2", [...])
def test_pipeline_browser(input1, input2) -> None:
    input_definitions = {
        "input1": input1,
        "input2": input2,
    }
    outputs, definitions = pipeline_browser.run(defs=input_definitions)

    assert ...
```
