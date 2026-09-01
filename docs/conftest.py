import pathlib

import pytest
from pytest_examples import CodeExample, find_examples


WEBSITE_DOCS_DIR = pathlib.Path("website/docs").absolute()
EXAMPLES_DIR = (WEBSITE_DOCS_DIR / "examples").absolute()

REQUIRES_SHARED_STATE = {
    "general-usage/dataset-access/dataset.md",
    "dlt-ecosystem/transformations/dbt/dbt.md",
    "hub/transformations/index.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-modal.md",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--pages",
        nargs="*",
        default=None,
        type=pathlib.Path,
        help=(
            "Select specific docs pages to check. Accepts multiple values, which allows"
            " pre-commit hooks to append all the changed files after `--page`."
        ),
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    page_paths = metafunc.config.getoption("pages")
    if page_paths:
        examples = [example for page_path in page_paths for example in find_examples(page_path)]
    else:
        examples = list(find_examples(WEBSITE_DOCS_DIR))

    lint_params = []
    typecheck_params = []
    for example in examples:
        # implicitly, snippets with `python` are pseudo code
        if not example.prefix.split()[:1] == ["py"]:
            continue

        marks = [pytest.mark.example] if example.path.is_relative_to(EXAMPLES_DIR) else []
        lint_params.append(pytest.param(example, marks=marks))
        if "notype" in example.prefix_tags():
            typecheck_marks = marks + [pytest.mark.skip("Found `notype` directive.")]
        else:
            typecheck_marks = marks

        typecheck_params.append(pytest.param(example, marks=typecheck_marks))

    # execution needs to be grouped per page
    run_examples: dict[pathlib.Path, list[CodeExample]] = {}
    for example in examples:
        if not example.prefix.split()[:1] == ["py"]:
            continue

        # later we check that a page containing at least one `noexecute` directive gets skipped
        if (
            "execute" in example.prefix_tags()
            or "noexecute" in example.prefix_tags()
        ):
            run_examples.setdefault(example.path, []).append(example)

    run_params = []
    for path, examples in run_examples.items():
        marks = []
        if path.is_relative_to(EXAMPLES_DIR):
            marks.append(pytest.mark.example)

        if any("noexecute" in example.prefix_tags() for example in examples):
            marks.append(pytest.mark.skip("Found a `noexecute` directive in one of the snippet of this page."))

        param = pytest.param(
            path,
            examples,
            str(path.relative_to(WEBSITE_DOCS_DIR)) in REQUIRES_SHARED_STATE,
            marks=marks,
            id=str(path),
        )
        run_params.append(param)

    if metafunc.definition.get_closest_marker("lint_snippets"):
        metafunc.parametrize("example", lint_params, ids=str)

    elif metafunc.definition.get_closest_marker("typecheck_snippets"):
        metafunc.parametrize("example", typecheck_params, ids=str)

    elif metafunc.definition.get_closest_marker("run_snippets"):
        metafunc.parametrize(
            ("page_relative_path", "page_examples", "shared_state"), run_params
        )
