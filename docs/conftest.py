import pathlib

import pytest
from pytest_examples import CodeExample, find_examples


WEBSITE_DOCS_DIR = pathlib.Path("website/docs").absolute()
EXAMPLES_DIR = (WEBSITE_DOCS_DIR / "examples").absolute()
# TODO: fix the docs and shrink this list. Some failures are genuine docs issues that
# weren't caught by previous checks.
TYPECHECK_IGNORE = {
    "dlt-ecosystem/verified-sources/",
    "dlt-ecosystem/destinations/clickhouse.md",
    "dlt-ecosystem/destinations/databricks.md",
    "dlt-ecosystem/destinations/destination.md",
    "dlt-ecosystem/destinations/ducklake.md",
    "dlt-ecosystem/destinations/fabric.md",
    "dlt-ecosystem/destinations/iceberg.md",
    "dlt-ecosystem/destinations/lance.md",
    "dlt-ecosystem/destinations/lancedb.md",
    "dlt-ecosystem/destinations/mssql.md",
    "dlt-ecosystem/destinations/qdrant.md",
    "dlt-ecosystem/destinations/sqlalchemy.md",
    "dlt-ecosystem/destinations/synapse.md",
    "dlt-ecosystem/destinations/weaviate.md",
    "dlt-ecosystem/transformations/add-map.md",
    "dlt-ecosystem/transformations/dbt/dbt.md",
    "dlt-ecosystem/transformations/python.md",
    "dlt-ecosystem/transformations/sql.md",
    "general-usage/credentials/advanced.md",
    "general-usage/credentials/complex_types.md",
    "general-usage/credentials/setup.md",
    "general-usage/customising-pipelines/removing_columns.md",
    "general-usage/data-enrichments/currency_conversion_data_enrichment.md",
    "general-usage/data-enrichments/url-parser-data-enrichment.md",
    "general-usage/data-enrichments/user_agent_device_data_enrichment.md",
    "general-usage/dataset-access/dataset.md",
    "general-usage/destination.md",
    "general-usage/incremental/advanced-state.md",
    "general-usage/incremental/cursor.md",
    "general-usage/merge-loading.md",
    "general-usage/resource.md",
    "general-usage/schema-contracts.md",
    "general-usage/schema.md",
    "general-usage/source.md",
    "general-usage/state.md",
    "hub/data-quality/index.md",
    "hub/ingestion/dashboard.md",
    "hub/ingestion/ms-sql.md",
    "hub/pipeline-operations/deployments.md",
    "hub/pipeline-operations/job-configuration.md",
    "hub/pipeline-operations/secrets-management.md",
    "hub/pipeline-operations/triggers.md",
    "hub/transformations/explore-and-transform.md",
    "hub/transformations/index.md",
    "reference/performance.md",
    "release-notes/1.12.1.md",
    "release-notes/1.17.md",
    "release-notes/1.18.md",
    "release-notes/1.19.md",
    "running-in-production/running.md",
    "tutorial/filesystem.md",
    "tutorial/load-data-from-an-api.md",
    "tutorial/rest-api.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-modal.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-dagster.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-prefect.md",
    "walkthroughs/deploy-a-pipeline/orchestrate-with-dlthub.md",
}

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
        # TODO explicitly set `nolint` on individual snippets
        if any(ignored in str(example.path) for ignored in TYPECHECK_IGNORE):
            typecheck_marks = marks + [pytest.mark.skip("File is ignored.")]
        elif "notype" in example.prefix_tags():
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
            path, examples, str(path) in REQUIRES_SHARED_STATE,
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
