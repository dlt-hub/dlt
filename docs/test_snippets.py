import re
import subprocess
import sys
import contextlib
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pytest_examples import CodeExample, EvalExample, find_examples

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.utils import set_working_dir

# Don't remove these imports. They are automatically applied fixtures
from tests.utils import (  # noqa: F401
    auto_test_run_context,
    autouse_test_storage,
    preserve_environ,
    deactivate_pipeline,
)

WEBSITE_DOCS_DIR = Path("website/docs")

# TODO: fix the docs and shrink this list. Some failures are genuine docs issues that
# weren't caught by previous checks.
TYPECHECK_IGNORE = {
    # verified-sources docs consistently follow this "numbered steps" pattern
    "dlt-ecosystem/verified-sources/",
    # individual pages elsewhere with the same issue (or other, un-triaged failures)
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

TYPECHECK_PREAMBLE = Path("docs_snippets_stub.py").read_text(encoding="utf-8")
TYPECHECK_PREAMBLE_LINE_COUNT = TYPECHECK_PREAMBLE.count("\n") + 1

# (page path relative to `website/docs`, snippets share state across the page)
#
# "shared state" pages are meant to be read (and run) top to bottom - later snippets
# reuse variables an earlier snippet on the same page defines - so they run in program
# order, threading `module_globals` through. Other pages have independent snippets, each
# running with a fresh set of globals.
EXECUTE_PAGES: list[tuple[str, bool]] = [
    ("general-usage/destination.md", False),
    ("general-usage/schema.md", False),
    ("general-usage/dataset-access/dataset.md", True),
    ("reference/performance.md", False),
    ("running-in-production/running.md", False),
    ("tutorial/load-data-from-an-api.md", False),
    ("dlt-ecosystem/transformations/dbt/dbt.md", True),
    ("hub/transformations/index.md", True),
    ("walkthroughs/deploy-a-pipeline/deploy-with-modal.md", True),
]


# implicit convention: `py` fences are real, checked snippets. `python` fences are illustrative/pseudo-code
# TODO parse a `nolint` directive in the code fence (e.g., ```py nolint)
examples = [
    example for example in find_examples("website/docs") if example.prefix.split()[:1] == ["py"]
]

typecheck_examples = [
    example
    for example in examples
    if not any(ignored in str(example.path) for ignored in TYPECHECK_IGNORE)
]


def _executable_examples(page: Path) -> list[CodeExample]:
    return [
        example for example in find_examples(str(page))
        if example.prefix.split()[:1] == ["py"]
        and "execute" in example.prefix_tags()
    ]


def _patch_ty_diagnostic_location(output: str, snippet_file: Path, example: CodeExample) -> str:
    """Rewrite diagnostic location from `ty` type checker.

    The test write snippets to temporary files because `ty` doesn't accept source code
    as `stdin`. The diagnostics will point to these temporary files. This function maps
    the temporary file location to the location of the snippets in the docs.
    """

    def _rewrite_location(match: "re.Match[str]") -> str:
        line = int(match.group(1)) - TYPECHECK_PREAMBLE_LINE_COUNT
        column = match.group(2) or ""
        return f"{example.path}:{example.start_line + line}{column}"

    pattern = rf"{re.escape(str(snippet_file))}:(\d+)(:\d+)?"
    return re.sub(pattern, _rewrite_location, output)


@contextlib.contextmanager
def _providers_for_page(page_dir: Path):
    """Loads secrets from `website/docs/.dlt` and config from the page's own `.dlt/`, and
    changes the working directory to the page's folder for the duration of the test (e.g.
    the dbt runner snippet expects `profiles.yml` next to `dbt.md`).
    """
    secret_dir = str(WEBSITE_DOCS_DIR / ".dlt")
    config_dir = str(page_dir / ".dlt")

    def _initial_providers(self: Any) -> list[Any]:
        return [
            EnvironProvider(),
            SecretsTomlProvider(settings_dir=secret_dir),
            ConfigTomlProvider(settings_dir=config_dir),
        ]

    with (
        set_working_dir(str(page_dir)),
        patch(
            "dlt.common.runtime.run_context.RunContext.initial_providers",
            _initial_providers,
        ),
    ):
        Container()[PluggableRunContext].reload_providers()
        try:
            sys.path.insert(0, str(page_dir))
            yield
        finally:
            sys.path.remove(str(page_dir))


@pytest.mark.parametrize("example", examples, ids=str)
def test_lint_snippets(example: CodeExample, eval_example: EvalExample):
    """Lint snippets"""
    if eval_example.update_examples:
        eval_example.format_ruff(example)
    else:
        eval_example.lint_ruff(example)


@pytest.mark.parametrize("example", typecheck_examples, ids=str)
def test_typecheck_snippets(example: CodeExample, tmp_path: Path):
    """Type check snippets"""
    snippet_file = tmp_path / f"{example.module_name}.py"
    content = TYPECHECK_PREAMBLE + "\n" + example.source
    snippet_file.write_text(content, encoding="utf-8")

    result = subprocess.run(
        ["ty", "check", "--color=always", str(snippet_file)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = _patch_ty_diagnostic_location(
            result.stdout + result.stderr, snippet_file, example
        )
        pytest.fail(output, pytrace=False)


@pytest.mark.parametrize(
    "page,shared_state",
    EXECUTE_PAGES,
    ids=[page for page, _ in EXECUTE_PAGES]
)
def test_run_snippets(
    page: str,
    shared_state: bool,
    eval_example: EvalExample,
) -> None:
    """Run snippets

    If shared_state is True, share the state between individual snippets
    of the same docs page.
    """
    page_path = WEBSITE_DOCS_DIR / page
    examples = _executable_examples(page_path)

    with _providers_for_page(page_path.parent):
        module_globals: dict[str, Any] = {}
        for example in examples:
            if shared_state:
                if eval_example.update_examples:
                    module_globals = eval_example.run_print_update(
                        example, module_globals=module_globals
                    )
                else:
                    module_globals = eval_example.run_print_check(
                        example, module_globals=module_globals
                    )
            else:
                if eval_example.update_examples:
                    eval_example.run_print_update(example, module_globals={})
                else:
                    eval_example.run_print_check(example, module_globals={})
