"""
Lints (and, with `--update-examples`, formats/fixes) Python code embedded in the docs
markdown files, using `pytest_examples` + `ruff`. Also type checks each snippet in
isolation with `ty` (https://github.com/astral-sh/ty).
"""
import re
import subprocess
from pathlib import Path

import pytest
from pytest_examples import CodeExample, EvalExample, find_examples

examples = [example for example in find_examples("website/docs") if example.prefix == "py"]

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
    "release-notes/1.12.1.md",
    "release-notes/1.17.md",
    "release-notes/1.18.md",
    "release-notes/1.19.md",
    "running-in-production/running.md",
    "tutorial/filesystem.md",
    "tutorial/load-data-from-an-api.md",
    "tutorial/rest-api.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-dagster.md",
    "walkthroughs/deploy-a-pipeline/deploy-with-prefect.md",
    "walkthroughs/deploy-a-pipeline/orchestrate-with-dlthub.md",
}

typecheck_examples = [
    example
    for example in examples
    if not any(ignored in str(example.path) for ignored in TYPECHECK_IGNORE)
]

TYPECHECK_PREAMBLE = Path("docs_snippets_stub.py").read_text(encoding="utf-8")
TYPECHECK_PREAMBLE_LINES = TYPECHECK_PREAMBLE.count("\n") + 1  # +1 for the blank line joiner


@pytest.mark.parametrize("example", examples, ids=str)
def test_lint(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format_ruff(example)
    else:
        eval_example.lint_ruff(example)


def _relocate_diagnostics(output: str, snippet_file: Path, example: CodeExample) -> str:
    """Rewrite diagnostic location"""

    def _rewrite_location(match: "re.Match[str]") -> str:
        line = int(match.group(1)) - TYPECHECK_PREAMBLE_LINES
        column = match.group(2) or ""
        return f"{example.path}:{example.start_line + line}{column}"

    return re.sub(
        rf"{re.escape(str(snippet_file))}:(\d+)(:\d+)?",
        _rewrite_location,
        output,
    )


@pytest.mark.parametrize("example", typecheck_examples, ids=str)
def test_typecheck(example: CodeExample, tmp_path: Path):
    snippet_file = tmp_path / f"{example.module_name}.py"
    snippet_file.write_text(TYPECHECK_PREAMBLE + "\n" + example.source, encoding="utf-8")

    result = subprocess.run(
        ["ty", "check", "--color=always", str(snippet_file)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = _relocate_diagnostics(result.stdout + result.stderr, snippet_file, example)
        pytest.fail(output, pytrace=False)
