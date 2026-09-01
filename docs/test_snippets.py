import re
import subprocess
import sys
import contextlib
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pytest_examples import CodeExample, EvalExample

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

TYPECHECK_PREAMBLE = Path("docs_snippets_stub.py").read_text(encoding="utf-8")
TYPECHECK_PREAMBLE_LINE_COUNT = TYPECHECK_PREAMBLE.count("\n") + 1


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
        set_working_dir(str(page_dir.resolve())),
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


@pytest.mark.lint_snippets
def test_lint_snippets(example: CodeExample, eval_example: EvalExample):
    """Lint snippets"""
    # pytest-examples overrides the value set in `pyproject.toml`
    eval_example.set_config(target_version="py310")
    if eval_example.update_examples:
        eval_example.format_ruff(example)
    else:
        eval_example.lint_ruff(example)


@pytest.mark.typecheck_snippets
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


@pytest.mark.run_snippets
def test_run_snippets(
    page_relative_path: str,
    page_examples: list[CodeExample],
    shared_state: bool,
    eval_example: EvalExample,
) -> None:
    """Run docs snippets and standalone examples.

    Snippets (`snippets` marker) and examples (`examples` marker) share the same
    execution logic and can be selected independently, e.g. `pytest -m examples`.

    If shared_state is True, share the state between individual snippets
    of the same docs page.
    """
    page_path = WEBSITE_DOCS_DIR / page_relative_path

    module_globals: dict[str, Any] = {}
    with _providers_for_page(page_path.parent):
        for example in page_examples:
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
