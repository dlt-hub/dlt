import argparse
import os
import sys
from typing import Any, Callable, Dict, Iterator, Protocol, Sequence, Tuple
import pytest

from dlt.common import known_env
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.runtime.run_context import RunContext

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli._dlt import _create_parser, main
from dlt._workspace.cli.compose import ComposedExecutable
from dlt._workspace.cli.dlthub import utils as dlthub_utils_mod
from dlt._workspace.deployment import _run_helpers as run_helpers_mod

from tests.utils import get_test_storage_root, unload_modules_at_path
from tests.workspace.utils import isolated_workspace

TParseDlthub = Callable[[Sequence[str]], Tuple[argparse.Namespace, Dict[str, ComposedExecutable]]]
TMainDlthub = Callable[..., Tuple[int, str]]


class TRunDlthub(Protocol):
    def __call__(self, *argv: str, answer: bool = True) -> str: ...


@pytest.fixture(autouse=True)
def reset_echo_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resets echo interactivity globals before each test in tests/workspace/cli."""
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_DEFAULT", False)
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_VALUE", None)
    monkeypatch.setattr(fmt, "ALWAYS_CONFIRM", False)


@pytest.fixture(autouse=True)
def auto_isolated_workspace(
    autouse_test_storage, preserve_run_context
) -> Iterator[WorkspaceRunContext]:
    """Creates new isolated `empty` workspace in `_storage` (top level) folder. Makes sure that _storage
    folder is cleaned first and that previous run context (be it workspace, oss or project) is restored
    after test executes.
    """
    # activate `dev` profile which is a default. many tests use run pipelines in remote processes
    # and this profile will be assumed automatically
    with isolated_workspace("empty", profile="dev") as ctx:
        yield ctx


@pytest.fixture
def legacy_workspace_context(monkeypatch: pytest.MonkeyPatch) -> Iterator[RunContext]:
    """Nests a legacy (non-workspace) `RunContext` inside `auto_isolated_workspace`."""
    # with an active workspace the `dlt` host falls back to `dlthub`'s slim command set; the
    # legacy context restores the full legacy `dlt init`/`deploy`/`pipeline` command set
    with isolated_workspace("legacy", required="RunContext") as ctx:
        # pass working dir to subprocesses
        monkeypatch.setenv(known_env.DLT_DATA_DIR, os.path.join(ctx.run_dir, ".dlt"))
        yield ctx  # type: ignore[misc]


@pytest.fixture
def parse_dlthub(monkeypatch: pytest.MonkeyPatch) -> TParseDlthub:
    """Parses `dlthub` arguments the way `main` does. Returns them with the installed commands."""

    def _parse(argv: Sequence[str]) -> Tuple[argparse.Namespace, Dict[str, ComposedExecutable]]:
        monkeypatch.setattr(sys, "argv", ["dlthub", *argv])
        parser, pre_parser, installed = _create_parser("dlthub")
        # the pre-parser takes the global options from any position
        ns, remaining = pre_parser.parse_known_args(list(argv))
        return parser.parse_args(remaining, namespace=ns), installed

    return _parse


@pytest.fixture
def run_dlthub(parse_dlthub: TParseDlthub, capsys: pytest.CaptureFixture[str]) -> TRunDlthub:
    """Executes a `dlthub` command with `answer` given to its confirmation. Returns its output."""

    def _run(*argv: str, answer: bool = True) -> str:
        args, installed = parse_dlthub(argv)
        with fmt.always_choose(False, answer):
            # not via `main`: the exception of a refused command reaches the test
            installed[args.command].execute(args)
        return capsys.readouterr().out

    return _run


@pytest.fixture
def main_dlthub(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> TMainDlthub:
    """Runs `dlthub` with `argv` through `main` as a new process does. Returns the exit code and
    stderr."""

    def _main(*argv: str) -> Tuple[int, str]:
        # a new process creates the run context from the working dir
        if PluggableRunContext in Container():
            del Container()[PluggableRunContext]
        monkeypatch.setattr(sys, "argv", ["dlthub", *argv])
        try:
            code = main("dlthub")
        except SystemExit as ex:
            # argparse exits on a usage error
            code = int(ex.code or 0)
        return code, capsys.readouterr().err

    return _main


@pytest.fixture(autouse=True)
def isolated_manifest_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force `manifest_from_module` to run in a subprocess so tests don't accumulate
    sys.modules / sys.path / FileFinder cache entries across workspace rmtree+recreate.
    """
    original = run_helpers_mod.manifest_from_module

    def _isolated(name_or_path: str, environment: Any, use_all: bool = True) -> Any:
        return original(name_or_path, environment, use_all=use_all, isolated=True)

    monkeypatch.setattr(run_helpers_mod, "manifest_from_module", _isolated)
    monkeypatch.setattr(dlthub_utils_mod, "manifest_from_module", _isolated)


@pytest.fixture(autouse=True)
def auto_unload_init_modules() -> Iterator[None]:
    """Unloads modules that were imported wfrom temp storage"""
    try:
        yield
    finally:
        unload_modules_at_path(get_test_storage_root())
