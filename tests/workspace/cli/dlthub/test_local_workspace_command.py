"""Tests for `dlt workspace info` — fetch_deployment_info + view."""

import os
from functools import partial
from pathlib import Path
from typing import Any, Dict, List

import pytest

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import switch_context
from dlt._workspace.cli.dlthub._local_workspace_command import _print_deployment_info
from dlt._workspace.cli.dlthub.typing import TDeploymentManifestInfo
from dlt._workspace.cli.dlthub.utils import fetch_deployment_info
from dlt._workspace.environment import select_environment
from dlt._workspace.exceptions import ProfileNotDeclared
from dlt._workspace.settings_files import get_profile_pin_file, read_profile_pin
from dlt._workspace.workspace_definition import default_profile

from tests.workspace.cli.conftest import TParseDlthub, TRunDlthub
from tests.workspace.utils import declare_environment, isolated_workspace


def test_fetch_deployment_info_not_found(auto_isolated_workspace: Any) -> None:
    info = fetch_deployment_info()
    assert info["status"] == "not_found"


def test_fetch_deployment_info_generation_failed(auto_isolated_workspace: Any) -> None:
    ws_dir = auto_isolated_workspace.run_dir
    (Path(ws_dir) / "__deployment__.py").write_text("import definitely_not_installed_xyz\n")
    info = fetch_deployment_info()
    assert info["status"] == "generation_failed"
    assert "error" in info
    assert "definitely_not_installed_xyz" in info["error"]


def test_fetch_deployment_info_ok_categorizes_and_lists_triggers() -> None:
    # workspace case at tests/workspace/cases/workspaces/deployment_mixed/ has:
    #  - @pipeline_run load_fruitshop → category=pipeline (via deliver.pipeline_name)
    #  - @job batch_one → category=batch (no expose/deliver)
    #  - plain.py → category=batch (module-level job, no expose/deliver)
    with isolated_workspace("deployment_mixed", profile="dev"):
        info = fetch_deployment_info()

    assert info["status"] == "ok"
    assert info["total_jobs"] >= 3

    counts = info["counts_by_category"]
    assert counts.get("pipeline", 0) >= 1
    assert counts.get("batch", 0) >= 2

    by_ref = {j["job_ref"]: j for j in info["jobs"]}
    # @job-decorated jobs get section from module name → jobs.__deployment__.<name>
    batch_one = by_ref["jobs.__deployment__.batch_one"]
    assert batch_one["default_trigger"] == "schedule: 0 2 * * *"
    assert batch_one["category"] == "batch"
    assert batch_one["display_label"] == "batch_one (__deployment__)"
    # load_fruitshop delivers to the 'fruitshop' pipeline → label has pipeline suffix
    load_fs = by_ref["jobs.__deployment__.load_fruitshop"]
    assert load_fs["default_trigger"] == "schedule: 0 3 * * *"
    assert load_fs["category"] == "pipeline"
    assert load_fs["display_label"] == "load_fruitshop (fruitshop)"
    # plain module → no section, plain label
    assert by_ref["jobs.plain"]["display_label"] == "plain"


def test_print_deployment_info_not_found_renders_single_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {"status": "not_found"}
    _print_deployment_info(info, verbosity=0)
    out = capsys.readouterr().out
    assert "no manifest found" in out
    assert "__deployment__.py" in out


def test_print_deployment_info_generation_failed_non_verbose_hides_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "generation_failed",
        "error": "ImportError: bad",
    }
    _print_deployment_info(info, verbosity=0)
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "manifest generation failed" in combined
    assert "ImportError: bad" not in combined


def test_print_deployment_info_generation_failed_verbose_shows_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "generation_failed",
        "error": "ImportError: bad",
    }
    _print_deployment_info(info, verbosity=1)
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "manifest generation failed" in combined
    assert "ImportError: bad" in combined


def test_print_deployment_info_ok_non_verbose_shows_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "ok",
        "total_jobs": 3,
        "counts_by_category": {"pipeline": 2, "notebook": 1},
        "jobs": [],
    }
    _print_deployment_info(info, verbosity=0)
    out = capsys.readouterr().out
    assert "3" in out and "job(s)" in out
    assert "2 pipeline(s)" in out
    assert "1 notebook(s)" in out
    # no per-job lines in non-verbose
    lines = [line for line in out.splitlines() if line.startswith("  ")]
    assert lines == []


def test_print_deployment_info_ok_verbose_lists_jobs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    info: TDeploymentManifestInfo = {
        "status": "ok",
        "total_jobs": 2,
        "counts_by_category": {"pipeline": 1, "notebook": 1},
        "jobs": [
            {
                "job_ref": "jobs.backfill",
                "display_label": "backfill",
                "category": "pipeline",
                "default_trigger": "schedule: 0 2 * * *",
                "triggers": ["tag:nightly"],
            },
            {
                "job_ref": "jobs.dashboard",
                "display_label": "dashboard",
                "category": "notebook",
                "triggers": [],
            },
        ],
    }
    _print_deployment_info(info, verbosity=1)
    out = capsys.readouterr().out
    assert "schedule: 0 2 * * *" in out
    assert "tag:nightly" in out
    assert "(interactive)" in out  # no triggers on notebook
    assert "backfill" in out
    assert "dashboard" in out


# dlthub local + top-level info argparse routing tests


def test_dlthub_local_info_parses(auto_isolated_workspace: Any, parse_dlthub: TParseDlthub) -> None:
    args, _ = parse_dlthub(["local", "info"])
    assert args.local_op == "info"


def test_dlthub_local_info_verbose(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    args, _ = parse_dlthub(["local", "-v", "info"])
    assert args.verbosity == 1


def test_dlthub_local_run_parses(auto_isolated_workspace: Any, parse_dlthub: TParseDlthub) -> None:
    args, _ = parse_dlthub(["local", "run", "myjob"])
    assert args.local_op == "run"
    assert args.selector_or_job_ref == "myjob"


def test_dlthub_local_show_parses(auto_isolated_workspace: Any, parse_dlthub: TParseDlthub) -> None:
    args, _ = parse_dlthub(["local", "show", "--edit"])
    assert args.local_op == "show"
    assert args.edit is True


def test_dlthub_local_clean_parses(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    args, _ = parse_dlthub(["local", "clean"])
    assert args.local_op == "clean"
    assert args.skip_local_data_dir is False


def test_dlthub_local_clean_skip_local_data_dir(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    args, _ = parse_dlthub(["local", "clean", "--skip-local-data-dir"])
    assert args.skip_local_data_dir is True


def test_dlthub_local_clean_rejects_profile_arg(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    """`local clean` always targets the current profile — extra positional must error."""
    with pytest.raises(SystemExit):
        parse_dlthub(["local", "clean", "tests"])


def test_dlthub_local_profile_use(
    auto_isolated_workspace: Any,
    monkeypatch: pytest.MonkeyPatch,
    run_dlthub: TRunDlthub,
) -> None:
    # e2e: run the full CLI dispatch for `local profile use`, then make sure that
    # `.dlt/profile-name` records the profile and that a fresh context runs with it
    monkeypatch.delenv(WORKSPACE__PROFILE, raising=False)
    ctx = auto_isolated_workspace
    assert ctx.profile == "dev"
    assert read_profile_pin(ctx) is None
    use = partial(run_dlthub, "local", "profile", "use")

    # declined: the active profile does not change
    use("prod", answer=False)
    assert read_profile_pin(ctx) is None
    out = use("prod")
    assert "dlt will make prod (environment prod) the active profile" in out
    assert "now use environment prod and its default write profile prod." in out
    # `.dlt/profile-name` records the profile, a new run context runs with it
    assert os.path.isfile(get_profile_pin_file(ctx))
    assert read_profile_pin(ctx) == "prod"
    reloaded = switch_context(ctx.run_dir)
    assert reloaded.profile == "prod"

    # an environment is refused, a profile of an environment selects the environment
    declare_environment(reloaded, "ci")
    with pytest.raises(ProfileNotDeclared, match="`ci` is an environment"):
        use("ci")
    assert read_profile_pin(reloaded) == "prod"
    out = use("ci-read")
    assert "now use environment ci and its default write profile ci-write." in out
    reloaded = switch_context(ctx.run_dir)
    assert reloaded.profile == "ci-read"
    assert default_profile(select_environment(), "write") == "ci-write"
    # a local profile keeps the default environment
    out = use("tests")
    assert "use the default environment prod" in out
    # WORKSPACE__PROFILE overrides the recorded profile
    monkeypatch.setenv(WORKSPACE__PROFILE, "dev")
    assert "WORKSPACE__PROFILE=dev is set in this shell" in use("prod")
    # an undeclared name is refused
    with pytest.raises(ProfileNotDeclared, match="dlthub profile add staging --local"):
        use("staging")
    assert read_profile_pin(reloaded) == "prod"


def test_dlthub_local_clean_deletes_dirs(
    auto_isolated_workspace: Any, monkeypatch: pytest.MonkeyPatch, parse_dlthub: TParseDlthub
) -> None:
    from dlt._workspace.cli import echo as fmt

    Path(auto_isolated_workspace.data_dir).mkdir(parents=True, exist_ok=True)
    Path(auto_isolated_workspace.local_dir).mkdir(parents=True, exist_ok=True)
    (Path(auto_isolated_workspace.data_dir) / "marker.txt").write_text("x")
    (Path(auto_isolated_workspace.local_dir) / "marker.txt").write_text("x")

    monkeypatch.setattr(fmt, "ALWAYS_CONFIRM", True)

    args, installed = parse_dlthub(["local", "clean"])
    installed["local"].execute(args)

    # delete_local_data recreates the dirs empty after wiping; the markers must be gone
    assert not (Path(auto_isolated_workspace.data_dir) / "marker.txt").exists()
    assert not (Path(auto_isolated_workspace.local_dir) / "marker.txt").exists()


def test_dlthub_local_clean_skip_local_data_dir_preserves_local_data(
    auto_isolated_workspace: Any, monkeypatch: pytest.MonkeyPatch, parse_dlthub: TParseDlthub
) -> None:
    from dlt._workspace.cli import echo as fmt

    Path(auto_isolated_workspace.data_dir).mkdir(parents=True, exist_ok=True)
    Path(auto_isolated_workspace.local_dir).mkdir(parents=True, exist_ok=True)
    (Path(auto_isolated_workspace.data_dir) / "marker.txt").write_text("x")
    (Path(auto_isolated_workspace.local_dir) / "marker.txt").write_text("x")

    monkeypatch.setattr(fmt, "ALWAYS_CONFIRM", True)

    args, installed = parse_dlthub(["local", "clean", "--skip-local-data-dir"])
    installed["local"].execute(args)

    # locally loaded data (local_dir) preserved; pipelines working dir (data_dir) wiped
    assert (Path(auto_isolated_workspace.local_dir) / "marker.txt").exists()
    assert not (Path(auto_isolated_workspace.data_dir) / "marker.txt").exists()


def test_dlthub_local_clean_user_declines_confirmation(
    auto_isolated_workspace: Any, monkeypatch: pytest.MonkeyPatch, parse_dlthub: TParseDlthub
) -> None:
    from dlt._workspace.cli import echo as fmt

    Path(auto_isolated_workspace.data_dir).mkdir(parents=True, exist_ok=True)
    (Path(auto_isolated_workspace.data_dir) / "marker.txt").write_text("x")

    # decline confirmation prompts — default for the confirm prompt is False
    monkeypatch.setattr(fmt, "ALWAYS_CHOOSE_DEFAULT", True)

    args, installed = parse_dlthub(["local", "clean"])
    installed["local"].execute(args)

    assert (Path(auto_isolated_workspace.data_dir) / "marker.txt").exists()


def test_dlthub_local_schema_parses(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    args, _ = parse_dlthub(["local", "schema", "schema.yaml"])
    assert args.local_op == "schema"
    assert args.file == "schema.yaml"


def test_dlthub_local_telemetry_parses(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    args, _ = parse_dlthub(["local", "telemetry"])
    assert args.local_op == "telemetry"


@pytest.mark.parametrize(
    "argv,expected",
    [
        (
            ["local", "pipeline", "info", "mypipe"],
            {"local_op": "pipeline", "operation": "info", "pipeline_name": "mypipe"},
        ),
        (
            ["local", "pipeline", "drop", "mypipe", "events", "--drop-all"],
            {
                "local_op": "pipeline",
                "operation": "drop",
                "pipeline_name": "mypipe",
                "resources": ["events"],
                "drop_all": True,
            },
        ),
        (
            ["local", "pipeline", "load-package", "mypipe", "load_xyz"],
            {
                "local_op": "pipeline",
                "operation": "load-package",
                "pipeline_name": "mypipe",
                "load_id": "load_xyz",
            },
        ),
        (
            ["local", "pipeline", "list"],
            {"local_op": "pipeline", "operation": "list"},
        ),
        (
            ["local", "pipeline"],
            {"local_op": "pipeline", "operation": None, "pipeline_name": None},
        ),
    ],
    ids=["info", "drop", "load-package", "list", "no-verb"],
)
def test_dlthub_local_pipeline_verb_first(
    auto_isolated_workspace: Any,
    parse_dlthub: TParseDlthub,
    argv: List[str],
    expected: Dict[str, Any],
) -> None:
    args, _ = parse_dlthub(argv)
    for key, value in expected.items():
        assert getattr(args, key) == value, f"{key}: got {getattr(args, key)!r} != {value!r}"


def test_dlthub_local_pipeline_info_without_name_prints_usage(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    # regression: PipelineCommand.execute calls self.parser.print_usage() when
    # pipeline_name is missing. The DlthubLocalWorkspaceCommand reuses a single
    # PipelineCommand instance with `parser` set to the inline pipeline subparser.
    from dlt._workspace.cli.exceptions import CliCommandException

    args, installed = parse_dlthub(["local", "pipeline", "info"])
    with pytest.raises(CliCommandException):
        installed["local"].execute(args)


def test_dlthub_workspace_removed(auto_isolated_workspace: Any, parse_dlthub: TParseDlthub) -> None:
    with pytest.raises(SystemExit):
        parse_dlthub(["workspace", "info"])


def test_dlthub_top_level_schema_removed(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    # `schema` only exists under `dlthub local schema`, not top level
    with pytest.raises(SystemExit):
        parse_dlthub(["schema"])


def test_dlthub_top_level_telemetry_removed(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    with pytest.raises(SystemExit):
        parse_dlthub(["telemetry"])


def test_dlthub_top_level_info_removed(
    auto_isolated_workspace: Any, parse_dlthub: TParseDlthub
) -> None:
    # `info` only exists under `dlthub local info`, not top level
    with pytest.raises(SystemExit):
        parse_dlthub(["info"])


@pytest.mark.parametrize(
    "argv,expected_destructive",
    [
        # destructive: direct `local` ops
        (["local", "run", "myjob"], True),
        (["local", "clean"], True),
        # destructive: nested `local pipeline` ops (operation in inner subparser)
        (["local", "pipeline", "drop", "mypipe"], True),
        # read-only: should NOT fire the banner
        (["local", "info"], False),
    ],
    ids=[
        "local-run",
        "local-serve",
        "pipeline-drop",
        "local-info",
    ],
)
def test_is_destructive_local_op(
    auto_isolated_workspace: Any,
    parse_dlthub: TParseDlthub,
    argv: List[str],
    expected_destructive: bool,
) -> None:
    from dlt._workspace.cli.dlthub.commands import _is_destructive_local_op

    args, _ = parse_dlthub(argv)
    assert _is_destructive_local_op(args) is expected_destructive


@pytest.mark.parametrize(
    "argv,expected",
    [
        (["local", "run", "myjob"], None),
        (["local", "-v", "run", "myjob"], 2),
        (["local", "-v", "run", "myjob", "-c", "agent.verbosity=0"], "0"),
    ],
    ids=["default", "verbose", "explicit-wins"],
)
def test_dlthub_local_run_passes_verbosity_to_the_agent(
    auto_isolated_workspace: Any,
    monkeypatch: pytest.MonkeyPatch,
    parse_dlthub: TParseDlthub,
    argv: List[str],
    expected: Any,
) -> None:
    """`dlthub local -v run` sets agent verbosity to 2, and `-c agent.verbosity` overrides it."""
    from dlt._workspace.cli.dlthub import _local_workspace_command as local_command
    from dlt._workspace.deployment import _run_helpers

    seen: Dict[str, Any] = {}

    def _fetch_run_info(**kwargs: Any) -> None:
        seen.update(kwargs["cli_config"])
        return None

    monkeypatch.setattr(_run_helpers, "fetch_run_info", _fetch_run_info)
    args, _ = parse_dlthub(argv)
    local_command.execute_run(args)

    assert seen.get("agent", {}).get("verbosity") == expected
