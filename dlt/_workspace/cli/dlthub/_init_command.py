"""dlthub init — workspace scaffold writer and views."""

import os
from pathlib import Path
from typing import Dict, List, Optional

import tomlkit

from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli._pipeline_files import (
    TEMPLATE_FILES,
    get_single_file_templates_storage,
)
from dlt._workspace.cli._write_state import WorkspaceWriteState
from dlt._workspace.cli.dlthub.typing import TInitFileEntry, TInitPlan
from dlt._workspace.cli.dlthub.utils import GITIGNORE_FILE
from dlt._workspace.deployment.requirements import (
    add_pyproject_dependencies,
    append_lines,
    merge_requirements_txt,
    render_pyproject,
)
from dlt._workspace.deployment.typing import TDependencyChange
from dlt._workspace.run_context import format_display_path
from dlt._workspace.settings_files import dump_yaml
from dlt._workspace.workspace_definition import create_built_in_workspace_definition
from dlt._workspace.workspace_file import WORKSPACE_FILE_NAME, build_stored_workspace_definition

MERGE_MARKER = "# added by dlthub init"


def init_dlthub_workspace(
    plan: TInitPlan, *, dry_run: bool = False, overwrite: bool = False
) -> Dict[str, str]:
    """Scaffold a dlthub workspace from the plan."""
    run_dir = plan["run_dir"]
    # default keeps user files; --overwrite replaces the dlthub-managed scaffold but
    # NEVER touches secrets.toml or config.toml (user-owned: creds + workspace settings)
    keep_existing = not overwrite
    state = WorkspaceWriteState(
        FileStorage(run_dir, makedirs=True),
        os.path.join(run_dir, ".dlt"),
    )

    templates = get_single_file_templates_storage()
    workspace_name_literal = tomlkit.string(plan["project_name"]).as_string()
    # config.toml needs the workspace name injected so runtime can resolve WorkspaceSettings
    for file_name in TEMPLATE_FILES:
        if not templates.has_file(file_name):
            continue
        # `file_name` uses `/` (TEMPLATE_FILES literal); split so the joined dest path
        # uses the platform separator and matches paths built by callers via os.path.join
        dest_path = os.path.join(run_dir, *file_name.split("/"))
        if os.path.basename(file_name) == "config.toml":
            # workspaces always emit _dlt_load_id for arrow/parquet data too (json data
            # already gets it) so every row carries its load id regardless of source format
            body = (
                templates.load(file_name)
                + "\n[normalize.parquet_normalizer]\n"
                + "# emit _dlt_load_id for arrow/parquet data\n"
                + "add_dlt_load_id = true\n"
                + f"\n[workspace.settings]\nname = {workspace_name_literal}\n"
            )
            # always preserve existing config.toml, even with --overwrite
            state.add_new_file(dest_path, body, accept_existing=True)
        elif file_name == ".gitignore" and plan["gitignore_additions"]:
            with open(dest_path, encoding="utf-8") as f:
                existing = f.read()
            state.add_new_file(
                dest_path, append_lines(existing, plan["gitignore_additions"], MERGE_MARKER)
            )
        else:
            state.add_file_copy(
                templates.make_full_path(file_name),
                dest_path,
                accept_existing=keep_existing,
            )

    # secrets.toml is always preserved so user creds survive a re-init
    state.add_new_file(
        os.path.join(run_dir, ".dlt", "secrets.toml"),
        "# add your dlt secrets here\n",
        accept_existing=True,
    )

    # marks an active dlthub workspace and declares the built-in profiles and environments
    workspace_file = os.path.join(run_dir, ".dlt", WORKSPACE_FILE_NAME)
    state.add_new_file(
        workspace_file,
        dump_yaml(
            build_stored_workspace_definition(
                create_built_in_workspace_definition(), workspace_file
            )
        ),
        accept_existing=keep_existing,
    )

    dependency_path = os.path.join(run_dir, plan["dependency_system"])
    dependency_writes = get_dependency_writes(plan)
    if plan["dependency_system"] == "pyproject.toml":
        # uv merges into an existing pyproject.toml after the commit
        if not dependency_writes:
            state.add_new_file(
                dependency_path,
                render_pyproject(
                    plan["project_name"],
                    plan["dependency_specs"],
                    plan["uv_sources"],
                    plan["requires_python"],
                ),
                accept_existing=keep_existing,
            )
    elif dependency_writes:
        with open(dependency_path, encoding="utf-8") as f:
            existing = f.read()
        state.add_new_file(
            dependency_path, merge_requirements_txt(existing, dependency_writes, MERGE_MARKER)
        )
    else:
        state.add_new_file(
            dependency_path,
            "\n".join(plan["requirements_lines"]) + "\n",
            accept_existing=keep_existing,
        )

    if dry_run:
        return state.preview()
    # existing files are replaced only when staged without `accept_existing`
    written = state.commit(allow_overwrite=True)
    if plan["dependency_system"] == "pyproject.toml":
        add_pyproject_dependencies(dependency_writes, plan["uv_sources"], Path(run_dir))
    return written


def get_dependency_writes(plan: TInitPlan) -> List[TDependencyChange]:
    """The dependency changes that a merge writes, without the declared newer versions it keeps."""
    return [change for change in plan["dependency_changes"] if change["op"] != "newer"]


def _print_init_file(plan: TInitPlan, entry: TInitFileEntry) -> None:
    labels = {
        "create": "[CREATE]",
        "skip": "[SKIP]",
        "merge": "[MERGE]",
        "overwrite": "[OVERWRITE]",
    }
    fmt.echo(
        "    %-12s%s"
        % (labels[entry["status"]], format_display_path(entry["path"], plan["run_dir"]))
    )
    if entry["status"] != "merge":
        return
    if os.path.basename(entry["path"]) == GITIGNORE_FILE:
        for pattern in plan["gitignore_additions"]:
            fmt.echo("      + %s" % pattern)
        return
    for change in get_dependency_writes(plan):
        if change["op"] == "add":
            fmt.echo("      + %s" % change["spec"])
        else:
            fmt.echo("      ~ %s -> %s" % (change["declared"], change["spec"]))


def _print_newer_dependencies(plan: TInitPlan) -> None:
    for change in plan["dependency_changes"]:
        if change["op"] == "newer":
            fmt.warning(
                "%s in %s is newer than %s, kept"
                % (change["declared"], plan["dependency_system"], change["spec"])
            )


def _print_init_plan(plan: TInitPlan, agent_name: Optional[str] = None) -> None:
    """Show the user what `dlthub init` will write."""
    fmt.echo()
    fmt.echo("Creating dlthub workspace at %s" % fmt.bold(plan["run_dir"]))
    fmt.echo("  project name: %s" % fmt.bold(plan["project_name"]))
    if agent_name:
        fmt.echo("  AI coding agent: %s" % fmt.bold(agent_name))
    fmt.echo(
        "  dependency system: %s%s"
        % (
            fmt.bold(plan["dependency_system"]),
            " (uv detected)" if plan["uv_available"] else " (no uv on PATH)",
        )
    )
    fmt.echo("  files:")
    for f in plan["files"]:
        _print_init_file(plan, f)
    dependency_file = os.path.join(plan["run_dir"], plan["dependency_system"])
    if any(
        f["path"] == dependency_file and f["status"] in ("create", "overwrite")
        for f in plan["files"]
    ):
        fmt.echo("  dependencies:")
        for spec in plan["dependency_specs"]:
            fmt.echo("    %s" % spec)
    fmt.echo()
    _print_newer_dependencies(plan)


def _print_init_merge_error(plan: TInitPlan) -> None:
    if plan["merge_error"] == "no_project_table":
        fmt.error(
            "%s has no [project] table, so dlthub init cannot add the dependencies to it. Add a"
            " [project] table or re-run with %s or %s."
            % (
                plan["dependency_system"],
                fmt.bold("--dependencies requirements"),
                fmt.bold("--skip"),
            )
        )
    else:
        fmt.error(
            "uv is needed to add dependencies to %s but was not found on PATH. Install uv or add"
            " these dependencies yourself: %s"
            % (
                plan["dependency_system"],
                ", ".join(change["spec"] for change in get_dependency_writes(plan)),
            )
        )


def _print_init_changes(plan: TInitPlan) -> None:
    """Show the existing files that `dlthub init` will merge into or overwrite."""
    fmt.echo("dlthub init will change these existing files:")
    for f in plan["files"]:
        if f["status"] in ("merge", "overwrite"):
            _print_init_file(plan, f)
    _print_newer_dependencies(plan)


def _print_init_welcome(plan: TInitPlan, agent_configured: bool = False) -> None:
    """Print next-steps after a successful init."""
    # the install hint follows the file that was actually written, not uv presence:
    # `uv sync` only resolves pyproject.toml; requirements.txt always uses pip
    fmt.echo()
    fmt.echo("Workspace ready at %s." % fmt.bold(plan["run_dir"]))
    if plan["dependency_system"] == "pyproject.toml":
        fmt.echo("* Install dependencies: %s" % fmt.bold("uv sync"))
    else:
        fmt.echo("* Install dependencies: %s" % fmt.bold("pip install -r requirements.txt"))
    fmt.echo("* Add credentials in %s." % fmt.bold(".dlt/secrets.toml"))
    fmt.echo("* Run %s to verify." % fmt.bold("dlthub local info"))
    if not agent_configured:
        fmt.echo("* Configure your AI coding agent: %s." % fmt.bold("dlthub ai init"))
    fmt.echo()
