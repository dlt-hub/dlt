"""dlthub init — workspace scaffold writer and views."""

import os
from typing import Any, Dict, List

import tomlkit

from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli._pipeline_files import (
    TEMPLATE_FILES,
    get_single_file_templates_storage,
)
from dlt._workspace.cli._write_state import WorkspaceWriteState
from dlt._workspace.cli.dlthub.typing import TInitPlan


def init_dlthub_workspace(
    plan: TInitPlan, *, dry_run: bool = False, force: bool = False
) -> Dict[str, str]:
    """Scaffold a dlthub workspace from the plan."""
    run_dir = plan["run_dir"]
    # default keeps user files; --force overwrites the dlthub-managed scaffold but
    # NEVER touches secrets.toml or config.toml (user-owned: creds + workspace settings)
    keep_existing = not force
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
            body = (
                templates.load(file_name)
                + f"\n[workspace.settings]\nname = {workspace_name_literal}\n"
            )
            # always preserve existing config.toml — even with --force
            state.add_new_file(dest_path, body, accept_existing=True)
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

    # marker that flips this directory into an active dlthub workspace
    state.add_new_file(
        os.path.join(run_dir, ".dlt", ".workspace"),
        "",
        accept_existing=keep_existing,
    )

    if plan["dependency_system"] == "pyproject.toml":
        state.add_new_file(
            os.path.join(run_dir, "pyproject.toml"),
            _render_pyproject(plan["project_name"], plan["dependency_specs"], plan["uv_sources"]),
            accept_existing=keep_existing,
        )
    else:
        state.add_new_file(
            os.path.join(run_dir, "requirements.txt"),
            "\n".join(plan["requirements_lines"]) + "\n",
            accept_existing=keep_existing,
        )

    if dry_run:
        return state.preview()
    # the controller has gated on `workspace_exists` + `--force`; skip the in-state check
    return state.commit(allow_overwrite=True)


def _render_pyproject(name: str, deps: List[str], uv_sources: Dict[str, Dict[str, Any]]) -> str:
    """Build a `pyproject.toml` for a dlthub workspace."""
    # `[tool.uv.sources]` is emitted only when at least one of dlt/dlthub/dlthub-client
    # is non-PyPI, so the new workspace reproduces the developer's local install
    doc = tomlkit.document()
    project = tomlkit.table()
    project["name"] = name
    project["version"] = "0.0.1"
    project["description"] = "A dlthub workspace"
    deps_array = tomlkit.array()
    for d in deps:
        deps_array.append(d)
    deps_array.multiline(True)
    project["dependencies"] = deps_array
    doc["project"] = project

    if uv_sources:
        sources_table = tomlkit.table()
        for pkg, src in uv_sources.items():
            entry = tomlkit.inline_table()
            for k, v in src.items():
                entry[k] = v
            sources_table[pkg] = entry
        uv_table = tomlkit.table()
        uv_table["sources"] = sources_table
        tool_table = tomlkit.table()
        tool_table["uv"] = uv_table
        doc["tool"] = tool_table

    return tomlkit.dumps(doc)


def _print_init_plan(plan: TInitPlan) -> None:
    """Show the user what `dlthub init` will create."""
    fmt.echo()
    fmt.echo("Creating dlthub workspace at %s" % fmt.bold(plan["run_dir"]))
    fmt.echo("  project name: %s" % fmt.bold(plan["project_name"]))
    fmt.echo(
        "  dependency system: %s%s"
        % (
            fmt.bold(plan["dependency_system"]),
            " (uv detected)" if plan["uv_available"] else " (no uv on PATH)",
        )
    )
    fmt.echo("  files:")
    for f in plan["files"]:
        if f["status"] == "create":
            label = "[CREATE]   "
        elif f["status"] == "skip":
            label = "[KEEP]     "
        else:
            label = "[CONFLICT] "
        fmt.echo("    %s%s" % (label, f["path"]))
    fmt.echo()


def _print_init_welcome(plan: TInitPlan) -> None:
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
    fmt.echo("* Configure your AI coding agent: %s." % fmt.bold("dlthub ai init"))
    fmt.echo()
