import os
import shutil
from typing import Any

import dlt
from dlt._workspace.profile import LOCAL_PROFILES
from dlt.cli.exceptions import CliCommandException
from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
    ProfilesRunContext,
)
from dlt.common.storages.file_storage import FileStorage

from dlt.cli import echo as fmt


def display_run_context_info() -> None:
    run_context = dlt.current.run_context()
    if isinstance(run_context, ProfilesRunContext):
        if run_context.default_profile != run_context.profile:
            # print warning
            fmt.echo(
                "Profile %s activated on %s"
                % (
                    fmt.style(run_context.profile, fg="yellow", reset=True),
                    fmt.bold(run_context.name),
                ),
                err=True,
            )


def add_mcp_arg_parser(subparsers: Any, help_str: str, default_sse_port: int) -> None:
    command_parser = subparsers.add_parser(
        "mcp",
        help=help_str,
    )
    command_parser.add_argument("--stdio", action="store_true", help="Use stdio transport mode")
    command_parser.add_argument(
        "--port",
        type=int,
        default=default_sse_port,
        help=f"SSE port to use (default: {default_sse_port})",
    )


def _may_safe_delete_local(run_context: RunContextBase, deleted_dir_type: str) -> bool:
    deleted_dir = getattr(run_context, deleted_dir_type)
    for ctx_attr, label in (
        ("run_dir", "run dir (workspace root)"),
        ("settings_dir", "settings dir"),
    ):
        if os.path.abspath(deleted_dir) == os.path.abspath(getattr(run_context, ctx_attr)):
            fmt.error(
                f"{deleted_dir_type} `deleted_dir` is the same as {label} and cannot be deleted"
            )
            return False
    return True


def _wipe_dir(
    run_context: RunContextBase, dir_attr: str, echo_template: str, recreate_dirs: bool = True
) -> None:
    """echo, safely wipe and optionally recreate a directory from run context.

    Args:
        run_context: Current run context.
        dir_attr: Attribute name on the run context that holds the directory path, eg. "local_dir".
        echo_template: Template used to echo the action to the user. Must contain a single %s placeholder for the styled path.
        recreate_dirs: when True, recreate the directory after deletion.
    """
    dir_path = getattr(run_context, dir_attr, None)
    if not dir_path:
        raise CliCommandException()

    # ensure we never attempt to operate on run_dir or settings_dir
    if not _may_safe_delete_local(run_context, dir_attr):
        raise CliCommandException()

    # show relative path to the user when shorter
    display_dir = os.path.relpath(dir_path, ".")
    if len(display_dir) > len(dir_path):
        display_dir = dir_path

    fmt.echo(echo_template % fmt.style(display_dir, fg="yellow"))

    if os.path.exists(dir_path):
        shutil.rmtree(dir_path, onerror=FileStorage.rmtree_del_ro)

    if recreate_dirs:
        os.makedirs(dir_path, exist_ok=True)


def delete_local_data(
    run_context: RunContextBase, skip_data_dir: bool, recreate_dirs: bool = True
) -> None:
    if isinstance(run_context, ProfilesRunContext):
        if run_context.profile not in LOCAL_PROFILES:
            fmt.warning("You will clean local data for a profile")
    else:
        fmt.error("Cannot delete local data for a context without profiles")
        raise CliCommandException()

    # delete all files in locally loaded data (if present)
    _wipe_dir(
        run_context,
        "local_dir",
        "Will delete locally loaded data in %s",
        recreate_dirs,
    )

    # delete pipeline working folders & other entities data unless explicitly skipped
    if not skip_data_dir:
        _wipe_dir(
            run_context,
            "data_dir",
            "Will delete pipeline working folders & other entities data %s",
            recreate_dirs,
        )
