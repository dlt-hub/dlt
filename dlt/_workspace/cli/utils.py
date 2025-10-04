import os
import shutil

import dlt
from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
    ProfilesRunContext,
)
from dlt.common.storages.file_storage import FileStorage

from dlt.cli import echo as fmt


def display_run_context_info() -> None:
    run_context = dlt.current.run_context()
    # NOTE: runtime check on protocol is slow
    if isinstance(run_context, ProfilesRunContext):
        fmt.echo(
            "(workspace: %s, profile: %s)"
            % (fmt.bold(run_context.name), fmt.bold(run_context.profile))
        )


def remove_local_data(run_context: RunContextBase, skip_data_dir: bool) -> None:
    # delete all files in locally loaded data
    if local_dir := run_context.local_dir:
        # show relative path to the user
        display_dir = os.path.relpath(local_dir, ".")

        if len(display_dir) > len(local_dir):
            display_dir = local_dir
        fmt.echo("Will delete locally loaded data in %s" % fmt.style(display_dir, fg="yellow"))
        # if os.path.exists(local_dir):
        #     shutil.rmtree(local_dir, onerror=FileStorage.rmtree_del_ro)
        # create temp dir
        os.makedirs(local_dir, exist_ok=True)
    else:
        fmt.echo("local_dir not defined, locally loaded data not wiped out")
    if not skip_data_dir:
        data_dir = run_context.data_dir
        fmt.echo(
            "Will delete pipeline working folders & other entities data %s"
            % fmt.style(data_dir, fg="yellow")
        )
        # if os.path.exists(data_dir):
        #     shutil.rmtree(data_dir, onerror=FileStorage.rmtree_del_ro)
        # create data dir
        os.makedirs(data_dir, exist_ok=True)
