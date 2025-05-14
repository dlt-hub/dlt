import os
import shutil
from pathlib import Path
from typing import get_args, Literal, Set, Union

from dlt.cli import echo as fmt
from dlt.cli.init_command import DEFAULT_VERIFIED_SOURCES_REPO
from dlt.common import git
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.common.runtime import run_context


TSupportedIde = Literal[
    "cursor",
    "continue",
    "cline",
    "claude_desktop",
]

SUPPORTED_IDES: Set[TSupportedIde] = list(get_args(TSupportedIde))  # type: ignore
VERIFIED_SOURCES_AI_BASE_DIR = "ai"
AI_CONTRIBUTE_URL = (
    os.path.splitext(DEFAULT_VERIFIED_SOURCES_REPO)[0]
    + "/tree/master/"
    + VERIFIED_SOURCES_AI_BASE_DIR
)

# TODO Claude Desktop: rules need to be named `CLAUDE.md`, allow command to append to it
# TODO Continue: rules need to be in YAML file, allow command to properly edit it
# TODO generate more files based on the specifics of the source README and the destination


def ai_setup_command(
    ide: TSupportedIde,
    branch: Union[str, None] = None,
    repo: str = DEFAULT_VERIFIED_SOURCES_REPO,
) -> None:
    """Get AI rules files into your local project for the selected IDE.

    Get the source and destination directories for the rules files.
    Files found in the source directory will be copied into the destination directory.
    """
    # where dlt-hub/verified-sources is cloned
    fmt.echo("Looking up IDE rules and configuration %s..." % fmt.bold(repo))
    src_storage = git.get_fresh_repo_files(repo, get_dlt_repos_dir(), branch=branch)
    if not src_storage.has_folder(VERIFIED_SOURCES_AI_BASE_DIR):
        fmt.warning(
            "Support for ai command not found in repo %s branch %s"
            % (fmt.bold(repo), fmt.bold(branch or "<default>"))
        )
        return
    src_dir = Path(src_storage.make_full_path(VERIFIED_SOURCES_AI_BASE_DIR)) / ide

    # where the command is ran, i.e., project root
    dest_dir = Path(run_context.active().run_dir)
    copied_files = 0

    for src_sub_path in src_dir.rglob("*"):
        if src_sub_path.is_dir():
            continue

        if src_sub_path.name == ".message":
            # display message, do not copy
            fmt.echo(src_sub_path.read_text(encoding="utf-8"))
            continue

        copied_files += 1
        dest_file_path = dest_dir / src_sub_path.relative_to(src_dir)
        if dest_file_path.exists():
            fmt.warning(f"Existing rules file found at {dest_file_path.absolute()}; Skipping.")
            continue

        if not dest_file_path.parent.exists():
            dest_file_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(src_sub_path, dest_file_path)

    if copied_files == 0:
        fmt.echo(
            "%s%s is not yet supported. No files were found."
            % (fmt.bold(ide), fmt.style("", bold=False))
        )
    else:
        fmt.echo(
            "%s file(s) supporting %s were copied." % (fmt.bold(str(copied_files)), fmt.bold(ide))
        )
    fmt.note(
        "Help us to build better support for %s by contributing better rules, prompts or configs"
        " in %s" % (ide, AI_CONTRIBUTE_URL)
    )


# TODO create a command to create a copy-pasteable MCP server config


def mcp_command() -> None: ...
