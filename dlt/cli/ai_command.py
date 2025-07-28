import os
import shutil
from pathlib import Path
from typing import List, Tuple, get_args, Literal, Set, Union, Dict

from dlt.cli import echo as fmt
from dlt.common import git
from dlt.common.pipeline import get_dlt_repos_dir
from dlt.common.runtime import run_context

TSupportedIde = Literal[
    "amp",
    "codex",
    "cody",
    "cursor",
    "continue",
    "cline",
    "claude",
    "windsurf",
    "copilot",
]

LOCATION_IN_BASE_DIR: Dict[TSupportedIde, str] = {
    "amp": "AGENT.md",
    "codex": "AGENT.md",
    "cody": ".sourcegraph",
    "cursor": ".cursor",
    "continue": ".continue",
    "cline": ".clinerules",
    "claude": "CLAUDE.md",
    "windsurf": ".windsurf",
    "copilot": ".github",
}

SUPPORTED_IDES: Set[TSupportedIde] = set(get_args(TSupportedIde))
VERIFIED_SOURCES_AI_BASE_DIR = "ai"

# TODO Claude: rules need to be named `CLAUDE.md`, allow command to append to it
# TODO Continue: rules need to be in YAML file, allow command to properly edit it
# TODO generate more files based on the specifics of the source README and the destination


def _copy_repo_files(
    src_dir: Path, dest_dir: Path, warn_on_overwrite: bool = True
) -> Tuple[List[str], int]:
    """
    Copy either a single .md file or all files under a directory into dest_dir.
    1. Single .md files (e.g. CLAUDE.md) are directly copied.
    2. Rule files that follow a specific a folder structure (e.g .cursor/rules/) follow that structure.
    """
    copied_files = []
    count_files = 0

    if src_dir.is_file() and src_dir.suffix == ".md":
        files = [src_dir]
        src_root = src_dir.parent  # strip parent so relative_path == 'file.md'
        dest_root = dest_dir  # single .md file goes directly under dest_dir
    else:
        files = [path for path in src_dir.rglob("*") if path.is_file()]
        src_root = src_dir
        dest_root = dest_dir / src_dir.name  # preserve top-level folder

    for src_path in files:
        if src_path.name == ".message":
            # display message, do not copy
            fmt.echo(src_path.read_text(encoding="utf-8"))
            continue

        count_files += 1

        relative_path = src_path.relative_to(src_root)
        dest_file_path = dest_root / relative_path

        if dest_file_path.exists():
            if warn_on_overwrite:
                fmt.warning(f"Existing rules file found at {dest_file_path.absolute()}; Skipping.")
            continue

        dest_file_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dest_file_path)
        copied_files.append(src_path.name)

    return copied_files, count_files


def ai_setup_command(
    ide: TSupportedIde,
    location: str,
    branch: Union[str, None] = None,
    hide_warnings: bool = False,
) -> None:
    """Get AI rules files into your local project for the selected IDE.

    Get the source and destination directories for the rules files.
    Files found in the source directory will be copied into the destination directory.
    """
    # where dlt-hub/verified-sources is cloned
    fmt.echo("Looking up IDE rules and configuration %s..." % fmt.bold(location))
    src_storage = git.get_fresh_repo_files(location, get_dlt_repos_dir(), branch=branch)
    if not src_storage.has_folder(VERIFIED_SOURCES_AI_BASE_DIR):
        fmt.warning(
            "Support for ai command not found in repo %s branch %s"
            % (fmt.bold(location), fmt.bold(branch or "<default>"))
        )
        return

    suffix = LOCATION_IN_BASE_DIR.get(ide)
    src_dir = Path(src_storage.make_full_path(VERIFIED_SOURCES_AI_BASE_DIR)) / suffix

    # where the command is ran, i.e., project root
    dest_dir = Path(run_context.active().run_dir)
    copied_files, count_files = _copy_repo_files(src_dir, dest_dir, not hide_warnings)
    if count_files == 0:
        fmt.echo(
            "%s%s is not yet supported. No files were found."
            % (fmt.bold(ide), fmt.style("", bold=False))
        )
    else:
        if copied_files:
            fmt.echo(
                "%s file(s) supporting %s were copied."
                % (fmt.bold(str(len(copied_files))), fmt.bold(ide))
            )

    if not hide_warnings:
        # refer to contribute README in the repo
        ai_contribute_url = (
            os.path.splitext(location)[0] + "/tree/master/" + VERIFIED_SOURCES_AI_BASE_DIR
        )
        fmt.note(
            "Help us to build better support for %s by contributing better rules, prompts or"
            " configs in %s" % (ide, ai_contribute_url)
        )


def vibe_source_setup(
    source: str,
    location: str,
    branch: Union[str, None] = None,
) -> None:
    """Copies files from vibe sources repo into the current working folder"""

    fmt.echo("Looking up in dltHub for rules, docs and snippets for %s..." % fmt.bold(source))
    src_storage = git.get_fresh_repo_files(location, get_dlt_repos_dir(), branch=branch)
    if not src_storage.has_folder(source):
        fmt.warning("We have nothing for %s at dltHub yet." % fmt.bold(source))
        return
    src_dir = Path(src_storage.make_full_path(source))

    # where the command is ran, i.e., project root
    dest_dir = Path(run_context.active().run_dir)
    copied_files, count_files = _copy_repo_files(src_dir, dest_dir)
    if count_files == 0:
        fmt.warning("We have nothing for %s at dltHub yet." % fmt.bold(source))
    else:
        fmt.echo(
            "%s file(s) supporting %s were copied:" % (fmt.bold(str(count_files)), fmt.bold(source))
        )
        for file in copied_files:
            fmt.echo(fmt.bold(file))


# TODO create a command to create a copy-pasteable MCP server config
def mcp_command() -> None: ...
