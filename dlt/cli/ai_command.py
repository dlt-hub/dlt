import shutil
from pathlib import Path
from typing import get_args, Literal, Set

import dlt
from dlt.cli import echo as fmt
from dlt.common.runtime import run_context
from dlt.common.storages.file_storage import FileStorage


TSupportedIde = Literal[
    "cursor",
    "continue",
    "cline",
    "claude_desktop",
]

SUPPORTED_IDES: Set[TSupportedIde] = set(get_args(TSupportedIde))
IDE_RULES_SPECS: dict[TSupportedIde, tuple[str, str]] = {
    "cursor": ("cursor", ".cursorrules"),
    "continue": ("continue", ".continue"),
    "cline": ("cline", ".clinerules"),
    "claude_desktop": ("claude", ".claude"),
}
RULES_TEMPLATE_RELATIVE_PATH = "sources/_ai_static_files/rules"

# TODO Claude Desktop: rules need to be named `CLAUDE.md`, allow command to append to it
# TODO Continue: rules need to be in YAML file, allow command to properly edit it
# TODO generate more files based on the specifics of the source README and the destination

def ai_setup_command(ide: TSupportedIde = None) -> None:
    """Get AI rules files into your local project for the selected IDE.
    
    Get the source and destination directories for the rules files.
    Files found in the source directory will be copied into the destination directory.
    """
    specs = IDE_RULES_SPECS[ide]

    src_storage = FileStorage(str(Path(dlt.__file__).parent.joinpath(RULES_TEMPLATE_RELATIVE_PATH)))
    dest_storage = FileStorage(run_context.active().run_dir)

    src_dir = Path(src_storage.make_full_path(specs[0]))
    dest_dir = Path(dest_storage.make_full_path(specs[1]))

    dest_dir.mkdir(parents=True, exist_ok=True)
    for src_file_path in src_dir.iterdir():
        dest_file_path = dest_dir / src_file_path.name
        if dest_file_path.exists():
            fmt.warning(f"Existing rules file found at {dest_file_path.absolute()}. Skipping.")
            continue
        
        shutil.copy2(src_file_path, dest_file_path)


# TODO create a command to create a copy-pasteable MCP server config


def mcp_command() -> None:
    ...