import json
import shutil
from pathlib import Path

import pytest

from dlt.common.libs import git
from dlt._workspace.cli.ai.utils import (
    DEFAULT_AI_WORKBENCH_BRANCH,
    DEFAULT_AI_WORKBENCH_REPO,
)
from dlt._workspace.typing import TToolkitInfo

# known toolkits in the repo (init is now visible)
KNOWN_TOOLKITS = ["data-exploration", "init", "rest-api-pipeline", "dlthub-runtime"]


def make_mock_toolkit_info(
    name: str = "test-toolkit",
    version: str = "1.0.0",
    description: str = "",
    tags: None = None,
    workflow_entry_skill: str = "",
) -> TToolkitInfo:
    """Build a ``TToolkitInfo`` for tests."""
    meta = TToolkitInfo(
        name=name,
        version=version,
        description=description,
        tags=list(tags or []),
    )
    if workflow_entry_skill:
        meta["workflow_entry_skill"] = workflow_entry_skill
    return meta


def make_mock_toolkit(toolkit_name: str = "test-toolkit", with_mcp: bool = False) -> Path:
    """Create a mock toolkit directory with skills, commands, and rules."""
    toolkit_dir = Path("repo") / toolkit_name
    meta_dir = toolkit_dir / ".claude-plugin"
    meta_dir.mkdir(parents=True)
    (meta_dir / "plugin.json").write_text(
        json.dumps({"name": toolkit_name, "version": "0.1.0", "description": "Test toolkit"}),
        encoding="utf-8",
    )

    skill_dir = toolkit_dir / "skills" / "find-source"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: find-source\n---\nFind a source.", encoding="utf-8"
    )
    (skill_dir / "helper.py").write_text("# helper code\n", encoding="utf-8")

    cmd_dir = toolkit_dir / "commands"
    cmd_dir.mkdir(parents=True)
    (cmd_dir / "bootstrap.md").write_text(
        "---\nname: Bootstrap\ndescription: Set up project\n---\n# Bootstrap\nDo stuff",
        encoding="utf-8",
    )

    rules_dir = toolkit_dir / "rules"
    rules_dir.mkdir(parents=True)
    (rules_dir / "coding.md").write_text(
        "---\nalwaysApply: true\ndescription: Coding rule\n---\n# Coding Style\nFollow these.",
        encoding="utf-8",
    )

    (toolkit_dir / ".claudeignore").write_text("secrets.toml\n*.secrets.toml\n", encoding="utf-8")

    if with_mcp:
        (toolkit_dir / "mcp.json").write_text(
            json.dumps(
                {
                    "dlt-workspace-mcp": {
                        "type": "stdio",
                        "command": "uv",
                        "args": ["run", "dlt", "workspace", "mcp", "--stdio"],
                    }
                }
            ),
            encoding="utf-8",
        )

    return toolkit_dir


def make_mock_workbench() -> Path:
    """Create a mock workbench base dir with 2 toolkits and init."""
    base = Path("workbench")
    base.mkdir()

    # init toolkit
    init_dir = base / "init"
    init_meta = init_dir / ".claude-plugin"
    init_meta.mkdir(parents=True)
    (init_meta / "plugin.json").write_text(
        json.dumps({"name": "init", "version": "1.0.0", "description": "Init toolkit"}),
        encoding="utf-8",
    )
    init_rules = init_dir / "rules"
    init_rules.mkdir()
    (init_rules / "base.md").write_text(
        "---\ndescription: Base rules\n---\n# Base\nAlways follow these.", encoding="utf-8"
    )
    (init_dir / ".claudeignore").write_text("_storage\n", encoding="utf-8")

    # rest-api toolkit
    rest_dir = base / "rest-api-pipeline"
    rest_meta = rest_dir / ".claude-plugin"
    rest_meta.mkdir(parents=True)
    (rest_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "rest-api-pipeline",
                "version": "0.1.0",
                "description": "REST API source pipeline toolkit",
            }
        ),
        encoding="utf-8",
    )
    (rest_meta / "toolkit.json").write_text(
        json.dumps({"dependencies": ["init"], "workflow_entry_skill": "find-source"}),
        encoding="utf-8",
    )
    # skill
    skill_dir = rest_dir / "skills" / "find-source"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: find-source\ndescription: Find and configure a REST API source\n---\n"
        "# Find Source\nInstructions here.",
        encoding="utf-8",
    )
    # command
    cmd_dir = rest_dir / "commands"
    cmd_dir.mkdir(parents=True)
    (cmd_dir / "bootstrap.md").write_text(
        "---\nname: bootstrap\ndescription: Set up a new REST API pipeline project\n---\n"
        "# Bootstrap\nSteps.",
        encoding="utf-8",
    )
    # rule
    rules_dir = rest_dir / "rules"
    rules_dir.mkdir(parents=True)
    (rules_dir / "coding.md").write_text(
        "---\nname: coding\ndescription: Coding style guidelines\n---\n# Coding\nFollow these.",
        encoding="utf-8",
    )
    # mcp
    (rest_dir / "mcp.json").write_text(
        json.dumps(
            {
                "dlt-workspace-mcp": {
                    "type": "stdio",
                    "command": "uv",
                    "args": ["run", "dlt", "workspace", "mcp", "--stdio"],
                }
            }
        ),
        encoding="utf-8",
    )
    # ignore
    (rest_dir / ".claudeignore").write_text("*.secrets.toml\n", encoding="utf-8")

    # sql-database toolkit (minimal)
    sql_dir = base / "sql-database"
    sql_meta = sql_dir / ".claude-plugin"
    sql_meta.mkdir(parents=True)
    (sql_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "sql-database",
                "version": "0.1.0",
                "description": "SQL database source toolkit",
            }
        ),
        encoding="utf-8",
    )
    (sql_meta / "toolkit.json").write_text(
        json.dumps({"dependencies": ["init"]}),
        encoding="utf-8",
    )

    # unlisted toolkit (listed: false)
    unlisted_dir = base / "bootstrap"
    unlisted_meta = unlisted_dir / ".claude-plugin"
    unlisted_meta.mkdir(parents=True)
    (unlisted_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "bootstrap",
                "version": "0.1.0",
                "description": "Bootstrap toolkit",
            }
        ),
        encoding="utf-8",
    )
    (unlisted_meta / "toolkit.json").write_text(
        json.dumps({"dependencies": ["init"], "listed": False}),
        encoding="utf-8",
    )

    return base


def make_versioned_workbench(version: str = "1.0.0") -> Path:
    """Create a mock workbench with init and a versioned toolkit."""
    base = Path("workbench")
    base.mkdir(exist_ok=True)

    # init toolkit
    init_dir = base / "init"
    init_meta = init_dir / ".claude-plugin"
    init_meta.mkdir(parents=True, exist_ok=True)
    (init_meta / "plugin.json").write_text(
        json.dumps({"name": "init", "version": "1.0.0", "description": "Init toolkit"}),
        encoding="utf-8",
    )
    init_rules = init_dir / "rules"
    init_rules.mkdir(exist_ok=True)
    (init_rules / "base.md").write_text(
        "---\ndescription: Base rules\n---\n# Base\nAlways follow these.", encoding="utf-8"
    )

    # versioned toolkit
    tk_dir = base / "my-toolkit"
    tk_meta = tk_dir / ".claude-plugin"
    tk_meta.mkdir(parents=True, exist_ok=True)
    (tk_meta / "plugin.json").write_text(
        json.dumps(
            {
                "name": "my-toolkit",
                "version": version,
                "description": "Test toolkit",
                "keywords": ["testing", "dlt"],
                "dependencies": ["init"],
            }
        ),
        encoding="utf-8",
    )
    rules_dir = tk_dir / "rules"
    rules_dir.mkdir(exist_ok=True)
    (rules_dir / "coding.md").write_text(
        "---\ndescription: Coding rule\n---\n# Coding\nFollow these.", encoding="utf-8"
    )

    return base


@pytest.fixture(scope="session")
def workbench_repo(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Clone the ai workbench repo once per session."""
    cache_dir = tmp_path_factory.mktemp("cached_ai_workbench")
    storage = git.get_fresh_repo_files(
        DEFAULT_AI_WORKBENCH_REPO,
        str(cache_dir),
        branch=DEFAULT_AI_WORKBENCH_BRANCH,
    )
    # NOTE: make another copy if DEFAULT_AI_WORKBENCH_REPO is a local folder not to drop it
    target = cache_dir / "workbench_copy"
    shutil.copytree(Path(storage.storage_path), target)
    return str(target)
