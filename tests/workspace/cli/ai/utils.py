import json
import shutil
from pathlib import Path
from typing import Optional

import pytest
import yaml

from dlt.common.libs import git
from dlt._workspace.cli.ai.agents import AI_AGENTS
from dlt._workspace.cli.ai.utils import (
    DEFAULT_AI_WORKBENCH_BRANCH,
    DEFAULT_AI_WORKBENCH_REPO,
    compute_file_hash,
)
from dlt._workspace.typing import TToolkitIndexEntry, TToolkitInfo

# known toolkits in the repo (init is now visible)
KNOWN_TOOLKITS = ["data-exploration", "init", "rest-api-pipeline", "dlthub-runtime"]
INSTALLABLE_TOOLKITS = [t for t in KNOWN_TOOLKITS if t != "init"]
AGENT_NAMES = ["claude", "cursor", "codex"]


def assert_toolkit_install(
    project_root: Path,
    toolkit_name: str,
    agent_name: str,
    *,
    check_dependencies: bool = True,
) -> TToolkitIndexEntry:
    """Validate a toolkit was installed correctly: index, files, hashes, MCP.

    Returns the index entry for further assertions by the caller.
    """
    index_path = project_root / ".dlt" / ".toolkits"
    assert index_path.is_file(), ".toolkits index missing at %s" % index_path

    index = yaml.safe_load(index_path.read_text(encoding="utf-8"))
    assert toolkit_name in index, "%s not in index (keys: %s)" % (
        toolkit_name,
        list(index.keys()),
    )

    entry: TToolkitIndexEntry = index[toolkit_name]

    # required scalar fields
    assert entry.get("agent") == agent_name, "agent mismatch: %s" % entry.get("agent")
    assert entry.get("version"), "version is empty"
    assert entry.get("description"), "description is empty"
    assert entry.get("installed_at"), "installed_at is empty"

    # every tracked file must exist and its hash must match
    files = entry.get("files", {})
    assert isinstance(files, dict) and len(files) > 0, "files dict is empty"
    for rel_path, file_info in files.items():
        abs_path = project_root / rel_path
        assert abs_path.is_file(), "tracked file missing: %s" % abs_path
        expected_hash = file_info["sha3_256"]
        actual_hash = compute_file_hash(abs_path)
        assert actual_hash == expected_hash, "hash mismatch for %s" % rel_path

    # dependency toolkits must also be in the index
    if check_dependencies:
        for dep_name in entry.get("dependencies", []):
            assert dep_name in index, "dependency %s not in index" % dep_name

    # MCP servers recorded in index must exist in agent config
    mcp_servers: Optional[list] = entry.get("mcp_servers")  # type: ignore[assignment]
    if mcp_servers:
        agent = AI_AGENTS[agent_name]()
        mcp_path = agent.mcp_config_path(project_root)
        assert mcp_path.is_file(), "MCP config missing at %s" % mcp_path
        content = mcp_path.read_text(encoding="utf-8")
        parsed_servers = agent.parse_mcp_servers(content)
        for srv in mcp_servers:
            assert srv in parsed_servers, "MCP server %s not in config" % srv

    return entry


def make_mock_toolkit_info(
    name: str = "test-toolkit",
    version: str = "1.0.0",
    description: str = "",
    tags: None = None,
    workflow_entry_skill: str = "",
) -> TToolkitInfo:
    """Build a `TToolkitInfo` for tests."""
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
