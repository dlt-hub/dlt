import json
import os
import shutil
from pathlib import Path
from typing import List, Optional

AI_CASES_DIR = os.path.join(os.path.dirname(__file__), "cases")

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
    mcp_servers: Optional[List[str]] = entry.get("mcp_servers")
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


MOCK_AGENTS_MD_TEMPLATE = (
    "## ALWAYS ACTIVATE those skills\nthey are essential for ANY work in this project\n"
)


def _ensure_init_agents_template(base: Path) -> None:
    """Create init/AGENTS.md template in workbench base if missing."""
    init_dir = base / "init"
    agents_md = init_dir / "AGENTS.md"
    if not agents_md.exists():
        init_dir.mkdir(parents=True, exist_ok=True)
        agents_md.write_text(MOCK_AGENTS_MD_TEMPLATE, encoding="utf-8")


def make_mock_toolkit(toolkit_name: str = "test-toolkit", with_mcp: bool = False) -> Path:
    """Copy mock_toolkit case into repo/<toolkit_name>, patch name in plugin.json."""
    toolkit_dir = Path("repo") / toolkit_name
    _ensure_init_agents_template(toolkit_dir.parent)
    shutil.copytree(os.path.join(AI_CASES_DIR, "mock_toolkit"), toolkit_dir)
    # patch toolkit name
    plugin_json = toolkit_dir / ".claude-plugin" / "plugin.json"
    meta = json.loads(plugin_json.read_text(encoding="utf-8"))
    meta["name"] = toolkit_name
    plugin_json.write_text(json.dumps(meta), encoding="utf-8")
    if not with_mcp:
        (toolkit_dir / "mcp.json").unlink()
    return toolkit_dir


def make_mock_workbench() -> Path:
    """Copy the mock_workbench case into cwd and return the base path."""
    base = Path("workbench")
    shutil.copytree(os.path.join(AI_CASES_DIR, "mock_workbench"), base)
    return base


def make_versioned_workbench(version: str = "1.0.0") -> Path:
    """Copy the versioned_workbench case into cwd, patch my-toolkit version."""
    base = Path("workbench")
    shutil.copytree(os.path.join(AI_CASES_DIR, "versioned_workbench"), base, dirs_exist_ok=True)
    # patch the version in plugin.json
    plugin_json = base / "my-toolkit" / ".claude-plugin" / "plugin.json"
    meta = json.loads(plugin_json.read_text(encoding="utf-8"))
    meta["version"] = version
    plugin_json.write_text(json.dumps(meta), encoding="utf-8")
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
