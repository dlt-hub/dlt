"""Plans and applies `ai toolkit update`, without CLI output.

An update renders a toolkit with the same agent transforms as an install and
compares every resulting file with the disk and with the hash recorded at
install time. Files the user edited or deleted and files not installed by the
toolkit are skipped unless forced. Nothing is ever deleted.

Note: `install --overwrite` hashes whole skill directories, so user files inside
them may be recorded as installed. Such a file counts as unchanged-since-install
and is overwritten if the toolkit later ships a file at the same path.
"""
from pathlib import Path
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Union,
)

from dlt._workspace.cli.dlthub.ai.agents import InstallAction, _AIAgent
from dlt._workspace.cli.dlthub.ai.planning import plan_mcp_actions, plan_toolkit_components
from dlt._workspace.cli.dlthub.ai.typing import TToolkitIndexEntry, TToolkitInfo
from dlt._workspace.cli.dlthub.ai.utils import (
    compute_file_hash,
    make_toolkit_entry,
    resolve_toolkit_dependencies,
    safe_copy_file,
    safe_write_text,
    save_toolkit_entry,
)

TFileState = Literal["new", "updated", "unchanged", "modified", "deleted", "untracked"]
TFileTracking = Literal["disk", "recorded", "none"]


class FileCandidate(NamedTuple):
    """A single file the toolkit wants in the project."""

    rel_path: str
    """Path relative to the project root with `/` separators, the key in the toolkits index."""
    dest_path: Path
    content: Union[str, Path]
    """Rendered text to save, or a source file to copy verbatim."""
    source: InstallAction
    """The component action that produces the file."""


class FileObservation(NamedTuple):
    """State of a candidate's destination on disk."""

    exists: bool
    matches: bool
    """Destination already has the desired content."""
    disk_hash: Optional[str]


class FileDecision(NamedTuple):
    state: TFileState
    write: bool
    tracking: TFileTracking
    """Hash kept in the index: of the file on disk after the update, the previously recorded one, or none."""


class FileUpdate(NamedTuple):
    candidate: FileCandidate
    decision: FileDecision


class ToolkitUpdatePlan(NamedTuple):
    """Everything an update of one toolkit will write, skip and keep."""

    updates: List[FileUpdate]
    orphans: List[str]
    """Tracked files the toolkit no longer ships. Left in place and kept tracked."""
    shared: List[InstallAction]
    """Merges into files all toolkits share: agent MCP config, Codex `AGENTS.md`."""
    warnings: List[str]
    """Validation warnings for skills, commands or rules that were left out."""


def to_rel_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def expand_file_candidates(
    actions: Iterable[InstallAction], project_root: Path
) -> Dict[str, FileCandidate]:
    """Flattens install actions into one candidate per destination file.

    `copytree` actions expand to the files of the source directory. When several
    actions target the same file the last one wins, e.g. the Codex save that
    caps a skill description replaces the verbatim copy of `SKILL.md`.
    """
    candidates: Dict[str, FileCandidate] = {}

    def _add(dest_path: Path, content: Union[str, Path], source: InstallAction) -> None:
        rel_path = to_rel_path(dest_path, project_root)
        candidates[rel_path] = FileCandidate(rel_path, dest_path, content, source)

    for action in actions:
        if action.op == "copytree":
            src_dir = Path(action.content_or_path)
            for src in sorted(src_dir.rglob("*")):
                if src.is_file():
                    _add(action.dest_path / src.relative_to(src_dir), src, action)
        else:
            _add(action.dest_path, action.content_or_path, action)
    return candidates


def observe_file(candidate: FileCandidate) -> FileObservation:
    """Reads the destination of `candidate`.

    Rendered text is compared after newline normalization so files written with
    platform line endings match; copied files are compared byte for byte.
    """
    dest = candidate.dest_path
    if not dest.exists() and not dest.is_symlink():
        return FileObservation(exists=False, matches=False, disk_hash=None)
    if not dest.is_file():
        # a directory or dangling symlink in the way: never matches, never clean
        return FileObservation(exists=True, matches=False, disk_hash=None)
    # compare content rather than hashes: text written on Windows has CRLF line endings,
    # so its disk hash never equals a hash of the rendered `str`
    if isinstance(candidate.content, Path):
        matches = dest.read_bytes() == candidate.content.read_bytes()
    else:
        try:
            matches = dest.read_text(encoding="utf-8") == candidate.content
        except UnicodeDecodeError:
            matches = False
    return FileObservation(exists=True, matches=matches, disk_hash=compute_file_hash(dest))


def write_candidate(candidate: FileCandidate) -> None:
    dest = candidate.dest_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(candidate.content, Path):
        safe_copy_file(candidate.content, dest)
    else:
        safe_write_text(dest, candidate.content)


def classify_file(
    observed: FileObservation, recorded_hash: Optional[str], force: bool
) -> FileDecision:
    """Decides whether to write a file and which hash the index keeps.

    Args:
        observed: State of the destination on disk.
        recorded_hash: Hash recorded when the toolkit last wrote the file, `None` if untracked.
        force: Write files that would otherwise be skipped.

    Returns:
        The decision. Skipped files keep the recorded hash so local edits stay detectable.
    """
    tracked = recorded_hash is not None
    if observed.matches:
        return FileDecision("unchanged", False, "disk")

    state: TFileState
    write: bool
    if not observed.exists:
        state, write = ("deleted", force) if tracked else ("new", True)
    elif not tracked:
        state, write = "untracked", force
    elif observed.disk_hash == recorded_hash:
        state, write = "updated", True
    else:
        state, write = "modified", force

    tracking: TFileTracking = "disk" if write else "recorded" if tracked else "none"
    return FileDecision(state, write, tracking)


def plan_file_updates(
    candidates: Iterable[FileCandidate],
    recorded_files: Mapping[str, Mapping[str, str]],
    force: bool,
) -> List[FileUpdate]:
    """Observes and classifies every candidate against the toolkit's recorded `files`."""
    updates: List[FileUpdate] = []
    for candidate in candidates:
        recorded = recorded_files.get(candidate.rel_path)
        decision = classify_file(
            observe_file(candidate), recorded["sha3_256"] if recorded else None, force
        )
        updates.append(FileUpdate(candidate, decision))
    return updates


def resolvable_dependencies(name: str, dep_map: Dict[str, List[str]]) -> List[str]:
    """Dependencies of `name` in install order, none when they are circular.

    Lets an update of all toolkits plan around a cycle: every toolkit that reaches
    the cycle reports it when it is updated itself, so it does not block the others.
    """
    try:
        return resolve_toolkit_dependencies(name, dep_map)
    except ValueError:
        return []


def toolkit_update_order(installed: Collection[str], dep_map: Dict[str, List[str]]) -> List[str]:
    """Installed toolkits, each after the installed toolkits it depends on."""
    order: List[str] = []
    for name in sorted(installed):
        for dep in [*resolvable_dependencies(name, dep_map), name]:
            if dep in installed and dep not in order:
                order.append(dep)
    return order


def failed_dependencies(
    name: str, dep_map: Dict[str, List[str]], failed: Collection[str]
) -> List[str]:
    """Dependencies of `name` among the `failed` toolkits, in install order."""
    return [dep for dep in resolvable_dependencies(name, dep_map) if dep in failed]


def present_components(updates: Iterable[FileUpdate]) -> List[InstallAction]:
    """Component actions with at least one file on disk after the update, in plan order.

    Skipped files do not count: a component the user deleted is not registered again
    in shared targets such as Codex `AGENTS.md`.
    """
    present = (
        update.candidate.source
        for update in updates
        if update.decision.write or update.decision.state == "unchanged"
    )
    # dict keeps the first occurrence of each action, unlike a set
    return list(dict.fromkeys(present))


def plan_toolkit_update(
    name: str,
    entry: TToolkitIndexEntry,
    toolkit_dir: Path,
    agent: _AIAgent,
    project_root: Path,
    force: bool,
) -> ToolkitUpdatePlan:
    """Plans the update of an installed toolkit from its workbench directory. Reads only.

    Args:
        name: Name of the installed toolkit.
        entry: The toolkit's entry in the toolkits index.
        toolkit_dir: The toolkit's directory in the fetched workbench.
        agent: The agent the toolkit was installed for.
        project_root: Root of the project the toolkit is installed in.
        force: Write files that would otherwise be skipped.

    Returns:
        The files to write and skip, orphaned files and merges into shared files.
    """
    components, warnings = plan_toolkit_components(
        toolkit_dir, agent, project_root, name, overwrite=True
    )
    # servers already in the agent config are never replaced, even when forced
    mcp_actions = plan_mcp_actions(toolkit_dir, agent, project_root, overwrite=False)

    recorded_files: Dict[str, Any] = entry.get("files", {})
    candidates = expand_file_candidates(components, project_root)
    updates = plan_file_updates(candidates.values(), recorded_files, force)
    orphans = sorted(set(recorded_files) - set(candidates))
    shared = mcp_actions + agent.shared_actions(
        present_components(updates), project_root, workbench_base=toolkit_dir.parent
    )
    return ToolkitUpdatePlan(updates, orphans, shared, warnings)


def apply_toolkit_update(plan: ToolkitUpdatePlan) -> None:
    """Writes the files and shared merges of `plan`. Does not touch the toolkits index."""
    for candidate, decision in plan.updates:
        if decision.write:
            write_candidate(candidate)
    for action in plan.shared:
        assert isinstance(action.content_or_path, str), "shared targets are rendered text"
        action.dest_path.parent.mkdir(parents=True, exist_ok=True)
        safe_write_text(action.dest_path, action.content_or_path)


def update_toolkit_entry(
    plan: ToolkitUpdatePlan,
    entry: TToolkitIndexEntry,
    toolkit_meta: TToolkitInfo,
    agent_name: str,
) -> None:
    """Records an applied `plan` in the toolkit's index entry.

    Nothing is saved when the entry would not change, so `installed_at` keeps the
    time of the last real change.
    """
    recorded_files: Dict[str, Any] = entry.get("files", {})
    files: Dict[str, Any] = {path: recorded_files[path] for path in plan.orphans}
    for candidate, decision in plan.updates:
        if decision.tracking == "disk":
            files[candidate.rel_path] = {"sha3_256": compute_file_hash(candidate.dest_path)}
        elif decision.tracking == "recorded":
            files[candidate.rel_path] = recorded_files[candidate.rel_path]

    mcp_servers = set(entry.get("mcp_servers", []))
    for action in plan.shared:
        if action.kind == "mcp":
            # source_name is ", ".join(sorted(new_servers))
            mcp_servers.update(s.strip() for s in action.source_name.split(",") if s.strip())

    updated = make_toolkit_entry(toolkit_meta, agent_name, files, sorted(mcp_servers))
    current = {key: value for key, value in entry.items() if key != "installed_at"}
    if updated != current:
        save_toolkit_entry(updated)
