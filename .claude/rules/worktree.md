# Working in a git worktree

When a worktree is active (created via `/create-worktree`, `/worktree-issue`, or the user says "work in worktree X"), apply these rules for ALL subsequent operations.

Let `WORKTREE` = the absolute worktree path (e.g., `/home/user/src/dlt/.worktrees/fix-3562`).

## Shell cwd behavior

Claude Code resets Bash cwd to the project root only when `cd` targets a path **outside** the project root. Paths **within** the project root (including `.worktrees/`) persist across calls.

- **Bash**: run `cd WORKTREE` once to switch; subsequent calls will stay there
- **git**: `git -C WORKTREE <command>` also works
- **Read/Grep/Glob/Edit/Write**: always use absolute paths under `WORKTREE`

## Running Python and tests

Always use `uv run` in worktrees — never bare `python` or `pytest`:

```
uv run pytest tests/... -v
uv run python -c "..."
```

## Worktree location convention

All worktrees live under `<repo-root>/.worktrees/<name>`.

## Plans created in a worktree

When creating a plan while working in a worktree, **always** include the worktree path at the top of the plan:

```markdown
## Working environment
- Worktree: <absolute worktree path>
- Branch: <branch name>
- Execute: cd <absolute worktree path> before any work
```

This is critical because plan→execute transitions start a new session whose cwd resets to the project root. Without the worktree path in the plan, the execute phase will modify files in the main repo instead of the worktree.
