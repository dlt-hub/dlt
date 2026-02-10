# Working in a git worktree

When a worktree is active (created via `/create-worktree`, `/worktree-issue`, or the user says "work in worktree X"), apply these rules for ALL subsequent operations.

Let `WORKTREE` = the absolute worktree path (e.g., `/home/user/src/dlt/.worktrees/fix-3562`).

## Shell cwd resets every call

Claude Code resets Bash cwd to the project root after **every** Bash call. `cd` does not persist.

- **Bash**: `cd WORKTREE && <command>` — required every time
- **git**: `git -C WORKTREE <command>` also works
- **Read/Grep/Glob/Edit/Write**: always use absolute paths under `WORKTREE`

## Running Python and tests

Always use `uv run` in worktrees — never bare `python` or `pytest`:

```
cd WORKTREE && uv run pytest tests/... -v
cd WORKTREE && uv run python -c "..."
```

## Worktree location convention

All worktrees live under `<repo-root>/.worktrees/<name>`.
