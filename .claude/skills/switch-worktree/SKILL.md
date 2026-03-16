---
name: switch-worktree
description: Switch the current session to work in an existing git worktree
argument-hint: <worktree-name>
---

# Switch to worktree

Switch the current session to an existing worktree.

## Steps

### 1. Resolve the worktree

If `$ARGUMENTS` is empty, list available worktrees and ask the user to pick one:

```
git worktree list
```

Otherwise, let `NAME` = `$ARGUMENTS`. The worktree path is `<repo-root>/.worktrees/<NAME>`.

Verify the worktree exists in `git worktree list` output. If not, stop with an error.

Let `WORKTREE` = the absolute path to the worktree.

### 2. Switch cwd

```
cd WORKTREE
```

Then verify the switch took effect:

```
pwd
```

If `pwd` does not show `WORKTREE`, stop with an error — the cwd did not persist.

### 3. Report

```
Switched to worktree: WORKTREE
Branch: <current branch from git branch --show-current>
```
