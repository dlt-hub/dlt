---
name: create-worktree
description: Create or reuse a git worktree for a pull request or branch so reviews and work happen in isolation
argument-hint: <name> [--pr <number>] [--branch <ref>]
---

# Git Worktree

Set up an isolated git worktree.

Parse `$ARGUMENTS` to extract:
- The worktree name (required) — used as the directory name under `.worktrees/`
- `--pr <number>` (optional) — checkout a GitHub PR into the worktree using `gh pr checkout`
- `--branch <ref>` (optional) — create the worktree from a specific branch or ref

## Steps

### 1. Determine worktree path

The worktree path is: `<repo-root>/.worktrees/<name>`

For example, if the repo root is `/home/user/src/dlt` and the name is `review-pr-3584`, the worktree path is `/home/user/src/dlt/.worktrees/review-pr-3584`.

Create the `.worktrees` directory if it does not exist:

```
mkdir -p <repo-root>/.worktrees
```

### 2. Create or reuse the worktree

Check if a worktree already exists at that path:

```
git worktree list
```

- **If a worktree already exists** at the target path, reuse it.
  - If `--pr` was given, run `cd <worktree-path> && gh pr checkout <number>` to make sure it is up to date.
  - If `--branch` was given, run `git -C <worktree-path> checkout <ref>`.
- **If no worktree exists** at the target path, create one:
  - If `--pr` was given, first resolve the PR's branch name: `gh pr view <number> --json headRefName -q .headRefName`
  - If `--pr` or `--branch` was given, check `git worktree list` output for any **other** worktree that already has the same branch checked out. If found:
    - Use `AskUserQuestion` to ask the user: "Branch `<branch>` is already checked out in worktree `<existing-path>`. Use that worktree instead?"
    - If the user says yes, use the existing worktree path (skip creation, proceed to step 3 with the existing path).
    - If the user says no, **STOP** — do not create a new worktree. Print a message explaining that the branch is already checked out elsewhere and exit.
  - If no conflict, create the worktree:
    - If `--branch` was given: `git worktree add .worktrees/<name> <ref>`
    - Otherwise: `git worktree add .worktrees/<name> --detach`
    - If `--pr` was given, then also: `cd <worktree-path> && gh pr checkout <number>`
- `gh pr checkout` handles fetching, fork tracking, and branch setup automatically.

### 3. Report

```
Worktree ready: <worktree-path>
```
