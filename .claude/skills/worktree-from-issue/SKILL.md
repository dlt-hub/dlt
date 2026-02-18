---
name: worktree-from-issue
description: Create a git worktree with a new branch for implementing a fix or feature for a GitHub issue
argument-hint: <category>/<issue-number>-<short-description>
---

# Git Worktree for Issue Implementation

Set up an isolated git worktree with a new branch to implement a fix or feature for a GitHub issue.

Parse `$ARGUMENTS` to extract the branch name in format `<category>/<issue-number>-<short-description>`.

## Branch Naming Convention

Branches must follow the pattern from CONTRIBUTING.md:

```
{category}/{ticket-id}-description-of-the-branch
```

Categories: **feat**, **fix**, **exp**, **test**, **docs**, **keep** (all lowercase, dashes, no underscores).

For `feat` and `fix`, a ticket number is mandatory.

Examples:
- `fix/3562-clickhouse-drop-sync-perf`
- `feat/1234-add-new-destination`

## Steps

### 1. Parse and validate

Extract from `$ARGUMENTS`:
- `branch_name`: Full branch name (e.g., `fix/3562-clickhouse-drop-sync-perf`)
- `category`: Prefix before `/` (e.g., `fix`)
- `issue_number`: Number after `/` and before first `-` (e.g., `3562`)
- `short_name`: Directory-friendly version: `{category}-{issue_number}-{first-few-words}` (e.g., `fix-3562-clickhouse-drop-sync`)

Validate the branch name matches pattern `(feat|fix|exp|test|docs|keep)/[0-9]+-[a-z0-9-]+`.

### 2. Verify the issue exists

```
gh issue view <issue_number> --json number,title,state
```

Warn if the issue doesn't exist or is closed, but continue.

### 3. Fetch latest devel

Fetch the newest devel from origin so the branch starts from the latest code:

```
git fetch origin devel
```

### 4. Create the worktree

Use `/create-worktree` skill:

```
/create-worktree <short_name> --branch origin/devel
```

Then create the feature branch inside the worktree:

```
git -C <worktree-path> checkout -b <branch_name>
```

### 5. Set up dev environment

Use `/worktree-make-dev` skill to set up the development environment in the worktree. This runs `make dev` and copies `secrets.toml`.

Note which secrets source was used — this determines what destinations can be tested.

### 6. Verify cwd

After dev setup, verify the cwd is still in the worktree:

```
pwd
```

If `pwd` does not show the worktree path, run `cd <worktree-path>` and verify again. Stop with an error if the cwd cannot be set.

### 7. Report

Report the worktree info and available test destinations:

```
Worktree ready: <worktree-path>
Branch: <branch_name> (based on origin/devel)
Issue: #<issue_number> - <issue_title>
```

Include the test credential availability from `/worktree-make-dev` output — downstream skills need this to know which destinations can be tested.
