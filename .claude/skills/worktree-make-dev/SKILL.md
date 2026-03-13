---
name: worktree-make-dev
description: Set up a dev environment in an existing worktree (make dev + copy secrets)
argument-hint: <worktree-path>
---

# Set up dev environment in worktree

Prepare the worktree at `$ARGUMENTS` for development.

## Steps

### 1. Run setup script

```
bash tools/setup_worktree_env.sh <worktree-path>
```

The script runs `make dev` and copies secrets without exposing credentials in the conversation. If it fails, report the error and stop.

### 2. Report

Parse the script output for `SECRETS_SOURCE=` to determine what was used:

- **`repo_root`**: Full credentials available — all destinations including remote (snowflake, bigquery, etc.)
- **`dev_template`**: Local only — duckdb, dummy, filesystem, and container-based destinations (postgres, clickhouse, etc.)
- **`none`**: No secrets found — warn the user

```
Dev environment ready: <worktree-path>
- make dev: done
- secrets.toml: <source>
```
