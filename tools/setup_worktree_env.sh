#!/usr/bin/env bash
# Set up a dev environment in a git worktree:
#   1. Run make dev
#   2. Copy test secrets (repo root or dev template fallback)
#
# Usage: tools/setup_worktree_env.sh <worktree-path>

set -euo pipefail

WORKTREE="${1:?Usage: $0 <worktree-path>}"
WORKTREE="$(cd "$WORKTREE" && pwd)"

# find the main worktree (repo root) -- the first entry in `git worktree list`
REPO_ROOT="$(git -C "$WORKTREE" worktree list --porcelain | head -1 | sed 's/^worktree //')"
if [ "$REPO_ROOT" = "$WORKTREE" ]; then
    # we ARE the main worktree, no separate repo root to copy from
    REPO_ROOT=""
fi

echo "==> Worktree: $WORKTREE"
echo "==> Repo root: $REPO_ROOT"

# 1. make dev
echo "==> Running make dev..."
(cd "$WORKTREE" && make dev)

# 2. copy secrets
SECRETS_DST="$WORKTREE/tests/.dlt/secrets.toml"
mkdir -p "$(dirname "$SECRETS_DST")"

SECRETS_SRC_REPO="${REPO_ROOT:+$REPO_ROOT/tests/.dlt/secrets.toml}"
SECRETS_SRC_DEV="$WORKTREE/tests/.dlt/dev.secrets.toml"

if [ -n "$SECRETS_SRC_REPO" ] && [ -f "$SECRETS_SRC_REPO" ]; then
    cp "$SECRETS_SRC_REPO" "$SECRETS_DST"
    echo "==> secrets.toml: copied from repo root (full credentials)"
    echo "SECRETS_SOURCE=repo_root"
elif [ -f "$SECRETS_SRC_DEV" ]; then
    cp "$SECRETS_SRC_DEV" "$SECRETS_DST"
    echo "==> secrets.toml: copied from dev template (local only)"
    echo "SECRETS_SOURCE=dev_template"
else
    echo "==> WARNING: no secrets source found"
    echo "SECRETS_SOURCE=none"
fi

echo "==> Done"
