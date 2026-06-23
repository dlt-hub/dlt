#!/usr/bin/env bash
# Cursor beforeShellExecution hook: inject the repo's git/commit rules as agent
# context before a git/gh command runs. We deliberately emit no `permission` key
# so Cursor's normal allow/ask flow is unchanged — this only adds guidance.
set -euo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.command // ""')
root=$(git rev-parse --show-toplevel 2>/dev/null || echo "")
rule="$root/.agents/rules/git-github.md"

case "$cmd" in
  git\ *|gh\ *)
    if [ -n "$root" ] && [ -r "$rule" ]; then
      jq -n --rawfile c "$rule" '{agent_message:$c}'
    else
      printf '{}\n'
    fi
    ;;
  *)
    printf '{}\n'
    ;;
esac
