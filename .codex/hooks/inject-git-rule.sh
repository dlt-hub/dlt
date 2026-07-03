#!/usr/bin/env bash
# Codex PreToolUse hook: inject the repo's git/commit rules as additionalContext
# before a git/gh shell command runs. Emits nothing for other commands, so the
# tool call proceeds unchanged.
set -euo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""')
root=$(git rev-parse --show-toplevel 2>/dev/null || echo "")
rule="$root/.agents/rules/git-github.md"

case "$cmd" in
  git\ *|gh\ *)
    if [ -n "$root" ] && [ -r "$rule" ]; then
      jq -n --rawfile c "$rule" \
        '{hookSpecificOutput:{hookEventName:"PreToolUse",additionalContext:$c}}'
    fi
    ;;
esac
exit 0
