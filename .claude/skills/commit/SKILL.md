---
name: commit
description: Stage changes and write a clean Conventional-Commit message that follows the repo's git rules
argument-hint: [-- <optional subject hint or scope>]
---

# Commit

Create a commit that follows `.agents/rules/git-github.md`. Invoke as `/commit`, or
use these steps whenever you are about to commit.

Parse `$ARGUMENTS`: anything after `--` is an optional hint (a subject, a scope, or
"only the dashboard files") — guidance, not the literal message.

## Steps

### 1. Inspect

```
git status
git diff --staged
git diff
```

Decide what belongs in the commit. If nothing is staged, stage the relevant changes
with `git add <paths>` (or `git add -A` when everything belongs together). **NEVER
stage secret-looking files** (`.env`, `*secrets.toml`, credentials) — they are also
denied in `settings.json`.

### 2. Compose the message

Apply `.agents/rules/git-github.md`:

- One line: `{type}: subject` or `{type}({scope}): subject` — lowercase, imperative,
  no trailing period. Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`.
- **Subject line only** for the vast majority of commits. Add a body line *only* for a
  non-obvious *why*, max 3 lines total. Never narrate *what* the diff does.
- **No footers** — no `Co-Authored-By`, no "Generated with ..." trailers.
- **No emojis** anywhere in the message.

### 3. Commit

```
git commit -m "$(cat <<'EOF'
fix(dashboard): keep the selector when no pipeline is selected
EOF
)"
```

### 4. Confirm

```
git log -1 --format=%B
```

Verify it is a single Conventional-Commit subject (plus an optional why) with no
footers. **Do not push and do not amend pushed commits** unless the user explicitly
asks.
