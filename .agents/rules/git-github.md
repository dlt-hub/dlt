---
name: git-github
description: Rules for using git and the GitHub CLI in dlt. Read when committing, creating PRs, or running any git/gh command.
---

# dlt — Git & GitHub

Rules for git and GitHub CLI usage in this repo, for any coding agent. They override the agent's defaults where they differ. See `CONTRIBUTING.md` for the human-facing version.

## When to use

Apply whenever running `git` or `gh`, drafting commit messages, or creating/updating PRs.

## Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
{type}: short imperative subject
# or, when one area is in focus
{type}({scope}): short imperative subject
```

- **Types** (lowercase, matching branch categories): `feat`, `fix`, `docs`, `test`, plus `refactor`/`chore` for non-functional changes.
- **Scope** is optional — an area like `dashboard`, `cli`, `destinations`, `extract`, `normalize`. Comma-separate multiple.
- **Subject is lowercase, imperative, no trailing period.**
- **No emojis** anywhere in the message — not in the subject, not in the body.
- **Subject line only for the vast majority of commits.**
- **Max 3 lines total** including any body. If you can't say it in 3 lines, the commit is probably too big — split it.
- **Code-comment rules apply to commit bodies too.** Don't explain *what* the diff does (the diff shows that). Add a body line only when the *why* is non-obvious and not already in the PR description.
- **No footers.** No `Co-Authored-By` lines, no "Generated with ..." trailers. Treat the agent as a dev tool, not a co-author. This overrides the agent's default.
- **No references to the task/ticket/caller** in the message unless the user asks ("as discussed", "per review feedback", etc.).

Format (HEREDOC):

```
git commit -m "$(cat <<'EOF'
fix(dashboard): keep the selector when no pipeline is selected
EOF
)"
```

## Cleaning up before a squash-merge

GitHub hides everything after the first line in the commit list, so multi-line
bodies and footers are easy to overlook — and squashing several such commits
floods the merge commit. **When squash-merging, clean the squash message down to
a single Conventional-Commit subject line** (plus a short *why* body only if it
genuinely helps). Never let `Co-Authored-By` / "Generated with" lines survive
into the merge.

## When to commit

- **Only when the user explicitly asks.** Finishing a task is not a trigger; "looks good" is not a trigger. Wait for an explicit "commit this" / "make a commit".
- `git add -A` is fine — but NEVER stage anything secret-looking (`.env`, `*secrets.toml`, credentials).

## Branches

From `CONTRIBUTING.md` — all lowercase, dashes, no underscores:

```
{category}/{ticket-id}-description-of-the-branch
# example:
feat/4922-add-avro-support
```

- **Categories**: `feat` (ticket required), `fix` (ticket required), `exp`, `test`, `docs`, `keep`.
- **Branch from `devel`.** Feature branches and most fixes go to `devel`.
- Don't create branches unless asked. Don't switch the branch a worktree is on without asking.

## Pull requests

- PRs **target `devel`**. Agents may **NEVER** merge to `master`.
- **Link the PR to its ticket**, or describe the change clearly enough for someone without prior context.
- **Creating a PR requires the branch to be pushed already.** If `gh pr create` would need a push, stop and ask the user to push.
- Confirm the title and body with the user before running `gh pr create`.

## Hard rules for the coding agent (never without an explicit ask)

- **Don't push.** No `git push`/`git push -u`. The user pushes.
- **Don't merge PRs.** No `gh pr merge`, no auto-merge.
- **Don't force-push, ever.** If history needs rewriting, describe it and let the user do it.
- **Don't `--amend` pushed commits**, and never use `--no-verify` or skip hooks/signing.
- **Don't touch `master`** — it's the release branch (`devel → master` on release day).

## Safe read-only commands (no confirmation needed)

- `git status`, `git diff`, `git log`, `git show`, `git branch`, `git remote -v`
- `gh pr view`, `gh pr list`, `gh pr checks`, `gh pr diff`, `gh issue view`, `gh api` for GET requests

## Quick checklist before any git/gh action

1. Push, merge, force-push, or amend of a pushed commit? → **Stop**, the user does this.
2. New commit? → Did the user explicitly ask? If no, **stop**.
3. Is the subject `type:` / `type(scope):`, lowercase, 1–3 lines total, free of "what" narration? If no, rewrite.
4. Is the commit free of footers (no `Co-Authored-By`, no "Generated with") and emojis? If no, trim.
5. Squash-merging? → Clean the squash message to a single subject line.
