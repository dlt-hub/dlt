# Pre-push local verification

Optional pre-push checks via [prek](https://github.com/pre-commit/prek): run `make fl` and/or
`make test-common-p` before you push, but only when files in each check’s scope have changed since
the last successful run.

The hook is **opt-in**. Without `.prek/local.toml`, the pre-push hook does nothing.

## Quick start

1. Copy `local.example.toml` to `local.toml` and set each check `mode` (`off`, `auto`, or `confirm`).
2. From the repo root: `make install-prepush-hooks`
3. Push as usual. Bypass once: `git push --no-verify`
4. Preview without pushing: `make prek-dry`. Run the gate manually: `make prek`
5. Remove the hook: `make uninstall-prepush-hooks`

## Prerequisites

- Root dev env: `make dev` (for `make fl` and `make test-common-p`)
- Docs Python env: `cd docs && make dev` (once). `make fl` also runs `npm install` in `docs/website` for Biome.
- Optional `[gate] only_when_pr_open = true`: requires [GitHub CLI](https://cli.github.com/) (`gh auth login`)

## Configuration (`local.toml`)

Copy from `local.example.toml`. Gitignored — per-developer only.

| Section | Keys | Meaning |
|---------|------|---------|
| `[gate]` | `only_when_pr_open` | If `true`, skip all checks unless the current branch has an open PR (`gh pr view`) |
| `[lint]` | `mode` | How to handle stale lint scope |
| `[test_common_p]` | `mode` | How to handle stale common-test scope |

### Modes

| Mode | When scope is stale |
|------|---------------------|
| `off` | Never run this check |
| `auto` | Run the make target |
| `confirm` | Ask on the terminal; declining aborts the push |

**Confirm prompts** look like: `Run make fl before push? [Y/n] `

- Enter or `y` / `yes` → run the check
- `n` / `no` → abort push
- Non-interactive stdin (no TTY) → treated as declined (push blocked)

## What runs

Checks run in order; a failed lint blocks tests.

| Check | Make target | Command recorded in state |
|-------|-------------|---------------------------|
| `lint` | `fl` | `make fl` |
| `test_common_p` | `test-common-p` | `make test-common-p` |

`make fl` runs format (root, docs, website deps) in parallel, then root and docs lint in parallel.

A check runs only when its **fingerprint** (hash of tracked files in scope) is not among the
last 50 successful passes stored in `.prek/.state.toml` (also gitignored). Each pass records
`fingerprint`, `passed_at`, and `command`. That history lets branch switches and reverts reuse
a prior pass when the scoped tree matches again. Passing prepends a record and trims the list
to 50 entries per check.

Example:

```toml
[[lint.passes]]
fingerprint = "abc123..."
passed_at = "2026-05-29T12:00:00+00:00"
command = "make fl"
```

After `make install-prepush-hooks`, successful `make fl` and `make test-common-p` also update
state (no extra commands). Plain `make lint` does not update prek state.

## Unstaged changes on push

prek may stash unstaged edits to `~/.cache/prek/patches/` while the hook runs, then restore them.
Built-in prek behavior (from pre-commit), not configurable. Keeps lint/tests from failing on WIP you
are not pushing.

## Scopes (`pyproject.toml`)

Defines which tracked files invalidate each check (`[tool.prek.scopes.lint]` and
`[tool.prek.scopes.test_common_p]`). Edit when adding new trees that should trigger
re-lint or re-test.

**Lint** — `dlt`, `tests`, `tools`, `docs` (`.py`, `.md`, `.ipynb`), plus root/docs config and
embedded-snippet lint setup files.

**Common tests** — `dlt` and selected `tests/*` suites (see `[tool.prek.scopes]` in
`pyproject.toml`), plus `pyproject.toml`,
`uv.lock`, `tests/conftest.py`, `tests/load/test_dummy_client.py`.

Inspect a fingerprint:

```bash
uv run python -m tools.prek fingerprint lint
uv run python -m tools.prek fingerprint test_common_p
```

## Makefile targets

| Target | Purpose |
|--------|---------|
| `make install-prepush-hooks` | Install prek pre-push hook and enable state recording (fails if another pre-push hook exists) |
| `make uninstall-prepush-hooks` | Remove the prek pre-push hook (no-op if none; fails if hook is not from prek) |
| `make prek` | Run the gate now (same logic as on push) |
| `make prek-dry` | Show what would run; no make, no state update |
| `make fl` | Format root + docs (parallel), then lint root + docs (parallel) |

prek is installed with `uv tool install`, not as a repo dependency.

## Troubleshooting

**Existing pre-push hook** — `make install-prepush-hooks` refuses to install if `.git/hooks/pre-push`
already exists and is not from prek. prek cannot share the hook file with another tool. Remove or
relocate your hook first, or skip prek setup for now.

**Uninstall with a foreign hook** — `make uninstall-prepush-hooks` only removes a prek-managed hook.
If `.git/hooks/pre-push` exists but was not installed via `make install-prepush-hooks`, uninstall
refuses to run so your hook is not deleted.

**Hook never runs checks** — Ensure `.prek/local.toml` exists and at least one check has `mode` not
`off`. Run `make prek-dry` to see whether the gate is active and which checks are stale.

**Gate skipped** — With `only_when_pr_open = true`, there must be an open PR on the current branch.

**Docs lint fails** — Run `cd docs && make dev`, then `make fl` (or `cd docs && make format && make lint`).

**Want to re-run after a pass** — Delete the check’s section from `.prek/.state.toml`, or change a
file in that check’s scope.

**Stale fingerprint / wrong cache** — Delete the check’s section from `.prek/.state.toml`, or change a
file in that check’s scope. State keeps up to 50 pass records per check for branch hopping.

## Files in this directory

| File | Role |
|------|------|
| `README.md` | This guide |
| `local.example.toml` | Config template |
| `prek.toml` | prek hook definition (`uv run python -m tools.prek`) |

Implementation and tests: `tools/prek.py` (run via `python -m tools.prek`).

Gitignored: `local.toml`, `.state.toml`, `.enabled`
