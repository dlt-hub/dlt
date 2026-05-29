# Pre-push local verification

Optional pre-push checks via [prek](https://github.com/pre-commit/prek): run `make lint` and/or
`make test-common-p` before you push, but only when files in each check’s scope have changed since
the last successful run.

The hook is **opt-in**. Without `.prek/local.toml`, the pre-push hook does nothing.

## Quick start

1. Copy `local.example.toml` to `local.toml` and set each check `mode` (`off`, `auto`, or `confirm`).
2. From the repo root: `make setup-hooks`
3. Push as usual. Bypass once: `git push --no-verify`
4. Preview without pushing: `make prek-dry`. Run the gate manually: `make prek`
5. Remove the hook: `make uninstall-hooks`

## Prerequisites

- Root dev env: `make dev` (for `make lint` and `make test-common-p`)
- Docs lint is part of `make lint`. Set up the docs project once: `cd docs && make dev`
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

**Confirm prompts** look like: `Run make lint before push? [Y/n] `

- Enter or `y` / `yes` → run the check
- `n` / `no` → abort push
- Non-interactive stdin (no TTY) → treated as declined (push blocked)

## What runs

Checks run in order; a failed lint blocks tests.

| Check | Make target | Command recorded in state |
|-------|-------------|---------------------------|
| `lint` | `lint` | `make lint` |
| `test_common_p` | `test-common-p` | `make test-common-p` |

`make lint` includes root linters plus docs lint (`cd docs && make lint`).

A check runs only when its **fingerprint** (hash of tracked files in scope) differs from the last
successful entry in `.prek/.state.toml` (also gitignored). Passing updates state for that check only.

## Scopes (`scopes.toml`)

Defines which tracked files invalidate each check. Edit when adding new trees that should trigger
re-lint or re-test.

**Lint** — `dlt`, `tests`, `tools`, `docs` (`.py`, `.md`, `.ipynb`), plus root/docs config and
embedded-snippet lint setup files.

**Common tests** — `dlt` and selected `tests/*` suites (see `scopes.toml`), plus `pyproject.toml`,
`uv.lock`, `tests/conftest.py`, `tests/load/test_dummy_client.py`.

Inspect a fingerprint:

```bash
uv run python .prek/fingerprint.py lint
uv run python .prek/fingerprint.py test_common_p
```

## Makefile targets

| Target | Purpose |
|--------|---------|
| `make setup-hooks` | Install prek and the pre-push hook |
| `make uninstall-hooks` | Remove the pre-push hook |
| `make prek` | Run the gate now (same logic as on push) |
| `make prek-dry` | Show what would run; no make, no state update |

prek is installed with `uv tool install`, not as a repo dependency.

## Troubleshooting

**Hook never runs checks** — Ensure `.prek/local.toml` exists and at least one check has `mode` not
`off`. Run `make prek-dry` to see whether the gate is active and which checks are stale.

**Gate skipped** — With `only_when_pr_open = true`, there must be an open PR on the current branch.

**Docs lint fails** — Run `cd docs && make dev`, then `cd docs && make lint` to see errors.

**Want to re-run after a pass** — Delete the check’s section from `.prek/.state.toml`, or change a
file in that check’s scope.

**Stale fingerprint / wrong cache** — Same as above; state stores the last successful fingerprint
per check.

## Files in this directory

| File | Role |
|------|------|
| `README.md` | This guide |
| `local.example.toml` | Config template |
| `scopes.toml` | Fingerprint inputs per check |
| `gate.py` | Gate logic, prompts, make invocation, state |
| `fingerprint.py` | Scope hashing |
| `pre-push-gate.sh` | prek entrypoint |
| `prek.toml` | prek hook definition |
| `plan.md` | Maintainer notes / design sketch |

Gitignored: `local.toml`, `.state.toml`
