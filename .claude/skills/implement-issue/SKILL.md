---
name: implement-issue
description: Triage, plan, and implement a fix or feature for a GitHub issue end-to-end
argument-hint: <issue-number> [-- <hints or context from maintainer>]
---

# Implement Issue

Parse `$ARGUMENTS` to extract:
- Everything before the first `--` (or all of it if no `--`) is the **issue number**. Referred to as `ISSUE` below.
- Everything after the first `--` is **maintainer hints** — domain knowledge, suspected root cause, pointers to relevant code, or constraints. If absent, there are no hints.

## Steps

### 1. Fetch the issue

```
gh issue view ISSUE --json title,body,comments,labels,state,assignees,author,createdAt,milestone
```

Produce a concise summary: title, reporter, component/area involved, error message, reproduction steps, expected behavior.

### 2. Determine category and gather hints

#### Category

Derive the branch category from the issue labels:

| Label contains | Category |
|---|---|
| `bug` | `fix` |
| `enhancement`, `feature` | `feat` |
| `test` | `test` |
| `documentation` | `docs` |

If no matching label exists, ask the human for the category.

#### Hints

Collect implementation hints from all available sources, in priority order:

1. **Maintainer hints** from `$ARGUMENTS` (after `--`)
2. **Issue comments** — look for maintainer/contributor replies with technical guidance, root cause analysis, or implementation suggestions
3. **Issue body** — the reporter may include debugging details, stack traces, or configuration

If the combined hints from these sources provide sufficient direction (suspected root cause, relevant code areas, constraints), proceed directly to step 3 without asking the human.

If hints are insufficient — the issue is vague, the root cause is unclear, or there are multiple possible approaches — ask the human for guidance.

### 3. Research the big picture

Before jumping to a fix, verify assumptions. This step prevents wasted effort on the wrong solution.

#### 3a. Understand the feature area

Use Explore agents to thoroughly investigate the relevant code paths. Trace the flow from user-facing API to the point of failure. Read the code — don't guess.

#### 3b. Validate the reporter's setup

Check whether the reporter is using the library correctly:

- Is their configuration valid for the component they're using?
- Are there special conventions, settings, or patterns they should be using instead?
- Does the documentation describe the expected behavior differently?

#### 3c. Check external dependencies and integrations

If the issue involves specific external systems (destinations, sources, file formats, etc.):

- Read the relevant implementation in the codebase (factory, capabilities, client, etc.).
- Understand how the component interacts with the external system.
- If needed, consult external documentation via WebFetch/WebSearch.

#### 3d. Determine: bug or user error?

Based on research, conclude one of:

- **Bug**: The library has a defect. Proceed to step 4.
- **User error**: The reporter's configuration is wrong. Draft a response explaining the correct approach. Ask the human to confirm before posting.
- **Enhancement**: The library works as designed but could be improved. Discuss scope with the human before proceeding.

Report findings to the human before moving on.

### 4. Set up workspace

Before planning, create the working environment so plan-mode exploration runs against a clean branch.

#### 4a. Determine branch name

Use the category from step 2 and the issue number:
```
{category}/{issue-number}-{short-description}
```

Example: `fix/3529-dedup-sort-escape`

#### 4b. Create worktree and dev environment

Invoke the `/worktree-issue` skill with the branch name:
```
/worktree-issue {branch-name}
```

This creates the worktree, checks out a new branch from `origin/devel`, and runs `/worktree-make-dev`.

Note the worktree path — referred to as `WORKTREE` below.

Verify that the cwd is now inside the worktree:

```
pwd
```

If `pwd` does not show `WORKTREE`, run `cd WORKTREE` and verify again. Stop with an error if the cwd cannot be set. Once confirmed, subsequent Bash calls will run inside the worktree automatically.

### 5. Draft implementation plan

Enter plan mode and design a plan that **will pass the `/review-pr` skill**. The review-pr skill checks:

1. **Code quality**: Style conforming to `@CLAUDE.md`, no over-engineering, proper use of existing patterns and utilities.
2. **Test coverage**: Required scenarios identified, tests in the right location, no duplication, proper parametrization.
3. **Documentation**: Public API changes need doc updates.
4. **Backward compatibility**: Breaking changes flagged with deprecation path.
5. **Dependencies**: Changes to `pyproject.toml` flagged.

The plan must include:

- **Working environment**: Worktree path, branch name, and `cd` instruction (see `@.claude/rules/worktree.md`).
- **Root cause**: Precise code location and explanation.
- **Fix**: Minimal change following existing patterns in surrounding code.
- **Test**: A test that **fails without the fix and passes with it**. Place tests where similar functionality is already tested. Follow `@CLAUDE.md` test guidelines (parallel safety, platform independence, proper fixtures).
- **Test scope**: Identify the right test level — `make test-common` for extract/normalize/common work, destination-specific tests for load work. See `@CLAUDE.md` "Testing strategy" section.
- **Verification commands**: Exact selective pytest commands to run (specific test functions, not entire modules).
- **Files changed**: List with expected line counts.

Present the plan via ExitPlanMode for human approval.

### 6. Implement the fix

Apply the changes from the approved plan. Follow these rules:

- **Read before editing**: Always read the target file first. Never edit blind.
- **Minimal changes**: Only change what the plan specifies. Don't refactor surrounding code, add comments to unchanged code, or "improve" things not in scope.
- **Follow local patterns**: Match the style of surrounding code exactly — imports, naming, spacing, comment style.
- **Type hints**: All new function signatures need full type hints with generic parametrization (`Dict[str, Any]` not `dict`).
- **Test conventions**: `def test_*() -> None:`, pytest fixtures, `@pytest.mark.parametrize` for variants.

### 7. Verify

Run verification in this order. Stop and fix issues before proceeding to the next step.

#### 7a. Run the new test selectively

Run only the new/changed test functions using the commands from the plan. Be selective — target specific test functions, not entire modules.

#### 7b. Regression proof (for external system interactions)

When the fix involves an external system (destination, source, API) where you don't have full visibility into the behavior — a "black box" — verify experimentally that the test actually catches the bug:

- Temporarily revert the fix
- Run the new test on a configuration where the bug manifests
- Confirm it **fails** with an error matching the issue report
- Restore the fix

This is valuable when you cannot fully predict how the external system handles the input (e.g., how a database treats unquoted identifiers, how an API responds to edge-case payloads). It proves the test is meaningful and not a no-op.

Skip this step when the code path is fully internal and you can reason about correctness from the code alone.

#### 7c. Format and lint

```
make format && make lint
```

Both must pass cleanly. Fix any issues before reporting.

### 8. Report

Summarize results:

```
## Fix for #ISSUE: <title>

**Branch**: {branch-name}
**Worktree**: WORKTREE

### Changes
- `<file>`: <description> (<lines> lines)
- ...

### Verification
- <test-function> on <config>: passed
- <test-function> on <config> without fix: FAILED (confirms bug) [if applicable]
- make format: pass
- make lint: pass

Ready to commit.
```

Wait for the human to confirm before committing.
