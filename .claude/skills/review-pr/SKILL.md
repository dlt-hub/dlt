---
name: review-pr
description: Analyze a GitHub pull request including diff, comments, related issues, and local code context
argument-hint: <pr-url-or-number> [-- <optional instructions>]
---

# Review Pull Request

Parse `$ARGUMENTS` to extract the PR identifier and optional reviewer instructions:
- Everything before the first `--` (or all of it, if there is no `--`) is the **PR identifier** (URL or number). Referred to as `PR_ID` below.
- Everything after the first `--` is **reviewer instructions** — additional focus areas or context from the caller. If absent, there are no special instructions.

Analyze the pull request: PR_ID

## Steps

Run the following steps. Gather data in parallel where possible.

### 1. Set up worktree

Resolve the PR number:

```
gh pr view PR_ID --json number
```

Use the number to form the worktree name `review-pr-<number>`.

Invoke the `/create-worktree review-pr-<number> --pr <number>` skill to create an isolated worktree for this PR. Note the worktree path from the skill output — referred to as `WORKTREE` below.

Verify that the cwd is now inside the worktree:

```
pwd
```

If `pwd` does not show `WORKTREE`, run `cd WORKTREE` and verify again. Stop with an error if the cwd cannot be set. Once confirmed, subsequent Bash calls will run inside the worktree automatically.

### 2. Fetch PR metadata and diff

Use `gh` to collect all PR information in parallel:

- `gh pr view PR_ID --json title,body,author,state,baseRefName,headRefName,number,url,comments,reviews,labels,milestone`
- `gh pr diff PR_ID`

### 3. Check dependency changes in `pyproject.toml`

If the PR diff (from step 2) touches `pyproject.toml`, run the automated dependency checker.

First, fetch the latest `devel` from origin so the comparison is against the current state:

```
git fetch origin devel
```

Then run the script — always compare against `origin/devel` regardless of the PR's base branch:

```
python tools/check_dependency_changes.py origin/devel
```

The script finds the merge-base, simulates a three-way merge (like GitHub), and reports:
- **Main dependencies** (`[project.dependencies]`) — displayed as WARNING (these affect every user)
- **Optional dependencies** (`[project.optional-dependencies]`) — displayed as WARNING (these affect users of specific extras)
- **Dev/group dependencies** (`[dependency-groups]`) — listed for awareness (no impact on end users)
- **Merge conflicts** — if `pyproject.toml` would conflict, the script warns and falls back to showing the PR's intended changes

Include the script output verbatim in the review. If the script exits with code 1 (changes detected), flag dependency changes in the review verdict. If it exits with code 2 (conflicts), flag that `pyproject.toml` needs conflict resolution.

If `pyproject.toml` is not in the diff, skip this step.

### 4. Check CI lint and mypy status

Use `gh pr checks PR_ID` to retrieve the CI check results. Look for checks matching `lint on all python versions / lint*` — these run mypy, ruff, flake8, bandit, and docstring checks across Python 3.9–3.13 (defined in `.github/workflows/lint.yml`).

- Report the status of each lint check (pass/fail/pending).
- If any lint check has failed, fetch its logs with `gh run view <run-id> --log-failed` to identify the specific errors (mypy type errors, ruff violations, etc.).
- Include lint/mypy status in the final review verdict. A failing lint check should be flagged as a required fix.

### 5. Analyze related issues

Extract any issue references from the PR body (e.g. `fixes #1234`, `closes #1234`, `#1234`). For each referenced issue, fetch it with:

- `gh issue view <number> --json title,body,author,state,comments,labels`

Understand the original problem being solved.

### 6. Analyze the code

Using absolute paths under `WORKTREE`:

- Read the changed files in full to understand context beyond the diff.
- Look at callers/callees of modified functions.
- Check if similar patterns exist elsewhere that should also be updated.
- Review the coding style and code patterns to conform with @CLAUDE.md
- If **reviewer instructions** were provided, pay special attention to the areas or concerns they describe during this analysis.
- Mention only crucial finding to the user in the initial overview. Avoid code snippets except if critical bugs were found

### 7. Review test coverage

#### 7a. Define the testing scope

Based on the PR changes, identify what needs to be tested:

- What are the distinct behaviors introduced or modified?
- What are the edge cases and error paths?
- What are the integration boundaries (e.g., does the change affect how components interact)?

Produce a checklist of required test scenarios before evaluating existing tests.

#### 7b. Evaluate existing tests against the checklist

- Compare the PR's tests against the required scenarios checklist. Flag any gaps.
- Flag any tests that verify things outside the PR's scope or test obvious/trivial behavior that adds no value.

#### 7c. Verify tests are in the right modules

- Search the test directory for where similar functionality is already tested (grep for related function names, class names, or feature keywords).
- New tests should live alongside related existing tests, not in new files unless the PR introduces an entirely new module.
- Flag tests placed in the wrong location.

#### 7d. Check for duplication and over-testing

- Look for tests that cover nearly identical cases and could be merged or parametrized with `@pytest.mark.parametrize`.
- Look for repeated setup/assertion logic across tests that should be extracted into a helper or fixture.
- If multiple tests differ only in a single variable (e.g., `True`/`False`, different type values), they must be parametrized into one test function.
- Flag any copy-paste test code. Minimizing test code maintenance burden is a priority.

#### 7e. Bug Fixes Should Have Regression Tests
If the PR fixes a bug:
- Is there a test that would have caught the bug before the fix?
- Does the test verify the specific scenario from the bug report?

### 8. Check documentation coverage

If the PR modifies public API (new parameters, changed behavior, new configuration options, new functions/classes exposed to users):

- Search `WORKTREE/docs/website/docs/` for existing documentation covering the topic of the PR.
- If the topic is already documented and the PR changes its behavior or adds new options, verify that the PR also updates the relevant doc pages.
- If documentation updates are missing, flag this in the review as a required change.
- If the topic is not documented at all, this is not a blocker — don't require docs for previously undocumented internals.

### 9. Produce the review

Write a structured analysis covering:

- **Summary**: What the PR does and why, referencing the linked issue.
- **Changes**: Breakdown of changes should be concise, avoid code samples
- **Dependency Changes**: If `pyproject.toml` was modified, include the warnings and listings from step 3. Main and optional dependency changes must appear as yellow WARNING blocks. Omit this section if no dependency changes were found.
- **CI Lint & Mypy**: Status of lint checks across Python versions. If any failed, list the specific errors.
- **Observations & Potential Issues**: Bugs, edge cases, missing error handling, performance concerns, memory issues, security issues, backward compatibility.
- **Breaking Changes and Public API**: Follow the rules on breaking changes and Public API and report issues.
- **Test Coverage**: Required scenarios checklist with pass/fail status, misplaced tests, duplication issues, parametrization opportunities.
- **Documentation**: Whether doc updates are needed and if they were provided.
- **Reviewer Instructions**: If reviewer instructions were provided, include a dedicated section addressing each point raised. If no instructions were provided, omit this section.
- **Verdict**: Overall assessment.
