---
name: Test Coverage Check
description: Ensures new features and bug fixes include appropriate tests, tests follow dlt's parallel-safe patterns, and test organization mirrors the source structure.
---

# Test Coverage Check

## Context

dlt has ~3,100+ tests organized in `tests/` mirroring the `dlt/` source structure. The project uses pytest with xdist for parallel execution. See `.claude/rules/testing.md` for the authoritative test conventions (module-based tests, parametrize with human-readable ids, parallel safety, destination test patterns, cross-platform requirements).

## Understand what the PR and issue is about

### 1. Fetch PR metadata and diff

Use `gh` to collect all PR information in parallel:

- `gh pr view PR_ID --json title,body,author,state,baseRefName,headRefName,number,url,comments,reviews,labels,milestone`
- `gh pr diff PR_ID`

### 2. Analyze related issues

Extract any issue references from the PR body (e.g. `fixes #1234`, `closes #1234`, `#1234`). For each referenced issue, fetch it with:

- `gh issue view <number> --json title,body,author,state,comments,labels`

Understand the original problem being solved.

### 3. Analyze the code

- Read the changed files in full to understand context beyond the diff.
- Ask yourself: what test coverage is necessary for this code

### 4. Pull existing tests on the topic

## What to Check

#### 1. Define the testing scope

Based on the PR changes, identify what needs to be tested:

- What are the distinct behaviors introduced or modified?
- What are the edge cases and error paths?
- What are the integration boundaries (e.g., does the change affect how components interact)?

Produce a checklist of required test scenarios before evaluating existing tests.

#### 2. Evaluate existing tests against the checklist

- Compare the PR's tests against the required scenarios checklist. Flag any gaps.
- Flag any tests that verify things outside the PR's scope or test obvious/trivial behavior that adds no value.

#### 3. Verify tests are in the right modules

- Search the test directory for where similar functionality is already tested (grep for related function names, class names, or feature keywords).
- New tests should live alongside related existing tests, not in new files unless the PR introduces an entirely new module.
- Flag tests placed in the wrong location.

#### 4. Check for duplication and over-testing

- Look for tests that cover nearly identical cases and could be merged or parametrized with `@pytest.mark.parametrize`.
- Look for repeated setup/assertion logic across tests that should be extracted into a helper or fixture.
- If multiple tests differ only in a single variable (e.g., `True`/`False`, different type values), they must be parametrized into one test function.
- Flag any copy-paste test code. Minimizing test code maintenance burden is a priority.

### 5. Bug Fixes Should Have Regression Tests
If the PR fixes a bug:
- Is there a test that would have caught the bug before the fix?
- Does the test verify the specific scenario from the bug report?
