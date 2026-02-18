---
name: Documentation Sync Check
description: Ensures that user-facing changes to behavior, APIs, configuration, or destinations are accompanied by corresponding documentation updates in docs/website/.
---

# Documentation Sync Check

## Context

dlt's documentation lives in `docs/website/` as a Docusaurus site. The CI already detects whether a PR has "changes outside docs" to decide if tests should run, but there is no automated check that code changes that affect user-facing behavior are accompanied by documentation updates.

This documentation must follow existing documentation rules and associated skills. See `.claude/rules/documentation.md` for formatting requirements (snippet linting, code fence language tags, frontmatter).

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

### 4. Pull existing documentation on the topic

### 5. Recall BREAKING CHANGE rules for public APIs and behaviors

See `.claude/rules/public-api.md` for the authoritative definition of public API, breaking changes, and deprecation policy.

## What to Check

### 1. Changes to public API
If the PR modifies public API (new parameters, changed behavior, new configuration options, new functions/classes exposed to users):

- Search docs for existing documentation covering the topic of the PR.
- If the topic is already documented and the PR changes its behavior or adds new options, verify that the PR also updates the relevant doc pages.
- If the topic is not documented at all, this is not a blocker — don't require docs for previously undocumented internals.
- Run `python tools/check_api_breaking.py check` to detect breaking changes (renamed/removed public symbols, changed signatures). If it exits with code 1, breaking changes were found — these **must** have corresponding documentation updates and migration notes.

### 2. Behavioral Changes

If the PR changes documented behavior FOLLOW the public API rules - always identify existing documentation and make sure it still correspond to the code
