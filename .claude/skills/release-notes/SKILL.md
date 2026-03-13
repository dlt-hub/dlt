---
name: release-notes
description: Generate release notes between two git tags with categorized PR summaries and author attribution
argument-hint: <git-tag> [-- <previous-tag>]
---

# Generate Release Notes

Parse `$ARGUMENTS` to extract:
- Everything before `--` is the **release tag** (e.g. `1.22.0`). Referred to as `TAG` below.
- Everything after `--` is an optional **previous tag** override. If absent, auto-detect the previous tag.

## Step 1: Validate the tag

Check that `TAG` exists as a git tag:

```
git tag -l TAG
```

If the tag does not exist, stop with an error: "Tag `TAG` not found. Please create the tag first and re-run."

## Step 2: Determine the previous tag

If a previous tag was provided in `$ARGUMENTS`, validate it exists and use it as `LOWER`.

Otherwise, find the previous tag automatically:

```
git tag --sort=-v:refname
```

Walk the sorted list to find the tag immediately before `TAG` (by version order). Set that as `LOWER`.

Validate `LOWER` exists. Stop with an error if no previous tag can be determined.

## Step 3: Gather commits

Get all commits between the two tags:

```
git log --oneline LOWER..TAG
```

Filter out:
- Merge commits (starting with "Merge pull request" or "Merge branch")
- Version bump commits (starting with "bumps to version" or "bumps to")

Extract PR numbers from the remaining commit messages (pattern `#NNNN`). Every PR number must appear in the final release notes — none may be silently dropped.

## Step 4: Fetch PR metadata

For each PR number, fetch metadata using `gh`. Use Task agents to run in parallel batches for efficiency:

```
gh pr view NUMBER --repo dlt-hub/dlt --json number,title,labels,body,author
```

Collect for each PR:
- **number**: PR number
- **title**: PR title
- **author**: `author.login`
- **labels**: list of label names
- **body**: PR description (first 500 chars sufficient for categorization)

### Handling low-quality PR titles or descriptions

If a PR title is vague (e.g. just an issue number, single word) or the body is empty/unhelpful:

1. Check if the PR references an issue (e.g. `fixes #NNN`, `closes #NNN`, `#NNN` in body). If so, fetch the issue:
   ```
   gh issue view NNN --repo dlt-hub/dlt --json title,body
   ```
2. If still unclear, look at the PR diff to understand what changed:
   ```
   gh pr diff NUMBER --repo dlt-hub/dlt
   ```
3. Write a clear, accurate summary based on what you find.

## Step 5: Categorize PRs

Assign each PR to exactly one category using these rules (evaluated in order). **Every PR must be classified** — verify the total count matches.

### Breaking Changes
Collect separately (appears at the top, not a standalone category — the PR also appears in its main category). A PR has breaking changes if:
- It has a `breaking` label, OR
- Its body contains "breaking change" (case-insensitive), OR
- Its title contains "breaking"

Extract the specific breaking change description from the PR body.

### Highlights
A PR is a highlight if:
- It has a `highlight` label, OR
- It is a significant new feature (title starts with `feat` or `(feat)`) AND the change is impactful — meaning it introduces a new user-facing capability, enables a major use case, or represents substantial effort
- Use judgment: not every feature is a highlight. A highlight should be something users would be excited about.

Highlighted PRs also appear in their main category (Core Library, etc.) but with a brief entry pointing to the Highlights section.

### Docs
- Title or body indicates documentation-only changes
- Files changed are only in `docs/`
- Has `documentation` label

### Chores
- CI/CD improvements, test infrastructure, repo maintenance
- Has labels like `ci full`, `tech-debt`, `ci`
- Title starts with `chore`, `repo:`, `ci:`, or the change only affects tests/tooling

### Core Library
Everything else — features, bug fixes, enhancements to the library itself.

## Step 6: Identify new contributors

For each release author, find their first ever merged PR:

```
gh search prs --repo dlt-hub/dlt --author AUTHOR --merged --sort created --order asc --limit 1 --json number,createdAt
```

A contributor is **new** if their first merged PR number falls within the `LOWER..TAG` commit range.

## Step 7: Format the release notes

Write the notes in this exact structure:

```markdown
## dlt TAG Release Notes

## Breaking Changes

(Only if there are breaking changes. Numbered list.)

1. **Short description** (#NUMBER @author) — Explanation of what broke and how it affects users.

## Highlights

- **Feature name** (#NUMBER @author) — Description of the feature and why it matters.

## Core Library

- **Feature/fix name** (#NUMBER @author) — Brief description.
- **Fix: short description** (#NUMBER @author)

## Docs

- Description (#NUMBER @author)

## Chores

- **Title** (#NUMBER @author) — Brief description if non-obvious.

## New Contributors

- @author made their first contribution in #NUMBER
```

## Formatting rules:
- PR references use `#NUMBER` format (GitHub auto-links them)
- Authors use `@login` format (GitHub auto-links them)
- Features and significant fixes get bold titles with em-dash descriptions
- Minor fixes can be single-line bold entries without descriptions
- Omit empty sections entirely (including New Contributors if none)
- Within Core Library, group features first, then fixes

## Step 8: Present for approval

Display the formatted release notes to the user for review. Include a summary:
- Total PRs analyzed
- Breakdown by category (with counts)
- Number of unique contributors (and how many are new)
- List any PRs where you had to infer the description from issue or diff

Ask the user to approve or request changes before writing.

## Step 9: Write to file

Only after user approval, write the release notes to `/tmp/release-notes-TAG.md`.

Report the file path to the user.
