---
name: review-issues-severity
description: Find and prioritize open GitHub issues by severity, community impact, and maintainer abandonment
argument-hint: [-- <optional filters or focus areas>]
---

# Review Issues by Severity

Parse `$ARGUMENTS` for optional filters:
- Label filters (e.g., `question`, `bug`)
- Focus areas (e.g., "merge disposition", "Arrow", "ClickHouse")
- If absent, perform a broad review across all open issues.

## Steps

### 1. Gather candidate issues

Run the following GitHub API queries **in parallel** to surface issues matching different severity signals:

#### a) Long discussions (high comment count)
```bash
gh api 'repos/dlt-hub/dlt/issues?state=open&per_page=100&sort=comments&direction=desc&page=1' \
  --jq '.[] | "\(.number)\t\(.comments)\t\(.updated_at | split("T")[0])\t\(.created_at | split("T")[0])\t\([.labels[].name] | join(","))\t\(.title)"'
```

#### b) Issues with specific labels (if filters provided)
```bash
gh api 'repos/dlt-hub/dlt/issues?state=open&labels=LABEL&per_page=100&sort=comments&direction=desc' \
  --jq '.[] | "\(.number)\t\(.comments)\t\(.updated_at | split("T")[0])\t\(.created_at | split("T")[0])\t\([.labels[].name] | join(","))\t\(.title)"'
```

#### c) Recently stale issues (updated 2+ months ago, with maintainer comments)
Look for issues where `updated_at` is old relative to the current date but had prior maintainer engagement.

### 2. Deep-dive top candidates

For the top 10-15 most promising candidates, fetch full details using a subagent or parallel `gh issue view` calls:

```bash
gh issue view -R dlt-hub/dlt NUMBER --json title,body,comments,labels,createdAt,updatedAt,author,assignees
```

For each issue, extract:
- **Participants:** who commented, their association (MEMBER = maintainer, NONE/CONTRIBUTOR = community)
- **Timeline:** when maintainers last engaged, how long since last response
- **Problem description:** what breaks and under what conditions
- **Reproduction quality:** is there a clean repro script?
- **Current status:** fixed? workaround? stalled? abandoned?
- **Production severity:** does this cause data loss, crashes, or silent corruption?

### 3. Classify each issue

Apply these criteria to each issue:

| Signal | What to look for |
|--------|-----------------|
| **Serious problem** | Data corruption, silent data loss, hard crashes, cascading failures, security issues |
| **Long discussion** | 5+ comments, especially with multiple community reporters hitting the same issue |
| **Maintainer abandoned** | Maintainer commented but last maintainer response is 2+ months old, no linked PR, reporter follow-ups unanswered |
| **Question label** | Issue has `question` label — these often represent real bugs initially miscategorized |
| **Community effort** | Reporter provided detailed repro scripts, root-cause analysis, or proposed fixes that went unacknowledged |

### 4. Prioritize

Assign priority tiers:

- **P0 — Critical:** Silent data corruption, cascading failures, confirmed root cause with no fix. Production systems at risk.
- **P1 — High:** Hard crashes in common deployment patterns, blocking features for significant user segments, confirmed bugs with stalled fixes.
- **P2 — Medium:** Broken features in specific environments, intermittent failures, missing warnings that lead users astray.
- **P3 — Low:** Documentation gaps, environment-specific issues with known workarounds, feature requests with community PRs pending review.

Within each tier, rank by:
1. Seriousness of the problem in production (data loss > crash > degraded performance > inconvenience)
2. Negative community impact if not solved (effort wasted by reporters, users hitting the issue independently)
3. Amount of effort already invested by reporters (repro scripts, root-cause analysis, proposed PRs)

### 5. Output format

Present results as:

#### Per-issue analysis (grouped by priority tier)
For each issue:
- **Issue link, title, comment count, last activity date**
- **Problem:** 2-3 sentence description of what breaks
- **Why this priority:** key severity signal
- **Abandonment signal:** when maintainer last engaged, what was left unresolved
- **Community effort:** what the reporter invested (repro scripts, analysis, PRs)

#### Summary matrix
A table with columns: Priority | Issue # | Type | Production Impact | Community Effort | Maintainer Status

#### Cross-cutting patterns
Note any clusters of related issues (e.g., multiple issues sharing a root cause) or systemic patterns in maintainer response.
