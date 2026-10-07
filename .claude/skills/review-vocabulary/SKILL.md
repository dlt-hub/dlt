---
name: review-vocabulary
description: Review and rewrite the prose a branch adds — docstrings, comments, user-facing messages, docs — against dlt's fixed vocabulary and Simplified Technical English. Invoke as /review-vocabulary.
argument-hint: "[<branch-or-base-ref>] [-- <extra focus or scope notes>]"
disable-model-invocation: true
---

# Review Vocabulary

Rewrite the prose a branch adds so dlt says one thing one way. Two inputs govern every
decision: the **fixed vocabulary** in `references/vocabulary/`, indexed below, and the
**Simplified Technical English** rules in `references/simple-english.md`.

**Never invoke this on your own.** A maintainer runs it with `/review-vocabulary`. It rewrites
text across dozens of files and changes CI test ids, so it is not a background cleanup.

Parse `$ARGUMENTS`:
- Everything before the first `--` is the **base ref** to diff against. Defaults to
  `$(git merge-base origin/devel HEAD)`.
- Everything after the first `--` is extra focus from the maintainer.

## Read first

1. `references/simple-english.md` — the 53 rules. **Cite only rule numbers that exist in that
   file.** The numbering is unintuitive and models invent it. Rule 3.1 is about verb forms, not
   sentence length.
2. `references/simple-english-use-cases.md` — needed for the error-message shape.
3. The vocabulary groups the diff touches, picked from the index below. They override the general
   rules where they disagree.

## Scope

Diff with `git diff <base>` — **not** `git diff <base>...HEAD`, which misses uncommitted work
that will ship in the same PR.

In scope, when the branch **added or changed** the line:

| Kind | Where |
|---|---|
| Docstrings | `dlt/`, `tests/` |
| `#` comments | `dlt/`, `tests/` |
| User-facing messages | `raise X("...")`, `logger.*`, `warnings.warn`, helpers that build a message |
| Test function names | `tests/` — renames only under the narrow gate below |
| Documentation | `docs/website/docs/**` |
| Snippet files a page pulls from | `*snippets.py` — comments as docs, code as a test (see below) |

When the maintainer asks for a **whole-file** docs review, pre-existing prose in those files is in
scope too. Say which mode you are in; the default is added-or-changed lines only.

Out of scope:

- **Pre-existing prose**, in the default mode. Leave it even when it breaks every rule. List what
  you skipped and why.
- Code, identifiers, signatures, type annotations — except snippet code, which gets a separate
  review against the repo's test rules.
- SQL keywords and anything already in backticks, CLI commands, file paths, config keys.
- `pytest.param(id=...)` strings, fixture names, parameter names.
- Assertions on exact generated SQL.
- Prose moved verbatim from another file — check the base ref before claiming it is new.

## Classification

Get this right first; every other rule depends on it.

| Text | Mode | Limit | Shape |
|---|---|---|---|
| Docstrings, `#` comments | descriptive | 25 words (Rule 6.3) | no imperative, one new fact per sentence (Rule 6.1) |
| Exception strings, warnings, log lines | **procedural** | **20 words (Rule 5.1)** | what happened (simple past), the cause, then the fix as an imperative |
| Docs — how-to, steps, install | procedural | 20 words | imperative, condition before command (Rule 5.4) |
| Docs — concept, reference, architecture | descriptive | 25 words | no imperative |

**Messages keep their remedies.** Many errors end in an instruction — "Materialize the dataset…",
"Set a permanent `pipeline_salt`…". Improve the wording: make it imperative, condition first,
under 20 words, one instruction per sentence. **Never delete a remedy. Never invent one** where
the message has none — a remedy needs facts the string does not interpolate.

**Count the assembled sentence, not the fragment.** Some messages are built by a helper that wraps
a short reason in a frame — `_no_data_location(reason)` in
`dlt/common/destination/client.py` is the current example. Read the helper first, then judge each
reason as the clause it becomes inside the finished sentence. A four-word reason can push the
assembled sentence past 20 words.

**Docs carry their own untouchables:** fenced code blocks, `<!--@@@DLT_SNIPPET ...-->` markers,
front-matter, link targets and anchor slugs.

**Headings are anchors.** Renaming one breaks every link to it. Before you rename, grep the whole
docs tree for the old slug and update each hit in the same change. When a rename would need an edit
in a file another agent owns, hand it back rather than reaching across.

### Snippet files are BOTH tests and documentation

A page that uses `<!--@@@DLT_SNIPPET ./x_snippets.py::name-->` pulls real code from a real test.
`docs/pyproject.toml` collects `*snippets.py` with `*_snippet` functions, so **docs CI executes
them**. They therefore get two reviews at once, against two different rule sets:

| Part of the file | Treat as | Rules |
|---|---|---|
| the code | a test | `.claude/rules/testing.md`, `coding-style.md`, `imports.md` |
| the `#` comments | user-facing docs | this vocabulary + STE, classified per comment |

Consequences worth knowing:

- **Comments inside a snippet render to the reader**, so a stale or wrong one is a doc bug, not a
  code-hygiene nit. Judge them as prose. A comment that labels a step is INSTRUCTION; a comment
  that explains behavior is REFERENCE.
- **The repo comment rule still applies to the code.** Default to no comment. But a snippet is
  teaching material, so a comment that would be redundant in library code can earn its place here.
  Say which of the two you applied when they conflict.
- **Never change snippet code to satisfy a prose rule.** Changing code changes what CI runs. If the
  code is wrong, report it as a code finding with its own justification.
- **A fenced code block is NOT a snippet.** It is untested. When a page mixes the two, its fenced
  examples can drift from the API while its snippets cannot. Flag the untested ones as unverified
  rather than assuming they work — and consider recommending they become snippets.

## The fixed vocabulary

One word, one meaning, one part of speech (Rules 1.11, 9.4). Apply the tables before any other
rewriting.

The vocabulary is organised in **groups**, one per area of dlt. A group is self-contained: its
included terms, its excluded terms, and the rulings that are easy to get wrong in that area. Each
group is one file in `references/vocabulary/`. Groups grow independently — a review only needs the
groups its diff touches, and adding an area means adding a group, not editing the others.

Groups defined so far:

| Group | Covers | Read when the diff touches |
|---|---|---|
| [G1 — Data access and locations](references/vocabulary/g1-data-access-and-locations.md) | destinations, datasets, physical locations, join compatibility | `dlt/dataset/`, `dlt/common/destination/`, `dlt/destinations/`, the dataset-access docs |
| [G2 — Attach and foreign datasets](references/vocabulary/g2-attach-and-foreign-datasets.md) | cross-destination joins, attach info, catalog aliases | `TAttachInfo`, `ATTACH`, foreign datasets, catalog aliases |
| [G3 — Transformations and materialization](references/vocabulary/g3-transformations-and-materialization.md) | relations, transformations, model jobs, eager and lazy paths | relations, model jobs, `materialize` in `dlt/dataset/`, `dlt/extract/` or `dlt/load/`, the transformation docs |
| [G4 — Identifiers and SQL generation](references/vocabulary/g4-identifiers-and-sql-generation.md) | naming conventions, case-folding, query binding | `dlt/common/normalizers/naming/`, `dlt/common/schema/`, sql clients, lineage and query generation |
| [G5 — Configuration and credentials](references/vocabulary/g5-configuration-and-credentials.md) | configs, credentials, secrets | `dlt/common/configuration/`, credentials classes, `config.toml`, `secrets.toml` |
| [G6 — Merge strategies and merge conditions](references/vocabulary/g6-merge-strategies-and-merge-conditions.md) | merge strategies, deletes, retirement, source filter, destination scope, placeholders | `dlt/destinations/sql_jobs.py`, a destination's merge job, `merge_key`, `scd2`, `cdc`, `merge-loading.md` |
| [G7 — Jobs, triggers and the deployment manifest](references/vocabulary/g7-jobs-triggers-and-the-deployment-manifest.md) | jobs, job runs, launchers, triggers, selectors | `dlt/_workspace/deployment/` outside `agent/`, `dlt/hub/run.py`, `__deployment__.py` files, `dlthub local run` |
| [G8 — Agents, loops and prompts](references/vocabulary/g8-agents-loops-and-prompts.md) | agent definitions, agent jobs, agent runs, loops, system prompts, user turns, traces | `dlt/_workspace/deployment/agent/`, `launchers/agent.py`, `AGENT.md` files, `run.agent` |
| [G9 — Workspace access and tools](references/vocabulary/g9-workspace-access-and-tools.md) | the `access` declaration, axes, verbs, tools, feature groups | `access` blocks, `TWorkspaceAccess`, `dlt/_workspace/mcp/`, the tools a loop wires |

Read every group the diff touches before you audit, and give each subagent only the groups its
files map to. G1 to G5 are short: when the mapping is unclear, read them all.

Two rules apply across every group:

- **`dlt` is the sentence subject in messages.** That is how an error gets active voice with a
  named agent (Rule 3.6): "dlt cannot join…", "dlt cannot determine…". House style.
- **A banned word is banned for one meaning, not always.** Every group names its legal
  exceptions. Check them before "fixing" a hit.

### Legal technical nouns — never replace, any group (Rules 1.5, 1.8)

attach, attach alias, attach info, attach statement, catalog, config, data location, dataset,
destination, destination scope, duckdb, iceberg, materialization, merge condition, model job, partition,
pipeline, placeholder, relation, scanner, snapshot (`cdc` only), source filter, vended.

access, access axis, agent, agent definition, agent definition reference, agent file, agent job,
agent output, agent run, agent trace, default trigger, feature group, instructions, job, job definition, job ref, job result,
job run, launcher, loop, placeholder, runner, runtime, selector, skill, system prompt, tool, toolkit,
trigger, turn, user turn, verb.

## Rules this codebase breaks most

The repo comment rule overrides every row. Default to no comment. Keep at most one short line for
a non-obvious WHY. Prefer a rewrite that shrinks the line count. When a comment only restates the
code, delete it.

## Test function names

A name is an identifier. Rename it for one of two reasons only:

- it contradicts the fixed vocabulary
- it carries a noun chain of more than three words (Rule 2.1)

Never rename for style. Apply two checks first:

- **Does the name mirror the API it tests?** Then keep it. `test_attach_info_built_once_per_relation`
  tracks `Relation._attach_infos()`, and that link beats vocabulary purity.
- **Every rename changes a CI test id.** Flag each one on its own. Afterwards, grep the repo for
  the old name.

## Workflow

### 1. Audit

Propose. Do not edit.

Split the diff across subagents by area. **File sets must not overlap** — two agents writing one
file corrupt each other.

Give each agent its base ref, its file list, the group files its files map to, the two cross-group
rules and the classification table. Tell it to read `references/simple-english.md` itself, so rule
numbers come from the file.

Collect five groups. Give every finding a **file:line, rule number, current text, rewrite**:

1. Vocabulary violations
2. Structural violations
3. Identifier renames
4. Content corrections — false statements. Run the code before you report one.
5. Test assertions that break — see the trap below.

Write the proposal to a file. A few hundred findings do not fit in a chat reply. Report the counts
and the open decisions.

**Stop. Wait for approval.** When the vocabulary does not settle a decision, ask.

### 2. Apply, in two phases

Docstrings and message strings share files, so they cannot run together.

- **Phase A:** docstrings, comments, renames, assertion fixes, content corrections.
- **Phase B:** message strings.

Tell every Phase B agent that the line numbers moved. It must find each string by content. Pin any
substring a test asserts.

Every applying agent obeys four rules:

- Apply only your own findings, only in your own files.
- **Never touch code.** Not a signature, not a return type, not a call site. Agents break this
  rule, and a functionally-equivalent change passes every test. Verify it afterwards.
- Preserve every f-string placeholder: same names, same count, same order.
- Keep lines under 100 chars. Black does not reflow a string or a comment.

### 3. Verify

Run every step. Each one caught a real defect.

**a. Prose-only check.** Compare the AST with docstrings stripped, against the **branch tip**.
Comments never reach the AST, so any difference means code or a string changed. Explain every file
that appears. Against the merge-base the whole PR appears and the signal is lost.

```python
import ast
def strip(t):
    for n in ast.walk(t):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            n.body = [x for x in n.body if not (isinstance(x, ast.Expr)
                      and isinstance(x.value, ast.Constant) and isinstance(x.value.value, str))]
    return t
# compare ast.dump(strip(ast.parse(old))) with ast.dump(strip(ast.parse(new)))
```

**b. The four-step self-check** from `references/simple-english.md`, over the added lines. Add a
sweep for the excluded vocabulary.

**c. `make format`.** Black reports no change when the agents did their job.

**d. `make lint`.** For pages under `docs/website/docs`, also `make md-fix && make md-check` in
`docs/`. A rewrite that lengthens a table cell past 35 words fails CI.

**e. Run the tests.** This step is not optional.

## The trap: a message rewrite breaks test assertions

Tests match on error text. A grep for the phrases you changed misses some. A parametrized test
hides its assertion far from the message.

One pass found them in two rounds. A grep found five. The test run then surfaced eight more. One of
those was `match="cannot be determined"`. The frame sentence had become "dlt cannot determine the
data location".

- Grep `tests/` for `match=`, `in str(exc`, and `in str(reject`.
- Check the **negative** assertions too. `assert "can join" not in ...` still passes against
  "cannot join", because no space follows `can`.
- Run the suites anyway. The grep is a head start, not the check.

## Adding a new term or a new group

Never just record a term. Take the eight steps in `references/vocabulary/adding-terms.md`.

Amend the vocabulary mid-review when you must. Then tell every running agent what inverted. They
audited under the old table.

## Deliverable

- The proposal file, with counts and your open decisions.
- After approval: the edits, the verification output, and what you did **not** apply.
- Never commit. The maintainer commits.
