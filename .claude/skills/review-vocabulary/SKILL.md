---
name: review-vocabulary
description: Review and rewrite the prose a branch adds — docstrings, comments, user-facing messages, docs — against dlt's fixed vocabulary and Simplified Technical English. Invoke as /review-vocabulary.
argument-hint: "[<branch-or-base-ref>] [-- <extra focus or scope notes>]"
disable-model-invocation: true
---

# Review Vocabulary

Rewrite the prose a branch adds so dlt says one thing one way. Two inputs govern every
decision: the **fixed vocabulary** in this file, and the **Simplified Technical English**
rules in `references/simple-english.md`.

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
3. The fixed vocabulary below. It overrides the general rules where they disagree.

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
included terms, its excluded terms, and the rulings that are easy to get wrong in that area. Groups
grow independently — a review only needs the groups its diff touches, and adding an area means
adding a group, not editing the others.

Groups defined so far:

| Group | Covers |
|---|---|
| [G1 — Data access and locations](#g1--data-access-and-locations) | destinations, datasets, physical locations, join compatibility |
| [G2 — Attach and foreign datasets](#g2--attach-and-foreign-datasets) | cross-destination joins, attach info, catalog aliases |
| [G3 — Transformations and materialization](#g3--transformations-and-materialization) | relations, transformations, model jobs, eager and lazy paths |
| [G4 — Identifiers and SQL generation](#g4--identifiers-and-sql-generation) | naming conventions, case-folding, query binding |
| [G5 — Configuration and credentials](#g5--configuration-and-credentials) | configs, credentials, secrets |
| [G6 — Jobs, triggers and the deployment manifest](#g6--jobs-triggers-and-the-deployment-manifest) | jobs, job runs, launchers, triggers, selectors |
| [G7 — Agents, loops and prompts](#g7--agents-loops-and-prompts) | agent specs, loops, system prompts, user turns, traces |
| [G8 — Workspace access and tools](#g8--workspace-access-and-tools) | the `access` declaration, axes, verbs, tools, feature groups |

Two rules apply across every group:

- **`dlt` is the sentence subject in messages.** That is how an error gets active voice with a
  named agent (Rule 3.6): "dlt cannot join…", "dlt cannot determine…". House style.
- **A banned word is banned for one meaning, not always.** Every group below names its legal
  exceptions. Check them before "fixing" a hit.

---

### G1 — Data access and locations

**Included**

| Concept | Write |
|---|---|
| getting to data (verb) | **access** |
| the adjective | **accessible**, **inaccessible** |
| the negative, as a verb | **cannot access** |
| where the data physically sits | **data location** |

**Excluded**

| Never | Because |
|---|---|
| reach, reaches, reachable, in reach of, out of reach, get to | one word for one concept — `access` |
| physical location, physical destination, physical dataset (in prose) | say **data location**; the method `data_location()` keeps its name |

**Rulings**

- **`access` is a verb for the act, and a noun for what a job may touch.** Keep the verb for the
  act of reading data: "the engine accesses the data", not "data access is one-way". The noun is
  legal only in the sense G8 defines — the `access` declaration, and the grant that answers it.
  dlt's docs already write "grant access", "read access" and "denied access" 40 times over; that
  usage was always compliant and stays.
- **`reach` is not always `access`.** "`SET SESSION` would not reach the cloned sessions" means
  *propagate to*, not *read data from*. A literal swap changes the meaning — restructure
  (Rule 9.1).

---

### G2 — Attach and foreign datasets

**Included**

| Concept | Write |
|---|---|
| the `TAttachInfo` object | **attach info** |
| the `TAttachStatement` object | **attach statement** |
| the SQL keyword | `` `ATTACH` `` in backticks |
| the action in prose | **attach** (lowercase, a verb) |
| the catalog a foreign dataset lands under | **attach alias** |

**Excluded**

| Never | Because |
|---|---|
| descriptor (for `TAttachInfo`) | `attach info` matches the type and the method `_attach_infos()` |
| bare `ATTACH` as a prose noun, `ATTACHed`, "attaches" as a plural noun | backtick the keyword, or use the verb |
| attach instructions | one name — **attach statements** |

**Rulings**

- **`descriptor` is legal for the Python descriptor protocol.** `dlt/common/utils.py` describes a
  real Python descriptor. The ban covers naming the `TAttachInfo` object only.
- **`attach info` and `attach statement` are different things.** One is the whole descriptor for a
  foreign dataset; the other is a single SQL statement inside it. Do not collapse them.

---

### G3 — Transformations and materialization

**Included**

| Concept | Write |
|---|---|
| the deferred path | **lazy materialization** |
| the load job that runs it | **model job** |
| the immediate path | **eager materialization** |

**Excluded**

| Never | Because |
|---|---|
| model extraction | not a thing — it is a **model job** |
| executed here | say **eager materialization** |

**Rulings**

- **`lazy` and `eager` are legal only for materialization.** dlt has both. Using `lazily` to mean
  *on first use* (memoization) is a second meaning for one word — write "on the first read".
- **A model job is the artifact; lazy materialization is the path.** Use the one you mean.

---

### G4 — Identifiers and SQL generation

**Included**

| Concept | Write |
|---|---|
| identifier case handling | **case-fold**, **case-folds**, **case-folding** |

**Excluded**

| Never | Because |
|---|---|
| casefold, post-fold (in prose) | one spelling — **case-fold**; the identifier `casefold_identifier` keeps its name |

**Rulings**

- **Hyphenated compounds on the `fold` root are legal** — "foreign-folded output column" reads
  correctly and is in use. The ban is on the bare spelling `casefold` in prose.

---

### G5 — Configuration and credentials

**Included**

| Concept | Write |
|---|---|
| a resolved settings object | **config** |

**Excluded**

| Never | Because |
|---|---|
| configuration (for the object) | `config`; keep "configuration error" when it names `ConfigurationValueError` |

---

### G6 — Jobs, triggers and the deployment manifest

**Included**

| Concept | Write |
|---|---|
| the unit a workspace deploys and runs | **job** |
| one execution of it | **job run** |
| its identity | **job ref** |
| its entry in the manifest | **job definition** |
| the dlt module that starts a job | **launcher** |
| the machine the platform gives the job | **runner** |
| the dltHub platform itself | **runtime** |
| the string that starts a job | **trigger** |
| the pattern that expands into triggers | **selector** |
| the trigger a manual run stands in for | **default trigger** |
| the file that describes every job | **deployment manifest** |

**Excluded**

| Never | Because |
|---|---|
| task, workload, script (for a dlt job) | one name — **job**; the runtime's `Script` model keeps its own name |
| job execution, invocation (for one run) | **job run**, the noun the manifest and the beacon use |
| primary trigger | **default trigger**, the name of the manifest field |
| manifest (unqualified, for `AGENT.md`) | **deployment manifest** is the only manifest; see G7 |

**Rulings**

- **`run` is a verb; `job run` is the noun.** "dlt runs the job" and "the job run failed". The
  command `dlthub local run` is a technical name (Rule 8.6) and stays.
- **launcher, runner and runtime are three things.** The launcher is dlt code. The runner is a
  machine. The runtime is the platform. Never swap them, and never write "the runtime launches".
- **`task` is legal in two places only:** the work an agent must do (G7), and an Airflow task when
  the sentence says "Airflow task". It is never a dlt job.

---

### G7 — Agents, loops and prompts

**Included**

| Concept | Write |
|---|---|
| the whole declaration | **agent spec** (`TAgentSpec`) |
| the file it is read from | **`AGENT.md`**, and **agent file** for the path |
| the block the manifest carries | **agent definition** |
| the binding to one agent framework | **loop**, or **agent loop** |
| the text the model gets as its role and task | **system prompt** |
| the first message of the run | **user turn** |
| what a person tells this run to do | **instructions** |
| one model request | **turn** |
| the record of what the loop ran and did | **agent trace** |
| `{{ name }}` in a body | **placeholder** |

**Excluded**

| Never | Because |
|---|---|
| harness (for the Claude Code CLI) | the **dltHub AI Harness** owns that word; write **the Claude Code CLI** |
| adapter (for a loop) | dlt's adapters are `bigquery_adapter` and friends; write **loop** |
| prompt (bare) | say which one: **system prompt** or **user turn** |
| spec (bare, for an agent) | bare `spec` is the configspec; write **agent spec** |
| trace (bare, for an agent) | bare `trace` is the pipeline trace; write **agent trace** |
| manifest (for `AGENT.md`) | **agent file**; the manifest is the deployment manifest (G6) |
| framework (as a loop's name) | name it: **pydantic-ai**, **claude-agent-sdk** |

**Rulings**

- **`instructions` means two opposite things across the boundary.** In dlt it is the user turn. In
  pydantic-ai, `AgentSpec.instructions` is the system prompt. Qualify every mention of the
  framework field: "pydantic-ai's `instructions` field (its system prompt)".
- **`turn` is one model request.** The **user turn** is the first message. A turn counter counts
  requests. Do not let one word carry both without the qualifier.
- **`loop` is the agent loop.** In a file that also has Python loops, write **agent loop** once,
  then `loop`.
- **Model names and aliases are technical nouns** (Rule 1.8): `sonnet`, `opus`, `claude-sonnet-5`.

---

### G8 — Workspace access and tools

**Included**

| Concept | Write |
|---|---|
| the declaration of what a job may touch | **access** (the `access` block, `TWorkspaceAccess`) |
| one of its four keys | **access axis** |
| a value on an axis | **verb** |
| what the runtime or a loop does with the declaration | **grant**, **deny** (verbs) |
| what it did grant | **granted access** |
| what it did not grant | **denied access** |
| what a loop supplies beyond the declaration | **over-granted** |
| what a tool needs before it is served | **required access** |
| what a model can call | **tool** |
| the MCP grouping a manifest requests | **feature group** |

**Excluded**

| Never | Because |
|---|---|
| permission, entitlement, scope (for the declaration) | **access**, the word dlt's docs already use; `permission` is legal only when naming the SDK's `permission_mode` |
| narrowed, narrowing | say the fact: **with less access** |
| grant (as a noun) | **the declared access**; `granted` as an adjective is legal (Rule 3.3) |
| capability (for a dlt tool) | **tool**; legal only for pydantic-ai `capabilities` |
| ceiling, floor (as prose metaphor) | say what it does: "access is the most a loop can wire" |
| honor, honour, honored, unhonoured (any spelling) | access is **granted** or **denied**, never honored |

**Rulings**

- **Grant and deny are the verbs.** A job declares access; the runtime and the loop grant what they
  can; the rest is denied. dlt's docs already write "grant access" 13 times and "denied access"
  twice, so this is the house pairing, not a new one. A loop that supplies more than the
  declaration **over-grants**.
- **The declaration is a request, not a claim.** A manifest `access` block says what the job wants.
  Nothing in it is granted until the runtime says so. Say "the job declares", never "the job has".
- **`access` is the noun, and no synonym joins it.** `permissions`, `scopes`, `capabilities` and
  `entitlements` each name this concept somewhere in the industry; dlt named it `access` before this
  feature existed — the built-in `access` profile is coarse-grained data access, and `profile_for()`
  derives that profile from `access.data`. One concept, one word (Rule 1.11).
- **`tools:` in an `AGENT.md` holds feature groups, not tools.** Write "feature groups" whenever the
  sentence is about that field, or a reader counts 4 tools and gets 19.
---

### Legal technical nouns — never replace, any group (Rules 1.5, 1.8)

attach, attach alias, attach info, attach statement, catalog, config, data location, dataset,
destination, duckdb, iceberg, materialization, model job, pipeline, relation, scanner, vended.

access, access axis, agent, agent definition, agent file, agent spec, agent trace, default trigger,
feature group, instructions, job, job definition, job ref, job run, launcher, loop, placeholder,
runner, runtime, selector, skill, system prompt, tool, toolkit, trigger, turn, user turn, verb.

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

Give each agent its base ref, its file list, this vocabulary, and the classification table. Tell it
to read `references/simple-english.md` itself, so rule numbers come from the file.

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

**d. `make lint`.**

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

Never just record a term. Take these eight steps.

1. **Pick the group.** No group fits? Add one. Give it the next `G<n>`, an index entry, and the
   three parts: included, excluded, rulings. A group without excluded terms is not finished.
2. **Research the usage.** Grep `dlt/`, `tests/` and `docs/` for the word and every synonym. Count
   the hits. Read enough to find the meanings in play. One word often covers two concepts.
3. **Derive the banned set.** A term is useless without one. For each synonym and inflection,
   decide: banned, or legal with another meaning? Both halves go in the group.
4. **Check the upstream interface.** The vocabulary must not contradict a public dlt method or
   type name. `access` won because `needs_attach` already said "accesses its data". `attach info`
   won because the type is `TAttachInfo`.
5. **Name the false positives.** `descriptor` is banned for `TAttachInfo` and correct for the
   Python descriptor protocol. Put the exception in the rulings, or the next run "fixes" it.
6. **Fix the part of speech.** Say noun or verb, and ban the other use. `access` is a verb, so
   "data access" is a violation.
7. **Test the ban before you write it.** Grep for the word you intend to exclude. Compliant prose
   already uses it? Then the ban is too wide. Narrow it to the meaning you mean. `inaccessible`
   and `foreign-folded` passed this check and stay legal. `casefold` and `attach instructions`
   failed it and are banned.
8. **Update this file.** Add rows to both tables. Add a ruling when the term has a legal exception.
   State what becomes newly compliant and what becomes newly non-compliant. A term that flips
   direction turns compliant prose into findings.

Amend the vocabulary mid-review when you must. Then tell every running agent what inverted. They
audited under the old table.

## Deliverable

- The proposal file, with counts and your open decisions.
- After approval: the edits, the verification output, and what you did **not** apply.
- Never commit. The maintainer commits.
