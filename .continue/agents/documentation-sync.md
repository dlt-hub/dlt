---
name: Documentation Sync Check
description: Ensures that user-facing changes to behavior, APIs, configuration, or destinations are accompanied by corresponding documentation updates in docs/website/.
---

# Documentation Sync Check

## Context

dlt's documentation lives in `docs/website/` as a Docusaurus site. The CI already detects whether a PR has "changes outside docs" to decide if tests should run, but there is no automated check that code changes that affect user-facing behavior are accompanied by documentation updates.

Common areas where docs fall out of sync:
- New configuration options added but not documented
- New destination features without usage examples
- Changed API signatures without updated reference docs
- New CLI commands without docs

## Documentation Structure

```
docs/website/docs/
├── intro.md
├── reference/              # API reference
├── dlt-ecosystem/
│   ├── destinations/       # Per-destination guides
│   ├── verified-sources/   # Source documentation
│   └── ...
├── general-usage/          # Core concepts (pipeline, schema, state, etc.)
├── walkthroughs/           # Step-by-step guides
└── ...
```

Additionally:
- `docs/examples/` — Example projects (25+)
- `docs/website/sidebars.js` — Navigation structure

## What to Check

### 1. New Public API / Parameters

If the PR adds new parameters to `@source`, `@resource`, `@destination`, `pipeline()`, or other public APIs:

- Is the parameter documented in the relevant docs page?
- Are there usage examples?

### 2. New Destinations or Destination Features

If the PR adds or modifies a destination:

- Is there a corresponding page in `docs/website/docs/dlt-ecosystem/destinations/`?
- Are new configuration options documented?
- Are new adapter hints (partition, sort, cluster) explained?

### 3. New Sources

If the PR adds or modifies a built-in source:

- Is there documentation in `docs/website/docs/dlt-ecosystem/verified-sources/`?

### 4. Configuration Changes

If new TOML config keys or environment variables are introduced:

- Are they documented with examples in the relevant configuration docs?
- Do the docs show both `secrets.toml` and environment variable formats?

### 5. CLI Changes

If the PR modifies CLI commands (`dlt/_workspace/cli/`):

- The CI checks `make check-cli-docs` to ensure CLI docs are up to date
- But additional explanation or examples may be needed in the docs

### 6. Behavioral Changes

If the PR changes default behavior (e.g., default write disposition, naming conventions, file formats):

- Is the change documented?
- Are migration notes provided if users need to adjust?

## Exclusions

Not all changes require doc updates:
- Internal refactoring with no user-visible changes
- Test-only changes
- CI/build infrastructure changes
