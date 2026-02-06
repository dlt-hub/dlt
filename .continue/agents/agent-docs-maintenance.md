---
name: Agent Documentation Maintenance
description: Ensures that CLAUDE.md, AGENTS.md, and .continue/agents/ check files are kept up to date when project conventions, tooling, or architecture change.
---

# Agent Documentation Maintenance

## Context

This repository uses AI-assisted development and review through files like `CLAUDE.md` and agent checks in `.continue/agents/`. These files encode project conventions, patterns, and review guidelines that agents use to provide quality feedback. When the project evolves, these files must be updated too.

## What to Check

### 1. Convention Changes

If the PR changes project conventions:

- **Linting rules** (`tox.ini`, `mypy.ini`, `pyproject.toml` ruff/black/isort config): Update any agent checks that reference specific linting rules
- **Branch naming** (`CONTRIBUTING.md`): Update relevant documentation
- **CI workflows** (`.github/workflows/`): Update checks that reference CI behavior

### 2. Architecture Changes

If the PR changes module structure or architectural patterns:

- New module hierarchy rules: Update `module-hierarchy.md`
- New destination patterns: Update `destination-implementation.md`
- New configuration patterns: Update `configuration-patterns.md`

### 3. New Patterns

If the PR introduces a new pattern that should be followed by future contributors:

- Should it be documented in `CLAUDE.md`?
- Should a new agent check be created in `.continue/agents/`?
- Does it contradict any existing agent check guidance?

### 4. CLAUDE.md Accuracy

If `CLAUDE.md` exists, verify it still accurately reflects:

- Build and test commands
- Project structure overview
- Key conventions and patterns
- Common gotchas for contributors

### 5. Deprecation of Patterns

If old patterns are deprecated in favor of new ones:

- Are agent checks updated to recommend the new pattern?
- Are old patterns flagged as deprecated in the documentation?
