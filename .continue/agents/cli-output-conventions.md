---
name: CLI Output Conventions Check
description: Ensures CLI modules use fmt for user-facing output (not Python logger), import grouping follows conventions, and error messages use proper formatting with backticks around identifiers.
---

# CLI Output Conventions Check

## Context

dlt's CLI lives in `dlt/_workspace/cli/`. Reviewers enforce specific conventions for how CLI code communicates with users. The most important rule: **CLI modules must use `fmt` for user-facing output, not the Python `logger`**. Additionally, error messages throughout the codebase must use backticks around identifiers for clarity.

Reviewer quote: "please do not use logger in `cli` modules. we issue warnings via `fmt`"

## What to Check

### 1. No Logger in CLI Modules

Files under `dlt/_workspace/cli/` must not use Python's `logging` module for user-facing output:

```python
# BAD — in CLI module
import logging
logger = logging.getLogger(__name__)
logger.warning("Could not find scaffold")

# GOOD — in CLI module
from dlt._workspace.cli import fmt
fmt.warning("Could not find scaffold")
```

Note: `logger` is fine in non-CLI library code (`dlt/common/`, `dlt/extract/`, etc.). This rule is specific to `dlt/_workspace/cli/`.

### 2. Error Message Formatting

Throughout the codebase, error messages should:

- Use backticks around identifiers: `` f"Table `{table_name}` not found" `` not `f"Table {table_name} not found"`
- Be grammatically correct (reviewers catch typos like "duo" -> "due", "Fetche" -> "Fetch")
- Provide actionable context (what went wrong and what the user should do)

### 3. Import Grouping in CLI

CLI module imports should group `_workspace` imports together:

```python
# GOOD — grouped
from dlt._workspace.cli import fmt
from dlt._workspace.configuration import WorkspaceConfig

# BAD — mixed with other imports
from dlt.common import logger
from dlt._workspace.cli import fmt
from dlt.extract import decorators
from dlt._workspace.configuration import WorkspaceConfig
```

### 4. Configuration Class Placement

New configuration classes for CLI features should live in `dlt/_workspace/configuration/`, not outside the workspace module.
