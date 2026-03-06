---
paths:
  - "dlt/_workspace/cli/**"
  - "dlt/_workspace/mcp/**"
  - "dlt/_workspace/helpers/dashboard/**"
  - "dlt/_workspace/typing.py"
  - "tests/workspace/**"
---

# Workspace model/loader/view pattern for commands

When adding a new command (cli, mcp or dashboard) `dlt/_workspace` follows a limited MVC pattern so that data models stay independent
from presentation. The same model can be rendered by a CLI print function, returned
from an MCP tool, or displayed in a marimo dashboard.

## Layers

| Layer | Location | Convention |
|-------|----------|------------|
| **Model** | `dlt/_workspace/typing.py` | `TypedDict` (T-prefixed), plain data, no I/O |
| **Loader** | `dlt/_workspace/cli/ai/utils.py`, `cli/utils.py` | `fetch_*()` functions — collect data into a model, no `fmt.echo()` calls |
| **View** | `cli/*_command.py`, `cli/commands.py` | `_print_*()` or `print_*()` — render a model to CLI via `fmt.echo()` / `fmt.warning()` |
| **Controller** | argparse handlers, MCP tools, marimo notebooks | Calls loader, then passes model to the appropriate view |

NOTE: loaders are stored in cli module. to be refactored soon

## Rules

1. **Loaders return typed dicts, never print.**
   A loader (`fetch_ai_status`, `fetch_workspace_info`, `fetch_secrets_list`, ...)
   returns a `TypedDict` from `typing.py`. It must not call `fmt.echo()` or any
   display function.

2. **Views accept a model, never load.**
   A view (`_print_ai_status`, `print_workspace_info`, ...) takes the typed dict
   and formats it for human consumption. It must not call loaders or access
   providers/config directly.

3. **Controllers wire loader to view.**
   An argparse command function (decorated with `@track_command`) calls the loader,
   then the view. MCP tools call the loader and return the model (or a
   serialised form). Dashboard cells call the loader and render via marimo UI.

4. **Research existing types**
   * Research other `typing.py` modules and Literal definitions and types to find potentially reusable types.
   * Reuse, reconcile, modify existing types - also from `common` and other namespaces
   * Make **reconciliation** and model to be **clean** part of implementation plan. Request user input if needed

5. **Write good models**
   * Use `Literal` ie. for error or warning cases
   * Do not leak user messages into data model - view should do it
   * OK to store additional context ie. exception messages
   * Use `NotRequired` field to the model (e.g. `mcp_error: NotRequired[str]`) if element may not be present. In most cases that is better than setting to None


## Existing examples

- `fetch_workspace_info()` -> `TWorkspaceInfo` -> `print_workspace_info()`
- `fetch_ai_status()` -> `TAiStatusInfo` -> `_print_ai_status()`
- `fetch_secrets_list()` -> `List[TLocationInfo]` -> `ai_secrets_list_command()` (inline view)

## When adding new commands

Before writing a new CLI command, MCP tool, or dashboard cell that shows
workspace state, check whether a loader already exists. If not, create the
model in `typing.py` and the loader in the appropriate `utils.py` first,
then write the view / controller.
