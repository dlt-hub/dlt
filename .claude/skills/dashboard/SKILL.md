---
name: dashboard
description: Use when reading, editing, or creating files in dlt/_workspace/helpers/dashboard/ or tests/workspace/helpers/dashboard/ or tests/e2e/
user-invocable: false
---

# dlt Dashboard Development Guide

Consult this skill whenever working on the dashboard codebase. It tells you where files live, what each module does, and where new code belongs.

## Overview

The dashboard is a **marimo-based** reactive web UI for inspecting dlt pipelines, schemas, data, and run traces. It lives at `dlt/_workspace/helpers/dashboard/`.

## File Layout

```
dlt/_workspace/helpers/dashboard/
├── dlt_dashboard.py          # Main marimo app (~30 reactive cells)
├── dlt_dashboard_styles.css  # CSS: colors, badges, timeline, tables
├── runner.py                 # Subprocess launcher, CLI integration
├── config.py                 # DashboardConfiguration @configspec dataclass
├── const.py                  # Constants: port (2718), timeouts, colors, defaults
├── typing.py                 # Shared TypedDicts (TTableListItem, TLoadItem, etc.)
├── strings.py                # All user-facing UI text (~350 lines)
└── utils/
    ├── __init__.py           # Package exports
    ├── pipeline.py           # get_pipeline(), pipeline_details(), exception handling
    ├── queries.py            # SQL execution, caching (BoundedDict), row counts, loads
    ├── schema.py             # Table/column lists, schema version fetching
    ├── trace.py              # Trace rendering, sanitization
    ├── visualization.py      # Execution timeline HTML, load status badges
    ├── home.py               # Home page / workspace home rendering
    ├── ui.py                 # dlt_table(), section_marker(), error_callout(), BoundedDict
    ├── formatters.py         # Datetime humanization, string formatting
    └── data_quality.py       # dlthub data quality integration
```

## Architecture

- **Framework**: marimo — a reactive Python notebook that renders as a web app
- **Cell pattern**: `@app.cell(hide_code=True)` decorators define reactive cells in `dlt_dashboard.py`
- **Dependency tracking**: Cells automatically re-execute when their parameter dependencies change
- **Routing**: URL query params (`?pipeline=X&profile=Y`) via `mo.query_params()`
- **Caching**: `BoundedDict` (LRU, 64/32 entries) in `utils/queries.py` and `utils/schema.py`
- **Styling**: CSS variables for dlt brand colors (`--dlt-color-lime`, `--dlt-color-aqua`, `--dlt-color-purple`, `--dlt-color-pink`)

## Dashboard Sections (cells in dlt_dashboard.py)

1. **Profile & Pipeline Discovery** — reads workspace profiles, lists local pipelines
2. **Pipeline Attachment** — `dlt.attach()`, file watcher for auto-refresh
3. **Overview** — pipeline properties, destination info, state version
4. **Schema** — table browser with configurable column display
5. **Browse Data** — SQL editor with cached results and query history
6. **Trace** — execution timeline, step details, resolved config
7. **Loads** — load history with row counts and schema versions
8. **State** — raw JSON state viewer
9. **Data Quality** — integration with dlthub.data_quality
10. **Ibis Backend** — read-only Ibis connection

## How the Dashboard is Launched

Three CLI commands all call `runner.run_dashboard()`:

- `dlt dashboard` — workspace-level (`DashboardCommand` in `dlt/_workspace/cli/commands.py`)
- `dlt pipeline <name> show` — pipeline-specific (`dlt/_workspace/cli/_pipeline_command.py`)
- `dlt workspace show` — workspace-level (`dlt/_workspace/cli/_workspace_command.py`)

Edit mode (`--edit`) ejects `dlt_dashboard.py` and CSS to the cwd for user customization.

## Dependencies

From `pyproject.toml` `[workspace]` extra: `marimo>=0.14.5`, `duckdb>=0.9`, `ibis-framework>=12.0.0`, `pyarrow>=16.0.0`.

## Where to Put New Code

| What you're adding | Where it goes |
|---|---|
| New dashboard section/cell | `dlt_dashboard.py` — add `@app.cell(hide_code=True)` |
| New UI text/messages | `strings.py` — add to appropriate section |
| New data fetching/queries | `utils/queries.py` or new file in `utils/` |
| Schema-related helpers | `utils/schema.py` |
| Pipeline metadata helpers | `utils/pipeline.py` |
| Trace/execution helpers | `utils/trace.py` or `utils/visualization.py` |
| UI component helpers | `utils/ui.py` |
| Formatting/display helpers | `utils/formatters.py` |
| New TypedDicts | `typing.py` |
| New constants/defaults | `const.py` |
| Configuration options | `config.py` (`DashboardConfiguration` dataclass) |
| CSS styling | `dlt_dashboard_styles.css` |
| Data quality features | `utils/data_quality.py` |

## Coding Conventions

- **All user-facing text** goes in `strings.py`, never inline in cells
- **Cell-local variables must start with `_`** (underscore prefix). In marimo, variables that are only used within a single cell and should not be shared across cells must be prefixed with `_`. Only variables intended to be consumed by other cells should be unprefixed.
- **UI helpers**: Use `utils/ui.py` functions (`dlt_table()`, `section()`, `error_callout()`) for consistent UI
- **CSS classes**: `section-header`, `pipeline-execution-timeline`, `status-badge`, `dlt_table`
- **Badge classes**: `.badge-grey`, `.badge-yellow`, `.badge-green`, `.badge-red`
- **Data structures**: TypedDicts in `typing.py` for data passed between cells
- **Configuration**: `@configspec` dataclass in `config.py`

## Testing

### Test locations

```
tests/workspace/helpers/dashboard/    # Unit and integration tests
├── conftest.py                       # Fixtures: pipeline types and temp directories
├── test_pipeline_workflow.py         # Pipeline data retrieval integration
├── test_all_cells.py                 # Cell execution and dependency testing
├── test_visualization.py             # Timeline and badge rendering
├── test_ui_elements.py              # UI component styling
├── test_table_operations.py         # Table listing/filtering
├── test_trace_operations.py         # Trace parsing/display
├── test_query_operations.py         # SQL query execution
├── test_loads.py                    # Load history retrieval
├── test_data_formatting.py          # String formatting
├── test_pipeline_operations.py      # Pipeline metadata
└── test_dashboard_config.py         # Configuration resolution

tests/e2e/                           # Playwright browser tests
```

### Running tests

```bash
make test-workspace-dashboard          # unit/integration tests
make test-e2e-dashboard                # headless chromium e2e
make test-e2e-dashboard-headed         # visible browser e2e
make create-test-pipelines             # create pipelines for manual testing
```

### Test fixtures

Tests use parametrized pipeline fixtures via `ALL_PIPELINES` and `PIPELINES_WITH_LOAD` constants in `conftest.py`. Key fixtures: `success_pipeline_duckdb`, `success_pipeline_filesystem`, `extract_exception_pipeline`, `never_ran_pipeline`, `load_exception_pipeline`.

### Where to add new tests

- **Utils logic**: Add to the matching `test_*.py` file (e.g., new query helper → `test_query_operations.py`)
- **New section/cell**: Add cell execution test to `test_all_cells.py`, plus a dedicated test file if the section has significant logic
- **Visual/rendering**: `test_visualization.py` or `test_ui_elements.py`
- **E2E browser tests**: `tests/e2e/`
