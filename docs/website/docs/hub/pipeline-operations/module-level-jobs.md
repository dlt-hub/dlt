---
title: Module-level jobs
description: Deploy marimo notebooks, FastMCP servers, Streamlit apps, and plain Python modules as dltHub platform jobs without decorators, and tune them with module-level dunders
keywords: [dlthub platform, module-level jobs, dunders, trigger, expose, require, instance size, marimo, fastmcp, streamlit, deployment module]
---

# Module-level jobs

A job does not have to be a decorated function. When `__deployment__.py` imports a **module** and lists it in `__all__`, the manifest generator inspects the module itself and turns it into one job. No `@run.pipeline`, `@run.job`, or `@run.interactive` appears anywhere in the file. This is how notebooks, MCP servers, dashboards, and plain scripts are deployed.

```py
"""Analytics workspace."""

import sales_report       # marimo notebook -> interactive job
import warehouse_tools    # FastMCP server  -> interactive job
import ops_dashboard      # Streamlit app   -> interactive job
import backfill_script    # plain module    -> batch job

__all__ = ["sales_report", "warehouse_tools", "ops_dashboard", "backfill_script"]
```

A module-level job is named after its module and has no function part. `sales_report.py` becomes the job `jobs.sales_report`, and it reads configuration from the `[jobs.sales_report]` section. See [Job configuration via TOML](job-configuration.md#job-configuration-via-toml).

There are two detection paths: framework-detected modules and plain Python modules.

## Framework-detected modules

The generator probes an imported module for a known framework object. The detectors run in the order marimo, FastMCP, Streamlit, and the first match wins. A module that creates a `FastMCP` instance and also imports `streamlit` is deployed as an MCP server.

| Framework | Detected by | `expose.interface` | `expose.category` |
|-----------|-------------|--------------------|-------------------|
| [marimo](../../general-usage/dataset-access/marimo.md) | a `marimo.App` instance at module level (a variable named `app` is preferred, any other public name also matches) | `gui` | `notebook` |
| FastMCP | a `fastmcp.FastMCP` instance (`mcp`, `server`, and `app` are the preferred names) | `mcp` | `mcp` |
| [Streamlit](../cookbook/build-streamlit-dashboard.md) | the `streamlit` module present in the module namespace, under any alias | `gui` | `dashboard` |

All three produce **interactive** jobs: job type `interactive`, a single `http:` trigger, and `execute.concurrency` of `1`. Interactive jobs run under the `access` profile. See [Batch vs interactive](overview.md#batch-vs-interactive).

The description comes from the module docstring, prefixed with the framework's own title when it has one. A marimo notebook declared as `marimo.App(app_title="Revenue report")` with the docstring `Revenue by region.` gets the description `Revenue report: Revenue by region.` A FastMCP server uses its server name the same way.

## Plain Python modules

A local `.py` module that no framework detector claims becomes a **batch** job. The launcher starts it with `python -m <module>`, so the work has to sit under `if __name__ == "__main__":`. The manifest generator imports the module, so anything running at import time would run on every deploy.

```py
"""Rebuild the reporting tables from scratch."""

def main():
    ...

if __name__ == "__main__":
    main()
```

A module is only detected this way when it is local to the workspace: its file must live below the directory holding `__deployment__.py`, and installed packages are rejected (any path containing `site-packages`, `.venv`, `venv`, `.tox`, or `.nox`).

Unlike the framework detectors, this one adds **no triggers**. Without `__trigger__` the job is deployed but never scheduled, so you start it by hand with `dlthub run backfill_script` or from the dashboard.

## Module-level dunders

Four module-level names override what the detector produced. Set them at the top of the module file. They work for both detection paths.

| Dunder | Decorator equivalent | Effect |
|--------|----------------------|--------|
| `__doc__` (the module docstring) | the job function's docstring | Becomes the job description |
| `__trigger__` | `trigger=` | **Appended** to the triggers the detector set |
| `__expose__` | `expose=` | **Replaces** the detected `expose` dict |
| `__require__` | `require=` | Sets the job's `require` spec |

`__trigger__` takes a single trigger or a list, in the same forms accepted by `trigger=`: a `trigger.*` constructor, a bare cron string, or a raw `"type:expr"` string. See [Triggers and scheduling](triggers.md).

### Append versus replace

This marimo notebook adds an hourly trigger and its own presentation metadata:

```py
"""Weekly sales report."""

import marimo
from dlt.hub.run import trigger

__trigger__ = trigger.every("1h")
__expose__ = {"tags": ["report"], "starred": True}

app = marimo.App()
```

The job it generates:

```yaml
job_ref: jobs.sales_report
entry_point:
  module: sales_report
  function: null
  job_type: interactive
  launcher: dlt._workspace.deployment.launchers.marimo
expose:
  tags:
  - report
  starred: true
  manual: true
triggers:
- 'http:'
- every:1h
execute:
  concurrency: 1
description: Weekly sales report.
default_trigger: every:1h
```

Two things to read out of it:

- `__trigger__` **appended**: the detector's `http:` trigger is still there, `every:1h` was added next to it, and the generator picked the recurring one as `default_trigger`.
- `__expose__` **replaced**: the detector had set `interface: gui` and `category: notebook`, and neither survives. If you rely on those, spell them out yourself:

  ```py
  __expose__ = {
      "interface": "gui",
      "category": "notebook",
      "tags": ["report"],
      "starred": True,
  }
  ```

You do not set `manual: true` yourself. The generator defaults it so the job can be launched from the CLI and the dashboard. Pass `"manual": False` in `__expose__` to turn manual triggering off.

## Setting the instance size

`__require__` is the module-level equivalent of the decorator's `require=`, and it takes the same keys. To give a heavy batch job more CPU and memory, set the instance size on the module:

```py
"""Rebuild the reporting tables from scratch."""

__require__ = {"instance": {"size": "large"}, "dependency_groups": ["heavy"]}

def main():
    ...

if __name__ == "__main__":
    main()
```

The decorator form of the same requirement:

```py
@run.pipeline(my_pipeline, require={"instance": {"size": "large"}})
def heavy_sync():
    ...
```

Both end up as the job's `require` spec in the manifest. Jobs default to `small` when `instance` is not set. For the available sizes and what each costs against your run time budget, see [Instance size](job-configuration.md#instance-size).

:::note
`require.machine` is deprecated since dlt 1.29.0. Setting it, on the decorator or as `__require__`, emits a deprecation warning. Use `require={"instance": {"size": ...}}` instead.
:::

## Check what you are about to deploy

The manifest is generated locally, so you can read the resulting job definitions before anything reaches the dltHub platform:

```sh
# dump the full expanded manifest as YAML
dlthub deploy --show-manifest

# see what would change without applying
dlthub deploy --dry-run
```

Run the job locally first, addressing it by module name:

```sh
dlthub local run backfill_script     # batch module
dlthub local serve sales_report      # notebook, dashboard, or MCP server
```

## Next steps

- [Deployments](deployments.md): `__deployment__.py`, `dlthub deploy`, and reconciliation
- [Triggers and scheduling](triggers.md): everything `__trigger__` accepts
- [Job configuration](job-configuration.md): instance sizes, dependency groups, timeouts, TOML sections
