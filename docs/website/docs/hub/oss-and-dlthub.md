---
title: From dlt to dltHub
description: What you can do for free with dlt open source, and when you need dltHub
keywords: [dlt, dlthub, open source, pricing, free, paid, comparison, license]
---

:::tip
**Short answer:** Use `dlt` if you want a free, code-first ingestion library with no platform. Use dltHub if you want managed runtime, richer tooling, transformations, and data quality.
:::

## dlt

The [dlt](https://github.com/dlt-hub/dlt) open-source library lets you write and run data ingestion pipelines entirely in Python, with no platform dependency.

**What you get:**
- Core pipeline engine: extract, normalize, load
- Schema inference and evolution
- A curated set of verified open-source sources and destinations
- Write dispositions and incremental loading
- Pipeline state management
- All `dlt` CLI commands for pipeline inspection (`dlt pipeline …`)

**A good fit if:** You want a lightweight, code-first ingestion library and are comfortable managing orchestration, scheduling, and operations yourself.

---

## dltHub

dltHub builds on top of `dlt` and adds developer tooling, production runtime, transformations, data quality, and collaboration workflows.

Install with `dlt[hub]`. You can [self-issue](getting-started/installation.md#licensing-) a 30-day trial with `dlt license issue <scope>` before committing to a plan.

### Developer tooling

- [Workspace layout](workspace/init.md) — structured project scaffold (`dlt workspace init`)
- [Additional CLI commands](command-line-interface.md) — workspace and pipeline management
- [Pipeline dashboard](features/ai.md) — local Marimo-based UI for pipeline inspection
- [AI Workbench](../dlt-ecosystem/llm-tooling/llm-native-workflow.md) — AI-assisted workflows

### Production features

| Feature                                                                                 | What it gives you |
|-----------------------------------------------------------------------------------------|---|
| [Python transformations](features/transformations/index.md) (`@dlt.hub.transformation`) | Managed execution, schema enforcement |
| [Data quality checks](features/quality/data-quality.md)                                 | Validation rules, alerting |
| [Iceberg / DuckLake support](ecosystem/iceberg.md)                                      | Managed or BYO lakehouse |
| [MSSQL Change Tracking](ecosystem/ms-sql.md)                                            | Premium source |
| [Managed Platform](runtime/overview.md)                                                 | Cloud execution, APIs, infra |
| Operations & monitoring                                                                 | Run history, alerting, post-mortem debugging |
| Workspace collaboration                                                                 | Shared environments, access control, user management |

**A good fit if:** You are running pipelines in production, need transformations or data quality checks, want managed infra, or are working as a team.

---

## Summary

| | **dlt** | **dltHub** |
|---|---|---|
| **Primary goal** | Build ingestion pipelines | Build, run, and maintain pipelines in production |
| **Where it runs** | Local / self-managed | Local dev + managed cloud |
| **Package** | `dlt` | `dlt[hub]` |
| **License** | Apache 2.0 | Proprietary |


## Next steps

- [Install dltHub](getting-started/installation.md) — covers setup for all tiers
- [Self-issue a trial license](getting-started/installation.md#self-licensing) — explore paid features before committing
- [dltHub intro](intro.md) — overview of products and tiers
- [EULA](EULA.md) — full license terms
