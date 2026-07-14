---
title: The Playground workspace
description: What the Playground workspace is, when to use it, and when to create a dedicated workspace instead.
keywords: [playground, workspace, onboarding, dlthub platform, profiles]
---

# The Playground workspace

## What is the Playground workspace?

The **Playground workspace** is a regular dltHub [workspace](../getting-started/installation.md#what-is-a-dlthub-workspace) that the platform creates for you automatically when you create your account, so you always have somewhere to try dltHub without setting one up yourself. It is personal (single-member) and, unlike other workspaces, cannot be renamed or deleted.

When you run `uvx dlthub-start`, it connects your local project to this workspace and provides a sample pipeline that loads to the [Playground destination](../ingestion/playground.md), so data lands on dltHub-managed storage with no warehouse, bucket, or credentials to configure.

## When to use it

Use the Playground workspace to get started with dltHub:

* Complete onboarding and the guided tour (`uvx dlthub-start`).
* Learn how dltHub works: deployments, cloud runs, and the dashboard.
* Run examples, paired with the [Playground destination](../ingestion/playground.md) so you can load and explore data without any setup.

## When to create a new workspace

Create a dedicated workspace instead of using the Playground when any of these apply:

| Situation | Why a new workspace                                                                                                                                                                                   |
|---|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Real projects** | For actual work, create a dedicated workspace with a name that fits the project. Unlike the Playground, you can rename or delete it, so it is easier to organize and clean up.                        |
| **Team or collaboration** | Workspaces have [roles](../platform-capabilities/users-and-roles.md) (Owner, Viewer). Shared team work belongs in an organization workspace with proper access control, not your personal Playground. |
| **Environment isolation** | If you need to isolate a pipeline's environment, such as its dependencies, credentials, or configuration, you can give it its own workspace.                                                                                    |

You can create a workspace in the platform UI at [app.dlthub.com](https://app.dlthub.com), or from the CLI:

```sh
uv run dlthub login
uv run dlthub workspace connect "<name>" --create
```

Any organization **Member** can create workspaces; **Owners** additionally manage organization settings.

## Next steps

* [Workspace setup](../pipeline-operations/workspace-setup.md): configure destinations, config, and secrets.
* [Profiles](../pipeline-operations/profiles.md): target different destinations from the same code.
* [Deployments](../pipeline-operations/deployments.md): declare jobs and run them on the platform.
* [Users and roles](../platform-capabilities/users-and-roles.md): invite teammates and manage access.
