---
title: Users and roles
description: How users, organizations, and workspaces relate on the dltHub platform, and what each role can do.
keywords: [users, roles, permissions, organization, workspace, access control, RBAC, dltHub platform]
---

# Users and roles

The dltHub platform uses a two-level access model. Every user belongs to an **organization**, and within that organization users are granted access to one or more **workspaces**. A user's effective permissions are determined by the combination of their organization role and their per-workspace role.

## Authentication

Users authenticate against the dltHub platform in the following ways:

- **GitHub OAuth.** Interactive sign-in for both the Web UI ([app.dlthub.com](https://app.dlthub.com)) and the CLI (`dlthub login`). The same identity is used everywhere—your CLI session inherits the workspaces and roles granted to your GitHub account.
- **Google OAuth.** Interactive sign-in to the Web UI ([app.dlthub.com](https://app.dlthub.com)) with a Google account. As with GitHub OAuth, the same identity is used across the Web UI and CLI.
- **Email signup.** Register for the Web UI ([app.dlthub.com](https://app.dlthub.com)) with an email address and password when you don't want to use a third-party identity provider.
- **API keys.** Personal, long-lived tokens (prefixed `dlt_`) for non-interactive clients such as CI jobs or scripts. A key inherits the organization and workspace permissions of the user who created it. See [API keys](settings.md#api-keys) for creating, scoping, and revoking keys.

## Organization roles

Organization membership is a prerequisite for any workspace access — a user must be added to the organization before they can be granted a role in any workspace.

| Role     | Permissions                                                                                                                |
| -------- | -------------------------------------------------------------------------------------------------------------------------- |
| Owner    | Manage organization settings, manage members, create and delete workspaces, and access every workspace in the organization. |
| Member   | Create new workspaces in the organization, and access the workspaces they have been assigned to (with the role granted there). |

## Workspace roles

A workspace role is assigned per workspace and controls what a user can do inside that workspace. A user can hold different workspace roles in different workspaces.

| Role     | Permissions                                                                                                                                                   |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Owner    | Full access: manage members, edit workspace settings, start and cancel runs, manage schedules, manage public links, archive jobs, and publish interactive apps. |
| Viewer   | Read-only access to jobs, runs, logs, pipelines, deployments, and notebooks. Viewers can also launch jobs on the [`access` profile](../core-concepts/profiles-dlthub.md) — for example, running interactive notebooks. |

## Permission scope

Role-based restrictions apply to both the dashboard and the API, so a viewer cannot bypass restrictions by using the CLI.

- **Workspace owners** can launch, cancel, and schedule any job, change workspace configuration, manage members, and publish interactive applications.
- **Workspace viewers** have read access to all workspace data and can launch jobs that run under the `access` profile (notebooks and other interactive read-only workloads). They cannot launch or cancel `prod` jobs, edit schedules, change workspace settings, or manage members.
- **All roles** can view jobs, runs, logs, pipelines, deployments, and notebooks in the workspaces they have access to.

For details on which profiles are used for which workloads, see [Profiles in dltHub](../core-concepts/profiles-dlthub.md).

## Managing members

Members are managed from the workspace **Settings** page. The same flow is used to invite new users, change roles, and remove access.

- **Invite a user.** Add the user to the workspace from Settings and choose their workspace role at invitation time. The user must already belong to the organization, or be invited to it as part of the same flow.
- **Change a role.** Update the role from Settings; the new permissions take effect immediately.
- **Remove a user.** Removing a user from a workspace revokes their access to that workspace immediately. They remain in the organization and can be re-added to the same or other workspaces later without a new invitation.

## See also

- [Regions and data residency](regions.md)
- [Profiles in dltHub](../core-concepts/profiles-dlthub.md)
- [dltHub platform overview](overview.md)
