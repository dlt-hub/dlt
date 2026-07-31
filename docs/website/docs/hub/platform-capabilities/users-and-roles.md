---
title: Users and roles
description: How users, organizations, and workspaces relate on the dltHub platform, and what each role can do.
keywords: [users, roles, permissions, organization, workspace, access control, RBAC, dltHub platform]
---

# Users and roles

The dltHub platform uses a two-level access model. Every user belongs to an **organization**, and within that organization users are granted access to one or more **workspaces**. A user's effective permissions are determined by the combination of their organization role and their per-workspace role.

## Authentication

Users authenticate against the dltHub platform in the following ways:

- **GitHub OAuth.** Interactive sign-in for both the Web UI at [app.dlthub.com](https://app.dlthub.com) and the CLI via `dlthub login`. The same identity is used everywhere: your CLI session inherits the workspaces and roles granted to your GitHub account.
- **Google OAuth.** Interactive sign-in to the Web UI at [app.dlthub.com](https://app.dlthub.com) with a Google Account. As with GitHub OAuth, the same identity is used across the Web UI and CLI.
- **Email signup.** Register for the Web UI at [app.dlthub.com](https://app.dlthub.com) with an email address and password when you don't want to use a third-party identity provider.
- **API keys.** Long-lived tokens for non-interactive clients. [User API keys](settings.md#api-keys), prefixed `dlt_u_`, act on your behalf and inherit your organization and workspace permissions. [Workspace API keys](settings.md#workspace-api-keys), prefixed `dlt_sa_`, are scoped to a single workspace, hold a fixed workspace role (Developer or Viewer), and work independently of any user account.

## Organization roles

Every workspace user also holds a role in the organization that owns the workspace. Users invited to the organization are Owners or Members; users invited directly to a single workspace receive the Collaborator role automatically.

| Role         | Permissions                                                                                                                |
| ------------ | -------------------------------------------------------------------------------------------------------------------------- |
| Owner        | Manage organization settings, manage members, create workspaces, and access every workspace in the organization as a workspace owner. |
| Member       | Create new workspaces in the organization, and access the workspaces they have been assigned to or that grant a [default access](settings.md#default-access) level. |
| Collaborator | Access only the workspaces they were explicitly added to, with the role granted there. Default access levels don't apply to collaborators. |

## Workspace roles

A workspace role is assigned per workspace and controls what a user can do inside that workspace. A user can hold different workspace roles in different workspaces.

| Role      | Permissions                                                                                                                                                   |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Owner     | Everything a Developer can do, plus manage members and invites, edit workspace settings, and set the default access level. |
| Developer | Everything a Viewer can do, plus deploy scripts and deployments, start and cancel runs, manage schedules, manage public links, and archive jobs. |
| Viewer    | Read-only access to jobs, runs, logs, pipelines, deployments, and notebooks. Viewers can also launch jobs on the [`access` profile](../pipeline-operations/profiles.md), for example running interactive notebooks. |

## Permission scope

Role-based restrictions apply to both the dashboard and the API, so a viewer can't bypass restrictions by using the CLI.

- **Workspace owners** can do everything developers can, and additionally manage members and change workspace settings.
- **Workspace developers** can launch, cancel, and schedule any job, deploy scripts, manage public links, and publish interactive applications. They can't manage members or change workspace settings.
- **Workspace viewers** have read access to all workspace data and can launch jobs that run under the `access` profile, such as notebooks and other interactive read-only workloads. They can't launch `prod` jobs, cancel runs, edit schedules, change workspace settings, or manage members.
- **All roles** can view jobs, runs, logs, pipelines, deployments, and notebooks in the workspaces they have access to.

For details on which profiles are used for which workloads, see [Profiles in dltHub](../pipeline-operations/profiles.md).

## Managing members

Members are managed from the workspace **Settings** page. The same flow is used to invite new users, change roles, and remove access.

- **Invite a user.** Add the user to the workspace from Settings and choose their workspace role at invitation time. The user must already belong to the organization, or be invited to it as part of the same flow.
- **Change a role.** Update the role from Settings; the new permissions take effect immediately.
- **Remove a user.** Removing a user from a workspace revokes their access to that workspace immediately. They remain in the organization and can be re-added to the same or other workspaces later without a new invitation.

:::note
Collaborators are an exception to the last point: their organization membership is anchored by their workspace memberships, so a collaborator removed from their last workspace in the organization is removed from the organization as well.
:::

## See also

- [Regions and data residency](regions.md)
- [Profiles in dltHub](../pipeline-operations/profiles.md)
- [dltHub platform overview](../pipeline-operations/overview.md)
