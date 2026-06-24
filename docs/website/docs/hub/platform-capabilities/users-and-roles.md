---
title: Users and roles
description: How users, organizations, and workspaces relate on the dltHub platform, how to invite people, and what each role can do.
keywords: [users, roles, permissions, organization, workspace, access control, RBAC, invite, members, dltHub platform]
---

# Users and roles

The dltHub platform uses a two-level access model. Every user belongs to an **organization**, and within that organization users are granted access to one or more **workspaces**. A user's effective permissions are determined by the combination of their organization role and their per-workspace role.

## Authentication

Users authenticate against the dltHub platform in the following ways:

- **GitHub OAuth.** Interactive sign-in for both the Web UI ([app.dlthub.com](https://app.dlthub.com)) and the CLI (`dlthub login`). The same identity is used everywhere. Your CLI session inherits the workspaces and roles granted to your GitHub account.
- **Google OAuth.** Interactive sign-in to the Web UI ([app.dlthub.com](https://app.dlthub.com)) with a Google Account. As with GitHub OAuth, the same identity is used across the Web UI and CLI.
- **Email signup.** Register for the Web UI ([app.dlthub.com](https://app.dlthub.com)) with an email address and password when you don't want to use a third-party identity provider.
- **API keys.** Personal, long-lived tokens (prefixed `dlt_`) for non-interactive clients such as CI jobs or scripts. A key inherits the organization and workspace permissions of the user who created it. See [API keys](settings.md#api-keys) for creating, scoping, and revoking keys.

## Inviting people to your organization and workspaces

:::info Public preview
This feature is available as a preliminary public preview to all dltHub customers.
:::

You can invite teammates by email into a whole organization or into a specific workspace, and control what each person can do with [roles](#roles).

### Inviting someone

1. Open the **Settings** page for the organization or workspace you want to add someone to.
2. In the members section, find the pending-invites area and enter the person's **email address**.
3. Pick the **role** they should have (see [Roles](#roles) below).
4. Send the invite. It appears in the pending-invites list until it's accepted.

You can invite people who don't have an account yet. They'll be added automatically when they sign up and sign in with that email.

### Accepting an invite

There's nothing for the invited person to click. The next time they **sign in** with the email the invite was sent to, they are automatically added to the organization or workspace with the role you chose. Brand-new users skip the "create your own organization" step and land directly in the team that invited them.

### Revoking an invite

If you invited the wrong person or no longer want them to join, open the pending-invites list and **revoke** the invite. A revoked invite won't be accepted on sign-in. You can always send a new invite later.

## Roles

Roles decide what a member can see and do. Organizations and workspaces have separate roles.

### Organization roles

Organization membership is a prerequisite for any workspace access: a user must be added to the organization before they can be granted a role in any workspace.

| Role     | Can do |
| -------- | ------ |
| `owner`  | Full control: manage members and invites, change roles, manage workspaces and billing. |
| `member` | Standard access: work within the organization and the workspaces they belong to. |
| `collaborator`  | Limited access: typically someone invited to a single workspace rather than the whole org. |

### Workspace roles

A workspace role is assigned per workspace and controls what a user can do inside that workspace. A user can hold different workspace roles in different workspaces.

| Role        | Can do |
| ----------- | ------ |
| `owner`     | Full control: manage members, invites, settings, and content in the workspace. |
| `developer` | Edit access: create and edit work in the workspace (such as configurations, deployments, and run jobs). |
| `viewer`    | Read-only access: jobs, runs, logs, pipelines, deployments, and notebooks. Viewers can also launch jobs on the [access profile](../pipeline-operations/profiles.md), such as interactive notebooks. |

### How invites and roles combine

- Inviting someone to a **workspace** also adds them to the parent **organization** as a `collaborator`, so they can reach that workspace. If they are later removed from their last workspace in that organization, they are removed from the organization too.
- If someone has more than one pending invite for the same organization or workspace, they get the **most permissive** role. Accepting an invite never lowers a role they already have.

## Permission scope

Role-based restrictions apply to both the dashboard and the API, so a viewer can't bypass restrictions by using the CLI.

- **Workspace owners** can launch, cancel, and schedule any job, change workspace configuration, manage members, and publish interactive applications.
- **Workspace developers** can create and edit work in the workspace (configurations, deployments, and run jobs) and launch and cancel jobs, including on the `prod` profile. They can't manage members or change workspace settings.
- **Workspace viewers** have read access to all workspace data and can launch jobs that run under the `access` profile (notebooks and other interactive read-only workloads). They can't launch or cancel `prod` jobs, edit schedules, change workspace settings, or manage members.
- **All roles** can view jobs, runs, logs, pipelines, deployments, and notebooks in the workspaces they have access to.

For details on which profiles are used for which workloads, see [Profiles in dltHub](../pipeline-operations/profiles.md).

## Managing members

In the members section of an organization's or workspace's settings, you can:

- **Change a member's role** with the inline role dropdown. The new permissions take effect immediately.
- **Remove a member.** Removing a user from a workspace revokes their access to that workspace immediately.

A few rules keep things safe:

- You can't change **your own** role from the members table.
- An organization or workspace can have **multiple owners**, but the **last owner can't be removed or demoted**. Promote someone else to owner first.

## Multiple members per role

Every role can be held by any number of people; there are no single-holder roles. In particular, organizations and workspaces can have more than one **owner**, so responsibility isn't tied to a single person. Any owner can invite people, manage members, and change roles. The only limit is that the **last owner can't be removed or demoted**. Promote someone else to owner first.

## See also

- [Regions and data residency](regions.md)
- [Profiles in dltHub](../pipeline-operations/profiles.md)
- [dltHub platform overview](../pipeline-operations/overview.md)
