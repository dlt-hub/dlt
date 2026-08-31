---
title: Settings
description: Configure workspaces, organizations, and personal account settings, including members, usage, and API keys, on the dltHub platform.
keywords: [settings, workspace settings, organization settings, api keys, members, usage, dltHub platform]
---

# Settings

The Settings page is where you configure your workspace, manage your organization, and manage personal account settings. Settings are organized into three levels: **workspace**, **organization**, and **user**.

## Workspace settings

Workspace settings are scoped to a single workspace. Workspace [Owners](users-and-roles.md#workspace-roles) can edit them; other roles see the same fields as read-only.

- **Name and description.** Edit the workspace name and description, then save with **Update Workspace**.
- **Connection info.** Connection details that external tools and integrations use to talk to the workspace.
- **Environment variables.** Define plain and secret process environment variables for the whole workspace or for a specific profile. See [Environment variables](../pipeline-operations/environment-variables.md).
- **Alerts.** Subscribe to workspace alerts and get an email when a job run fails. See [dltHub Platform alerts](../notifications/email.md#dlthub-platform-alerts).
- **Usage chart.** A monthly bar chart showing workspace consumption for the previous six months.

### Members

The Members section lists the users who have access to the workspace and their assigned [workspace role](users-and-roles.md#workspace-roles).

Workspace owners can:

- Add a member from the organization and assign them a workspace role.
- Change an existing member's role.
- Remove a member from the workspace. Removal revokes workspace access immediately; the user remains in the organization.

:::note
Users invited directly to a workspace hold a regular workspace role here, but at the organization level they're tracked as [Collaborators](users-and-roles.md#organization-roles) rather than full organization members. Their organization membership only exists through their workspace access, so removing them from their last workspace in the organization also removes them from the organization.
:::

#### Default access

Besides adding members individually, workspace owners can set a **Default Access** level for the workspace. It controls what access organization members get without being explicitly invited:

- **Restricted.** Only invited people have access to the workspace.
- **Viewer.** All organization members receive the [Viewer role](users-and-roles.md#workspace-roles) in this workspace.
- **Developer.** All organization members receive the Developer role in this workspace.

:::note
Default access applies to organization members only. It doesn't extend to collaborators, who can only access the workspaces they were explicitly added to.
:::

See [Users and roles](users-and-roles.md) for the full permission model.

### Workspace API keys

:::warning
This feature is in public preview
:::

Workspace API keys are long-lived tokens that authenticate non-interactive clients on behalf of a workspace rather than a personal user account. Use them for automation that should keep working independently of any individual user's account or workspace membership.

Key properties:

- **Scope.** Tied to a single workspace. The key grants access to that workspace only.
- **Role.** Each key is created with a [workspace role](users-and-roles.md#workspace-roles), Developer (the default) or Viewer, fixed for the key's lifetime. A Developer key can deploy and run data-writing jobs; a Viewer key is read-only.
- **Prefix.** Workspace API keys are prefixed with `dlt_sa_`.
- **Expiration.** Every key expires, after 90 days by default and 365 days at most. An expired key stops authenticating.
- **Revocation.** Keys can be deleted at any time; a deleted key stops authenticating immediately.

:::caution
The plaintext value is displayed only once, at creation. It can't be retrieved later; if you lose it, delete the key and create a new one.
:::

#### Use a workspace key

Clients authenticate with a workspace key the same way as with a [user API key](#api-keys). Because the key is valid for a single workspace, the workspace id should be pinned as well. Set `api_key` in `.dlt/secrets.toml` and `workspace_id` in `.dlt/config.toml`, both under `[runtime]`:

```toml
# .dlt/secrets.toml
[runtime]
api_key = "dlt_sa_..."
```

```toml
# .dlt/config.toml
[runtime]
workspace_id = "your-workspace-id"
```

Both values can also be set as environment variables: `RUNTIME__API_KEY` and `RUNTIME__WORKSPACE_ID`. See [Workspace setup](../pipeline-operations/workspace-setup.md) for the full runtime configuration.

:::note
API key mode is non-interactive, the same as with a [user key](#use-a-user-key): `dlthub login` refuses to run while a key is configured, and the CLI never opens the workspace picker. Setting `workspace_id` explicitly is optional; running `dlthub workspace connect` once pins the key's workspace. Because the key is bound to a single workspace, `dlthub workspace connect` can't target any other workspace, and `dlthub workspace connect --create` isn't available.
:::

## Organization settings

Organization settings are accessed through the organization-level navigation and apply to every workspace in the organization. Only [organization Owners](users-and-roles.md#organization-roles) can edit them.

- **Name and description.** Edit the organization name and description.
- **Usage.** Aggregate usage metrics across all workspaces in the organization.
- **Members.** Invite users to the organization and assign them an Owner or Member [organization role](users-and-roles.md#organization-roles).

:::note
Users invited directly to a single workspace don't need organization membership; they appear in the member list with the Collaborator role.
:::

## User settings

User settings are personal to your account and are accessed from the user menu in the sidebar.

### API keys

API keys are personal, long-lived tokens that authenticate non-interactive clients on your behalf. For automation that shouldn't be tied to an individual account, use [workspace API keys](#workspace-api-keys) instead.

Key properties:

- **Scope.** Tied to your user account. A key inherits your organization and workspace permissions.
- **Prefix.** User API keys are prefixed with `dlt_u_`.
- **Expiration.** Every key expires, after 90 days by default and 365 days at most. An expired key stops authenticating.
- **Revocation.** Keys can be deleted at any time; a deleted key stops authenticating immediately.

:::caution
The plaintext value is displayed only once, at creation. It can't be retrieved later; if you lose it, delete the key and create a new one.
:::

#### Use a user key

A client authenticates by supplying the plaintext value as the `RUNTIME__API_KEY` setting. It's resolved like any other dlt runtime setting, so you can provide it two ways:

Environment variable, recommended for CI and headless runs:

```sh
export RUNTIME__API_KEY="dlt_u_..."
```

Or in `secrets.toml` under the `[runtime]` section:

```toml
[runtime]
api_key = "dlt_u_..."
```

When an API key is set, the CLI uses it for every request and skips the interactive OAuth flow. In fact, `dlthub login` refuses to run while a key is configured, and `dlthub logout` leaves the key in place; to switch back to interactive login, remove the key from your configuration. See [Workspace setup](../pipeline-operations/workspace-setup.md) for the full runtime configuration.

:::note
Because API key mode is non-interactive, the CLI never opens the workspace picker. Target a workspace explicitly by setting `RUNTIME__WORKSPACE_ID` alongside the key, or by running `dlthub workspace connect <name>` once to pin it.
:::

## See also

- [Users and roles](users-and-roles.md)
- [Environment variables](../pipeline-operations/environment-variables.md)
- [Regions and data residency](regions.md)
- [Send email notifications](../notifications/email.md)
- [dltHub platform overview](../pipeline-operations/overview.md)
