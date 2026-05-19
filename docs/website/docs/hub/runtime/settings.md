---
title: Settings
description: Configure workspaces, organizations, and personal account settings — including members, usage, and API keys — on the dltHub platform.
keywords: [settings, workspace settings, organization settings, api keys, members, usage, dltHub platform]
---

# Settings

The Settings page is where you configure your workspace, manage your organization, and manage personal account settings such as API keys. Settings are organized into three levels: **workspace**, **organization**, and **user**.

## Workspace settings

Workspace settings are scoped to a single workspace. Workspace [Owners](users-and-roles.md#workspace-roles) can edit them; other roles see the same fields as read-only.

- **Name and description.** Edit the workspace name and description. Changes save immediately.
- **Connection info.** Connection details that external tools and integrations use to talk to the workspace.
- **Usage chart.** A monthly bar chart showing workspace consumption for the previous six months.

### Members

The Members section lists the users who have access to the workspace and their assigned [workspace role](users-and-roles.md#workspace-roles).

Workspace owners can:

- Add a member from the organization and assign them a workspace role.
- Change an existing member's role.
- Remove a member from the workspace. Removal revokes workspace access immediately; the user remains in the organization.

See [Users and roles](users-and-roles.md) for the full permission model.

## Organization settings

Organization settings are accessed through the organization-level navigation and apply to every workspace in the organization. Only [organization Owners](users-and-roles.md#organization-roles) can edit them.

- **Name and description.** Edit the organization name and description.
- **Usage.** Aggregate usage metrics across all workspaces in the organization.
- **Members.** Invite users to the organization and assign them an [organization role](users-and-roles.md#organization-roles) (Owner or Member). Organization membership is required before a user can be granted any workspace role.

## User settings

User settings are personal to your account and are accessed from the user menu in the sidebar.

### API keys

API keys are personal, long-lived tokens that authenticate non-interactive clients on your behalf — for example, CLI tools or CI jobs that need to run without an interactive login.

Key properties:

- **Scope.** Tied to your user account. A key inherits your organization and workspace permissions.
- **Prefix.** All keys are prefixed with `dlt_`.
- **Storage.** Only a SHA-256 hash is stored on the server; the plaintext value is never persisted.
- **Expiration.** You choose an expiry between 1 and 365 days at creation.

#### Create a key

1. Open **User settings → API keys** and click **Create key**.
2. Enter a name and choose an expiry between 1 and 365 days.
3. Copy the plaintext value shown after creation.

:::caution
The plaintext value is displayed only once, at creation. It cannot be retrieved later — if you lose it, delete the key and create a new one.
:::

#### Delete a key

Existing keys can be deleted at any time. Once deleted, the key can no longer authenticate and any clients using it will start failing immediately.

## See also

- [Users and roles](users-and-roles.md)
- [Regions and data residency](regions.md)
- [dltHub platform overview](overview.md)
