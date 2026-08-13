---
title: Environment variables
description: Set workspace-scoped and profile-scoped environment variables for dltHub platform runs from the web app or the CLI
keywords: [environment variables, workspace variables, secrets, profiles, CLI, hub, dltHub]
---

# Environment variables

Workspace **owners** define environment variables on a dltHub workspace. A shared workspace set applies to every run; optional per-[profile](profiles.md) values apply when that profile is in use (for example `prod` or `access`). When a job starts, the platform merges those variables into the run’s process environment (`os.environ`).

Variables are managed on the platform — in the **web app** or with the **CLI** — separately from the deployable `.dlt` config tree. [Workspace owners](../platform-capabilities/users-and-roles.md#workspace-roles) can list and change them.

Related configuration on dlt and dltHub:

- How dlt resolves config and secrets from providers (including environment variables and TOML): [Credentials setup](../../general-usage/credentials/setup.md)
- How Hub stores and serves workspace secrets and vaults for cloud runs: [Secrets management](secrets-management.md)

## Scopes and precedence

| Scope | Meaning |
| ----- | ------- |
| **Workspace** | Shared variables applied to every run in the workspace |
| **Profile** | Variables applied when the run uses that profile |

When the same name exists in both scopes, the **profile value** is used.

On the dltHub platform, [batch jobs](overview.md#batch-vs-interactive) use the `prod` profile and [interactive jobs](overview.md#batch-vs-interactive) use the `access` profile. Profile-scoped variables for those names apply on top of the workspace-wide set. See [Profiles](profiles.md) and [Workspace setup](workspace-setup.md#understanding-workspace-profiles).

## Plain and secret variables

| Kind | Behavior |
| ---- | -------- |
| **Plain** | Values are readable when you list variables. An empty value is allowed. |
| **Secret** | Values are write-only. After save, the UI keeps them hidden and the CLI lists them masked. Secrets require a non-empty value. Update a secret by replacing it. |

## Manage variables in the web app

1. Open the workspace, go to **Settings**, and open **Environment Variables**.
2. Add or edit variables. Choose the **workspace** scope or a specific **profile**.
3. Optionally **import a `.env` file** to stage many entries at once.
4. Review the pending changes, then **Save**. Staged edits apply only after that explicit save.

:::caution
Secret values are visible only while you enter them. After save they are write-only — replace a secret to change its value.
:::

## Manage variables with the CLI

From a connected workspace (see [Workspace setup](workspace-setup.md)), owners use `dlthub variable`:

```sh
# List every scope (secret values are masked)
dlthub variable list

# List one scope
dlthub variable list --workspace
dlthub variable list --profile prod

# Set a plain workspace-wide variable (value from stdin, or pass --value)
echo "INFO" | dlthub variable set LOG_LEVEL --plain --workspace

# Set a secret for the prod profile (stdin / prompt keeps the value out of shell history)
dlthub variable set SENTRY_DSN --secret --profile prod

# Remove a variable
dlthub variable delete LOG_LEVEL --workspace -y
```

On `set`, choose both a kind (`--plain` or `--secret`) and a scope (`--workspace` or `--profile`).

Full command reference: [CLI reference](../command-line-interface.md).

## What runs receive

At run start, the platform injects the workspace-wide variables and the variables for that run’s profile into the process environment. Profile values override workspace-wide values for the same name.

The runtime keeps control of reserved and runtime-owned names. A run starts only after variable resolution succeeds with a complete environment.

## See also

- [Credentials setup](../../general-usage/credentials/setup.md) — dlt config providers (TOML, environment, vaults)
- [Secrets management](secrets-management.md) — Hub-managed secrets and external vaults for cloud runs
- [Profiles](profiles.md) — `dev`, `prod`, `access`, and custom profiles
- [Settings](../platform-capabilities/settings.md) — workspace settings entry point
- [Users and roles](../platform-capabilities/users-and-roles.md) — owner-only management
- [Job configuration](job-configuration.md) — per-job runner options
- [CLI reference](../command-line-interface.md) — generated `dlthub` command reference
