---
title: Environment variables
description: Set workspace-scoped and profile-scoped environment variables for dltHub platform runs from the web app or the CLI
keywords: [environment variables, workspace variables, secrets, profiles, CLI, hub, dltHub]
---

# Environment variables

Workspace **owners** define environment variables on a dltHub workspace. A shared workspace set applies to every run. Optional per-[profile](profiles.md) values apply when that profile is in use (for example `prod` or `access`). When a job starts, the dltHub platform merges those variables into the run’s process environment (`os.environ`).

Variables are managed on the platform — in the **web app** or with the **CLI** — rather than in the deployable `.dlt` config tree. They are not isolated from it, though: at run time they take priority over it, as described in [What runs receive](#what-runs-receive). [Workspace owners](../platform-capabilities/users-and-roles.md#workspace-roles) can list and change them.

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
3. Optionally **import a `.env` file** by copying its content, to stage many entries at once.
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

# Remove a variable (-y accepts the confirmation prompt)
dlthub variable delete LOG_LEVEL --workspace -y

# Succeed even if the variable is not there
dlthub variable delete LOG_LEVEL --workspace --allow-missing -y
```

On `set`, choose both a kind (`--plain` or `--secret`) and a scope (`--workspace` or `--profile`).

On `delete`, the two flags are orthogonal: `-y` accepts the confirmation prompt, while `--allow-missing` decides whether an absent variable is an error or a success. Scripts that must be repeatable usually want both.

Full command reference: [CLI reference](../command-line-interface.md).

## What runs receive

At run start, the dltHub platform injects the workspace-wide variables and the variables for that run’s profile into the process environment, with profile values overriding workspace-wide values for the same name.

Because they land in the process environment, `dlt` reads them through its [environment variables provider](../../general-usage/credentials/setup.md#environment-variables), which has the highest priority of all config providers. A variable named in the `SECTION__KEY` form therefore overrides the same key in the deployed `secrets.toml` or `config.toml` — a workspace variable `DESTINATION__POSTGRES__CREDENTIALS` wins over the value committed in `prod.secrets.toml`. See [Credentials setup](../../general-usage/credentials/setup.md) for the full provider order.

## See also

- [Credentials setup](../../general-usage/credentials/setup.md) — dlt config providers (TOML, environment, vaults)
- [Secrets management](secrets-management.md) — dltHub-managed secrets and external vaults for cloud runs
- [Profiles](profiles.md) — `dev`, `prod`, `access`, and custom profiles
- [Settings](../platform-capabilities/settings.md) — workspace settings entry point
