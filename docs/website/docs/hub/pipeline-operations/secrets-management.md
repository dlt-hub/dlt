---
title: Secrets management
description: How dltHub handles your secrets, and how to manage them safely in production with vaults and access controls.
keywords: [secrets, secrets management, vault, encryption, google secret manager, credentials, hub, dltHub]
---

# Secrets management

Pipelines deployed to dltHub need access to credentials (warehouse passwords, API keys, tokens) at run time. This page explains how dltHub handles those secrets for you, and the options you have for managing them yourself in production.

There are two broad approaches:

- **Let dltHub manage your secrets.** Put them in your workspace config and deploy. dltHub encrypts and stores them for you. This is the default and needs no extra setup.
- **Bring your own vault.** Keep secrets in an external secret manager (e.g. Google Secret Manager) and have dlt fetch them at run time. Recommended for production.

## How dltHub handles your secrets

Your secrets never live in plaintext on the platform. When you deploy, your configuration is transmitted over an encrypted channel and stored **encrypted at rest** in a secure, isolated store. That store can be reached by **only a single vault service identity, running inside a private network (VPC)**, so nothing else can touch it. At run time, your pipeline runner is granted **short-lived access** to only the secrets that run needs.

dltHub workspaces use **profiles** to separate environments. Deployed pipelines run under the `prod` profile, so put production secrets in `.dlt/prod.secrets.toml` and development secrets in `.dlt/dev.secrets.toml`. See [profiles](profiles.md) for how `dev`, `prod`, and `access` profiles work and when each is used.

For stronger isolation, especially in production, keep the secret values in your own vault instead.

## Use your own vault: Google Secret Manager

The most robust pattern for production is to keep your actual secret values in an external vault and let dlt fetch them at run time, so your `secrets.toml` holds only the small set of credentials needed to *reach* the vault, never the secrets themselves. dlt supports **Google Secret Manager** today; other vaults (AWS Secrets Manager, Azure Key Vault) are straightforward to add and welcome as contributions.

### 1. Add the dependency

dlt's Google Secret Manager provider requires `google-api-python-client`. Add it to your project dependencies so the deployed runner installs it too. Installing it only into your local environment will make the deployed run fail.

In `pyproject.toml`:

```toml
dependencies = [
    "dlt[hub]>=1.17.0",
    "google-api-python-client",
]
```

Then refresh your lockfile (`uv lock`) before deploying.

### 2. Create a Google service account and store your secrets

These steps are the same for OSS dlt and dltHub. See [Configure Google Secret Provider](../../general-usage/credentials/vaults.md#configure-google-secret-provider) in the OSS docs for:

- Creating a GCP service account with `roles/secretmanager.secretAccessor` and downloading its JSON key.
- Adding secrets to Google Secret Manager, including `dlt_secrets_toml` to store your `secrets.toml` as a single secret.
- The [naming convention](../../general-usage/credentials/vaults.md#naming-convention-for-google-secrets) for individual secrets.

### 3. Point dlt at the vault from your workspace

In `.dlt/secrets.toml`, enable the provider and add **only** the service-account credentials from the JSON key file you created. The actual secrets (your warehouse token, etc.) stay in the vault:

```toml
[providers]
enable_google_secrets = true

[providers.google_secrets]
only_secrets = false
list_secrets = false

[providers.google_secrets.credentials]
project_id = "<project_id>"
client_email = "<...>@<project_id>.iam.gserviceaccount.com"
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
```

You can also configure this with environment variables. See [Activate Google Secret Provider](../../general-usage/credentials/vaults.md#activate-google-secret-provider) for the equivalent setup.

### 4. Deploy and run

```sh
uv run dlthub deploy
uv run dlthub run my_job
```

`dlthub deploy` syncs your code and the vault bootstrap config; the runner then fetches the actual secrets from Google Secret Manager at run time. The secret values themselves are never stored in your deployed config.

## Harden access with static egress IPs

If your vault, or the warehouse it holds credentials for, supports IP allowlisting, restrict it so it only accepts traffic from your dltHub runners. Opt in per job by adding `require={"static_egress_ips": True}` to the job's `@run.pipeline` decorator:

```py
@run.pipeline(my_pipeline, require={"static_egress_ips": True})
def my_job():
    ...
```

Then allowlist your region's dltHub egress IP set on the resource. See [Static egress IPs](job-configuration.md#static-egress-ips) for the current IP lists by region.

## On the roadmap

A built-in dltHub vault is on the roadmap, so you'll be able to manage secrets natively without wiring up an external provider.

## See also

- [Adding credentials](../../walkthroughs/add_credentials.md): general guide to dlt credentials.
- [Vaults](../../general-usage/credentials/vaults.md): OSS reference for vault providers.
- [Profiles](profiles.md): how `dev`, `prod`, and `access` profiles separate environments.
- [Static egress IPs](job-configuration.md#static-egress-ips): pin job outbound traffic to known IPs for vault and destination allowlisting.
