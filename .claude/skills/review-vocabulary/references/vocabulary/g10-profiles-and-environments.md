# G10 — Profiles and environments

Two nouns carry the feature. A profile is what a process runs with. An environment is what a
deployment is made to. Every comparable platform has the same two layers (dbt profile and target,
Prefect and Spring profile, Meltano and Vercel environment, Databricks target), so the words stay
plain and the docs explain each one once.

**Included**

| Concept | Write |
|---|---|
| what controls access to the user's data: the credentials a pipeline or job runs with and their declared data access; settings, state and data folders are its second function | **profile** |
| a profile that never leaves the machine | **local profile** |
| a profile that deploys with its environment | **remote profile** |
| what a profile can do with the data: `none`, `read` or `write` | **data access** (the `data_access` field) |
| what a deployment is made to, with its remote profiles | **environment** |
| the profile of an environment that jobs of one data access get when they declare no profile | **default profile** of that data access ("the default profile with read data access") |
| the default profile with write data access, which a deployment is made with | **default write profile** |
| what a job needs from the data: the `data` axis of its `access` (G9) | the **data access** that the job **declares** |
| a profile with `<profile>.config.toml` or `<profile>.secrets.toml` | a profile that **has settings files** (`configured` in code) |
| the environment commands assume when none is named | **default environment** |
| the local profile commands assume when none is named | **default profile** (local) |
| the profile a command or process runs with (`ctx.profile`); `dlthub local profile use` records it in `.dlt/profile-name` | **active profile** |
| what `--profile` and `WORKSPACE__PROFILE` do to the recorded profile for one command or process | **override** |
| what `--environment` does to the default write profile of an environment | **activate** |
| an environment where the profile of a job gives exactly the access that the job declares | **strict** environment (`strict_access`), "strict about access" |
| the remote workspace the local workspace deploys to in an environment | **remote workspace** |
| what `dlthub workspace connect` does, per environment | **connect** the local workspace to a remote workspace |
| the state of the local workspace in an environment | **connected**, **not connected** |

**Excluded**

| Never | Because |
|---|---|
| environment (for a profile: "the `dev` environment", "profiles for `dev`, `prod` and `access` environments") | **profile**; an environment is what a deployment is made to |
| target, stack, stage (for an environment) | **environment**, the word GitHub, Vercel and Meltano use; `staging` as the name of one is legal, `target` stays legal for a table, a dataset or a control plane |
| pin, pinned, unpin (for the active profile) | **active profile**, **make `prod` the active profile**; the industry word is active (Prefect, Spring, `kubectl`), the file `.dlt/profile-name` is named once as where dlt records it. Identifiers such as `pin_profile` and `is_pinned` keep their names |
| current profile, profile in use, profile of the context | **active profile**, one word for the profile a process runs with; `dlthub profile list` marks it `(active)` and marks the profile in `.dlt/profile-name` `(recorded)` when an override makes them differ |
| head profile, linked profile, link, deployment profile (for the profiles of an environment) | earlier models; write **default write profile**, **default profile** and **the profiles of the environment** |
| strict about data access | **strict about access**; the rule covers every access axis |
| configured, unconfigured (for a profile, in docs and messages) | **has settings files**, **has no settings files**; say what is missing |
| bind, binding, bound (for the remote workspace) | **connect**, **connected**; `dlthub workspace connect` is the command, the docs never say "bind the repo" |
| connect an environment, connected environment | the workspace is connected, in an environment: **connect the local workspace to a remote workspace in `staging`** |
| env (in prose) | **environment**; the field `env` and the option `--env` keep their names |
| enforce, enforced | **strict**; "the environment is strict about access" |
| synced, synchronized, uploaded, pushed (for a remote profile) | **deploys with its environment**; the toml files go with the deployment, nothing syncs |
| local-only | **local profile**; the adjective repeats the noun |
| switch (for the active profile, in docs) | **make `x` the active profile**; `switch_profile` and `switch_context` keep their names as Python |
| permission, rights (for `data_access`) | **data access**; `rights` is legal for what a credential has at the destination, which dlt does not check |

**Rulings**

- **A deployment gives a job a profile. A job declares one. A person makes one the active
  profile.** Three verbs, three actors: "the deployment gives the notebook `access`", "the job
  declares `sensitive-access` with `require`", "make `tests` the active profile with
  `dlthub local profile use tests`".
- **`access` has three meanings and each has its words.** The built-in profile is `access`, in
  backticks (Rule 8.6), next to `prod` and `no-access`, the built-in profile without data access. A job's declaration is **access** (G9). What a profile can do is **data
  access**, never bare "access". Write "a profile with read data access", not "a read-access
  profile" and not "a read-only profile", unless the sentence is about the credential at the
  destination.
- **Data access is a declaration, not a check.** dlt reads `data_access` to pick a profile for a
  job and never checks what the credential can do. The page that introduces data access says
  so once: "you bring your own destination and set up the rights of each credential there".
- **The job declares, the profile gives.** A job declares the data access it needs; a profile
  gives data access. A deployment gives the job the default profile of the declared data access.
  In a strict environment the profile gives exactly what the job declares. In one that is not
  strict, a job whose default profile is missing or has no settings files gets the closest
  default profile with more data access. Declared is not enough: the profile must have settings
  files.
- **Override and activate are different verbs.** `--profile` and `WORKSPACE__PROFILE` override the
  recorded profile for one command or process: the profile they name is then the active profile.
  `--environment` activates the default write profile of an environment for the command. The recorded profile changes only with
  `dlthub local profile use`. When a sentence needs the recorded one, write "the active profile
  recorded in `.dlt/profile-name`".
- **The environment of a local profile is none.** `dlt.current.workspace().environment` is
  `None` on `dev`. Write "a local profile belongs to no environment", never "the local
  environment".
- **Legal exceptions.** `pin` for a job's outbound IPs and a dependency version. `target` for a
  target table, dataset or control plane. `bound` for a range. `environment` for the process
  environment, environment variables, a Python or virtual environment, a GitHub environment, when
  the sentence says which. `staging` for dlt's staging destination and dataset (G1, G6).
