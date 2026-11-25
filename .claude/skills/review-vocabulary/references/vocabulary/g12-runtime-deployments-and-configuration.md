# G12 — Runtime deployments and configuration

What a local workspace uploads to the dltHub Runtime and how it connects. Profiles,
environments, data access, `connect` and the remote workspace are G10; this group adds the
runtime's own nouns. The rulings for every runtime group are in G11.

**Included**

| Concept | Write |
|---|---|
| the packaged workspace code uploaded to the runtime | **deployment** |
| the file that describes every job of a deployment | **deployment manifest**, always qualified (G7) |
| the dependencies dlt exports with a deployment | **requirements** (the type `TWorkspaceRequirementsManifest` keeps its name) |
| the list of files in an upload | **file list** |
| `pyproject.toml`, `requirements.txt` | **dependency file** |
| the uploaded settings files of a workspace | **configuration**, always the full word |
| `config.toml`, `secrets.toml` and their profile variants | **settings files** (G10) |
| the organization a connection is written to | **connected organization** |
| the file that defines a job, as a command argument | **job file**; its path is the **job file path** |
| a value the runtime gives job runs | **variable**: workspace or profile level, **secret** or **plain** |
| a profile with read or write data access, where the context is clear | **read profile**, **write profile** — aliases for G10's "a profile with read data access" and "default write profile" |

**Excluded**

| Never | Because |
|---|---|
| manifest (unqualified) | **deployment manifest** (G7) |
| manifest (for the requirements, the file list or a dependency file) | **requirements**, **file list**, **dependency file** |
| config, configs (for the uploaded configuration) | **configuration**; `config` stays for dlt's resolved object (G5) and the `config` feature group name in backticks |
| script, script path (for the file that defines a job, or a plain `.py` file run as a job) | **job**, **job file**, **job file path**; `script` is a retired term (G7) |
| pin, pinned, pinning (for the organization of a connection) | **connected organization**, **connect**. G10's legal exceptions stand: a dependency version and outbound IPs are pinned |
| deploy (as a noun) | `` `dlthub deploy` `` for the command, **deployment** for what it uploads |
| sync (without a direction) | say the direction: "upload the configuration" (`dlt pipeline sync` pulls, `dlthub … sync` pushes) |
| grant (as a noun, for playground storage access) | **playground access**; G9 bans the noun |

**Rulings**

- **"read profile" is an alias, not a type.** Use it after the full form, or where the sentence
  already talks about data access: "the dashboard runs as the read profile". In a definition or a
  message that introduces the idea, write G10's "a profile with read data access".
- **Settings files and configuration are both correct, never swapped.** dlt reads settings files;
  the runtime stores the workspace's configuration, which is the uploaded set of settings files.
- **`deploy` has two meanings.** `dlt deploy` writes CI files, `dlthub deploy` uploads to the
  runtime. Name the command when the difference matters.
- **Newly non-compliant:** "manifest" alone, "config" for the runtime configuration, "script
  path", "pinned organization". **Newly compliant:** "read profile" and "write profile" where the
  context is clear.
