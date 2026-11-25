# G11 — Runtime, tenancy and members

The dltHub Runtime is the managed execution platform for dlt jobs. Its prose lives in the
`dlt-hub/runtime` repo (the `dlthub` CLI client, its SDK and MCP server, the services and the web
app) and in the hub docs. G11 to G13 cover it; G7 to G10 still govern the dlt concepts it uses.

**Rulings for every runtime group (G11 to G13)**

- **Docstrings use the imperative summary line**, including MCP tool descriptions: "List the job
  runs of the workspace." The STE rules govern the rest of the docstring.
- **`run` may stand alone when the context names one kind of run.** `get_job_runs()` can say
  "run". Write **job run** or **pipeline run** where both kinds are in play or the context is not
  clear.

**Included**

| Concept | Write |
|---|---|
| the managed execution platform | **dltHub Runtime** on first mention in a page or README, then **runtime** (G7) |
| the top-level tenant | **organization** |
| an isolated project space inside an organization | **workspace**; **remote workspace** and **local workspace** when both are in play (G10) |
| a person in an organization or workspace | **member** |
| what a member may do | **role** — organization: owner, member, collaborator; workspace: owner, developer, viewer |
| an invitation to join | **invite**; an invite is **revoked**, never deleted |
| a key for programmatic access | **personal API key**, **workspace API key** |
| where an organization's data lives, as the user picks it | **region** |
| the services that hold user data and run jobs, in engineering prose | **data plane**; also **control plane**, **compute plane** |
| the browser application | **dltHub web app** |

**Excluded**

| Never | Because |
|---|---|
| org, orgs (in prose) | **organization**; no ad-hoc abbreviations. `--org-id`, `org_id` and other identifiers keep their names (Rule 8.6) |
| platform, backend, server (as the product's name) | **runtime**. "platform" is legal as the category word ("a managed execution platform"); "server" is legal for HTTP facts ("the server answers 409") |
| project (for a workspace) | **workspace** |
| user API key, service-account API key | **personal API key**, **workspace API key**, the names the web app and the API use |
| web GUI, web UI, dltHub dashboard (for the web app) | **dltHub web app**; "dashboard" is the workspace dashboard (G13) |
| dataplane (one word, in prose) | **data plane**; `dataplane` identifiers and URL segments keep their spelling |
| data plane (in user-facing CLI and web copy) | **region**, the word the user picks from |

**Rulings**

- **Three things are called "runtime" in the client.** The product is **runtime**. The Python
  package `dlt_runtime` and dlt's `[runtime]` config section are always in backticks. Prose
  "runtime" means the product. G7 still holds: launcher, runner and runtime are three things.
- **`member` is a person and a role.** Write **the member role** when the organization role is
  meant: "Ask an organization owner for the member role."
- **Qualify a workspace only when both are in play.** "Connect the local workspace to a remote
  workspace." Alone, "workspace" is enough.
- **Newly non-compliant:** "org" in docstrings, comments and messages; "the platform" as a name;
  "web GUI"; "user API key". **Newly compliant:** "server" in HTTP-level statements.
