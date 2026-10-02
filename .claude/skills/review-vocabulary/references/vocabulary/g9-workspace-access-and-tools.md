# G9 — Workspace access and tools

**Included**

| Concept | Write |
|---|---|
| the declaration of what a job may touch | **access** (the `access` block, `TWorkspaceAccess`) |
| one of its four keys | **access axis** |
| a value on an axis | **verb** |
| what the runtime or a loop does with the declaration | **grant**, **deny** (verbs) |
| what it did grant | **granted access** |
| what it did not grant | **denied access** |
| what a loop supplies beyond the declaration | **over-granted** |
| what a tool needs before it is served | **required access** |
| what a model can call | **tool** |
| the MCP grouping a manifest requests | **feature group** |

**Excluded**

| Never | Because |
|---|---|
| permission, entitlement, scope (for the declaration) | **access**, the word dlt's docs already use; `permission` is legal only when naming the SDK's `permission_mode` |
| narrowed, narrowing | say the fact: **with less access** |
| grant (as a noun) | **the declared access**; `granted` as an adjective is legal (Rule 3.3) |
| capability (for a dlt tool) | **tool**; legal only for pydantic-ai `capabilities` |
| ceiling, floor (as prose metaphor) | say what it does: "access is the most a loop can wire" |
| honor, honour, honored, unhonoured (any spelling) | access is **granted** or **denied**, never honored |

**Rulings**

- **Grant and deny are the verbs.** A job declares access; the runtime and the loop grant what they
  can; the rest is denied. dlt's docs already write "grant access" 13 times and "denied access"
  twice, so this is the house pairing, not a new one. A loop that supplies more than the
  declaration **over-grants**.
- **The declaration is a request, not a claim.** A manifest `access` block says what the job wants.
  Nothing in it is granted until the runtime says so. Say "the job declares", never "the job has".
- **`access` is the noun, and no synonym joins it.** `permissions`, `scopes`, `capabilities` and
  `entitlements` each name this concept somewhere in the industry; dlt named it `access` before this
  feature existed — the built-in `access` profile is coarse-grained data access, and `profile_for()`
  derives that profile from `access.data`. One concept, one word (Rule 1.11).
- **`tools:` in an `AGENT.md` holds feature groups, not tools.** Write "feature groups" whenever the
  sentence is about that field, or a reader counts 4 tools and gets 19.
