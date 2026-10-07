# G2 — Attach and foreign datasets

**Included**

| Concept | Write |
|---|---|
| the `TAttachInfo` object | **attach info** |
| the `TAttachStatement` object | **attach statement** |
| the SQL keyword | `` `ATTACH` `` in backticks |
| the action in prose | **attach** (lowercase, a verb) |
| the catalog a foreign dataset lands under | **attach alias** |

**Excluded**

| Never | Because |
|---|---|
| descriptor (for `TAttachInfo`) | `attach info` matches the type and the method `_attach_infos()` |
| bare `ATTACH` as a prose noun, `ATTACHed`, "attaches" as a plural noun | backtick the keyword, or use the verb |
| attach instructions | one name — **attach statements** |

**Rulings**

- **`descriptor` is legal for the Python descriptor protocol.** `dlt/common/utils.py` describes a
  real Python descriptor. The ban covers naming the `TAttachInfo` object only.
- **`attach info` and `attach statement` are different things.** One is the whole descriptor for a
  foreign dataset; the other is a single SQL statement inside it. Do not collapse them.
