# G3 — Transformations and materialization

**Included**

| Concept | Write |
|---|---|
| the deferred path | **lazy materialization** |
| the load job that runs it | **model job** |
| the immediate path | **eager materialization** |

**Excluded**

| Never | Because |
|---|---|
| model extraction | not a thing — it is a **model job** |
| executed here | say **eager materialization** |

**Rulings**

- **`lazy` and `eager` are legal only for materialization.** dlt has both. Using `lazily` to mean
  *on first use* (memoization) is a second meaning for one word — write "on the first read".
- **A model job is the artifact; lazy materialization is the path.** Use the one you mean.
