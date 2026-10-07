# G6 — Merge strategies and merge conditions

**Included**

| Concept | Write |
|---|---|
| removing destination records (`delete-insert`, `upsert`, `cdc`, hard deletes) | **delete** |
| closing the validity of an `scd2` record | **retire** (never "delete" for `scd2`) |
| loaded records that `source_filter` does not select | **discard**, **discarded** |
| writing loaded records to the destination | **insert** (a new record), **update** (an existing one) |
| a record in the destination that the loaded data does not contain | **absent** |
| the part of a table a load replaces | **partition** (conceptual, as `merge_key` docs already use it) |
| the loaded data under `cdc` | **snapshot** |
| the upstream system whose records `cdc` mirrors | **source system** |
| the loaded records that the source filter selects (all loaded records without a filter) | **merge source** (a noun) |
| the `source_filter` option in prose | **source filter** |
| the `destination_scope` option in prose | **destination scope** |
| `source_filter`, `destination_scope` together | **merge conditions** |
| `{table}`, `{staging_table}` | **placeholders**; "expand" for substitution |
| data in docs and messages | **records**; **rows** only in code docstrings and comments, and in "nested rows" |

**Excluded**

| Never | Because |
|---|---|
| region, window, bare "scope" (for the records a merge can delete or retire) | say **destination scope** for the option, **partition** for the concept |
| input filter, output filter, merge filter(s), merge scope | old names — say **source filter**, **destination scope**, **merge conditions** |
| filtered staging (data, rows, records), filtered load, filtered loaded data, bare "the source" (for the merge source) | say **merge source** |
| bare "the source", upstream, origin (for the system `cdc` mirrors) | say **source system**; `@dlt.source` keeps its name |
| owns, is authoritative for (a load and its records) | say "the records that this load replaces" |
| clear, remove, wipe (for a merge delete) | one verb — **delete** |
| drop, exclude, filter out, land (for loaded records) | **discard** for records the source filter does not select, **insert** for records that are written |
| supersede, take precedence (between merge conditions and `merge_key`) | say "dlt ignores `merge_key`" |
| strand, migrate (a record that changes partition) | say "the record moves to another partition" |
| knob | say **setting** or name the hint |

**Rulings**

- **The bans are for the merge meaning only.** `region` stays legal for cloud regions and data columns,
  `window` for browser windows and the attribution window in `lag.md`, `scope` for OAuth and config scopes,
  `drop` for dropping tables and the `dlt pipeline drop` command, `remove` outside merge deletes.
- **"source" in "source filter" is the merge source**, the loaded data, as in `MERGE ... USING source`. It is
  not `@dlt.source`. Never shorten "source filter" to "the source", and never write "destination scope" as
  "the scope".
- **The merge source is what the merge works on.** Keys, `merge_key` partitions and absent records come from the
  merge source, not from all loaded records. The destination scope does not depend on it.
- **`loaded data` and `loaded records` stay legal** for everything loaded, before the source filter. Write
  "merge source" only where a source filter can apply. Without one, the two are the same.
- **"MERGE source" in SQL context is legal.** `MERGE ... USING` names its input the source; uppercase `MERGE`
  marks that meaning (Databricks comments).
- **Only strategies that delete or retire absent records have a destination scope**: `delete-insert`,
  `scd2`, `cdc`. `upsert` also deletes (with `hard_delete`), so "strategies that delete" is not the rule.
- **`delete` vs `retire`.** `scd2` never deletes on absence, so "`scd2` deletes absent records" is a content
  error, not only a vocabulary one.
- **`snapshot` is legal only for `cdc` loaded data.** Iceberg and Delta snapshots are a different technical noun.
  Do not write "snapshot" for Iceberg `upsert` limits: write "absent from the loaded data".
- **Placeholders differ per destination.** Do not write "`{table}` expands to the destination table" without
  naming the destination type: on Delta it expands to `target`.

Newly non-compliant: "region", "window", bare "scope", "owns", "clear", "supersede" in merge prose, and the
old option names ("input filter", "output filter", "merge filters"), and "filtered staging" or bare "the source"
for the merge source. Newly compliant: "destination scope" for the `destination_scope` option, "merge source".
