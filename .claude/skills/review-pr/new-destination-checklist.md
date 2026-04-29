# New destination review checklist

Use this checklist when reviewing a PR that adds a new dlt destination. These items are in addition to the standard review steps.

## 1. Shared destination test integration

- Check `tests/load/utils.py` `destinations_configs()` — is the new destination included in the right config groups (`default_vector_configs`, `default_sql_configs`, `read_only_sqlclient_configs`, etc.)?
- Check `tests/load/test_read_interfaces.py` — does the destination need special handling (chunk size skips at `_chunk_size()`, view-only table listing exclusions in `test_ibis_dataset_access`)?
- Check `tests/load/pipeline/test_restore_state.py` — state sync tests run via the config groups above, verify the destination participates.
- Check `tests/utils.py` — is the destination in `IMPLEMENTED_DESTINATIONS` and `NON_SQL_DESTINATIONS` (if applicable)?

## 2. Capabilities configuration

- Verify `_raw_capabilities()` in the factory: loader formats, merge strategies, replace strategies, type mapper, nested types, decimal precision, timestamp precision, recommended file size.
- Compare to the blueprint destination (ducklake, lancedb, filesystem) — are any caps missing or misconfigured?
- Check `parquet_format` settings if the destination writes parquet with nested types.

## 3. `WithLocalFiles` integration

- If the destination stores data locally (files, embedded databases), the top-level `DestinationClient*Configuration` **must** inherit `WithLocalFiles` — otherwise the pipeline cannot bind `pipeline_name`, `pipeline_working_dir`, `local_dir` into the config.
- If storage config is nested (e.g. `storage: FilesystemConfiguration`), `on_resolved()` must call `self.storage.attach_from(self)` followed by resolve or `normalize_bucket_url()`. Reference: ducklake pattern in `ducklake/configuration.py`.
- **Test it**: verify default relative paths resolve to `local_dir`, not cwd. Verify `pipeline_name` and `pipeline_working_dir` propagate to nested storage config.

## 4. SQL client — `WithTableScanners`

- If the destination provides a read-only SQL interface via DuckDB views (like lance, lancedb, filesystem), it should extend `WithTableScanners` (`dlt/destinations/impl/duckdb/sql_client.py`), not `DuckDbSqlClient` directly.
- `WithTableScanners` provides: in-memory DuckDB management, schema creation, sqlglot-based lazy view loading, `create_views_for_all_tables()` for ibis offload.
- The subclass implements three abstract methods: `create_view()`, `can_create_view()`, `should_replace_view()`.

## 5. Ibis integration

- Check `dlt/helpers/ibis.py` — is there a branch for the new destination's config class?
- For `WithTableScanners`-based destinations: use `sql_client.create_views_for_all_tables()` (not manual table iteration).
- Add the destination to the view-only exclusion list in `test_ibis_dataset_access` if it uses DuckDB views (won't see tables from other schemas).
- The ibis branch must come before any parent config class check to avoid wrong dispatch via `issubclass`.

## 6. Import rules

- Never import optional packages directly — use `dlt.common.libs.*` wrappers (`pyarrow`, `pandas`, `numpy`, etc.).
- Private API imports from third-party libs (underscore-prefixed functions) are fragile — flag them.
