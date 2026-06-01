from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional

from dlt.common.schema import TTableSchema

if TYPE_CHECKING:
    import pyarrow as pa

SUPPORTED_WRITE_DISPOSITIONS = frozenset({"replace", "append", "merge", "upsert", "insert-only"})


def resolve_write_disposition(table: TTableSchema, default: str) -> str:
    disposition = table.get("write_disposition") or default
    return disposition.lower()


def resolve_primary_key(table: TTableSchema) -> Optional[List[str]]:
    primary_key = table.get("primary_key")
    if primary_key is None:
        return None
    return list(primary_key)


def row_key(row: dict, keys: List[str]) -> tuple:
    values = tuple(row.get(key) for key in keys)
    missing = [k for k, v in zip(keys, values, strict=True) if v is None]
    if missing:
        raise ValueError(
            f"Primary key field(s) {missing} are None or missing in row -- cannot merge"
        )
    return values


def merge_rows(
    existing: List[dict],
    incoming: List[dict],
    *,
    primary_key: List[str],
) -> List[dict]:
    merged = list(existing)
    index = {row_key(row, primary_key): position for position, row in enumerate(merged)}
    for row in incoming:
        key = row_key(row, primary_key)
        if key in index:
            merged[index[key]] = row
        else:
            index[key] = len(merged)
            merged.append(row)
    return merged


def combine_tables(
    *,
    disposition: str,
    existing: Optional["pa.Table"],
    incoming: "pa.Table",
    primary_key: Optional[List[str]],
) -> "pa.Table":
    """Combine existing and incoming Arrow tables according to write disposition."""
    from dlt.common.libs.pyarrow import pyarrow

    if disposition == "replace" or existing is None or len(existing) == 0:
        return incoming
    if disposition == "append":
        # permissive fills missing columns with nulls so schema drift doesn't raise
        return pyarrow.concat_tables([existing, incoming], promote_options="permissive")
    keys = primary_key or ["_dlt_id"]
    if disposition in ("merge", "upsert"):
        merged = merge_rows(existing.to_pylist(), incoming.to_pylist(), primary_key=keys)
        return pyarrow.Table.from_pylist(merged)
    if disposition == "insert-only":
        existing_keys = {row_key(row, keys) for row in existing.to_pylist()}
        new_rows = [r for r in incoming.to_pylist() if row_key(r, keys) not in existing_keys]
        if not new_rows:
            return existing
        return pyarrow.concat_tables(
            [existing, pyarrow.Table.from_pylist(new_rows)], promote_options="permissive"
        )
    raise ValueError(
        f"Unsupported write_disposition {disposition!r}. "
        f"Expected one of: {', '.join(sorted(SUPPORTED_WRITE_DISPOSITIONS))}"
    )
