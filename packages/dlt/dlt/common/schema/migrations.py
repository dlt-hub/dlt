from typing import Dict, List, cast

from dlt.common.data_types import TDataType
from dlt.common.typing import DictStrAny
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    VERSION_TABLE_NAME,
    TSimpleRegex,
    TStoredSchema,
    TTableSchemaColumns,
    TColumnDefaultHint,
)
from dlt.common.schema.exceptions import SchemaEngineNoUpgradePathException
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    new_table,
    version_table,
    loads_table,
    migrate_complex_types,
)


def migrate_schema(schema_dict: DictStrAny, from_engine: int, to_engine: int) -> TStoredSchema:
    if from_engine == to_engine:
        return cast(TStoredSchema, schema_dict)

    if from_engine == 1 and to_engine > 1:
        schema_dict["includes"] = []
        schema_dict["excludes"] = []
        from_engine = 2
    if from_engine == 2 and to_engine > 2:
        from dlt.common.schema.normalizers import import_normalizers, configured_normalizers

        # current version of the schema
        current = cast(TStoredSchema, schema_dict)
        # add default normalizers and root hash propagation
        # use explicit None to get default settings. ignore any naming conventions
        normalizers = configured_normalizers(naming=None, json_normalizer=None)
        current["normalizers"], _, _ = import_normalizers(normalizers, normalizers)
        current["normalizers"]["json"]["config"] = {
            "propagation": {"root": {"_dlt_id": "_dlt_root_id"}}
        }
        # move settings, convert strings to simple regexes
        d_h: Dict[TColumnDefaultHint, List[TSimpleRegex]] = schema_dict.pop("hints", {})
        for h_k, h_l in d_h.items():
            d_h[h_k] = list(map(lambda r: TSimpleRegex("re:" + r), h_l))
        p_t: Dict[TSimpleRegex, TDataType] = schema_dict.pop("preferred_types", {})
        p_t = {TSimpleRegex("re:" + k): v for k, v in p_t.items()}

        current["settings"] = {
            "default_hints": d_h,
            "preferred_types": p_t,
        }
        # repackage tables
        old_tables: Dict[str, TTableSchemaColumns] = schema_dict.pop("tables")
        current["tables"] = {}
        for name, columns in old_tables.items():
            # find last path separator
            parent = name
            # go back in a loop to find existing parent
            while True:
                idx = parent.rfind("__")
                if idx > 0:
                    parent = parent[:idx]
                    if parent not in old_tables:
                        continue
                else:
                    parent = None
                break
            nt = new_table(name, parent)
            nt["columns"] = columns
            current["tables"][name] = nt
        # assign exclude and include to tables

        def migrate_filters(group: str, filters: List[str]) -> None:
            # existing filter were always defined at the root table. find this table and move filters
            for f in filters:
                # skip initial ^
                root = f[1 : f.find("__")]
                path = f[f.find("__") + 2 :]
                t = current["tables"].get(root)
                if t is None:
                    # must add new table to hold filters
                    t = new_table(root)
                    current["tables"][root] = t
                t.setdefault("filters", {}).setdefault(group, []).append("re:^" + path)  # type: ignore

        excludes = schema_dict.pop("excludes", [])
        migrate_filters("excludes", excludes)
        includes = schema_dict.pop("includes", [])
        migrate_filters("includes", includes)

        # upgraded
        from_engine = 3
    if from_engine == 3 and to_engine > 3:
        # set empty version hash to pass validation, in engine 4 this hash is mandatory
        schema_dict.setdefault("version_hash", "")
        from_engine = 4
    if from_engine == 4 and to_engine > 4:
        # replace schema versions table
        schema_dict["tables"][VERSION_TABLE_NAME] = version_table()
        schema_dict["tables"][LOADS_TABLE_NAME] = loads_table()
        from_engine = 5
    if from_engine == 5 and to_engine > 5:
        # replace loads table
        schema_dict["tables"][LOADS_TABLE_NAME] = loads_table()
        from_engine = 6
    if from_engine == 6 and to_engine > 6:
        # migrate from sealed properties to schema evolution settings
        schema_dict["settings"].pop("schema_sealed", None)
        schema_dict["settings"]["schema_contract"] = {}
        for table in schema_dict["tables"].values():
            table.pop("table_sealed", None)
            if not table.get("parent"):
                table["schema_contract"] = {}
        from_engine = 7
    if from_engine == 7 and to_engine > 7:
        schema_dict["previous_hashes"] = []
        from_engine = 8
    if from_engine == 8 and to_engine > 8:
        # add "seen-data" to all tables with _dlt_id, this will handle packages
        # that are being loaded
        for table in schema_dict["tables"].values():
            if "_dlt_id" in table["columns"]:
                x_normalizer = table.setdefault("x-normalizer", {})
                x_normalizer["seen-data"] = True
        from_engine = 9
    if from_engine == 9 and to_engine > 9:
        from dlt.common.schema.normalizers import import_normalizers

        # current = cast(TStoredSchema, schema_dict)

        normalizers = schema_dict["normalizers"]
        _, naming, _ = import_normalizers(normalizers)
        c_dlt_id = naming.normalize_identifier("_dlt_id")
        c_dlt_parent_id = naming.normalize_identifier("_dlt_parent_id")

        for table in schema_dict["tables"].values():
            # migrate complex -> json
            migrate_complex_types(table)
            # modify hints
            if dlt_id_col := table["columns"].get(c_dlt_id):
                # add row key only if unique is set
                dlt_id_col["row_key"] = dlt_id_col.get("unique", False)
            if parent_dlt_id_col := table["columns"].get(c_dlt_parent_id):
                # add parent key
                parent_dlt_id_col["parent_key"] = parent_dlt_id_col.get("foreign_key", False)
            # drop all foreign keys
            for column in table["columns"].values():
                column.pop("foreign_key", None)

        # migrate preferred types
        if settings := schema_dict.get("settings"):
            if p_t := settings.get("preferred_types"):
                for re_ in list(p_t.keys()):
                    if p_t[re_] == "complex":
                        p_t[re_] = "json"
        # migrate default hints
        if default_hints := schema_dict["settings"].get("default_hints"):
            # drop foreign key
            default_hints.pop("foreign_key", None)
            # add row and parent key
            default_hints["row_key"] = [TSimpleRegex(c_dlt_id)]
            default_hints["parent_key"] = [TSimpleRegex(c_dlt_parent_id)]

        # remove `generate_dlt_id` from normalizer
        if json_norm := normalizers.get("json"):
            if json_config := json_norm.get("config"):
                json_config.pop("generate_dlt_id", None)

        from_engine = 10
    if from_engine == 10 and to_engine > 10:
        schema_dict["normalizers"]["use_break_path_on_normalize"] = False
        from_engine = 11

    schema_dict["engine_version"] = from_engine
    if from_engine != to_engine:
        raise SchemaEngineNoUpgradePathException(
            schema_dict["name"], schema_dict["engine_version"], from_engine, to_engine
        )

    return cast(TStoredSchema, schema_dict)
