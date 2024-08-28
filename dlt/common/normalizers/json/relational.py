from functools import lru_cache
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, cast, TypedDict, Any
from dlt.common.json import json
from dlt.common.normalizers.exceptions import InvalidJsonNormalizer
from dlt.common.normalizers.typing import TJSONNormalizer, TRowIdType
from dlt.common.normalizers.utils import generate_dlt_id, DLT_ID_LENGTH_BYTES

from dlt.common.typing import DictStrAny, TDataItem, StrAny
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    TLoaderMergeStrategy,
    TColumnSchema,
    TColumnName,
    TSimpleRegex,
    DLT_NAME_PREFIX,
)
from dlt.common.schema.utils import (
    column_name_validator,
    get_validity_column_names,
    get_columns_names_with_prop,
    get_first_column_name_with_prop,
    get_merge_strategy,
)
from dlt.common.schema.exceptions import ColumnNameConflictException
from dlt.common.utils import digest128, update_dict_nested
from dlt.common.normalizers.json import (
    TNormalizedRowIterator,
    wrap_in_dict,
    DataItemNormalizer as DataItemNormalizerBase,
)
from dlt.common.validation import validate_dict


class RelationalNormalizerConfigPropagation(TypedDict, total=False):
    root: Optional[Dict[TColumnName, TColumnName]]
    tables: Optional[Dict[str, Dict[TColumnName, TColumnName]]]


class RelationalNormalizerConfig(TypedDict, total=False):
    generate_dlt_id: Optional[bool]
    max_nesting: Optional[int]
    propagation: Optional[RelationalNormalizerConfigPropagation]


class DataItemNormalizer(DataItemNormalizerBase[RelationalNormalizerConfig]):
    # known normalizer props
    C_DLT_ID = "_dlt_id"
    """unique id of current row"""
    C_DLT_LOAD_ID = "_dlt_load_id"
    """load id to identify records loaded together that ie. need to be processed"""
    C_DLT_ROOT_ID = "_dlt_root_id"
    """unique id of top level parent"""
    C_DLT_PARENT_ID = "_dlt_parent_id"
    """unique id of parent row"""
    C_DLT_LIST_IDX = "_dlt_list_idx"
    """position in the list of rows"""
    C_VALUE = "value"
    """for lists of simple types"""

    # other constants
    EMPTY_KEY_IDENTIFIER = "_empty"  # replace empty keys with this

    normalizer_config: RelationalNormalizerConfig
    propagation_config: RelationalNormalizerConfigPropagation
    max_nesting: int
    _skip_primary_key: Dict[str, bool]

    def __init__(self, schema: Schema) -> None:
        """This item normalizer works with nested dictionaries. It flattens dictionaries and descends into lists.
        It yields row dictionaries at each nesting level."""
        self.schema = schema
        self.naming = schema.naming
        self._reset()

    def _reset(self) -> None:
        # normalize known normalizer column identifiers
        self.c_dlt_id: TColumnName = TColumnName(self.naming.normalize_identifier(self.C_DLT_ID))
        self.c_dlt_load_id: TColumnName = TColumnName(
            self.naming.normalize_identifier(self.C_DLT_LOAD_ID)
        )
        self.c_dlt_root_id: TColumnName = TColumnName(
            self.naming.normalize_identifier(self.C_DLT_ROOT_ID)
        )
        self.c_dlt_parent_id: TColumnName = TColumnName(
            self.naming.normalize_identifier(self.C_DLT_PARENT_ID)
        )
        self.c_dlt_list_idx: TColumnName = TColumnName(
            self.naming.normalize_identifier(self.C_DLT_LIST_IDX)
        )
        self.c_value: TColumnName = TColumnName(self.naming.normalize_identifier(self.C_VALUE))

        # normalize config

        self.normalizer_config = self.schema._normalizers_config["json"].get("config") or {}  # type: ignore[assignment]
        self.propagation_config = self.normalizer_config.get("propagation", None)
        self.max_nesting = self.normalizer_config.get("max_nesting", 1000)
        self._skip_primary_key = {}
        # self.known_types: Dict[str, TDataType] = {}
        # self.primary_keys = Dict[str, ]

    # for those paths the complex nested objects should be left in place
    def _is_complex_type(self, table_name: str, field_name: str, _r_lvl: int) -> bool:
        # turn everything at the recursion level into complex type
        max_nesting = self.max_nesting
        schema = self.schema
        max_table_nesting = self._get_table_nesting_level(schema, table_name)
        if max_table_nesting is not None:
            max_nesting = max_table_nesting

        assert _r_lvl <= max_nesting
        if _r_lvl == max_nesting:
            return True

        # use cached value
        # path = f"{table_name}▶{field_name}"
        # or use definition in the schema
        column: TColumnSchema = None
        table = schema.tables.get(table_name)
        if table:
            column = table["columns"].get(field_name)
        if column is None or "data_type" not in column:
            data_type = schema.get_preferred_type(field_name)
        else:
            data_type = column["data_type"]

        return data_type == "complex"

    def _flatten(
        self, table: str, dict_row: DictStrAny, _r_lvl: int
    ) -> Tuple[DictStrAny, Dict[Tuple[str, ...], Sequence[Any]]]:
        out_rec_row: DictStrAny = {}
        out_rec_list: Dict[Tuple[str, ...], Sequence[Any]] = {}
        schema_naming = self.schema.naming

        def norm_row_dicts(dict_row: StrAny, __r_lvl: int, path: Tuple[str, ...] = ()) -> None:
            for k, v in dict_row.items():
                if k.strip():
                    norm_k = schema_naming.normalize_identifier(k)
                else:
                    # for empty keys in the data use _
                    norm_k = self.EMPTY_KEY_IDENTIFIER
                # if norm_k != k:
                #     print(f"{k} -> {norm_k}")
                child_name = (
                    norm_k if path == () else schema_naming.shorten_fragments(*path, norm_k)
                )
                # for lists and dicts we must check if type is possibly complex
                if isinstance(v, (dict, list)):
                    if not self._is_complex_type(table, child_name, __r_lvl):
                        # TODO: if schema contains table {table}__{child_name} then convert v into single element list
                        if isinstance(v, dict):
                            # flatten the dict more
                            norm_row_dicts(v, __r_lvl + 1, path + (norm_k,))
                        else:
                            # pass the list to out_rec_list
                            out_rec_list[path + (schema_naming.normalize_table_identifier(k),)] = v
                        continue
                    else:
                        # pass the complex value to out_rec_row
                        pass

                out_rec_row[child_name] = v

        norm_row_dicts(dict_row, _r_lvl)
        return out_rec_row, out_rec_list

    @staticmethod
    def get_row_hash(row: Dict[str, Any], subset: Optional[List[str]] = None) -> str:
        """Returns hash of row.

        Hash includes column names and values and is ordered by column name.
        Excludes dlt system columns.
        Can be used as deterministic row identifier.
        """
        row_filtered = {k: v for k, v in row.items() if not k.startswith(DLT_NAME_PREFIX)}
        if subset is not None:
            row_filtered = {k: v for k, v in row.items() if k in subset}
        row_str = json.dumps(row_filtered, sort_keys=True)
        return digest128(row_str, DLT_ID_LENGTH_BYTES)

    @staticmethod
    def _get_child_row_hash(parent_row_id: str, child_table: str, list_idx: int) -> str:
        # create deterministic unique id of the child row taking into account that all lists are ordered
        # and all child tables must be lists
        return digest128(f"{parent_row_id}_{child_table}_{list_idx}", DLT_ID_LENGTH_BYTES)

    def _link_row(self, row: DictStrAny, parent_row_id: str, list_idx: int) -> DictStrAny:
        assert parent_row_id
        row[self.c_dlt_parent_id] = parent_row_id
        row[self.c_dlt_list_idx] = list_idx

        return row

    @staticmethod
    def _extend_row(extend: DictStrAny, row: DictStrAny) -> None:
        row.update(extend)

    def _add_row_id(
        self,
        table: str,
        dict_row: DictStrAny,
        flattened_row: DictStrAny,
        parent_row_id: str,
        pos: int,
        _r_lvl: int,
    ) -> str:
        primary_key = False
        if _r_lvl > 0:  # child table
            primary_key = bool(
                self.schema.filter_row_with_hint(table, "primary_key", flattened_row)
            )
        row_id_type = self._get_row_id_type(self.schema, table, primary_key, _r_lvl)

        if row_id_type == "random":
            row_id = generate_dlt_id()
        else:
            if _r_lvl == 0:  # root table
                if row_id_type in ("key_hash", "row_hash"):
                    subset = None
                    if row_id_type == "key_hash":
                        subset = self._get_primary_key(self.schema, table)
                    # base hash on `dict_row` instead of `flattened_row`
                    # so changes in child tables lead to new row id
                    row_id = self.get_row_hash(dict_row, subset=subset)
            elif _r_lvl > 0:  # child table
                if row_id_type == "row_hash":
                    row_id = DataItemNormalizer._get_child_row_hash(parent_row_id, table, pos)
                    # link to parent table
                    self._link_row(flattened_row, parent_row_id, pos)

        flattened_row[self.c_dlt_id] = row_id
        return row_id

    def _get_propagated_values(self, table: str, row: DictStrAny, _r_lvl: int) -> StrAny:
        extend: DictStrAny = {}

        config = self.propagation_config
        if config:
            # mapping(k:v): propagate property with name "k" as property with name "v" in child table
            mappings: Dict[TColumnName, TColumnName] = {}
            if _r_lvl == 0:
                mappings.update(config.get("root") or {})
            if table in (config.get("tables") or {}):
                mappings.update(config["tables"][table])
            # look for keys and create propagation as values
            for prop_from, prop_as in mappings.items():
                if prop_from in row:
                    extend[prop_as] = row[prop_from]

        return extend

    # generate child tables only for lists
    def _normalize_list(
        self,
        seq: Sequence[Any],
        extend: DictStrAny,
        ident_path: Tuple[str, ...],
        parent_path: Tuple[str, ...],
        parent_row_id: Optional[str] = None,
        _r_lvl: int = 0,
    ) -> TNormalizedRowIterator:
        table = self.schema.naming.shorten_fragments(*parent_path, *ident_path)

        for idx, v in enumerate(seq):
            # yield child table row
            if isinstance(v, dict):
                yield from self._normalize_row(
                    v, extend, ident_path, parent_path, parent_row_id, idx, _r_lvl
                )
            elif isinstance(v, list):
                # to normalize lists of lists, we must create a tracking intermediary table by creating a mock row
                yield from self._normalize_row(
                    {"list": v},
                    extend,
                    ident_path,
                    parent_path,
                    parent_row_id,
                    idx,
                    _r_lvl + 1,
                )
            else:
                # list of simple types
                child_row_hash = DataItemNormalizer._get_child_row_hash(parent_row_id, table, idx)
                wrap_v = wrap_in_dict(self.c_value, v)
                wrap_v[self.c_dlt_id] = child_row_hash
                e = self._link_row(wrap_v, parent_row_id, idx)
                DataItemNormalizer._extend_row(extend, e)
                yield (table, self.schema.naming.shorten_fragments(*parent_path)), e

    def _normalize_row(
        self,
        dict_row: DictStrAny,
        extend: DictStrAny,
        ident_path: Tuple[str, ...],
        parent_path: Tuple[str, ...] = (),
        parent_row_id: Optional[str] = None,
        pos: Optional[int] = None,
        _r_lvl: int = 0,
    ) -> TNormalizedRowIterator:
        schema = self.schema
        table = schema.naming.shorten_fragments(*parent_path, *ident_path)
        # flatten current row and extract all lists to recur into
        flattened_row, lists = self._flatten(table, dict_row, _r_lvl)
        # always extend row
        DataItemNormalizer._extend_row(extend, flattened_row)
        # infer record hash or leave existing primary key if present
        row_id = flattened_row.get(self.c_dlt_id, None)
        if not row_id:
            row_id = self._add_row_id(table, dict_row, flattened_row, parent_row_id, pos, _r_lvl)

        # find fields to propagate to child tables in config
        extend.update(self._get_propagated_values(table, flattened_row, _r_lvl))

        # yield parent table first
        should_descend = yield (
            (table, schema.naming.shorten_fragments(*parent_path)),
            flattened_row,
        )
        if should_descend is False:
            return

        # normalize and yield lists
        for list_path, list_content in lists.items():
            yield from self._normalize_list(
                list_content,
                extend,
                list_path,
                parent_path + ident_path,
                row_id,
                _r_lvl + 1,
            )

    def extend_schema(self) -> None:
        """Extends Schema with normalizer-specific hints and settings.

        This method is called by Schema when instance is created or restored from storage.
        """
        config = cast(
            RelationalNormalizerConfig,
            self.schema._normalizers_config["json"].get("config") or {},
        )
        DataItemNormalizer._validate_normalizer_config(self.schema, config)

        # add hints, do not compile.
        self.schema._merge_hints(
            {
                "not_null": [
                    TSimpleRegex(self.c_dlt_id),
                    TSimpleRegex(self.c_dlt_root_id),
                    TSimpleRegex(self.c_dlt_parent_id),
                    TSimpleRegex(self.c_dlt_list_idx),
                    TSimpleRegex(self.c_dlt_load_id),
                ],
                "foreign_key": [TSimpleRegex(self.c_dlt_parent_id)],
                "root_key": [TSimpleRegex(self.c_dlt_root_id)],
                "unique": [TSimpleRegex(self.c_dlt_id)],
            },
            normalize_identifiers=False,  # already normalized
        )

        for table_name in self.schema.tables.keys():
            self.extend_table(table_name)

    def extend_table(self, table_name: str) -> None:
        """If the table has a merge write disposition, add propagation info to normalizer

        Called by Schema when new table is added to schema or table is updated with partial table.
        Table name should be normalized.
        """
        table = self.schema.tables.get(table_name)
        if not table.get("parent") and table.get("write_disposition") == "merge":
            DataItemNormalizer.update_normalizer_config(
                self.schema,
                {
                    "propagation": {
                        "tables": {
                            table_name: {
                                TColumnName(self.c_dlt_id): TColumnName(self.c_dlt_root_id)
                            }
                        }
                    }
                },
            )

    def normalize_data_item(
        self, item: TDataItem, load_id: str, table_name: str
    ) -> TNormalizedRowIterator:
        # wrap items that are not dictionaries in dictionary, otherwise they cannot be processed by the JSON normalizer
        if not isinstance(item, dict):
            item = wrap_in_dict(self.c_value, item)
        # we will extend event with all the fields necessary to load it as root row
        row = cast(DictStrAny, item)
        # identify load id if loaded data must be processed after loading incrementally
        row[self.c_dlt_load_id] = load_id
        if self._get_merge_strategy(self.schema, table_name) == "scd2":
            self._validate_validity_column_names(
                self.schema.name, self._get_validity_column_names(self.schema, table_name), item
            )

        yield from self._normalize_row(
            row,
            {},
            (self.schema.naming.normalize_table_identifier(table_name),),
        )

    @classmethod
    def ensure_this_normalizer(cls, norm_config: TJSONNormalizer) -> None:
        # make sure schema has right normalizer
        present_normalizer = norm_config["module"]
        if present_normalizer != __name__:
            raise InvalidJsonNormalizer(__name__, present_normalizer)

    @classmethod
    def update_normalizer_config(cls, schema: Schema, config: RelationalNormalizerConfig) -> None:
        cls._validate_normalizer_config(schema, config)
        existing_config = schema._normalizers_config["json"]
        cls.ensure_this_normalizer(existing_config)
        if "config" in existing_config:
            update_dict_nested(existing_config["config"], config)  # type: ignore
        else:
            existing_config["config"] = config

    @classmethod
    def get_normalizer_config(cls, schema: Schema) -> RelationalNormalizerConfig:
        norm_config = schema._normalizers_config["json"]
        cls.ensure_this_normalizer(norm_config)
        return cast(RelationalNormalizerConfig, norm_config.get("config", {}))

    @staticmethod
    def _validate_normalizer_config(schema: Schema, config: RelationalNormalizerConfig) -> None:
        """Normalizes all known column identifiers according to the schema and then validates the configuration"""

        def _normalize_prop(
            mapping: Mapping[TColumnName, TColumnName]
        ) -> Dict[TColumnName, TColumnName]:
            return {
                TColumnName(schema.naming.normalize_path(from_col)): TColumnName(
                    schema.naming.normalize_path(to_col)
                )
                for from_col, to_col in mapping.items()
            }

        # normalize the identifiers first
        propagation_config = config.get("propagation")
        if propagation_config:
            if "root" in propagation_config:
                propagation_config["root"] = _normalize_prop(propagation_config["root"])
            if "tables" in propagation_config:
                for table_name in propagation_config["tables"]:
                    propagation_config["tables"][table_name] = _normalize_prop(
                        propagation_config["tables"][table_name]
                    )

        validate_dict(
            RelationalNormalizerConfig,
            config,
            "./normalizers/json/config",
            validator_f=column_name_validator(schema.naming),
        )

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_table_nesting_level(schema: Schema, table_name: str) -> Optional[int]:
        table = schema.tables.get(table_name)
        if table:
            return table.get("x-normalizer", {}).get("max_nesting")  # type: ignore
        return None

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_merge_strategy(schema: Schema, table_name: str) -> Optional[TLoaderMergeStrategy]:
        return get_merge_strategy(schema.tables, table_name)

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_primary_key(schema: Schema, table_name: str) -> List[str]:
        if table_name not in schema.tables:
            return []
        table = schema.get_table(table_name)
        return get_columns_names_with_prop(table, "primary_key", include_incomplete=True)

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_validity_column_names(schema: Schema, table_name: str) -> List[Optional[str]]:
        return get_validity_column_names(schema.get_table(table_name))

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_row_id_type(
        schema: Schema, table_name: str, primary_key: bool, _r_lvl: int
    ) -> TRowIdType:
        if _r_lvl == 0:  # root table
            merge_strategy = DataItemNormalizer._get_merge_strategy(schema, table_name)
            if merge_strategy == "upsert":
                return "key_hash"
            elif merge_strategy == "scd2":
                x_row_version_col = get_first_column_name_with_prop(
                    schema.get_table(table_name),
                    "x-row-version",
                    include_incomplete=True,
                )
                if x_row_version_col == DataItemNormalizer.C_DLT_ID:
                    return "row_hash"
        elif _r_lvl > 0:  # child table
            merge_strategy = DataItemNormalizer._get_merge_strategy(schema, table_name)
            if merge_strategy in ("upsert", "scd2"):
                # these merge strategies rely on deterministic child row hash
                return "row_hash"
            if not primary_key:
                return "row_hash"
        return "random"

    @staticmethod
    def _validate_validity_column_names(
        schema_name: str, validity_column_names: List[Optional[str]], item: TDataItem
    ) -> None:
        """Raises exception if configured validity column name appears in data item."""
        for validity_column_name in validity_column_names:
            if validity_column_name in item.keys():
                raise ColumnNameConflictException(
                    schema_name,
                    "Found column in data item with same name as validity column"
                    f' "{validity_column_name}".',
                )
