from functools import lru_cache, partial
from typing import (
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    cast,
    Any,
)

from dlt.common.data_types.type_helpers import (
    PY_TYPE_TO_SC_TYPE,
    _COERCE_DISPATCH,
    coerce_value,
    py_type_to_sc_type,
)
from dlt.common.data_types.typing import TDataType
from dlt.common.normalizers.exceptions import InvalidJsonNormalizer
from dlt.common.normalizers.typing import TJSONNormalizer
from dlt.common.normalizers.utils import generate_dlt_id
from dlt.common.typing import DictStrAny, TDataItem, StrAny
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    C_DLT_ID,
    C_DLT_LOAD_ID,
    TColumnName,
    TSimpleRegex,
)
from dlt.common.schema.utils import (
    column_name_validator,
    get_root_table,
    get_first_column_name_with_prop,
)
from dlt.common.utils import update_dict_nested
from dlt.common.normalizers.json import (
    TNormalizedRowIterator,
    wrap_in_dict,
    DataItemNormalizer as DataItemNormalizerBase,
)
from dlt.common.normalizers.json.typing import (
    RelationalNormalizerConfig,
    RelationalNormalizerConfigPropagation,
)
from dlt.common.normalizers.json import helpers as normalize_helpers
from dlt.common.normalizers.json.helpers import (
    get_nested_row_hash,
    get_propagation_mapping,
    get_row_hash,
    requires_root_key,
)
from dlt.common.validation import validate_dict


class DataItemNormalizer(DataItemNormalizerBase[RelationalNormalizerConfig]):
    # known normalizer props
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
    RELATIONAL_CONFIG_TYPE: ClassVar[Type[RelationalNormalizerConfig]] = RelationalNormalizerConfig

    normalizer_config: RelationalNormalizerConfig
    propagation_config: RelationalNormalizerConfigPropagation
    max_nesting: int
    _skip_primary_key: Dict[str, bool]

    def __init__(self, schema: Schema) -> None:
        """This item normalizer works with nested dictionaries. It flattens dictionaries and descends into lists.
        It yields row dictionaries at each nesting level."""
        self.schema = schema
        self.naming = schema.naming
        self._load_id: str = None
        self._reset()

    def _reset(self) -> None:
        # normalize known normalizer column identifiers
        self.c_dlt_id: TColumnName = TColumnName(self.naming.normalize_identifier(C_DLT_ID))
        self.c_dlt_load_id: TColumnName = TColumnName(
            self.naming.normalize_identifier(C_DLT_LOAD_ID)
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
        # create cached versions of helper functions
        self._get_root_row_id_type = lru_cache(maxsize=None)(
            partial(normalize_helpers.get_root_row_id_type, self.schema)
        )
        self._shorten_fragments = lru_cache(maxsize=None)(
            partial(normalize_helpers.shorten_fragments, self.naming)
        )
        self._normalize_table_identifier = lru_cache(maxsize=None)(
            partial(normalize_helpers.normalize_table_identifier, self.schema, self.naming)
        )
        self._normalize_identifier = lru_cache(maxsize=None)(
            partial(normalize_helpers.normalize_identifier, self.schema, self.naming)
        )
        self._get_table_nesting_level = lru_cache(maxsize=None)(
            partial(normalize_helpers.get_table_nesting_level, self.schema)
        )
        self._get_primary_key = lru_cache(maxsize=None)(
            partial(normalize_helpers.get_primary_key, self.schema)
        )
        self._is_nested_type = lru_cache(maxsize=None)(
            partial(normalize_helpers.is_nested_type, self.schema)
        )
        self._should_be_nested = lru_cache(maxsize=None)(
            partial(normalize_helpers.should_be_nested, self.schema)
        )
        self._get_propagation_mapping = (
            lru_cache(maxsize=None)(partial(get_propagation_mapping, self.propagation_config))
            if self.propagation_config
            else None
        )

    @property
    def py_type_to_sc_type_map(self) -> Dict[Type[Any], TDataType]:
        return PY_TYPE_TO_SC_TYPE

    def py_type_to_sc_type(self, t: Type[Any]) -> TDataType:
        return py_type_to_sc_type(t)

    def can_coerce_type(self, to_type: TDataType, from_type: TDataType) -> bool:
        return to_type == from_type or (to_type, from_type) in _COERCE_DISPATCH

    def coerce_type(self, to_type: TDataType, from_type: TDataType, value: Any) -> Any:
        return coerce_value(to_type, from_type, value)

    def _flatten(
        self, table: str, dict_row: DictStrAny, _r_lvl: int
    ) -> Tuple[DictStrAny, Dict[Tuple[str, ...], Sequence[Any]]]:
        out_rec_row: DictStrAny = {}
        out_rec_list: Dict[Tuple[str, ...], Sequence[Any]] = {}

        # cache method lookups as locals
        normalize_identifier = self._normalize_identifier
        normalize_table_identifier = self._normalize_table_identifier
        shorten_fragments = self._shorten_fragments
        is_nested_type = self._is_nested_type
        EMPTY_KEY = self.EMPTY_KEY_IDENTIFIER

        stack: List[Tuple[Dict[str, Any], int, Tuple[str, ...]]] = [(dict_row, _r_lvl, ())]

        while stack:
            current_row, r_lvl, path = stack.pop()
            for k, v in current_row.items():
                norm_k = normalize_identifier(k) if (k and not k.isspace()) else EMPTY_KEY
                nested_name = norm_k if not path else shorten_fragments(*path, norm_k)
                v_type = type(v)
                if v_type is dict:
                    if not is_nested_type(table, nested_name, r_lvl):
                        stack.append((v, r_lvl - 1, path + (norm_k,)))
                        continue
                elif v_type is list:
                    if not is_nested_type(table, nested_name, r_lvl):
                        out_rec_list[path + (normalize_table_identifier(k),)] = v
                        continue

                out_rec_row[nested_name] = v

        return out_rec_row, out_rec_list

    def _add_row_id(
        self,
        table: str,
        dict_row: DictStrAny,
        flattened_row: DictStrAny,
        parent_row_id: str,
        pos: int,
        is_root: bool,
    ) -> str:
        if not is_root:
            row_id = get_nested_row_hash(parent_row_id, table, pos)
            flattened_row[self.c_dlt_parent_id] = parent_row_id
            flattened_row[self.c_dlt_list_idx] = pos
        else:  # root table
            row_id_type = self._get_root_row_id_type(table)
            if row_id_type in ("key_hash", "row_hash"):
                subset = None
                if row_id_type == "key_hash":
                    # primary key based hash must be performed on normalized names
                    subset = self._get_primary_key(table)
                    row_id = get_row_hash(flattened_row, subset=subset)
                else:
                    # base hash on `dict_row` instead of `flattened_row`
                    # so changes in nested tables lead to new row id
                    row_id = get_row_hash(dict_row)
            else:
                row_id = generate_dlt_id()

        flattened_row[self.c_dlt_id] = row_id
        return row_id

    def _get_propagated_values(
        self, table: str, row: DictStrAny, is_root: bool
    ) -> Optional[DictStrAny]:
        mappings = self._get_propagation_mapping(table, is_root)
        if not mappings:
            return None

        extend: DictStrAny = {}
        for prop_from, prop_as in mappings.items():
            if prop_from in row:
                extend[prop_as] = row[prop_from]

        return extend if extend else None

    # generate nested tables only for lists
    def _normalize_list(
        self,
        seq: Sequence[Any],
        extend: DictStrAny,
        ident_path: Tuple[str, ...],
        parent_path: Tuple[str, ...],
        parent_row_id: Optional[str] = None,
        _r_lvl: int = 0,
    ) -> TNormalizedRowIterator:
        # cache method lookups
        shorten_fragments = self._shorten_fragments
        should_be_nested = self._should_be_nested
        c_value = self.c_value

        table = shorten_fragments(*parent_path, *ident_path)
        is_root = not should_be_nested(table)
        for idx, v in enumerate(seq):
            # use type() for exact match - faster than isinstance()
            v_type = type(v)
            if v_type is dict:
                yield from self._normalize_row(
                    v, extend, ident_path, parent_path, parent_row_id, idx, _r_lvl
                )
            elif v_type is list:
                # to normalize lists of lists, we must create a tracking intermediary table
                yield from self._normalize_row(
                    {"list": v},
                    extend,
                    ident_path,
                    parent_path,
                    parent_row_id,
                    idx,
                    _r_lvl - 1,
                )
            else:
                # found non-dict in seq, so wrap it
                wrap_v: DictStrAny = {c_value: v}
                if extend:
                    wrap_v.update(extend)
                self._add_row_id(table, wrap_v, wrap_v, parent_row_id, idx, is_root)
                yield (table, parent_path, ident_path), wrap_v

    def _normalize_row(
        self,
        dict_row: DictStrAny,
        extend: DictStrAny,
        ident_path: Tuple[str, ...],
        parent_path: Tuple[str, ...] = (),
        parent_row_id: Optional[str] = None,
        pos: Optional[int] = None,
        _r_lvl: int = 0,
        is_root: bool = False,
    ) -> TNormalizedRowIterator:
        table = self._shorten_fragments(*parent_path, *ident_path)
        # flatten current row and extract all lists to recur into
        flattened_row, lists = self._flatten(table, dict_row, _r_lvl)
        # only extend row if there's something to extend
        if extend:
            flattened_row.update(extend)

        # identify load id if loaded data must be processed after loading incrementally
        if is_root := is_root or not self._should_be_nested(table):
            flattened_row[self.c_dlt_load_id] = self._load_id

        # infer record hash or leave existing primary key if present
        row_id = flattened_row.get(self.c_dlt_id)
        if not row_id:
            row_id = self._add_row_id(table, dict_row, flattened_row, parent_row_id, pos, is_root)

        # find fields to propagate to nested tables in config
        if self.propagation_config:
            propagated = self._get_propagated_values(table, flattened_row, is_root)
            if propagated:
                extend = {**extend, **propagated} if extend else propagated

        # yield parent table first
        should_descend = yield (table, parent_path, ident_path), flattened_row
        if should_descend is False:
            return

        # normalize and yield lists - pre-compute parent path once
        if lists:
            new_parent_path = parent_path + ident_path
            for list_path, list_content in lists.items():
                yield from self._normalize_list(
                    list_content,
                    extend,
                    list_path,
                    new_parent_path,
                    row_id,
                    _r_lvl - 1,
                )

    def extend_schema(self, extend_tables: bool = True) -> None:
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
                "parent_key": [TSimpleRegex(self.c_dlt_parent_id)],
                "root_key": [TSimpleRegex(self.c_dlt_root_id)],
                "unique": [TSimpleRegex(self.c_dlt_id)],
                "row_key": [TSimpleRegex(self.c_dlt_id)],
            },
            normalize_identifiers=False,  # already normalized
        )

        if extend_tables:
            for table_name in self.schema.tables.keys():
                self.extend_table(table_name)

    def extend_table(self, table_name: str) -> None:
        """If the table has a merge write disposition, add propagation info to normalizer

        Called by Schema when new table is added to schema or table is updated with partial table.
        Table name should be normalized.
        """
        # find root table
        root_table = get_root_table(self.schema.tables, table_name)
        root_table_name = root_table["name"]
        # add root key prop when merge disposition is used or any of nested tables needs row_key
        if requires_root_key(
            self.schema, root_table, self.normalizer_config.get("root_key_propagation")
        ):
            # get row id column from table, assume that we propagate it into c_dlt_root_id always
            c_dlt_id = get_first_column_name_with_prop(
                root_table, "row_key", include_incomplete=True
            )
            self.update_normalizer_config(
                self.schema,
                {
                    "propagation": {
                        "tables": {
                            root_table_name: {
                                TColumnName(c_dlt_id or self.c_dlt_id): TColumnName(
                                    self.c_dlt_root_id
                                )
                            }
                        }
                    }
                },
            )
        # clear all caches that depend on schema.tables since table was added/modified
        self._clear_schema_caches()

    def remove_table(self, table_name: str) -> None:
        """Called by the Schema when table is removed from it."""
        config = self.get_normalizer_config(self.schema)
        if propagation := config.get("propagation"):
            if tables := propagation.get("tables"):
                tables.pop(table_name, None)
        # clear all caches that depend on schema.tables since table was removed
        self._clear_schema_caches()

    def _clear_schema_caches(self) -> None:
        """Clear all lru_caches that depend on schema.tables."""
        self._get_table_nesting_level.cache_clear()
        self._get_primary_key.cache_clear()
        self._is_nested_type.cache_clear()
        self._should_be_nested.cache_clear()
        self._get_root_row_id_type.cache_clear()
        if self._get_propagation_mapping:
            self._get_propagation_mapping.cache_clear()

    def normalize_data_item(
        self, item: TDataItem, load_id: str, table_name: str
    ) -> TNormalizedRowIterator:
        # wrap items that are not dictionaries in dictionary, otherwise they cannot be processed by the JSON normalizer
        if not isinstance(item, dict):
            item = wrap_in_dict(self.c_value, item)
        self._load_id = load_id

        # get table name and nesting level
        root_table_name = self._normalize_table_identifier(table_name)
        max_nesting = self._get_table_nesting_level(root_table_name, self.max_nesting)

        yield from self._normalize_row(
            item,
            {},
            (root_table_name,),
            _r_lvl=max_nesting,  # we count backwards
            is_root=True,
        )

    @classmethod
    def ensure_this_normalizer(cls, norm_config: TJSONNormalizer) -> None:
        # make sure schema has right normalizer
        present_normalizer = norm_config["module"]
        if present_normalizer != cls.__module__:
            raise InvalidJsonNormalizer(cls.__module__, present_normalizer)

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

    @classmethod
    def _validate_normalizer_config(
        cls, schema: Schema, config: RelationalNormalizerConfig
    ) -> None:
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
            cls.RELATIONAL_CONFIG_TYPE,
            config,
            "./normalizers/json/config",
            validator_f=column_name_validator(schema.naming),
        )
