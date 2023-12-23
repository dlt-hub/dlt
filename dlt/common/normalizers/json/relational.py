from typing import Dict, List, Mapping, Optional, Sequence, Tuple, cast, TypedDict, Any
from dlt.common.data_types.typing import TDataType
from dlt.common.normalizers.exceptions import InvalidJsonNormalizer
from dlt.common.normalizers.typing import TJSONNormalizer
from dlt.common.normalizers.utils import generate_dlt_id, DLT_ID_LENGTH_BYTES

from dlt.common.typing import DictStrAny, DictStrStr, TDataItem, StrAny
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TColumnName, TSimpleRegex
from dlt.common.schema.utils import column_name_validator
from dlt.common.utils import digest128, update_dict_nested
from dlt.common.normalizers.json import (
    TNormalizedRowIterator,
    wrap_in_dict,
    DataItemNormalizer as DataItemNormalizerBase,
)
from dlt.common.validation import validate_dict

EMPTY_KEY_IDENTIFIER = "_empty"  # replace empty keys with this


class TDataItemRow(TypedDict, total=False):
    _dlt_id: str  # unique id of current row


class TDataItemRowRoot(TDataItemRow, total=False):
    _dlt_load_id: str  # load id to identify records loaded together that ie. need to be processed
    # _dlt_meta: TEventDLTMeta  # stores metadata, should never be sent to the normalizer


class TDataItemRowChild(TDataItemRow, total=False):
    _dlt_root_id: str  # unique id of top level parent
    _dlt_parent_id: str  # unique id of parent row
    _dlt_list_idx: int  # position in the list of rows
    value: Any  # for lists of simple types


class RelationalNormalizerConfigPropagation(TypedDict, total=False):
    root: Optional[Mapping[str, TColumnName]]
    tables: Optional[Mapping[str, Mapping[str, TColumnName]]]


class RelationalNormalizerConfig(TypedDict, total=False):
    generate_dlt_id: Optional[bool]
    max_nesting: Optional[int]
    propagation: Optional[RelationalNormalizerConfigPropagation]


class DataItemNormalizer(DataItemNormalizerBase[RelationalNormalizerConfig]):
    normalizer_config: RelationalNormalizerConfig
    propagation_config: RelationalNormalizerConfigPropagation
    max_nesting: int
    _skip_primary_key: Dict[str, bool]

    def __init__(self, schema: Schema) -> None:
        """This item normalizer works with nested dictionaries. It flattens dictionaries and descends into lists.
        It yields row dictionaries at each nesting level."""
        self.schema = schema
        self._reset()

    def _reset(self) -> None:
        self.normalizer_config = self.schema._normalizers_config["json"].get("config") or {}  # type: ignore
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
        if column is None:
            data_type = schema.get_preferred_type(field_name)
        else:
            data_type = column["data_type"]
        return data_type == "complex"

    def _flatten(
        self, table: str, dict_row: TDataItemRow, _r_lvl: int
    ) -> Tuple[TDataItemRow, Dict[Tuple[str, ...], Sequence[Any]]]:
        out_rec_row: DictStrAny = {}
        out_rec_list: Dict[Tuple[str, ...], Sequence[Any]] = {}
        schema_naming = self.schema.naming

        def norm_row_dicts(dict_row: StrAny, __r_lvl: int, path: Tuple[str, ...] = ()) -> None:
            for k, v in dict_row.items():
                if k.strip():
                    norm_k = schema_naming.normalize_identifier(k)
                else:
                    # for empty keys in the data use _
                    norm_k = EMPTY_KEY_IDENTIFIER
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
        return cast(TDataItemRow, out_rec_row), out_rec_list

    @staticmethod
    def _get_child_row_hash(parent_row_id: str, child_table: str, list_idx: int) -> str:
        # create deterministic unique id of the child row taking into account that all lists are ordered
        # and all child tables must be lists
        return digest128(f"{parent_row_id}_{child_table}_{list_idx}", DLT_ID_LENGTH_BYTES)

    @staticmethod
    def _link_row(row: TDataItemRowChild, parent_row_id: str, list_idx: int) -> TDataItemRowChild:
        assert parent_row_id
        row["_dlt_parent_id"] = parent_row_id
        row["_dlt_list_idx"] = list_idx

        return row

    @staticmethod
    def _extend_row(extend: DictStrAny, row: TDataItemRow) -> None:
        row.update(extend)  # type: ignore

    def _add_row_id(
        self, table: str, row: TDataItemRow, parent_row_id: str, pos: int, _r_lvl: int
    ) -> str:
        # row_id is always random, no matter if primary_key is present or not
        row_id = generate_dlt_id()
        if _r_lvl > 0:
            primary_key = self.schema.filter_row_with_hint(table, "primary_key", row)
            if not primary_key:
                # child table row deterministic hash
                row_id = DataItemNormalizer._get_child_row_hash(parent_row_id, table, pos)
                # link to parent table
                DataItemNormalizer._link_row(cast(TDataItemRowChild, row), parent_row_id, pos)
        row["_dlt_id"] = row_id
        return row_id

    def _get_propagated_values(self, table: str, row: TDataItemRow, _r_lvl: int) -> StrAny:
        extend: DictStrAny = {}

        config = self.propagation_config
        if config:
            # mapping(k:v): propagate property with name "k" as property with name "v" in child table
            mappings: DictStrStr = {}
            if _r_lvl == 0:
                mappings.update(config.get("root") or {})
            if table in (config.get("tables") or {}):
                mappings.update(config["tables"][table])
            # look for keys and create propagation as values
            for prop_from, prop_as in mappings.items():
                if prop_from in row:
                    extend[prop_as] = row[prop_from]  # type: ignore

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
        v: TDataItemRowChild = None
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
                    {"list": v}, extend, ident_path, parent_path, parent_row_id, idx, _r_lvl + 1
                )
            else:
                # list of simple types
                child_row_hash = DataItemNormalizer._get_child_row_hash(parent_row_id, table, idx)
                wrap_v = wrap_in_dict(v)
                wrap_v["_dlt_id"] = child_row_hash
                e = DataItemNormalizer._link_row(wrap_v, parent_row_id, idx)
                DataItemNormalizer._extend_row(extend, e)
                yield (table, self.schema.naming.shorten_fragments(*parent_path)), e

    def _normalize_row(
        self,
        dict_row: TDataItemRow,
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
        row_id = flattened_row.get("_dlt_id", None)
        if not row_id:
            row_id = self._add_row_id(table, flattened_row, parent_row_id, pos, _r_lvl)

        # find fields to propagate to child tables in config
        extend.update(self._get_propagated_values(table, flattened_row, _r_lvl))

        # yield parent table first
        should_descend = yield (table, schema.naming.shorten_fragments(*parent_path)), flattened_row
        if should_descend is False:
            return

        # normalize and yield lists
        for list_path, list_content in lists.items():
            yield from self._normalize_list(
                list_content, extend, list_path, parent_path + ident_path, row_id, _r_lvl + 1
            )

    def extend_schema(self) -> None:
        # validate config
        config = cast(
            RelationalNormalizerConfig, self.schema._normalizers_config["json"].get("config") or {}
        )
        DataItemNormalizer._validate_normalizer_config(self.schema, config)

        # quick check to see if hints are applied
        default_hints = self.schema.settings.get("default_hints") or {}
        if "not_null" in default_hints and "^_dlt_id$" in default_hints["not_null"]:
            return
        # add hints
        self.schema.merge_hints(
            {
                "not_null": [
                    TSimpleRegex("_dlt_id"),
                    TSimpleRegex("_dlt_root_id"),
                    TSimpleRegex("_dlt_parent_id"),
                    TSimpleRegex("_dlt_list_idx"),
                    TSimpleRegex("_dlt_load_id"),
                ],
                "foreign_key": [TSimpleRegex("_dlt_parent_id")],
                "root_key": [TSimpleRegex("_dlt_root_id")],
                "unique": [TSimpleRegex("_dlt_id")],
            }
        )

        for table_name in self.schema.tables.keys():
            self.extend_table(table_name)

    def extend_table(self, table_name: str) -> None:
        # if the table has a merge w_d, add propagation info to normalizer
        table = self.schema.tables.get(table_name)
        if not table.get("parent") and table.get("write_disposition") == "merge":
            DataItemNormalizer.update_normalizer_config(
                self.schema,
                {"propagation": {"tables": {table_name: {"_dlt_id": TColumnName("_dlt_root_id")}}}},
            )

    def normalize_data_item(
        self, item: TDataItem, load_id: str, table_name: str
    ) -> TNormalizedRowIterator:
        # wrap items that are not dictionaries in dictionary, otherwise they cannot be processed by the JSON normalizer
        if not isinstance(item, dict):
            item = wrap_in_dict(item)
        # we will extend event with all the fields necessary to load it as root row
        row = cast(TDataItemRowRoot, item)
        # identify load id if loaded data must be processed after loading incrementally
        row["_dlt_load_id"] = load_id
        yield from self._normalize_row(
            cast(TDataItemRowChild, row),
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
        norm_config = schema._normalizers_config["json"]
        cls.ensure_this_normalizer(norm_config)
        if "config" in norm_config:
            update_dict_nested(norm_config["config"], config)  # type: ignore
        else:
            norm_config["config"] = config

    @classmethod
    def get_normalizer_config(cls, schema: Schema) -> RelationalNormalizerConfig:
        norm_config = schema._normalizers_config["json"]
        cls.ensure_this_normalizer(norm_config)
        return cast(RelationalNormalizerConfig, norm_config.get("config", {}))

    @staticmethod
    def _validate_normalizer_config(schema: Schema, config: RelationalNormalizerConfig) -> None:
        validate_dict(
            RelationalNormalizerConfig,
            config,
            "./normalizers/json/config",
            validator_f=column_name_validator(schema.naming),
        )
