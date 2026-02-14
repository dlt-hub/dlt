from __future__ import annotations as _annotations
import collections.abc
from copy import copy
from typing import (
    Dict,
    Generic,
    Optional,
    Set,
    List,
    Tuple,
    Type,
    Union,
    Any,
)

from dlt.common.data_types import py_type_to_sc_type
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema import DataValidationError
from dlt.common.schema.typing import TSchemaEvolutionMode, TTableSchemaColumns
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from dlt.common.typing import (
    TypedDict,
    Annotated,
    get_args,
    get_origin,
    TypeVar,
    TDataItem,
    TDataItems,
    extract_union_types,
    is_annotated,
    is_optional_type,
    extract_inner_type,
    is_list_generic_type,
    is_dict_generic_type,
    is_subclass,
    is_union_type,
)
import warnings

from dlt.common.warnings import Dlt100DeprecationWarning

try:
    from pydantic import VERSION as PYDANTIC_VERSION

    if not PYDANTIC_VERSION.startswith("2."):
        raise ImportError(f"Found pydantic {PYDANTIC_VERSION} but dlt requires pydantic>=2.0")
    from pydantic import BaseModel, ValidationError, Json, create_model
    from pydantic.fields import FieldInfo
    from pydantic.warnings import PydanticDeprecationWarning

    warnings.filterwarnings("ignore", category=PydanticDeprecationWarning)
except ImportError:
    raise MissingDependencyException(
        "dlt Pydantic helpers", ["pydantic>=2.0"], "Pydantic 2.x is required"
    )

_TPydanticModel = TypeVar("_TPydanticModel", bound=BaseModel)


snake_case_naming_convention = SnakeCaseNamingConvention()


def _is_set_origin(origin: type) -> bool:
    """Check if a type origin is set-like (set or frozenset)."""
    try:
        return issubclass(origin, collections.abc.Set)
    except TypeError:
        return False


class ListModel(BaseModel, Generic[_TPydanticModel]):
    items: List[_TPydanticModel]


class DltConfig(TypedDict, total=False):
    """dlt configuration that can be attached to a Pydantic model.

    Example:
        >>> class ItemModel(BaseModel):
        >>>     field: int
        >>>     dlt_config: ClassVar[DltConfig] = {"return_validated_models": True}

    Options:
        skip_nested_types:
            If True, columns of complex types (`dict`, `list`, `BaseModel`) are excluded
            from the schema generated from the model.

        skip_complex_types:  # deprecated

        return_validated_models:
            If True, the Pydantic validator returns validated Pydantic model instances
            instead of converting them to dictionaries during extraction/transform steps.
            Defaults to False to preserve current behavior.

        is_authoritative_model:
            If True, the Pydantic model is treated as the authoritative source of table
            schema. Columns derived from the model are pre-merged into the schema before
            contract checks, so they bypass column/data_type contract enforcement.
            When None (default), dlt automatically determines whether to treat the model
            as authoritative.
    """

    skip_nested_types: bool
    skip_complex_types: bool  # deprecated
    return_validated_models: bool
    is_authoritative_model: Optional[bool]


def _build_discriminator_map(
    model: Type[BaseModel],
) -> Optional[Tuple[str, Dict[str, Type[BaseModel]]]]:
    """For a RootModel with a discriminated union, extract the discriminator field name
    and a mapping from literal discriminator values to their variant model classes.

    Returns None if the model is not a RootModel or has no string discriminator.
    """
    if not getattr(model, "__pydantic_root_model__", False):
        return None
    root_field = model.model_fields.get("root")
    if not root_field:
        return None

    ann = root_field.annotation
    discriminator: Optional[str] = None

    if is_annotated(ann):
        args = get_args(ann)
        # metadata may be FieldInfo directly or wrapped in a tuple by _process_annotation
        for a in args[1:]:
            items = a if isinstance(a, (list, tuple)) else (a,)
            for item in items:
                if isinstance(item, FieldInfo) and isinstance(item.discriminator, str):
                    discriminator = item.discriminator
                    break
            if discriminator:
                break
        union_args = get_args(args[0])
    else:
        return None

    if not discriminator or not union_args:
        return None

    mapping: Dict[str, Type[BaseModel]] = {}
    for member in union_args:
        member_field = member.model_fields.get(discriminator)
        if member_field:
            for lit_val in get_args(member_field.annotation):
                mapping[str(lit_val)] = member
    return discriminator, mapping


def resolve_variant_model(
    model: Type[BaseModel],
    item: Any,
    discriminator_map: Optional[Tuple[str, Dict[str, Type[BaseModel]]]] = None,
) -> Optional[Type[BaseModel]]:
    """Resolve a discriminated union variant from an item's discriminator value.

    Args:
        model: A pydantic model, typically a RootModel with a discriminated union.
        item: A dict or model instance to read the discriminator value from.
        discriminator_map: Pre-computed result of `_build_discriminator_map`. Computed
            from `model` when not provided.
    """
    if discriminator_map is None:
        discriminator_map = _build_discriminator_map(model)
    if discriminator_map is None:
        return model
    disc_field, mapping = discriminator_map
    disc_value = item.get(disc_field) if isinstance(item, dict) else getattr(item, disc_field, None)
    if disc_value is None:
        return None
    return mapping.get(str(disc_value))


def pydantic_to_table_schema_columns(
    model: Union[BaseModel, Type[BaseModel]],
) -> TTableSchemaColumns:
    """Convert a pydantic model to a table schema columns dict

    See also DltConfig for more control over how the schema is created

    Args:
        model: The pydantic model to convert. Can be a class or an instance.

    Returns:
        TTableSchemaColumns: table schema columns dict
    """
    # for RootModel with discriminated union, return columns common to all variants
    if getattr(model, "__pydantic_root_model__", False):
        disc_map = _build_discriminator_map(model)  # type: ignore[arg-type]
        if disc_map is not None:
            variants = list(disc_map[1].values())
            all_cols = [pydantic_to_table_schema_columns(v) for v in variants]
            common_keys = set.intersection(*(set(c.keys()) for c in all_cols))
            return {k: v for k, v in all_cols[0].items() if k in common_keys}

    skip_nested_types = False
    if hasattr(model, "dlt_config"):
        if "skip_complex_types" in model.dlt_config:
            warnings.warn(
                "`skip_complex_types` is deprecated, use `skip_nested_types` instead.",
                Dlt100DeprecationWarning,
                stacklevel=2,
            )
            skip_nested_types = model.dlt_config["skip_complex_types"]
        else:
            skip_nested_types = model.dlt_config.get("skip_nested_types", False)

    result: TTableSchemaColumns = {}
    model_cls: Type[BaseModel] = model if isinstance(model, type) else type(model)

    for field_name, field in model_cls.model_fields.items():
        annotation = field.annotation
        if inner_annotation := getattr(annotation, "inner_type", None):
            # This applies to pydantic.Json fields, the inner type is the type after json parsing
            # (In pydantic 2 the outer annotation is the final type)
            annotation = inner_annotation

        nullable = is_optional_type(annotation)

        inner_type = extract_inner_type(annotation)
        if is_union_type(inner_type):
            # TODO: order those types deterministically before getting first one
            # order of the types in union is in many cases not deterministic
            # https://docs.python.org/3/library/typing.html#typing.get_args
            first_argument_type = get_args(inner_type)[0]
            inner_type = extract_inner_type(first_argument_type)

        if inner_type is Json:  # Same as `field: Json[Any]`
            inner_type = Any  # type: ignore[assignment]

        if inner_type is Any:  # Any fields will be inferred from data
            continue

        if is_list_generic_type(inner_type):
            inner_type = list
        elif is_dict_generic_type(inner_type):
            inner_type = dict

        is_inner_type_pydantic_model = False
        name = field.alias or field_name
        try:
            data_type = py_type_to_sc_type(inner_type)
        except TypeError:
            if is_subclass(inner_type, BaseModel):
                data_type = "json"
                is_inner_type_pydantic_model = True
            else:
                # try to coerce unknown type to text
                data_type = "text"

        if is_inner_type_pydantic_model and not skip_nested_types:
            result[name] = {
                "name": name,
                "data_type": "json",
                "nullable": nullable,
            }
        elif is_inner_type_pydantic_model:
            # This case is for a single field schema/model
            # we need to generate snake_case field names
            # and return flattened field schemas
            schema_hints = pydantic_to_table_schema_columns(inner_type)

            for field_name, hints in schema_hints.items():
                schema_key = snake_case_naming_convention.make_path(name, field_name)
                result[schema_key] = {
                    **hints,
                    "name": snake_case_naming_convention.make_path(name, hints["name"]),
                    # if the outer field containing the nested mode is optional,
                    # then each field in the model itself has to be nullable as well,
                    # as otherwise we end up with flattened non-nullable optional nested fields
                    "nullable": hints["nullable"] or nullable,
                }
        elif data_type == "json" and skip_nested_types:
            continue
        else:
            result[name] = {
                "name": name,
                "data_type": data_type,
                "nullable": nullable,
            }

    return result


def column_mode_to_extra(column_mode: TSchemaEvolutionMode) -> str:
    extra = "forbid"
    if column_mode == "evolve":
        extra = "allow"
    elif column_mode == "discard_value":
        extra = "ignore"
    return extra


def extra_to_column_mode(extra: str) -> TSchemaEvolutionMode:
    if extra == "forbid":
        return "freeze"
    if extra == "allow":
        return "evolve"
    return "discard_value"


def get_extra_from_model(model: Type[BaseModel]) -> Optional[str]:
    """Returns the extra setting from the model or None if not explicitly set."""
    return model.model_config.get("extra")


def apply_schema_contract_to_model(
    model: Type[_TPydanticModel],
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode = "freeze",
    _child_models: Optional[Dict[int, Type[BaseModel]]] = None,
) -> Type[_TPydanticModel]:
    """Configures or re-creates `model` so it behaves according to `column_mode` and `data_mode` settings.

    `column_mode` sets the model behavior when unknown field is found.
    `data_mode` sets model behavior when known field does not validate. currently `evolve` and `freeze` are supported here.

    `discard_row` is implemented in `validate_item`.
    """
    # save dlt_config before model may be replaced by the evolve path
    original_dlt_config = getattr(model, "dlt_config", None)

    if data_mode == "evolve":
        # create a lenient model that accepts any data
        if getattr(model, "__pydantic_root_model__", False):
            from pydantic import RootModel as PydanticRootModel

            model = type(
                model.__name__ + "Any",
                (PydanticRootModel[Any],),
                {"__module__": model.__module__},
            )
        else:
            model = create_model(model.__name__ + "Any", **{n: (Any, None) for n in model.model_fields})  # type: ignore
    elif data_mode == "discard_value":
        raise NotImplementedError(
            "`data_mode='discard_value'`. Cannot discard defined fields with validation errors"
            " using Pydantic models."
        )

    extra = column_mode_to_extra(column_mode)

    if extra == (get_extra_from_model(model) or "ignore"):
        # no need to change the model, but restore dlt_config lost by the evolve path
        if original_dlt_config:
            model.dlt_config = original_dlt_config  # type: ignore[attr-defined]
        return model

    config = copy(model.model_config)
    config["extra"] = extra  # type: ignore[typeddict-item]

    if _child_models is None:
        _child_models = {}

    def _process_annotation(t_: Type[Any]) -> Type[Any]:
        """Recursively recreates models with applied schema contract"""
        if is_annotated(t_):
            a_t, *a_m = get_args(t_)
            return Annotated[_process_annotation(a_t), tuple(a_m)]  # type: ignore[return-value]
        origin = get_origin(t_)
        # tuple must be checked before is_list_generic_type (tuple is a Sequence)
        if origin is tuple:
            args = get_args(t_)
            if not args:
                return t_
            if len(args) == 2 and args[1] is Ellipsis:
                # variable-length: Tuple[T, ...]
                return origin[_process_annotation(args[0]), ...]  # type: ignore[no-any-return]
            # heterogeneous: Tuple[T1, T2, ...]
            processed = tuple(_process_annotation(a) for a in args)
            return origin[processed]  # type: ignore[no-any-return]
        elif is_list_generic_type(t_):
            l_t: Type[Any] = get_args(t_)[0]
            return origin[_process_annotation(l_t)]  # type: ignore[no-any-return]
        elif is_dict_generic_type(t_):
            k_t: Type[Any]
            v_t: Type[Any]
            k_t, v_t = get_args(t_)
            return origin[k_t, _process_annotation(v_t)]  # type: ignore[no-any-return]
        # set/frozenset are not Sequence or Mapping so need a separate check
        elif origin is not None and _is_set_origin(origin):
            s_t: Type[Any] = get_args(t_)[0]
            return origin[_process_annotation(s_t)]  # type: ignore[no-any-return]
        elif is_union_type(t_):
            u_t_s = tuple(_process_annotation(u_t) for u_t in extract_union_types(t_))
            return Union[u_t_s]  # type: ignore[return-value]
        elif is_subclass(t_, BaseModel):
            # types must be same before and after processing
            if id(t_) in _child_models:
                return _child_models[id(t_)]
            else:
                _child_models[id(t_)] = child_model = apply_schema_contract_to_model(
                    t_, column_mode, data_mode, _child_models=_child_models
                )
                return child_model
        return t_

    def _rebuild_annotated(f: Any) -> Type[Any]:
        if hasattr(f, "rebuild_annotation"):
            return f.rebuild_annotation()  # type: ignore[no-any-return]
        else:
            return f.annotation  # type: ignore[no-any-return]

    if getattr(model, "__pydantic_root_model__", False):
        from pydantic import RootModel as PydanticRootModel

        root_field = model.model_fields.get("root")
        if root_field:
            processed_ann = _process_annotation(_rebuild_annotated(root_field))
            new_rm = type(
                model.__name__ + "Extra" + extra.title(),
                (PydanticRootModel[processed_ann],),  # type: ignore[valid-type]
                {"__module__": model.__module__},
            )
            if original_dlt_config:
                new_rm.dlt_config = original_dlt_config  # type: ignore[attr-defined]
            return new_rm

    processed_fields = {
        n: (_process_annotation(_rebuild_annotated(f)), f) for n, f in model.model_fields.items()
    }

    # use __base__ to inherit validators (@field_validator, @model_validator)
    new_model: Type[_TPydanticModel] = create_model(  # type: ignore[call-overload]
        model.__name__ + "Extra" + extra.title(),
        __base__=model,
        **processed_fields,
    )
    # override config on the subclass and rebuild to pick up the new extra setting
    new_model.model_config = config
    new_model.model_rebuild(force=True)

    # restore dlt_config: the evolve path replaces model with a bare "Any" model that
    # lacks dlt_config. For the non-evolve path __base__ inheritance preserves it,
    # but setting it explicitly is harmless and keeps both paths consistent.
    if original_dlt_config:
        new_model.dlt_config = original_dlt_config  # type: ignore[attr-defined]
    return new_model


def create_list_model(
    model: Type[_TPydanticModel],
    column_mode: TSchemaEvolutionMode = "freeze",
    data_mode: TSchemaEvolutionMode = "freeze",
) -> Type[ListModel[_TPydanticModel]]:
    """Creates a model from `model` for validating list of items in batch.

    When `column_mode` or `data_mode` is `discard_row`, creates a lenient list model
    that uses WrapValidator to turn invalid items into None (filtered by the caller).
    Otherwise creates a strict list model for batch validation.
    """
    if column_mode == "discard_row" or data_mode == "discard_row":
        from pydantic.functional_validators import WrapValidator

        def _lenient_item_validator(value: Any, handler: Any) -> Optional[_TPydanticModel]:
            try:
                return handler(value)  # type: ignore[no-any-return]
            except ValidationError as val_err:
                # re-raise model_type errors (non-mapping items) — cannot discard those
                for err in val_err.errors():
                    if err["type"] == "model_type":
                        raise
                return None
            except Exception:
                return None

        item_type = Annotated[Optional[model], WrapValidator(_lenient_item_validator)]  # type: ignore[valid-type]
        return create_model(
            "LenientList" + model.__name__,
            items=(List[item_type], ...),  # type: ignore[return-value]
        )

    return create_model(
        "List" + model.__name__,
        items=(List[model], ...),  # type: ignore[return-value,valid-type]
    )


def _classify_validation_errors(
    table_name: str,
    model: Type[BaseModel],
    item: TDataItem,
    exc: ValidationError,
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode,
) -> None:
    """Classifies validation errors and raises DataValidationError for freeze mode.

    For discard_row mode, returns without raising so the caller can discard the item.
    For model_type errors (item is not a mapping), always re-raises.
    """
    for err in exc.errors():
        if err["type"] == "model_type":
            raise exc
        if err["type"] == "extra_forbidden":
            if column_mode == "freeze":
                raise DataValidationError(
                    None,
                    table_name,
                    str(err["loc"]),
                    "columns",
                    "freeze",
                    model,
                    {"columns": "freeze"},
                    item,
                    err["msg"],
                ) from exc
            elif column_mode == "discard_row":
                return
            raise NotImplementedError(f"`{column_mode=:}` not implemented for Pydantic validation")
        else:
            if data_mode == "freeze":
                raise DataValidationError(
                    None,
                    table_name,
                    str(err["loc"]),
                    "data_type",
                    "freeze",
                    model,
                    {"data_type": "freeze"},
                    item,
                    err["msg"],
                ) from exc
            elif data_mode == "discard_row":
                return
            raise NotImplementedError(f"`{data_mode=:}` not implemented for Pydantic validation")


def validate_and_filter_items(
    table_name: str,
    list_model: Type[ListModel[_TPydanticModel]],
    items: List[TDataItem],
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode,
) -> List[_TPydanticModel]:
    """Validates list of `item` with `list_model` and returns parsed Pydantic models."""
    if column_mode == "discard_row" or data_mode == "discard_row":
        # lenient path: WrapValidator already converted bad items to None
        result = list_model(items=items).items
        return [item for item in result if item is not None]

    # strict path: batch validate, classify errors on failure
    try:
        return list_model(items=items).items
    except ValidationError as e:
        for err in e.errors():
            if err["type"] == "model_type":
                raise
            # loc is ("items", <index>, <field>) for item-level errors
            if len(err["loc"]) < 2:
                # list-level error (e.g. items not a list) — re-raise as-is
                raise
            err_idx = int(err["loc"][1])
            err_item = items[err_idx]
            _classify_validation_errors(table_name, list_model, err_item, e, column_mode, data_mode)
        raise AssertionError("unreachable")


def validate_and_filter_item(
    table_name: str,
    model: Type[_TPydanticModel],
    item: TDataItems,
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode,
) -> Optional[_TPydanticModel]:
    """Validates `item` against model `model` and returns an instance of it.

    Returns None for discard_row when validation fails, raises DataValidationError
    for freeze mode.
    """
    try:
        return model.model_validate(item)
    except ValidationError as e:
        _classify_validation_errors(table_name, model, item, e, column_mode, data_mode)
        return None
