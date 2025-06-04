from __future__ import annotations as _annotations
from copy import copy
from typing import (
    Dict,
    Generic,
    Optional,
    Set,
    List,
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
from dlt.common.warnings import Dlt100DeprecationWarning

try:
    from pydantic import BaseModel, ValidationError, Json, create_model
except ImportError:
    raise MissingDependencyException(
        "dlt Pydantic helpers", ["pydantic"], "Both Pydantic 1.x and 2.x are supported"
    )

_PYDANTIC_2 = False
try:
    from pydantic import PydanticDeprecatedSince20

    _PYDANTIC_2 = True
    # hide deprecation warning
    import warnings

    warnings.simplefilter("ignore", category=PydanticDeprecatedSince20)
except ImportError:
    pass

_TPydanticModel = TypeVar("_TPydanticModel", bound=BaseModel)


snake_case_naming_convention = SnakeCaseNamingConvention()


class ListModel(BaseModel, Generic[_TPydanticModel]):
    items: List[_TPydanticModel]


class DltConfig(TypedDict, total=False):
    """dlt configuration that can be attached to Pydantic model

    Example below removes `nested` field from the resulting dlt schema.
    >>> class ItemModel(BaseModel):
    >>>     b: bool
    >>>     nested: Dict[str, Any]
    >>>     dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}
    """

    skip_nested_types: bool
    """If True, columns of complex types (`dict`, `list`, `BaseModel`) will be excluded from dlt schema generated from the model"""
    skip_complex_types: bool  # deprecated


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

    for field_name, field in model.__fields__.items():  # type: ignore[union-attr]
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


def get_extra_from_model(model: Type[BaseModel]) -> str:
    default_extra = "ignore"
    if _PYDANTIC_2:
        default_extra = model.model_config.get("extra", default_extra)
    else:
        default_extra = str(model.Config.extra) or default_extra  # type: ignore[attr-defined]
    return default_extra


def apply_schema_contract_to_model(
    model: Type[_TPydanticModel],
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode = "freeze",
) -> Type[_TPydanticModel]:
    """Configures or re-creates `model` so it behaves according to `column_mode` and `data_mode` settings.

    `column_mode` sets the model behavior when unknown field is found.
    `data_mode` sets model behavior when known field does not validate. currently `evolve` and `freeze` are supported here.

    `discard_row` is implemented in `validate_item`.
    """
    if data_mode == "evolve":
        # create a lenient model that accepts any data
        model = create_model(model.__name__ + "Any", **{n: (Any, None) for n in model.model_fields})  # type: ignore
    elif data_mode == "discard_value":
        raise NotImplementedError(
            "`data_mode='discard_value'`. Cannot discard defined fields with validation errors"
            " using Pydantic models."
        )

    extra = column_mode_to_extra(column_mode)

    if extra == get_extra_from_model(model):
        # no need to change the model
        return model

    if _PYDANTIC_2:
        config = copy(model.model_config)
        config["extra"] = extra  # type: ignore[typeddict-item]
    else:
        from pydantic.config import prepare_config

        config = copy(model.Config)  # type: ignore[attr-defined]
        config.extra = extra  # type: ignore[attr-defined]
        prepare_config(config, model.Config.__name__)  # type: ignore[attr-defined]

    _child_models: Dict[int, Type[BaseModel]] = {}

    def _process_annotation(t_: Type[Any]) -> Type[Any]:
        """Recursively recreates models with applied schema contract"""
        if is_annotated(t_):
            a_t, *a_m = get_args(t_)
            return Annotated[_process_annotation(a_t), tuple(a_m)]  # type: ignore[return-value]
        elif is_list_generic_type(t_):
            l_t: Type[Any] = get_args(t_)[0]
            return get_origin(t_)[_process_annotation(l_t)]  # type: ignore[no-any-return]
        elif is_dict_generic_type(t_):
            k_t: Type[Any]
            v_t: Type[Any]
            k_t, v_t = get_args(t_)
            return get_origin(t_)[k_t, _process_annotation(v_t)]  # type: ignore[no-any-return]
        elif is_union_type(t_):
            u_t_s = tuple(_process_annotation(u_t) for u_t in extract_union_types(t_))
            return Union[u_t_s]  # type: ignore[return-value]
        elif is_subclass(t_, BaseModel):
            # types must be same before and after processing
            if id(t_) in _child_models:
                return _child_models[id(t_)]
            else:
                _child_models[id(t_)] = child_model = apply_schema_contract_to_model(
                    t_, column_mode, data_mode
                )
                return child_model
        return t_

    def _rebuild_annotated(f: Any) -> Type[Any]:
        if hasattr(f, "rebuild_annotation"):
            return f.rebuild_annotation()  # type: ignore[no-any-return]
        else:
            return f.annotation  # type: ignore[no-any-return]

    new_model: Type[_TPydanticModel] = create_model(  # type: ignore[call-overload]
        model.__name__ + "Extra" + extra.title(),
        __config__=config,
        **{n: (_process_annotation(_rebuild_annotated(f)), f) for n, f in model.__fields__.items()},  # type: ignore[attr-defined]
    )
    # pass dlt config along
    dlt_config = getattr(model, "dlt_config", None)
    if dlt_config:
        new_model.dlt_config = dlt_config  # type: ignore[attr-defined]
    return new_model


def create_list_model(
    model: Type[_TPydanticModel], data_mode: TSchemaEvolutionMode = "freeze"
) -> Type[ListModel[_TPydanticModel]]:
    """Creates a model from `model` for validating list of items in batch according to `data_mode`

    Currently only freeze is supported. See comments in the code
    """
    # TODO: use LenientList to create list model that automatically discards invalid items
    #   https://github.com/pydantic/pydantic/issues/2274 and https://gist.github.com/dmontagu/7f0cef76e5e0e04198dd608ad7219573
    return create_model(
        "List" + __name__,
        items=(List[model], ...),  # type: ignore[return-value,valid-type]
    )


def validate_and_filter_items(
    table_name: str,
    list_model: Type[ListModel[_TPydanticModel]],
    items: List[TDataItem],
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode,
) -> List[_TPydanticModel]:
    """Validates list of `item` with `list_model` and returns parsed Pydantic models. If `column_mode` and `data_mode` are set
    this function will remove non validating items (`discard_row`) or raise on the first non-validating items (`freeze`). Note
    that the model itself may be configured to remove non validating or extra items as well.

    `list_model` should be created with `create_list_model` and have `items` field which this function returns.
    """
    try:
        return list_model(items=items).items
    except ValidationError as e:
        deleted: Set[int] = set()
        for err in e.errors():
            # TODO: we can get rid of most of the code if we use LenientList as explained above
            if len(err["loc"]) >= 2:
                err_idx = int(err["loc"][1])
                if err_idx in deleted:
                    # already dropped
                    continue
                err_item = items[err_idx - len(deleted)]
            else:
                # top level error which means misalignment of list model and items
                raise DataValidationError(
                    None,
                    table_name,
                    str(err["loc"]),
                    "columns",
                    "freeze",
                    list_model,
                    {"columns": "freeze"},
                    items,
                    err["msg"],
                ) from e
            # raise on freeze
            if err["type"] == "extra_forbidden":
                if column_mode == "freeze":
                    raise DataValidationError(
                        None,
                        table_name,
                        str(err["loc"]),
                        "columns",
                        "freeze",
                        list_model,
                        {"columns": "freeze"},
                        err_item,
                        err["msg"],
                    ) from e
                elif column_mode == "discard_row":
                    # pop at the right index
                    items.pop(err_idx - len(deleted))
                    # store original index so we do not pop again
                    deleted.add(err_idx)
                else:
                    raise NotImplementedError(
                        f"`{column_mode=:}` not implemented for Pydantic validation"
                    )
            else:
                if data_mode == "freeze":
                    raise DataValidationError(
                        None,
                        table_name,
                        str(err["loc"]),
                        "data_type",
                        "freeze",
                        list_model,
                        {"data_type": "freeze"},
                        err_item,
                        err["msg"],
                    ) from e
                elif data_mode == "discard_row":
                    items.pop(err_idx - len(deleted))
                    deleted.add(err_idx)
                else:
                    raise NotImplementedError(
                        f"`{column_mode=:}` not implemented for Pydantic validation"
                    )

        # validate again with error items removed
        return validate_and_filter_items(table_name, list_model, items, column_mode, data_mode)


def validate_and_filter_item(
    table_name: str,
    model: Type[_TPydanticModel],
    item: TDataItems,
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode,
) -> Optional[_TPydanticModel]:
    """Validates `item` against model `model` and returns an instance of it. If `column_mode` and `data_mode` are set
    this function will return None (`discard_row`) or raise on non-validating items (`freeze`). Note
    that the model itself may be configured to remove non validating or extra items as well."""
    try:
        return model.parse_obj(item)
    except ValidationError as e:
        for err in e.errors():
            # raise on freeze
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
                    ) from e
                elif column_mode == "discard_row":
                    return None
                raise NotImplementedError(
                    f"`{column_mode=:}` not implemented for Pydantic validation"
                )
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
                    ) from e
                elif data_mode == "discard_row":
                    return None
                raise NotImplementedError(
                    f"`{data_mode=:}` not implemented for Pydantic validation"
                )
        raise AssertionError("unreachable")
