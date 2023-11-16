from typing import Generic, Sequence, TypedDict, List, Type, Union, TypeVar, get_type_hints, get_args, Any

from dlt.common.exceptions import MissingDependencyException, DltException
from dlt.common.schema.typing import TSchemaEvolutionMode, TTableSchemaColumns
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.typing import TDataItem, TDataItems, is_optional_type, extract_inner_type, is_list_generic_type, is_dict_generic_type, is_union

try:
    from pydantic import BaseModel, ValidationError, Json, create_model
except ImportError:
    raise MissingDependencyException("dlt Pydantic helpers", ["pydantic"], "Both Pydantic 1.x and 2.x are supported")

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


class ListModel(BaseModel, Generic[_TPydanticModel]):
    items: List[_TPydanticModel]


class DltConfig(TypedDict, total=False):
    """dlt configuration that can be attached to Pydantic model

    Example below removes `nested` field from the resulting dlt schema.
    >>> class ItemModel(BaseModel):
    >>>     b: bool
    >>>     nested: Dict[str, Any]
    >>>     dlt_config: ClassVar[DltConfig] = {"skip_complex_types": True}
    """
    skip_complex_types: bool
    """If True, columns of complex types (`dict`, `list`, `BaseModel`) will be excluded from dlt schema generated from the model"""


def pydantic_to_table_schema_columns(model: Union[BaseModel, Type[BaseModel]]) -> TTableSchemaColumns:
    """Convert a pydantic model to a table schema columns dict

    See also DltConfig for more control over how the schema is created

    Args:
        model: The pydantic model to convert. Can be a class or an instance.


    Returns:
        TTableSchemaColumns: table schema columns dict
    """
    skip_complex_types = False
    if hasattr(model, "dlt_config"):
        skip_complex_types = model.dlt_config.get("skip_complex_types", False)

    result: TTableSchemaColumns = {}

    for field_name, field in model.__fields__.items():  # type: ignore[union-attr]
        annotation = field.annotation
        if inner_annotation := getattr(annotation, 'inner_type', None):
            # This applies to pydantic.Json fields, the inner type is the type after json parsing
            # (In pydantic 2 the outer annotation is the final type)
            annotation = inner_annotation
        nullable = is_optional_type(annotation)

        if is_union(annotation):
            inner_type = get_args(annotation)[0]
        else:
            inner_type = extract_inner_type(annotation)

        if inner_type is Json:  # Same as `field: Json[Any]`
            inner_type = Any

        if inner_type is Any:  # Any fields will be inferred from data
            continue

        if is_list_generic_type(inner_type):
            inner_type = list
        elif is_dict_generic_type(inner_type) or issubclass(inner_type, BaseModel):
            inner_type = dict

        name = field.alias or field_name
        try:
            data_type = py_type_to_sc_type(inner_type)
        except TypeError:
            # try to coerce unknown type to text
            data_type = "text"

        if data_type == 'complex' and skip_complex_types:
            continue

        result[name] = {
            "name": name,
            "data_type": data_type,
            "nullable": nullable,
        }

    return result

def apply_schema_contract_to_model(
    model: Type[_TPydanticModel],
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode = "freeze"
) -> Type[_TPydanticModel]:
    """Configures or re-creates `model` so it behaves according to `column_mode` and `data_mode` settings.

       `column_mode` sets the model behavior when unknown field is found.
       `data_mode` sets model behavior when known field does not validate. currently `evolve` and `freeze` are supported here.

       `discard_row` is implemented in `validate_item`.
    """
    if data_mode == "evolve":
        # create a lenient model that accepts any data
        model = create_model(model.__name__ + "Any", **{n:(Any, None) for n in model.__fields__})  # type: ignore[call-overload, attr-defined]
    elif data_mode == "discard_value":
        raise ValueError("data_mode is discard_value. Cannot discard defined fields with validation errors using Pydantic models. Not yet implemented.")

    extra = "forbid"
    if column_mode == "evolve":
        extra = "allow"
    elif column_mode == "discard_value":
        extra = "ignore"

    if _PYDANTIC_2:
        config = model.model_config
        config["extra"] = extra  # type: ignore[typeddict-item]
    else:
        config = model.Config  # type: ignore[attr-defined]
        config.extra = extra  # type: ignore[attr-defined]

    return create_model(  # type: ignore[no-any-return, call-overload]
        model.__name__ + "Extra" + extra.title(),
        __config__ = config,
        **{n:(f.annotation, f) for n, f in model.__fields__.items()}  # type: ignore[attr-defined]
    )


def create_list_model(model: Type[_TPydanticModel], data_mode: TSchemaEvolutionMode = "freeze") -> Type[ListModel[_TPydanticModel]]:
    """Creates a model from `model` for validating list of items in batch according to `data_mode`

       Currently only freeze is supported. See comments in the code
    """
    # TODO: use LenientList to create list model that automatically discards invalid items
    #   https://github.com/pydantic/pydantic/issues/2274 and https://gist.github.com/dmontagu/7f0cef76e5e0e04198dd608ad7219573
    return create_model(
        "List" + __name__,
        items=(List[model], ...)  # type: ignore[return-value,valid-type]
    )


def validate_items(
    list_model: Type[ListModel[_TPydanticModel]],
    items: List[TDataItem],
    column_mode: TSchemaEvolutionMode,
    data_mode: TSchemaEvolutionMode
) -> List[_TPydanticModel]:
    """Validates list of `item` with `list_model` and returns parsed Pydantic models

       `list_model` should be created with `create_list_model` and have `items` field which this function returns.
    """
    try:
        return list_model(items=items).items
    except ValidationError as e:
        delta_idx = 0
        for err in e.errors():
            if len(err["loc"]) >= 2:
                err_idx = int(err["loc"][1]) - delta_idx
                err_item = items[err_idx]
            else:
                # top level error which means misalignment of list model and items
                raise FullValidationError(list_model, items, e) from e
            # raise on freeze
            if err["type"] == 'extra_forbidden':
                if column_mode == "freeze":
                    raise FullValidationError(list_model, err_item, e) from e
                elif column_mode == "discard_row":
                    items.pop(err_idx)
                    delta_idx += 1

            else:
                if data_mode == "freeze":
                    raise FullValidationError(list_model, err_item, e) from e
                elif data_mode == "discard_row":
                    items.pop(err_idx)
                    delta_idx += 1

        # validate again with error items removed
        return validate_items(list_model, items, column_mode, data_mode)


def validate_item(model: Type[_TPydanticModel], item: TDataItems, column_mode: TSchemaEvolutionMode, data_mode: TSchemaEvolutionMode) -> _TPydanticModel:
    """Validates `item` against model `model` and returns an instance of it"""
    try:
        return model.parse_obj(item)
    except ValidationError as e:
        for err in e.errors(include_url=False, include_context=False):
            # raise on freeze
            if err["type"] == 'extra_forbidden':
                if column_mode == "freeze":
                    raise FullValidationError(model, item, e) from e
                elif column_mode == "discard_row":
                    return None
            else:
                if data_mode == "freeze":
                    raise FullValidationError(model, item, e) from e
                elif data_mode == "discard_row":
                    return None
        # validate again with error items removed
        return validate_item(model, item, column_mode, data_mode)


class FullValidationError(ValueError, DltException):
    def __init__(self, validator: Type[BaseModel], data_item: TDataItems, original_exception: Exception) ->None:
        self.original_exception = original_exception
        self.validator = validator
        self.data_item = data_item
        super().__init__(f"Extracted data item could not be validated with {validator}. Original message: {original_exception}")
