import contextlib
import functools
import inspect
from typing import Callable, Any, Type
from typing_extensions import get_type_hints, get_args

from dlt.common.exceptions import DictValidationException
from dlt.common.typing import (
    StrAny,
    is_literal_type,
    is_optional_type,
    extract_union_types,
    is_union_type,
    is_typeddict,
    is_list_generic_type,
    is_dict_generic_type,
    _TypedDict,
)


TFilterFunc = Callable[[str], bool]
TCustomValidator = Callable[[str, str, Any, Any], bool]


def validate_dict(
    spec: Type[_TypedDict],
    doc: StrAny,
    path: str,
    filter_f: TFilterFunc = None,
    validator_f: TCustomValidator = None,
) -> None:
    """Validate the `doc` dictionary based on the given typed dictionary specification `spec`.

    Args:
        spec (Type[_TypedDict]): The typed dictionary that `doc` should conform to.
        doc (StrAny): The dictionary to validate.
        path (str): The string representing the location of the dictionary
            in a hierarchical data structure.
        filter_f (TFilterFunc, optional): A function to filter keys in `doc`. It should
            return `True` for keys to be kept. Defaults to a function that keeps all keys.
        validator_f (TCustomValidator, optional): A function to perform additional validation
            for types not covered by this function. It should return `True` if the validation passes
            or raise DictValidationException on validation error. For types it cannot validate, it
            should return False to allow chaining.
            Defaults to a function that rejects all such types.
    Raises:
        DictValidationException: If there are missing required fields, unexpected fields,
            type mismatches or unvalidated types in `doc` compared to `spec`.

    Returns:
        None
    """
    # pass through filter
    filter_f = filter_f or (lambda _: True)
    # can't validate anything
    validator_f = validator_f or (lambda p, pk, pv, t: False)

    allowed_props = get_type_hints(spec)
    required_props = {k: v for k, v in allowed_props.items() if not is_optional_type(v)}
    # remove optional props
    props = {k: v for k, v in doc.items() if filter_f(k)}
    # check missing props
    missing = set(required_props.keys()).difference(props.keys())
    if len(missing):
        raise DictValidationException(
            f"In {path}: following required fields are missing {missing}", path
        )
    # check unknown props
    unexpected = set(props.keys()).difference(allowed_props.keys())
    if len(unexpected):
        raise DictValidationException(
            f"In {path}: following fields are unexpected {unexpected}", path
        )

    def verify_prop(pk: str, pv: Any, t: Any) -> None:
        # covers none in optional and union types
        if is_optional_type(t) and pv is None:
            return
        if is_union_type(t):
            # pass if value is none
            union_types = extract_union_types(t, no_none=True)
            # this is the case for optional fields
            if len(union_types) == 1:
                verify_prop(pk, pv, union_types[0])
            else:
                has_passed = False
                for ut in union_types:
                    with contextlib.suppress(DictValidationException):
                        verify_prop(pk, pv, ut)
                        has_passed = True
                if not has_passed:
                    type_names = [
                        (
                            str(get_args(ut))
                            if is_literal_type(ut)
                            else getattr(ut, "__name__", str(ut))
                        )
                        for ut in union_types
                    ]
                    raise DictValidationException(
                        f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__}."
                        f" One of these types expected: {', '.join(type_names)}.",
                        path,
                        pk,
                        pv,
                    )
        elif is_literal_type(t):
            a_l = get_args(t)
            if pv not in a_l:
                raise DictValidationException(
                    f"In {path}: field {pk} value {pv} not in allowed {a_l}", path, pk, pv
                )
        elif t in [int, bool, str, float]:
            if not isinstance(pv, t):
                raise DictValidationException(
                    f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__} while"
                    f" {t.__name__} is expected",
                    path,
                    pk,
                    pv,
                )
        elif is_typeddict(t):
            if not isinstance(pv, dict):
                raise DictValidationException(
                    f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__} while"
                    " dict is expected",
                    path,
                    pk,
                    pv,
                )
            validate_dict(t, pv, f"{path}/{pk}", filter_f, validator_f)
        elif is_list_generic_type(t):
            if not isinstance(pv, list):
                raise DictValidationException(
                    f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__} while"
                    " list is expected",
                    path,
                    pk,
                    pv,
                )
            # get a list element type from generic and process each list element.
            l_t = get_args(t)[0]
            for i, l_v in enumerate(pv):
                verify_prop(f"{pk}[{i}]", l_v, l_t)
        elif is_dict_generic_type(t):
            if not isinstance(pv, dict):
                raise DictValidationException(
                    f"In {path}: field {pk} value {pv} has invalid type {type(pv).__name__} while"
                    " dict is expected",
                    path,
                    pk,
                    pv,
                )
            # get a dict key and value type from generic and process each k: v of the dict.
            _, d_v_t = get_args(t)
            for d_k, d_v in pv.items():
                if not isinstance(d_k, str):
                    raise DictValidationException(
                        f"In {path}: field {pk} key {d_k} must be a string", path, pk, d_k
                    )
                verify_prop(f"{pk}[{d_k}]", d_v, d_v_t)
        elif t is Any:
            # pass everything with any type
            pass
        elif inspect.isclass(t) and isinstance(pv, t):
            # allow instances of classes
            pass
        else:
            type_name = getattr(t, "__name__", str(t))
            pv_type_name = getattr(type(pv), "__name__", str(type(pv)))
            # try to apply special validator
            if not validator_f(path, pk, pv, t):
                # type `t` cannot be validated by validator_f
                if inspect.isclass(t):
                    if not isinstance(pv, t):
                        raise DictValidationException(
                            f"In {path}: field {pk} expect class {type_name} but got instance of"
                            f" {pv_type_name}",
                            path,
                            pk,
                        )
                # TODO: when Python 3.9 and earlier support is
                # dropped, just __name__ can be used
                type_name = getattr(t, "__name__", str(t))
                raise DictValidationException(
                    f"In {path}: field {pk} has expected type {type_name} which lacks validator",
                    path,
                    pk,
                )

    # check allowed props
    for pk, pv in props.items():
        verify_prop(pk, pv, allowed_props[pk])


validate_dict_ignoring_xkeys = functools.partial(
    validate_dict, filter_f=lambda k: not k.startswith("x-")
)
