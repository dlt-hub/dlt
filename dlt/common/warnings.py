import functools
import warnings
import semver
import typing
import typing_extensions

from dlt.common.typing import (
    DictStrAny,
    _TypedDict,
    get_args,
    get_type_globals,
    get_type_hints,
    is_annotated,
    is_typeddict,
)
from dlt.version import __version__

VersionString = typing.Union[str, semver.Version]


class DltDeprecationWarning(DeprecationWarning):
    """A dlt specific deprecation warning.

    This warning is raised when using deprecated functionality in dlt. It provides information on when the
    deprecation was introduced and the expected version in which the corresponding functionality will be removed.

    Attributes:
        message: Description of the warning.
        since: Version in which the deprecation was introduced.
        expected_due: Version in which the corresponding functionality is expected to be removed.
    """

    def __init__(
        self,
        message: str,
        *args: typing.Any,
        since: VersionString,
        expected_due: VersionString = None,
    ) -> None:
        super().__init__(message, *args)
        self.message = message.rstrip(".")
        self.since = since if isinstance(since, semver.Version) else semver.Version.parse(since)
        if expected_due:
            expected_due = (
                expected_due
                if isinstance(expected_due, semver.Version)
                else semver.Version.parse(expected_due)
            )
        # we deprecate across major version since 1.0.0
        self.expected_due = expected_due if expected_due is not None else self.since.bump_major()

    def __str__(self) -> str:
        message = (
            f"{self.message}. Deprecated in dlt {self.since} to be removed in {self.expected_due}."
        )
        return message


class Dlt04DeprecationWarning(DltDeprecationWarning):
    V04 = semver.Version.parse("0.4.0")

    def __init__(self, message: str, *args: typing.Any, expected_due: VersionString = None) -> None:
        super().__init__(
            message, *args, since=Dlt04DeprecationWarning.V04, expected_due=expected_due
        )


class Dlt100DeprecationWarning(DltDeprecationWarning):
    V100 = semver.Version.parse("1.0.0")

    def __init__(self, message: str, *args: typing.Any, expected_due: VersionString = None) -> None:
        super().__init__(
            message, *args, since=Dlt100DeprecationWarning.V100, expected_due=expected_due
        )


# show dlt deprecations once
warnings.simplefilter("once", DltDeprecationWarning)


class TNoExtraKwargs(typing_extensions.TypedDict):
    """Empty schema for a `**kwargs` that only collects deprecated argument names."""


class SkipDeprecation:
    """Sentinel returned by a `Deprecated.convert` to write nothing to the replacement field."""


def _identity(value: typing.Any) -> typing.Any:
    return value


class Deprecated:
    """Marks a deprecation-schema field as deprecated in favor of `maps_to`.

    Used inside `Annotated[<old value type>, Deprecated(...)]` in a deprecation schema
    consumed by `apply_deprecations`.

    Args:
        maps_to (str): Name of the replacement field the old value is written to.
        convert (Callable[[Any], Any]): Maps the old value to the replacement value.
            Return `SkipDeprecation` to write nothing. Defaults to identity.
        message (Optional[str]): Custom deprecation message. Defaults to a generated one.
        since (Optional[VersionString]): Version the field was deprecated in. Overrides the
            default passed to `apply_deprecations`.
        expected_due (Optional[VersionString]): Version the field is removed in.
    """

    def __init__(
        self,
        maps_to: str,
        convert: typing.Callable[[typing.Any], typing.Any] = _identity,
        message: typing.Optional[str] = None,
        since: typing.Optional[VersionString] = None,
        expected_due: typing.Optional[VersionString] = None,
    ) -> None:
        self.maps_to = maps_to
        self.convert = convert
        self.message = message
        self.since = since
        self.expected_due = expected_due


def apply_deprecations(
    deprecation_spec: typing.Type[_TypedDict],
    doc: DictStrAny,
    *,
    path: str = ".",
    since: typing.Optional[VersionString] = None,
    expected_due: typing.Optional[VersionString] = None,
    warn: bool = True,
    remove: bool = True,
    prefer_new: bool = True,
    stacklevel: int = 2,
) -> DictStrAny:
    """Convert deprecated keys in `doc` to their replacements declared in `deprecation_spec`.

    For every `Annotated[..., Deprecated(...)]` field of `deprecation_spec` present in `doc`:
    emits a `DltDeprecationWarning` (unless `warn` is False), runs the marker's `convert`,
    writes the result under `Deprecated.maps_to`, and drops the old key (unless `remove` is
    False). A field typed as a nested deprecation `TypedDict` migrates its sub-document
    recursively. Mutates `doc` in place and returns it.

    Args:
        deprecation_spec (Type[_TypedDict]): TypedDict whose keys are deprecated field names.
        doc (DictStrAny): Dictionary to migrate in place.
        path (str): Location label used in the warning message.
        since (Optional[VersionString]): Default deprecation version for fields without one.
        expected_due (Optional[VersionString]): Default removal version.
        warn (bool): Emit `DltDeprecationWarning` for each converted field.
        remove (bool): Drop the old key after conversion.
        prefer_new (bool): When both old and replacement keys are present, keep the replacement.
        stacklevel (int): `warnings.warn` stacklevel, points at the caller's caller by default.

    Returns:
        DictStrAny: The same `doc`, mutated.
    """
    hints = get_type_hints(
        deprecation_spec, include_extras=True, globalns=get_type_globals(deprecation_spec)
    )
    for old_key, hint in hints.items():
        if old_key not in doc:
            continue
        if is_annotated(hint):
            _, *metadata = get_args(hint)
            marker = next((m for m in metadata if isinstance(m, Deprecated)), None)
            if marker is not None:
                if warn:
                    message = (
                        marker.message
                        or f"Field `{old_key}` is deprecated at `{path}`, use `{marker.maps_to}` instead"
                    )
                    warnings.warn(
                        DltDeprecationWarning(
                            message,
                            since=marker.since or since,
                            expected_due=marker.expected_due or expected_due,
                        ),
                        stacklevel=stacklevel,
                    )
                if not (prefer_new and marker.maps_to in doc):
                    converted = marker.convert(doc[old_key])
                    if converted is not SkipDeprecation:
                        doc[marker.maps_to] = converted
                if remove:
                    del doc[old_key]
                continue
        # a field typed as a nested deprecation schema migrates its sub-document
        if is_typeddict(hint) and isinstance(doc[old_key], dict):
            apply_deprecations(
                hint,
                doc[old_key],
                path=f"{path}.{old_key}",
                since=since,
                expected_due=expected_due,
                warn=warn,
                remove=remove,
                prefer_new=prefer_new,
                stacklevel=stacklevel,
            )
    return doc


if typing.TYPE_CHECKING or hasattr(typing_extensions, "deprecated"):
    deprecated = typing_extensions.deprecated
else:
    # ported from typing_extensions so versions older than 4.5.x may still be used
    _T = typing.TypeVar("_T")

    def deprecated(
        __msg: str,
        *,
        category: typing.Optional[typing.Type[Warning]] = DeprecationWarning,
        stacklevel: int = 1,
    ) -> typing.Callable[[_T], _T]:
        """Indicate that a class, function or overload is deprecated.

        Usage:

            @deprecated("Use B instead")
            class A:
                pass

            @deprecated("Use g instead")
            def f():
                pass

            @overload
            @deprecated("int support is deprecated")
            def g(x: int) -> int: ...
            @overload
            def g(x: str) -> int: ...

        When this decorator is applied to an object, the type checker
        will generate a diagnostic on usage of the deprecated object.

        The warning specified by ``category`` will be emitted on use
        of deprecated objects. For functions, that happens on calls;
        for classes, on instantiation. If the ``category`` is ``None``,
        no warning is emitted. The ``stacklevel`` determines where the
        warning is emitted. If it is ``1`` (the default), the warning
        is emitted at the direct caller of the deprecated object; if it
        is higher, it is emitted further up the stack.

        The decorator sets the ``__deprecated__``
        attribute on the decorated object to the deprecation message
        passed to the decorator. If applied to an overload, the decorator
        must be after the ``@overload`` decorator for the attribute to
        exist on the overload as returned by ``get_overloads()``.

        See PEP 702 for details.

        """

        def decorator(__arg: _T) -> _T:
            if category is None:
                __arg.__deprecated__ = __msg
                return __arg
            elif isinstance(__arg, type):
                original_new = __arg.__new__
                has_init = __arg.__init__ is not object.__init__

                @functools.wraps(original_new)
                def __new__(cls, *args, **kwargs):
                    warnings.warn(__msg, category=category, stacklevel=stacklevel + 1)
                    if original_new is not object.__new__:
                        return original_new(cls, *args, **kwargs)
                    # Mirrors a similar check in object.__new__.
                    elif not has_init and (args or kwargs):
                        raise TypeError(f"{cls.__name__}() takes no arguments")
                    else:
                        return original_new(cls)

                __arg.__new__ = staticmethod(__new__)
                __arg.__deprecated__ = __new__.__deprecated__ = __msg
                return __arg
            elif callable(__arg):

                @functools.wraps(__arg)
                def wrapper(*args, **kwargs):
                    warnings.warn(__msg, category=category, stacklevel=stacklevel + 1)
                    return __arg(*args, **kwargs)

                __arg.__deprecated__ = wrapper.__deprecated__ = __msg
                return wrapper
            else:
                raise TypeError(
                    "@deprecated decorator with non-None category must be applied to "
                    f"a class or callable, not {__arg!r}"
                )

        return decorator
