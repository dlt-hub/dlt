import functools
import warnings
import semver
import typing
import typing_extensions

from dlt.version import __version__

VersionString = typing.Union[str, semver.VersionInfo]


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
        self.since = (
            since if isinstance(since, semver.VersionInfo) else semver.parse_version_info(since)
        )
        if expected_due:
            expected_due = (
                expected_due
                if isinstance(expected_due, semver.VersionInfo)
                else semver.parse_version_info(expected_due)
            )
        self.expected_due = expected_due if expected_due is not None else self.since.bump_minor()

    def __str__(self) -> str:
        message = (
            f"{self.message}. Deprecated in dlt {self.since} to be removed in {self.expected_due}."
        )
        return message


class Dlt04DeprecationWarning(DltDeprecationWarning):
    V04 = semver.parse_version_info("0.4.0")

    def __init__(self, message: str, *args: typing.Any, expected_due: VersionString = None) -> None:
        super().__init__(
            message, *args, since=Dlt04DeprecationWarning.V04, expected_due=expected_due
        )


# show dlt deprecations once
warnings.simplefilter("once", DltDeprecationWarning)

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
