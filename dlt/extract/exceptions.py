from inspect import Signature, isgenerator
from typing import Any, Sequence, Set, Type

from dlt.common.exceptions import DltException
from dlt.common.reflection.exceptions import ReferenceImportError
from dlt.common.reflection.ref import ImportTrace
from dlt.common.reflection.inspect import isgeneratorfunction
from dlt.common.utils import get_callable_name


class ExtractorException(DltException):
    pass


class DltSourceException(DltException):
    pass


class DltResourceException(DltSourceException):
    def __init__(self, resource_name: str, msg: str) -> None:
        self.resource_name = resource_name
        super().__init__(msg)


class PipeException(DltException):
    def __init__(self, pipe_name: str, msg: str) -> None:
        self.pipe_name = pipe_name
        msg = f"In processing pipe `{pipe_name}`: " + msg
        super().__init__(msg)


class CreatePipeException(PipeException):
    def __init__(self, pipe_name: str, msg: str) -> None:
        super().__init__(pipe_name, msg)


class PipeItemProcessingError(PipeException):
    def __init__(self, pipe_name: str, msg: str) -> None:
        super().__init__(pipe_name, msg)


class PipeNotBoundToData(PipeException):
    def __init__(self, pipe_name: str, has_parent: bool) -> None:
        self.pipe_name = pipe_name
        self.has_parent = has_parent
        if has_parent:
            msg = (
                f"A pipe created from transformer `{pipe_name}` is unbound or its parent is unbound"
                " or empty. Provide a resource in `data_from` argument or bind resources with `|`"
                " operator."
            )
        else:
            msg = "Pipe is empty and does not have a resource at its head"
        super().__init__(pipe_name, msg)


class InvalidStepFunctionArguments(PipeException):
    def __init__(self, pipe_name: str, func_name: str, sig: Signature, call_error: str) -> None:
        self.func_name = func_name
        self.sig = sig
        super().__init__(
            pipe_name,
            f"Unable to call `{func_name}`: {call_error}. The mapping/filtering function"
            f" `{func_name}` requires first argument to take data item and optional second argument"
            f" named `meta`, but the signature is `{sig}`",
        )


class ResourceExtractionError(PipeException):
    def __init__(self, pipe_name: str, gen: Any, msg: str, kind: str) -> None:
        self.msg = msg
        self.kind = kind
        self.func_name = (
            gen.__name__
            if isgenerator(gen)
            else get_callable_name(gen) if callable(gen) else str(gen)
        )
        super().__init__(
            pipe_name,
            f"extraction of resource `{pipe_name}` in `{kind}` `{self.func_name}` caused an"
            f" exception: {msg}",
        )


class PipeGenInvalid(PipeException):
    def __init__(self, pipe_name: str, gen: Any) -> None:
        msg = (
            "A pipe generator element must be an Iterator (ie. list or generator function)."
            " Generator element is typically created from a `data` argument to pipeline.run or"
            " extract method."
        )
        msg += (
            " dlt will evaluate functions that were passed as data argument. If you passed a"
            " function the returned data type is not iterable. "
        )
        type_name = str(type(gen))
        msg += f" Generator type is {type_name}."
        if "DltSource" in type_name:
            msg += " Did you pass a @dlt.source decorated function without calling it?"
        if "DltResource" in type_name:
            msg += " Did you pass a function that returns dlt.resource without calling it?"

        super().__init__(pipe_name, msg)


class UnclosablePipe(PipeException):
    def __init__(self, pipe_name: str, gen: Any) -> None:
        type_name = str(type(gen))
        if gen_name := getattr(gen, "__name__", None):
            type_name = f"`{type_name}` ({gen_name})"
        msg = f"Pipe with gen of type `{type_name}` cannot be closed."
        if callable(gen) and isgeneratorfunction(gen):
            msg += " Closing of partially evaluated transformers is not yet supported."
        super().__init__(pipe_name, msg)


class ResourceNameMissing(DltResourceException):
    def __init__(self) -> None:
        super().__init__(
            None,
            """Resource name is missing. If you create a resource directly from data ie. from a list you must pass the name explicitly in `name` argument.
        Please note that for resources created from functions or generators, the name is the function name by default.""",
        )


class ResourceNotFoundError(DltResourceException, KeyError):
    def __init__(self, resource_name: str, context: str) -> None:
        self.resource_name = resource_name
        super().__init__(
            resource_name, f"Resource with `{resource_name=:}` could not be found: {context}"
        )


class InvalidResourceDataType(DltResourceException):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any], msg: str) -> None:
        self.item = item
        self._typ = _typ
        super().__init__(
            resource_name,
            f"Cannot create resource `{resource_name}` from specified data. If you want to process"
            " just one data item, enclose it in a list. "
            + msg,
        )


class InvalidParallelResourceDataType(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            "Parallel resource data must be a generator or a generator function. Resource"
            f" `{resource_name}` received data type `{_typ.__name__}`",
        )


class InvalidResourceDataTypeBasic(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            f"Resources cannot be strings or dictionaries but type `{_typ.__name__}` was provided."
            " Please pass your data in a list or as a function yielding items. If you want to"
            " process just one data item, enclose it in a list.",
        )


class InvalidResourceDataTypeMultiplePipes(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            "Resources with multiple parallel data pipes are not yet supported. This problem most"
            " often happens when you are creating a source with @dlt.source decorator that has"
            " several resources with the same name.",
        )


class InvalidTransformerDataTypeGeneratorFunctionRequired(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            "Transformer must be a function decorated with @dlt.transformer that takes data item as"
            " its first argument. Only first argument may be 'positional only'.",
        )


class InvalidTransformerGeneratorFunction(DltResourceException):
    def __init__(self, resource_name: str, func_name: str, sig: Signature, code: int) -> None:
        self.func_name = func_name
        self.sig = sig
        self.code = code
        msg = f"Transformer function `{func_name}` must take data item as its first argument. "
        if code == 1:
            msg += "The actual function does not take any arguments."
        elif code == 2:
            msg += f"Only the first argument may be 'positional only', actual signature is `{sig}`"
        elif code == 3:
            msg += f"The first argument cannot be keyword only, actual signature is `{sig}`"

        super().__init__(resource_name, msg)


class InvalidResourceDataTypeIsNone(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            "Resource data missing. Did you forget the return statement in @dlt.resource decorated"
            " function?",
        )


class InvalidResourceReturnsResource(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            "Resource returned another resource but the signature of the resource function is "
            "missing a correct type annotation (DltResource or derived class). Please annotate "
            f"function `{item}` correctly."
            "",
        )


class ResourceFunctionExpected(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            f"Expected function or callable as first parameter to resource `{resource_name}` but"
            f" type `{_typ.__name__}` found. Please decorate a function with `@dlt.resource`",
        )


class InvalidParentResourceDataType(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(
            resource_name,
            item,
            _typ,
            f"A parent resource of `{resource_name}` is of type `{_typ.__name__}`. Did you forget"
            " to use `@dlt.resource` decorator or `resource` function?",
        )


class InvalidParentResourceIsAFunction(DltResourceException):
    def __init__(self, resource_name: str, func_name: str) -> None:
        self.func_name = func_name
        super().__init__(
            resource_name,
            f"A data source `{func_name}` of a transformer `{resource_name}` is an undecorated"
            " function. Please decorate it with `@dlt.resource` or pass to `resource` function.",
        )


class DeletingResourcesNotSupported(DltResourceException):
    def __init__(self, source_name: str, resource_name: str) -> None:
        super().__init__(
            resource_name, f"Resource cannot be removed the the source `{source_name}`"
        )


class ParametrizedResourceUnbound(DltResourceException):
    def __init__(
        self, resource_name: str, func_name: str, sig: Signature, kind: str, error: str
    ) -> None:
        self.func_name = func_name
        self.sig = sig
        msg = (
            f"The `{kind}` `{resource_name}` is parametrized and expects following arguments:"
            f" `{sig}`. Did you forget to bind the `{func_name}` function? For example from"
            f" `source.{resource_name}.bind(...)`"
        )
        if error:
            msg += f" .Details: {error}"
        super().__init__(resource_name, msg)


class ResourceNotATransformer(DltResourceException):
    def __init__(self, resource_name: str, msg: str) -> None:
        super().__init__(resource_name, f"This resource is not a transformer: {msg}")


class InconsistentTableTemplate(DltSourceException):
    def __init__(self, reason: str) -> None:
        msg = f"A set of table hints provided to the resource is inconsistent: {reason}"
        super().__init__(msg)


class DataItemRequiredForDynamicTableHints(DltResourceException):
    def __init__(self, resource_name: str) -> None:
        super().__init__(
            resource_name,
            "Resource `{resource_name}` contains a dynamic hint (i.e., a value in the"
            " `@dlt.resource` decorator is set by a function), but the data to generate the hint is"
            " missing. Make sure that each data item contains the necessary field to compute the"
            " dynamic hint value.",
        )


class SourceDataIsNone(DltSourceException):
    def __init__(self, source_name: str) -> None:
        self.source_name = source_name
        super().__init__(
            f"No data returned or yielded from source function `{source_name}`. Did you forget the"
            " return statement?"
        )


class SourceExhausted(DltSourceException):
    def __init__(self, source_name: str) -> None:
        self.source_name = source_name
        super().__init__(
            f"Source `{source_name}` is exhausted or has active iterator. You can iterate or pass"
            " the source to dlt pipeline only once."
        )


class ResourcesNotFoundError(DltSourceException):
    def __init__(
        self, source_name: str, available_resources: Set[str], requested_resources: Set[str]
    ) -> None:
        self.source_name = source_name
        self.available_resources = available_resources
        self.requested_resources = requested_resources
        self.not_found_resources = requested_resources.difference(available_resources)
        msg = (
            f"The following resources could not be found in source `{source_name}`:"
            f" {self.not_found_resources}. Available resources are: {available_resources}"
        )
        super().__init__(msg)


class SourceNotAFunction(DltSourceException):
    def __init__(self, source_name: str, item: Any, _typ: Type[Any]) -> None:
        self.source_name = source_name
        self.item = item
        self.typ = _typ
        super().__init__(
            f"First parameter to the source `{source_name}` must be a function or callable but is"
            f" type `{_typ.__name__}`. Please decorate a function with @dlt.source"
        )


class SourceIsAClassTypeError(DltSourceException):
    def __init__(self, source_name: str, _typ: Type[Any]) -> None:
        self.source_name = source_name
        self.typ = _typ
        super().__init__(
            f"First parameter to the source `{source_name}` is a class `{_typ.__name__}`. Do not"
            " decorate classes with `@dlt.source`. Instead implement `.__call__()` in your class"
            " and pass instance of such class to `dlt.source()` directly"
        )


class CurrentSourceSchemaNotAvailable(DltSourceException):
    def __init__(self) -> None:
        super().__init__(
            "Current source schema is available only when called from a function decorated with"
            " dlt.source or dlt.resource"
        )


class CurrentSourceNotAvailable(DltSourceException):
    def __init__(self) -> None:
        super().__init__(
            "Current source is available only when called from a function decorated with"
            " dlt.resource or dlt.transformer during the extract step"
        )


class ExplicitSourceNameInvalid(DltSourceException):
    def __init__(self, source_name: str, schema_name: str) -> None:
        self.source_name = source_name
        self.schema_name = schema_name
        super().__init__(
            f"Your explicit `{source_name=:}` does not match explicit `{schema_name=:}`"
        )


class UnknownSourceReference(ReferenceImportError, DltSourceException, KeyError):
    def __init__(
        self, ref: str, qualified_refs: Sequence[str], traces: Sequence[ImportTrace]
    ) -> None:
        self.ref = ref
        self.qualified_refs = qualified_refs
        super().__init__(traces=traces)

    def __str__(self) -> str:
        msg = f"`{self.ref}` is not one of already registered sources. "
        if len(self.qualified_refs) == 1 and self.qualified_refs[0] == self.ref:
            pass
        else:
            msg += (
                " Following fully qualified refs were tried in the registry:\n\t%s\n"
                % "\n\t".join(self.qualified_refs)
            )
        if self.traces:
            msg += super().__str__()
        return msg


class InvalidSourceReference(DltSourceException):
    def __init__(self, ref: str) -> None:
        self.ref = ref
        msg = f"Destination reference `{ref}` has invalid format."
        super().__init__(msg)


class IncrementalUnboundError(DltResourceException):
    def __init__(self, cursor_path: str) -> None:
        super().__init__(
            "",
            f"The incremental definition with `{cursor_path=:}` is used without being bound to the"
            " resource. This most often happens when you create dynamic resource from a generator"
            " function that uses incremental. See"
            " https://dlthub.com/docs/general-usage/incremental-loading#incremental-loading-with-last-value"
            " for an example.",
        )
