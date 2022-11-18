from inspect import Signature
from typing import Any, Set, Type
from dlt.common.exceptions import DltException


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
        msg = f"In processing pipe {pipe_name}: " + msg
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
            msg = f"A pipe created from transformer {pipe_name} is unbound or its parent is unbound or empty. Provide a resource in `data_from` argument or bind resources with | operator."
        else:
            msg = "Pipe is empty and does not have a resource at its head"
        super().__init__(pipe_name, msg)


class InvalidStepFunctionArguments(PipeException):
    def __init__(self, pipe_name: str, func_name: str, sig: Signature, call_error: str) -> None:
        self.func_name = func_name
        self.sig = sig
        super().__init__(pipe_name, f"Unable to call {func_name}: {call_error}. The mapping/filtering function {func_name} requires first argument to take data item and optional second argument named 'meta', but the signature is {sig}")


class ResourceNameMissing(DltResourceException):
    def __init__(self) -> None:
        super().__init__(None, """Resource name is missing. If you create a resource directly from data ie. from a list you must pass the name explicitly in `name` argument.
        Please note that for resources created from functions or generators, the name is the function name by default.""")


# class DependentResourceIsNotCallable(DltResourceException):
#     def __init__(self, resource_name: str) -> None:
#         super().__init__(resource_name, f"Attempted to call the dependent resource {resource_name}. Do not call the dependent resources. They will be called only when iterated.")


class ResourceNotFoundError(DltResourceException, KeyError):
      def __init__(self, resource_name: str, context: str) -> None:
          self.resource_name = resource_name
          super().__init__(resource_name, f"Resource with a name {resource_name} could not be found. {context}")


class InvalidResourceDataType(DltResourceException):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any], msg: str) -> None:
        self.item = item
        self._typ = _typ
        super().__init__(resource_name, f"Cannot create resource {resource_name} from specified data. " + msg)


class InvalidResourceDataTypeAsync(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, "Async iterators and generators are not valid resources. Please use standard iterators and generators that yield Awaitables instead (for example by yielding from async function without await")


class InvalidResourceDataTypeBasic(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, f"Resources cannot be strings or dictionaries but {_typ.__name__} was provided. Please pass your data in a list or as a function yielding items. If you want to process just one data item, enclose it in a list.")


class InvalidResourceDataTypeFunctionNotAGenerator(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, "Please make sure that function decorated with @dlt.resource uses 'yield' to return the data.")


class InvalidResourceDataTypeMultiplePipes(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, "Resources with multiple parallel data pipes are not yet supported. This problem most often happens when you are creating a source with @dlt.source decorator that has several resources with the same name.")


class InvalidTransformerDataTypeGeneratorFunctionRequired(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ,
            "Transformer must be a function decorated with @dlt.transformer that takes data item as its first argument. Only first argument may be 'positional only'.")


class InvalidTransformerGeneratorFunction(DltResourceException):
    def __init__(self, resource_name: str, func_name: str, sig: Signature, code: int) -> None:
        self.func_name = func_name
        self.sig = sig
        self.code = code
        msg = f"Transformer function {func_name} must take data item as its first argument. "
        if code == 1:
            msg += "The actual function does not take any arguments."
        elif code == 2:
            msg += f"Only the first argument may be 'positional only', actual signature is {sig}"
        elif code == 3:
            msg += f"The first argument cannot be keyword only, actual signature is {sig}"

        super().__init__(resource_name, msg)


class InvalidResourceDataTypeIsNone(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, "Resource data missing. Did you forget the return statement in @dlt.resource decorated function?")


class ResourceFunctionExpected(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any, _typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, f"Expected function or callable as first parameter to resource {resource_name} but {_typ.__name__} found. Please decorate a function with @dlt.resource")


class InvalidParentResourceDataType(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, f"A parent resource of {resource_name} is of type {_typ.__name__}. Did you forget to use '@dlt.resource` decorator or `resource` function?")


class InvalidParentResourceIsAFunction(DltResourceException):
    def __init__(self, resource_name: str, func_name: str) -> None:
        self.func_name = func_name
        super().__init__(resource_name, f"A data source {func_name} of a transformer {resource_name} is an undecorated function. Please decorate it with '@dlt.resource' or pass to 'resource' function.")


class DeletingResourcesNotSupported(DltResourceException):
    def __init__(self, source_name: str, resource_name: str) -> None:
        super().__init__(resource_name, f"Resource cannot be removed the the source {source_name}")


class ParametrizedResourceUnbound(DltResourceException):
    def __init__(self, resource_name: str, func_name: str, sig: Signature, kind: str = "resource") -> None:
        self.func_name = func_name
        self.sig = sig
        super().__init__(resource_name, f"The {kind} {resource_name} is parametrized and expects following arguments: {sig}. Did you forget to call the {func_name} function? For example from `source.{resource_name}(...)")


class TableNameMissing(DltSourceException):
    def __init__(self) -> None:
        super().__init__("""Table name is missing in table template. Please provide a string or a function that takes a data item as an argument""")


class InconsistentTableTemplate(DltSourceException):
    def __init__(self, reason: str) -> None:
        msg = f"A set of table hints provided to the resource is inconsistent: {reason}"
        super().__init__(msg)


class DataItemRequiredForDynamicTableHints(DltResourceException):
    def __init__(self, resource_name: str) -> None:
        super().__init__(resource_name, f"""An instance of resource's data required to generate table schema in resource {resource_name}.
        One of table hints for that resource (typically table name) is a function and hint is computed separately for each instance of data extracted from that resource.""")


class SourceDataIsNone(DltSourceException):
    def __init__(self, source_name: str) -> None:
        self.source_name = source_name
        super().__init__(f"No data returned or yielded from source function {source_name}. Did you forget the return statement?")


class SourceExhausted(DltSourceException):
    def __init__(self, source_name: str) -> None:
        self.source_name = source_name
        super().__init__(f"Source {source_name} is exhausted or has active iterator. You can iterate or pass the source to dlt pipeline only once.")


class ResourcesNotFoundError(DltSourceException):
    def __init__(self, source_name: str, available_resources: Set[str], requested_resources: Set[str]) -> None:
        self.source_name = source_name
        self.available_resources = available_resources
        self.requested_resources = requested_resources
        self.not_found_resources = requested_resources.difference(available_resources)
        msg = f"The following resources could not be found in source {source_name}: {self.not_found_resources}. Available resources are: {available_resources}"
        super().__init__(msg)


class SourceNotAFunction(DltSourceException):
    def __init__(self, source_name: str, item: Any, _typ: Type[Any]) -> None:
        self.source_name = source_name
        self.item = item
        self.typ = _typ
        super().__init__(f"First parameter to the source {source_name} must be a function or callable but is {_typ.__name__}. Please decorate a function with @dlt.source")


class SourceIsAClassTypeError(DltSourceException):
    def __init__(self, source_name: str,  _typ: Type[Any]) -> None:
        self.source_name = source_name
        self.typ = _typ
        super().__init__(f"First parameter to the source {source_name} is a class {_typ.__name__}. Do not decorate classes with @dlt.source. Instead implement __call__ in your class and pass instance of such class to dlt.source() directly")
