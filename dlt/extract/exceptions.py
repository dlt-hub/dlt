from typing import Any, Type
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
    pass


class CreatePipeException(PipeException):
    pass


class PipeItemProcessingError(PipeException):
    pass


# class InvalidIteratorException(PipelineException):
#     def __init__(self, iterator: Any) -> None:
#         super().__init__(f"Unsupported source iterator or iterable type: {type(iterator).__name__}")


# class InvalidItemException(PipelineException):
#     def __init__(self, item: Any) -> None:
#         super().__init__(f"Source yielded unsupported item type: {type(item).__name}. Only dictionaries, sequences and deferred items allowed.")


class ResourceNameMissing(DltResourceException):
    def __init__(self) -> None:
        super().__init__(None, """Resource name is missing. If you create a resource directly from data ie. from a list you must pass the name explicitly in `name` argument.
        Please note that for resources created from functions or generators, the name is the function name by default.""")


class DependentResourceIsNotCallable(DltResourceException):
    def __init__(self, resource_name: str) -> None:
        super().__init__(resource_name, f"Attempted to call the dependent resource {resource_name}. Do not call the dependent resources. They will be called only when iterated.")


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
        super().__init__(resource_name, item, _typ, "Please make sure that function decorated with @resource uses 'yield' to return the data.")


class InvalidResourceDataTypeMultiplePipes(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, "Resources with multiple parallel data pipes are not yet supported. This problem most often happens when you are creating a source with @source decorator that has several resources with the same name.")


class InvalidDependentResourceDataTypeGeneratorFunctionRequired(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, "Dependent resource must be a decorated function that takes data item as its only argument.")


class InvalidParentResourceDataType(InvalidResourceDataType):
    def __init__(self, resource_name: str, item: Any,_typ: Type[Any]) -> None:
        super().__init__(resource_name, item, _typ, f"A parent resource of {resource_name} is of type {_typ.__name__}. Did you forget to use '@resource` decorator or `resource` function?")


class InvalidParentResourceIsAFunction(DltResourceException):
    def __init__(self, resource_name: str, func_name: str) -> None:
        self.func_name = func_name
        super().__init__(resource_name, f"A parent resource {func_name} of dependent resource {resource_name} is a function. Please decorate it with '@resource' or pass to 'resource' function.")


class TableNameMissing(DltSourceException):
    def __init__(self) -> None:
        super().__init__("""Table name is missing in table template. Please provide a string or a function that takes a data item as an argument""")


class InconsistentTableTemplate(DltSourceException):
    def __init__(self, reason: str) -> None:
        msg = f"A set of table hints provided to the resource is inconsistent: {reason}"
        super().__init__(msg)


class DataItemRequiredForDynamicTableHints(DltSourceException):
    def __init__(self, resource_name: str) -> None:
        self.resource_name = resource_name
        super().__init__(f"""An instance of resource's data required to generate table schema in resource {resource_name}.
        One of table hints for that resource (typically table name) is a function and hint is computed separately for each instance of data extracted from that resource.""")
