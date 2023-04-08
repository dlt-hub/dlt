from typing import Generic, TypeVar, Any, Optional, Callable, List, TypedDict, get_origin, Sequence
import inspect
from functools import wraps

from jsonpath import JSONPath

import dlt
from dlt.common.json import json
from dlt.common.typing import DictStrAny, TDataItem, TDataItems, TFun, extract_inner_type, is_optional_type
from dlt.common.schema.typing import TColumnKey
from dlt.common.configuration import configspec, known_sections, resolve_configuration, ConfigFieldMissingException
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.pipeline import _resource_state
from dlt.common.utils import digest128
from dlt.extract.exceptions import PipeException
from dlt.extract.utils import resolve_column_value
from dlt.extract.typing import FilterItem, TTableHintTemplate


TCursorValue = TypeVar("TCursorValue", bound=Any)
LastValueFunc = Callable[[Sequence[TCursorValue]], Any]


class IncrementalColumnState(TypedDict):
    last_value: Optional[Any]
    unique_hashes: List[str]


class IncrementalCursorPathMissing(PipeException):
    def __init__(self, pipe_name: str, json_path: str, item: TDataItem) -> None:
        self.json_path = json_path
        self.item = item
        msg = f"Cursor element with JSON path {json_path} was not found in extracted data item. All data items must contain this path. Use the same names of fields as in your JSON document - if those are different from the names you see in database."
        super().__init__(pipe_name, msg)


class IncrementalPrimaryKeyMissing(PipeException):
    def __init__(self, pipe_name: str, primary_key_column: str, item: TDataItem) -> None:
        self.primary_key_column = primary_key_column
        self.item = item
        msg = f"Primary key column {primary_key_column} was not found in extracted data item. All data items must contain this column. Use the same names of fields as in your JSON document."
        super().__init__(pipe_name, msg)


@configspec
class Incremental(BaseConfiguration, Generic[TCursorValue]):
    """Adds incremental extraction for a resource by storing a cursor value in persistent state.

    The cursor could for example be a timestamp for when the record was created and you can use this to load only
    new records created since the last run of the pipeline.

    To use this the resource function should have an argument either type annotated with `Incremental` or a default `Incremental` instance.
    For example:

    >>> @dlt.resource(primary_key='id')
    >>> def some_data(created_at=dlt.sources.incremental('created_at', '2023-01-01T00:00:00Z'):
    >>>    yield from request_data(created_after=created_at.last_value)

    When the resource has a `primary_key` specified this is used to deduplicate overlapping items with the same cursor value.

    Args:
        cursor_path: The name or a JSON path to an cursor field. Uses the same names of fields as in your JSON document, before they are normalized to store in the database.
        initial_value: Optional value used for `last_value` when no state is available, e.g. on the first run of the pipeline. If not provided `last_value` will be `None` on the first run.
        last_value_func: Callable used to determine which cursor value to save in state. It is called with a list of the stored state value and all cursor vals from currently processing items. Default is `max`
    """
    cursor_path: str = None
    initial_value: Optional[Any] = None

    def __init__(
            self,
            cursor_path: str = dlt.config.value,
            initial_value: Optional[TCursorValue]=None,
            last_value_func: Optional[LastValueFunc[TCursorValue]]=max,
    ) -> None:
        self.cursor_path = cursor_path
        if self.cursor_path:
            self.cursor_path_p = JSONPath(cursor_path)
        self.last_value_func = last_value_func
        self.initial_value = initial_value
        self.resource_name: Optional[str] = None

    def copy(self) -> "Incremental[TCursorValue]":
        return self.__class__(self.cursor_path, initial_value=self.initial_value, last_value_func=self.last_value_func)

    def on_resolved(self) -> None:
        self.cursor_path_p = JSONPath(self.cursor_path)

    def parse_native_representation(self, native_value: Any) -> None:
        if isinstance(native_value, Incremental):
            self.cursor_path = native_value.cursor_path
            self.initial_value = native_value.initial_value
            self.last_value_func = native_value.last_value_func
            self.cursor_path_p = self.cursor_path_p
            self.resource_name = self.resource_name
        else:  # TODO: Maybe check if callable(getattr(native_value, '__lt__', None))
            # Passing bare value `incremental=44` gets parsed as initial_value
            self.initial_value = native_value
        self.__is_resolved__ = not self.is_partial()


    def get_state(self, resource_state: DictStrAny) -> IncrementalColumnState:
        """Given resource state, returns a state fragment for particular cursor column"""
        state_params: IncrementalColumnState = resource_state.setdefault('incremental', {}).setdefault(self.cursor_path, {})
        # if state params is empty
        if len(state_params) == 0:
            # set the default like this, setdefault evaluates the default no matter if it is needed or not. and our default is heavy
            state_params.update(
                {
                    "last_value": json.loads(json.dumps(self.initial_value)),
                    'unique_hashes': []
                }
            )
        return state_params

    @property
    def last_value(self) -> Optional[TCursorValue]:
        s = self.get_state(_resource_state(self.resource_name))
        return s['last_value']  # type: ignore

    def __str__(self) -> str:
        return f"Incremental at {id(self)} for resource {self.resource_name} with cursor path: {self.cursor_path} initial {self.initial_value} lv_func {self.last_value_func}"


class IncrementalResourceWrapper(FilterItem):
    _incremental: Optional[Incremental[Any]] = None
    """Keeps the injectable incremental"""

    def __init__(self, resource_name: str, primary_key: Optional[TTableHintTemplate[TColumnKey]] = None) -> None:
        self.resource_name = resource_name
        self.primary_key = primary_key
        self.incremental_state: IncrementalColumnState = None
        self.state_initial_value: Any = None
        self._f = self.transform  # type: ignore

    @staticmethod
    def should_wrap(sig: inspect.Signature) -> bool:
        return IncrementalResourceWrapper.get_incremental_arg(sig) is not None

    @staticmethod
    def get_incremental_arg(sig: inspect.Signature) -> Optional[inspect.Parameter]:
        incremental_param: Optional[inspect.Parameter] = None
        for p in sig.parameters.values():
            annotation = extract_inner_type(p.annotation)
            annotation = get_origin(annotation) or annotation
            if (inspect.isclass(annotation) and issubclass(annotation, Incremental)) or isinstance(p.default, Incremental):
                incremental_param = p
                break
        return incremental_param

    def wrap(self, sig: inspect.Signature, func: TFun) -> TFun:
        """Wrap the callable to inject an `Incremental` object configured for the resource.
        """
        incremental_param = self.get_incremental_arg(sig)
        assert incremental_param, "Please use `should_wrap` to decide if to call this function"

        @wraps(func)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            p = incremental_param
            assert p is not None
            default_incremental: Incremental[Any] = None
            new_incremental: Incremental[Any] = None

            bound_args = sig.bind(*args, **kwargs)
            if isinstance(p.default, Incremental):
                default_incremental = p.default.copy()
                default_incremental.resource_name = self.resource_name

            if p.name in bound_args.arguments:
                explicit_value = bound_args.arguments[p.name]
                if isinstance(explicit_value, Incremental):
                    # Explicit Incremental instance is  untouched
                    explicit_value.resource_name = self.resource_name
                    new_incremental = explicit_value
                elif default_incremental:
                    # Passing only initial value explicitly updates the default instance
                    default_incremental.initial_value = explicit_value
                    bound_args.arguments[p.name] = default_incremental
                    new_incremental = default_incremental

            if not new_incremental:
                new_incremental = bound_args.arguments[p.name] = default_incremental
            self._incremental = new_incremental
            return func(*bound_args.args, **bound_args.kwargs)

        return _wrap  # type: ignore

    def unique_value(self, row: TDataItem) -> str:
        try:
            if self.primary_key:
                return digest128(json.dumps(resolve_column_value(self.primary_key, row), sort_keys=True))
            else:
                return digest128(json.dumps(row, sort_keys=True))
        except KeyError as k_err:
            raise IncrementalPrimaryKeyMissing(self.resource_name, k_err.args[0], row)

    def transform(self, row: TDataItem) -> bool:
        if row is None:
            return True

        row_values = self._incremental.cursor_path_p.parse(row)
        if len(row_values) == 0:
            raise IncrementalCursorPathMissing(self.resource_name, self._incremental.cursor_path, row)

        last_value = self.incremental_state['last_value']
        row_value = json.loads(json.dumps(row_values[0]))  # For now the value needs to match deserialized presentation from state
        check_values = ([last_value] if last_value is not None else []) + [row_value]
        new_value = self._incremental.last_value_func(check_values)
        if last_value == new_value:
            # we store row id for all records with the current "last_value" in state and use it to deduplicate
            if self._incremental.last_value_func([row_value]) == last_value:
                unique_value = self.unique_value(row)
                if unique_value in self.incremental_state['unique_hashes']:
                    return False
                # add new hash only if the record row id is same as current last value
                self.incremental_state['unique_hashes'].append(unique_value)
                return True
            # skip the record that is not a last_value or new_value: that record was already processed
            check_values = ([self.state_initial_value] if self.state_initial_value is not None else []) + [row_value]
            new_value = self._incremental.last_value_func(check_values)
            if new_value == self.state_initial_value:
                return False
            else:
                return True
        if new_value != last_value:
            unique_value = self.unique_value(row)
            self.incremental_state.update({'last_value': new_value, 'unique_hashes': [unique_value]})
        return True

    def bind(self) -> "IncrementalResourceWrapper":
        # get the state only once for the whole list
        # TODO: cache the state, this can be done if we are sure that each processing pipe has a separate instance of this class. which is hard to do now.
        self.incremental_state = None
        self.state_initial_value = None
        return self

    def __call__(self, items: TDataItems, meta: Any=None) -> TDataItems:
        if not self.incremental_state:
            self.incremental_state = self._incremental.get_state(_resource_state(self.resource_name))
            self.state_initial_value = self.incremental_state['last_value']
        return super().__call__(items, meta)
