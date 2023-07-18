from typing import Generic, TypeVar, Any, Optional, Callable, List, TypedDict, get_origin, Sequence
import inspect
from functools import wraps
from datetime import datetime  # noqa: I251

import dlt
from dlt.common.json import json
from dlt.common.jsonpath import compile_path, find_values, JSONPath
from dlt.common.typing import TDataItem, TDataItems, TFun, extract_inner_type, is_optional_type
from dlt.common.schema.typing import TColumnKey
from dlt.common.configuration import configspec, ConfigurationValueError
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.pipeline import resource_state
from dlt.common.utils import digest128
from dlt.extract.exceptions import IncrementalUnboundError, PipeException
from dlt.extract.pipe import Pipe
from dlt.extract.utils import resolve_column_value
from dlt.extract.typing import FilterItem, SupportsPipe, TTableHintTemplate
from dlt.common import pendulum


TCursorValue = TypeVar("TCursorValue", bound=Any)
LastValueFunc = Callable[[Sequence[TCursorValue]], Any]


class IncrementalColumnState(TypedDict):
    initial_value: Optional[Any]
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
class Incremental(FilterItem, BaseConfiguration, Generic[TCursorValue]):
    """Adds incremental extraction for a resource by storing a cursor value in persistent state.

    The cursor could for example be a timestamp for when the record was created and you can use this to load only
    new records created since the last run of the pipeline.

    To use this the resource function should have an argument either type annotated with `Incremental` or a default `Incremental` instance.
    For example:

    >>> @dlt.resource(primary_key='id')
    >>> def some_data(created_at=dlt.sources.incremental('created_at', '2023-01-01T00:00:00Z'):
    >>>    yield from request_data(created_after=created_at.last_value)

    When the resource has a `primary_key` specified this is used to deduplicate overlapping items with the same cursor value.

    Alternatively you can use this class as transform step and add it to any resource. For example:
    >>> @dlt.resource
    >>> def some_data():
    >>>     last_value = dlt.sources.incremental.from_existing_state("some_data", "item.ts")
    >>>     ...
    >>>
    >>> r = some_data().add_step(dlt.sources.incremental("item.ts", initial_value=now, primary_key="delta"))
    >>> info = p.run(r, destination="duckdb")

    Args:
        cursor_path: The name or a JSON path to an cursor field. Uses the same names of fields as in your JSON document, before they are normalized to store in the database.
        initial_value: Optional value used for `last_value` when no state is available, e.g. on the first run of the pipeline. If not provided `last_value` will be `None` on the first run.
        last_value_func: Callable used to determine which cursor value to save in state. It is called with a list of the stored state value and all cursor vals from currently processing items. Default is `max`
        primary_key: Optional primary key used to deduplicate data. If not provided, a primary key defined by the resource will be used. Pass a tuple to define a compound key. Pass empty tuple to disable unique checks
        end_value: Optional value used to load a limited range of records between `initial_value` and `end_value`.
            Use in conjunction with `initial_value`, e.g. load records from given month `incremental(initial_value="2022-01-01T00:00:00Z", end_value="2022-02-01T00:00:00Z")`
            Note, when this is set the incremental filtering is stateless and `initial_value` always supersedes any previous incremental value in state.
    """
    cursor_path: str = None
    # TODO: Support typevar here
    initial_value: Optional[Any] = None
    end_value: Optional[Any] = None

    def __init__(
            self,
            cursor_path: str = dlt.config.value,
            initial_value: Optional[TCursorValue]=None,
            last_value_func: Optional[LastValueFunc[TCursorValue]]=max,
            primary_key: Optional[TTableHintTemplate[TColumnKey]] = None,
            end_value: Optional[TCursorValue] = None
    ) -> None:
        self.cursor_path = cursor_path
        if self.cursor_path:
            self.cursor_path_p: JSONPath = compile_path(cursor_path)
        self.last_value_func = last_value_func
        self.initial_value = initial_value
        """Initial value of last_value"""
        self.end_value = end_value
        self.start_value: Any = initial_value
        """Value of last_value at the beginning of current pipeline run"""
        self.resource_name: Optional[str] = None
        self.primary_key: Optional[TTableHintTemplate[TColumnKey]] = primary_key
        self._cached_state: IncrementalColumnState = None
        """State dictionary cached on first access"""
        super().__init__(self.transform)

        self.end_out_of_range: bool = False
        """Becomes true on the first item that is out of range of `end_value`. I.e. when using `max` function this means a value that is equal or higher"""
        self.start_out_of_range: bool = False
        """Becomes true on the first item that is out of range of `initial_value`. I.e. when using `max` this is a value that is lower than `initial_value`"""

    @classmethod
    def from_existing_state(cls, resource_name: str, cursor_path: str) -> "Incremental[TCursorValue]":
        """Create Incremental instance from existing state."""
        state = Incremental._get_state(resource_name, cursor_path)
        i = cls(cursor_path, state["initial_value"])
        i.resource_name = resource_name
        return i

    def copy(self) -> "Incremental[TCursorValue]":
        return self.__class__(
            self.cursor_path, initial_value=self.initial_value, last_value_func=self.last_value_func, primary_key=self.primary_key, end_value=self.end_value
        )

    def merge(self, other: "Incremental[TCursorValue]") -> "Incremental[TCursorValue]":
        """Create a new incremental instance which merges the two instances.
        Only properties which are not `None` from `other` override the current instance properties.

        This supports use cases with partial overrides, such as:
        >>> def my_resource(updated=incremental('updated', initial_value='1970-01-01'))
        >>>     ...
        >>>
        >>> my_resource(updated=incremental(initial_value='2023-01-01', end_value='2023-02-01'))
        """
        kwargs = dict(self, last_value_func=self.last_value_func, primary_key=self.primary_key)
        for key, value in dict(other, last_value_func=other.last_value_func, primary_key=other.primary_key).items():
            if value is not None:
                kwargs[key] = value
        return self.__class__(**kwargs)

    def on_resolved(self) -> None:
        self.cursor_path_p = compile_path(self.cursor_path)
        if self.end_value is not None and self.initial_value is None:
            raise ConfigurationValueError(
                "Incremental 'end_value' was specified without 'initial_value'. 'initial_value' is required when using 'end_value'."
            )
        # Ensure end value is "higher" than initial value
        if self.end_value is not None and self.last_value_func([self.end_value, self.initial_value]) != self.end_value:
            if self.last_value_func in (min, max):
                adject = 'higher' if self.last_value_func is max else 'lower'
                msg = f"Incremental 'initial_value' ({self.initial_value}) is {adject} than 'end_value` ({self.end_value}). 'end_value' must be {adject} than 'initial_value'"
            else:
                msg = (
                    f"Incremental 'initial_value' ({self.initial_value}) is greater than 'end_value' ({self.end_value}) as determined by the custom 'last_value_func'. "
                    f"The result of '{self.last_value_func.__name__}([end_value, initial_value])' must equal 'end_value'"
                )
            raise ConfigurationValueError(msg)

    def parse_native_representation(self, native_value: Any) -> None:
        if isinstance(native_value, Incremental):
            self.cursor_path = native_value.cursor_path
            self.initial_value = native_value.initial_value
            self.last_value_func = native_value.last_value_func
            self.end_value = native_value.end_value
            self.cursor_path_p = self.cursor_path_p
            self.resource_name = self.resource_name
        else:  # TODO: Maybe check if callable(getattr(native_value, '__lt__', None))
            # Passing bare value `incremental=44` gets parsed as initial_value
            self.initial_value = native_value
        if not self.is_partial():
            self.resolve()

    def get_state(self) -> IncrementalColumnState:
        """Returns an Incremental state for a particular cursor column"""
        if not self.resource_name:
            raise IncrementalUnboundError(self.cursor_path)

        if self.end_value is not None:
            # End value uses mock state. We don't want to write it.
            return {
                'initial_value': self.initial_value,
                'last_value': self.initial_value,
                'unique_hashes': []
            }

        self._cached_state = Incremental._get_state(self.resource_name, self.cursor_path)
        if len(self._cached_state) == 0:
            # set the default like this, setdefault evaluates the default no matter if it is needed or not. and our default is heavy
            self._cached_state.update(
                {
                    "initial_value": self.initial_value,
                    "last_value": self.initial_value,
                    'unique_hashes': []
                }
            )
        return self._cached_state

    @staticmethod
    def _get_state(resource_name: str, cursor_path: str) -> IncrementalColumnState:
        state: IncrementalColumnState = resource_state(resource_name).setdefault('incremental', {}).setdefault(cursor_path, {})
        # if state params is empty
        return state

    @property
    def last_value(self) -> Optional[TCursorValue]:
        s = self.get_state()
        return s['last_value']  # type: ignore

    def unique_value(self, row: TDataItem) -> str:
        try:
            if self.primary_key:
                return digest128(json.dumps(resolve_column_value(self.primary_key, row), sort_keys=True))
            elif self.primary_key is None:
                return digest128(json.dumps(row, sort_keys=True))
            else:
                return None
        except KeyError as k_err:
            raise IncrementalPrimaryKeyMissing(self.resource_name, k_err.args[0], row)

    def transform(self, row: TDataItem) -> bool:
        if row is None:
            return True

        row_values = find_values(self.cursor_path_p, row)
        if not row_values:
            raise IncrementalCursorPathMissing(self.resource_name, self.cursor_path, row)
        row_value = row_values[0]

        # For datetime cursor, ensure the value is a timezone aware datetime.
        # The object saved in state will always be a tz aware pendulum datetime so this ensures values are comparable
        if isinstance(row_value, datetime):
            row_value = pendulum.instance(row_value)

        incremental_state = self._cached_state
        last_value = incremental_state['last_value']
        last_value_func = self.last_value_func

        # Check whether end_value has been reached
        # Filter end value ranges exclusively, so in case of "max" function we remove values >= end_value
        if self.end_value is not None and (
            last_value_func((row_value, self.end_value)) != self.end_value or last_value_func((row_value, )) == self.end_value
        ):
            self.end_out_of_range = True
            return False

        check_values = (row_value,) + ((last_value, ) if last_value is not None else ())
        new_value = last_value_func(check_values)
        if last_value == new_value:
            processed_row_value = last_value_func((row_value, ))
            # we store row id for all records with the current "last_value" in state and use it to deduplicate
            if processed_row_value == last_value:
                unique_value = self.unique_value(row)
                # if unique value exists then use it to deduplicate
                if unique_value:
                    if unique_value in incremental_state['unique_hashes']:
                        return False
                    # add new hash only if the record row id is same as current last value
                    incremental_state['unique_hashes'].append(unique_value)
                return True
            # skip the record that is not a last_value or new_value: that record was already processed
            check_values = (row_value,) + ((self.start_value,) if self.start_value is not None else ())
            new_value = last_value_func(check_values)
            # Include rows == start_value but exclude "lower"
            if new_value == self.start_value and processed_row_value != self.start_value:
                self.start_out_of_range = True
                return False
            else:
                return True
        else:
            incremental_state["last_value"] = new_value
            unique_value = self.unique_value(row)
            if unique_value:
                incremental_state["unique_hashes"] = [unique_value]

        return True

    def bind(self, pipe: SupportsPipe) -> "Incremental[TCursorValue]":
        "Called by pipe just before evaluation"
        # bind the resource/pipe name
        if self.is_partial():
            raise IncrementalCursorPathMissing(pipe.name, None, None)
        self.resource_name = pipe.name
        # set initial value from last value, in case of a new state those are equal
        self.start_value = self.last_value
        # cache state
        self._cached_state = self.get_state()
        return self

    def __str__(self) -> str:
        return f"Incremental at {id(self)} for resource {self.resource_name} with cursor path: {self.cursor_path} initial {self.initial_value} lv_func {self.last_value_func}"


class IncrementalResourceWrapper(FilterItem):
    _incremental: Optional[Incremental[Any]] = None
    """Keeps the injectable incremental"""

    def __init__(self, resource_name: str, primary_key: Optional[TTableHintTemplate[TColumnKey]] = None) -> None:
        self.resource_name = resource_name
        self.primary_key = primary_key
        self.incremental_state: IncrementalColumnState = None

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
            new_incremental: Incremental[Any] = None
            bound_args = sig.bind(*args, **kwargs)

            if p.name in bound_args.arguments:
                explicit_value = bound_args.arguments[p.name]
                if isinstance(explicit_value, Incremental):
                    # Explicit Incremental instance is merged with default
                    # allowing e.g. to only update initial_value/end_value but keeping default cursor_path
                    if isinstance(p.default, Incremental):
                        new_incremental = p.default.merge(explicit_value)
                    else:
                        new_incremental = explicit_value.copy()
                elif isinstance(p.default, Incremental):
                    # Passing only initial value explicitly updates the default instance
                    new_incremental = p.default.copy()
                    new_incremental.initial_value = explicit_value
            elif isinstance(p.default, Incremental):
                new_incremental = p.default.copy()


            if not new_incremental or new_incremental.is_partial():
                if is_optional_type(p.annotation):
                    bound_args.arguments[p.name] = None  # Remove partial spec
                    return func(*bound_args.args, **bound_args.kwargs)
                raise ValueError(f"{p.name} Incremental has no default")
            # set the incremental only if not yet set or if it was passed explicitly
            # NOTE: the _incremental may be also set by applying hints to the resource see `set_template` in `DltResource`
            if p.name in bound_args.arguments or not self._incremental:
                self._incremental = new_incremental
            self._incremental.resolve()
            # in case of transformers the bind will be called before this wrapper is set: because transformer is called for a first time late in the pipe
            self._incremental.bind(Pipe(self.resource_name))
            bound_args.arguments[p.name] = self._incremental
            return func(*bound_args.args, **bound_args.kwargs)

        return _wrap  # type: ignore

    def bind(self, pipe: SupportsPipe) -> "IncrementalResourceWrapper":
        if self._incremental:
            self._incremental.bind(pipe)
        return self

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        if not self._incremental:
            return item
        if self._incremental.primary_key is None:
            self._incremental.primary_key = self.primary_key
        return self._incremental(item, meta)
