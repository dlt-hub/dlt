from typing import Generic, TypeVar, Any, Optional, Callable, List, TypedDict, get_origin, Sequence
import inspect
from functools import wraps

from jsonpath import JSONPath

from dlt.common.json import json
from dlt.common.typing import DictStrAny, TDataItem, TDataItems, TFun, extract_inner_type, is_optional_type
from dlt.common.schema.typing import TColumnKey
from dlt.common.configuration import configspec, known_sections, resolve_configuration, ConfigFieldMissingException
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.pipeline import _resource_state, state as _state
from dlt.common.utils import digest256
from dlt.extract.utils import resolve_column_value
from dlt.extract.typing import TTableHintTemplate


TCursorValue = TypeVar("TCursorValue", bound=Any)
LastValueFunc = Callable[[Sequence[TCursorValue]], Any]


class IncrementalColumnState(TypedDict):
    last_value: Optional[Any]
    unique_hashes: List[str]


@configspec
class IncrementalConfigSpec(BaseConfiguration):
    cursor_column: str = None
    initial_value: Optional[Any] = None

    def parse_native_representation(self, native_value: Any) -> None:
        if isinstance(native_value, Incremental):
            self.cursor_column = native_value.cursor_column
            self.initial_value = native_value.initial_value
        else:  # TODO: Maybe check if callable(getattr(native_value, '__lt__', None))
            # Passing bare value `incremental=44` gets parsed as initial_value
            self.initial_value = native_value


class Incremental(Generic[TCursorValue]):
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
        cursor_column: The name of the cursor column. This can be a column name or any valid JSON path.
        initial_value: Optional value used for `last_value` when no state is available, e.g. on the first run of the pipeline. If not provided `last_value` will be `None` on the first run.
        last_value_func: Callable used to determine which cursor value to save in state. It is called with a list of the stored state value and all cursor vals from currently processing items. Default is `max`
    """
    resource_name: str = None

    def __init__(
            self,
            cursor_column: str,
            initial_value: Optional[TCursorValue]=None,
            last_value_func: Optional[LastValueFunc[TCursorValue]]=None,
    ) -> None:
        assert cursor_column, "`cursor_column` must be a column name or json path"
        self.cursor_column = cursor_column
        self.cursor_column_p = JSONPath(cursor_column)
        self.last_value_func = last_value_func or max
        self.initial_value =  initial_value

    def copy(self) -> "Incremental[TCursorValue]":
        return self.__class__(self.cursor_column, initial_value=self.initial_value, last_value_func=self.last_value_func)

    @classmethod
    def from_config(cls, cfg: IncrementalConfigSpec, orig: Optional["Incremental[Any]"]) -> "Incremental[Any]":
        # TODO: last_value_func from name
        kwargs = dict(cursor_column=cfg.cursor_column, initial_value=cfg.initial_value)
        if orig:
            kwargs['last_value_func'] = orig.last_value_func
        return cls(**kwargs)

    def get_state(self, resource_state: DictStrAny) -> IncrementalColumnState:
        """Given resource state, returns a state fragment for particular cursor column"""
        state_params: IncrementalColumnState = resource_state.setdefault('incremental', {}).setdefault(self.cursor_column, {})
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


class IncrementalResourceWrapper:
    _incremental: Optional[Incremental[Any]] = None
    """Keeps the injectable incremental"""

    def __init__(self, resource_name: str, source_section: str, primary_key: Optional[TTableHintTemplate[TColumnKey]] = None) -> None:
        self.resource_sections = (known_sections.SOURCES, source_section, resource_name)
        self.resource_name = resource_name
        self.primary_key = primary_key

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

    def wrap(self, func: TFun) -> TFun:
        """Wrap the callable to inject an `Incremental` object configured for the resource.
        """
        sig = inspect.signature(func)
        incremental_param = self.get_incremental_arg(sig)
        assert incremental_param, "Please use `should_wrap` to decide if to call this function"

        @wraps(func)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            p = incremental_param
            assert p is not None
            default_incremental: Optional[Incremental[Any]] = None
            new_incremental: Optional[Incremental[Any]] = None
            new_kwargs = {}
            if isinstance(p.default, Incremental):
                default_incremental = p.default.copy()
                default_incremental.resource_name = self.resource_name
            if p.name in kwargs:
                explicit_value = kwargs[p.name]
                if isinstance(explicit_value, Incremental):
                    # Explicit Incremental instance is  untouched
                    explicit_value.resource_name = self.resource_name
                    new_incremental = explicit_value
                elif default_incremental:
                    # Passing only initial value explicitly updates the default instance
                    default_incremental.initial_value = explicit_value
                    new_kwargs[p.name] = default_incremental
                    new_incremental = default_incremental
            if not new_incremental:
                try:
                    cfg = resolve_configuration(
                        IncrementalConfigSpec(), sections=self.resource_sections + (p.name, ), explicit_value=default_incremental
                    )
                except ConfigFieldMissingException:
                    if not is_optional_type(p.annotation):
                        raise
                else:
                    new_incremental = new_kwargs[p.name] = Incremental.from_config(cfg, default_incremental)
                    new_incremental.resource_name = self.resource_name
            self._incremental = new_incremental
            kwargs.update(new_kwargs)
            return func(*args, **kwargs)

        return _wrap  # type: ignore

    def transform(self, state: IncrementalColumnState, row: TDataItem, primary_key: Optional[TTableHintTemplate[TColumnKey]]) -> bool:
        if row is None:
            return True

        last_value = state['last_value']
        row_value = json.loads(
            json.dumps(self._incremental.cursor_column_p.parse(row)[0])
        )  # For now the value needs to match deserialized presentation from state
        check_values = ([last_value] if last_value is not None else []) + [row_value]
        new_value = self._incremental.last_value_func(check_values)
        if primary_key:
            unique_value = digest256(json.dumps(resolve_column_value(primary_key, row), sort_keys=True))
        else:
            unique_value = digest256(json.dumps(row, sort_keys=True))
        if last_value == new_value:
            if unique_value in state['unique_hashes']:
                return False
            state['unique_hashes'].append(unique_value)
            return True
        if new_value != last_value:
            state.update({'last_value': new_value, 'unique_hashes': [unique_value]})
        return True

    def __call__(self, items: TDataItems, meta: Any=None) -> TDataItems:
        # get the state only once for the whole list
        # TODO: cache the state, this can be done if we are sure that each processing pipe has a separate instance of this class. which is hard to do now.
        incremental_state = self._incremental.get_state(_resource_state(self.resource_name))
        # TODO: reuse FilterItem from typing.py does exactly the same
        if isinstance(items, list):
            filtered = [item for item in items if self.transform(incremental_state, item, self.primary_key) is True]
            if not filtered:
                return None
            else:
                return filtered

        return items if self.transform(incremental_state, items, self.primary_key) is True else None
