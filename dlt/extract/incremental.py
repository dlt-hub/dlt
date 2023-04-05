from hashlib import sha1
from typing import Generic, TypeVar, Any, Optional, Callable, List, TypedDict, get_origin
import inspect
from functools import wraps

from jsonpath import JSONPath

from dlt.common.json import json
from dlt.common.typing import TDataItem, TDataItems, TFun, extract_inner_type, is_optional_type
from dlt.common.schema.typing import TColumnKey
from dlt.common.configuration import configspec, known_sections, resolve_configuration, ConfigFieldMissingException
from dlt.common.configuration.specs import BaseConfiguration
from dlt.extract.utils import resolve_column_value
from dlt.extract.typing import TTableHintTemplate


TCursorValue = TypeVar("TCursorValue", bound=Any)
TUniqueValue = TypeVar("TUniqueValue", bound=Any)
LastValueFunc = Callable[..., Any]



class IncrementalColumnState(TypedDict):
    last_value: Optional[Any]
    unique_keys: List[Any]


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
        # else:
        #     raise NativeValueError(self.__class__, native_value, 'Invalid')


class Incremental(Generic[TCursorValue, TUniqueValue]):
    def __init__(
            self,
            cursor_column: str,
            initial_value: Optional[TCursorValue]=None,
            last_value_func: Optional[LastValueFunc]=None,
    ) -> None:
        assert cursor_column, "`cursor_column` must be a column name or json path"
        self.cursor_column = cursor_column
        self.cursor_column_p = JSONPath(cursor_column)
        self.initial_value = initial_value
        self.last_value_func = last_value_func or max
        from dlt.pipeline.current import resource_state  # TODO: Resolve circular import
        self._state = resource_state

    def copy(self) -> "Incremental[TCursorValue, TUniqueValue]":
        return self.__class__(self.cursor_column, initial_value=self.initial_value, last_value_func=self.last_value_func)

    @classmethod
    def from_config(cls, cfg: IncrementalConfigSpec) -> "Incremental[Any, Any]":
        # TODO: last_value_func from name
        return cls(cfg.cursor_column, initial_value=cfg.initial_value)

    @property
    def state(self) -> IncrementalColumnState:
        return self._state().setdefault('incremental', {}).setdefault(self.cursor_column, {'last_value': json.loads(json.dumps(self.initial_value)), 'unique_keys': []}) # type: ignore

    @property
    def last_value(self) -> Optional[TCursorValue]:
        return self.state['last_value']  # type: ignore

    @property
    def unique_keys(self) -> List[TUniqueValue]:
        return self.state['unique_keys']

    def __call__(self, row: TDataItem, primary_key: Optional[TTableHintTemplate[TColumnKey]]) -> bool:
        if row is None:
            return True
        state = self.state
        last_value = state['last_value']
        row_value = json.loads(json.dumps(self.cursor_column_p.parse(row)[0]))  # For now the value needs to match deserialized presentation from state
        if primary_key:
            unique_value = resolve_column_value(primary_key, row)
        else:
            # TODO: Need to think about edge cases.
            # e.g. if schema has changed since last run, row may have new columns
            # do we then still want to load a duplicate?
            unique_value = sha1(json.dumpb(row, sort_keys=True)).hexdigest()
        if last_value == row_value:
            if unique_value in state['unique_keys']:
                return False
            state['unique_keys'].append(unique_value)
            return True
        new_value = self.last_value_func(last_value, row_value) if last_value is not None else row_value
        if new_value != last_value:
            state.update({'last_value': new_value, 'unique_keys': [unique_value]})
        return True


incremental = Incremental


class IncrementalResourceWrapper:
    _transform: Optional[Incremental[Any, Any]] = None

    def __init__(self, resource_name: str, source_section: str, primary_key: Optional[TTableHintTemplate[TColumnKey]] = None) -> None:
        self.resource_sections = (known_sections.SOURCES, source_section, resource_name)
        self.primary_key = primary_key

    def wrap(self, func: TFun) -> TFun:
        """Wrap the callable to inject an `Incremental` object configured for the resource.
        """
        # TODO: Optimize: should look for annotations/defaults outside wrapper and skip adding wrapper/transform step if none
        @wraps(func)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            sig = inspect.signature(func)
            new_kwargs = {}

            for p in sig.parameters.values():
                annotation = extract_inner_type(p.annotation)
                annotation = get_origin(annotation) or annotation
                default_incremental: Optional[Incremental[Any, Any]] = None
                if (inspect.isclass(annotation) and issubclass(annotation, Incremental)) or isinstance(p.default, Incremental):
                    if isinstance(p.default, Incremental):
                        default_incremental = p.default.copy()
                    if p.name in kwargs:
                        explicit_value = kwargs[p.name]
                        if isinstance(explicit_value, Incremental):
                            # Explicit Incremental instance is  untouched
                            self._transform = explicit_value
                            break
                        elif default_incremental:
                            # Passing only initial value explicitly updates the default instance
                            default_incremental.initial_value = explicit_value
                            new_kwargs[p.name] = default_incremental
                            self._transform = default_incremental
                            break
                    try:
                        cfg = resolve_configuration(
                            IncrementalConfigSpec(), sections=self.resource_sections + (p.name, ), explicit_value=default_incremental
                        )
                    except ConfigFieldMissingException:
                        if not is_optional_type(p.annotation):
                            raise
                    else:
                        self._transform = new_kwargs[p.name] = Incremental.from_config(cfg)
                        break
            kwargs.update(new_kwargs)
            return func(*args, **kwargs)
        return _wrap  # type: ignore

    def __call__(self, items: TDataItems, meta: Any=None) -> TDataItems:
        if not self._transform:
            return items
        if isinstance(items, list): # TODO: Handle lists efficiently
            return [item for item in items if self._transform(item, self.primary_key) is True]
        return items if self._transform(items, self.primary_key) is True else None
