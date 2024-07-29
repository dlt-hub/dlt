import os
from datetime import datetime  # noqa: I251
from typing import Generic, ClassVar, Any, Optional, Type, Dict
from typing_extensions import get_origin, get_args

import inspect
from functools import wraps

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.pendulum import pendulum
from dlt.common.jsonpath import compile_path
from dlt.common.typing import (
    TDataItem,
    TDataItems,
    TFun,
    TSortOrder,
    extract_inner_type,
    get_generic_type_argument_from_instance,
    is_optional_type,
    is_subclass,
)
from dlt.common.schema.typing import TColumnNames
from dlt.common.configuration import configspec, ConfigurationValueError
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.pipeline import resource_state
from dlt.common.data_types.type_helpers import (
    coerce_from_date_types,
    coerce_value,
    py_type_to_sc_type,
)

from dlt.extract.exceptions import IncrementalUnboundError
from dlt.extract.incremental.exceptions import (
    IncrementalCursorPathMissing,
    IncrementalPrimaryKeyMissing,
)
from dlt.extract.incremental.typing import IncrementalColumnState, TCursorValue, LastValueFunc
from dlt.extract.pipe import Pipe
from dlt.extract.items import SupportsPipe, TTableHintTemplate, ItemTransform
from dlt.extract.incremental.transform import (
    JsonIncremental,
    ArrowIncremental,
    IncrementalTransform,
)

try:
    from dlt.common.libs.pyarrow import is_arrow_item
except MissingDependencyException:
    is_arrow_item = lambda item: False

try:
    from dlt.common.libs.pandas import pandas
except MissingDependencyException:
    pandas = None


@configspec
class Incremental(ItemTransform[TDataItem], BaseConfiguration, Generic[TCursorValue]):
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
        row_order: Declares that data source returns rows in descending (desc) or ascending (asc) order as defined by `last_value_func`. If row order is know, Incremental class
                    is able to stop requesting new rows by closing pipe generator. This prevents getting more data from the source. Defaults to None, which means that
                    row order is not known.
        allow_external_schedulers: If set to True, allows dlt to look for external schedulers from which it will take "initial_value" and "end_value" resulting in loading only
            specified range of data. Currently Airflow scheduler is detected: "data_interval_start" and "data_interval_end" are taken from the context and passed Incremental class.
            The values passed explicitly to Incremental will be ignored.
            Note that if logical "end date" is present then also "end_value" will be set which means that resource state is not used and exactly this range of date will be loaded
    """

    # this is config/dataclass so declare members
    cursor_path: str = None
    # TODO: Support typevar here
    initial_value: Optional[Any] = None
    end_value: Optional[Any] = None
    row_order: Optional[TSortOrder] = None
    allow_external_schedulers: bool = False

    # incremental acting as empty
    EMPTY: ClassVar["Incremental[Any]"] = None
    placement_affinity: ClassVar[float] = 1  # stick to end

    def __init__(
        self,
        cursor_path: str = None,
        initial_value: Optional[TCursorValue] = None,
        last_value_func: Optional[LastValueFunc[TCursorValue]] = max,
        primary_key: Optional[TTableHintTemplate[TColumnNames]] = None,
        end_value: Optional[TCursorValue] = None,
        row_order: Optional[TSortOrder] = None,
        allow_external_schedulers: bool = False,
    ) -> None:
        # make sure that path is valid
        if cursor_path:
            compile_path(cursor_path)
        self.cursor_path = cursor_path
        self.last_value_func = last_value_func
        self.initial_value = initial_value
        """Initial value of last_value"""
        self.end_value = end_value
        self.start_value: Any = initial_value
        """Value of last_value at the beginning of current pipeline run"""
        self.resource_name: Optional[str] = None
        self._primary_key: Optional[TTableHintTemplate[TColumnNames]] = primary_key
        self.row_order = row_order
        self.allow_external_schedulers = allow_external_schedulers

        self._cached_state: IncrementalColumnState = None
        """State dictionary cached on first access"""
        super().__init__(lambda x: x)  # TODO:

        self.end_out_of_range: bool = False
        """Becomes true on the first item that is out of range of `end_value`. I.e. when using `max` function this means a value that is equal or higher"""
        self.start_out_of_range: bool = False
        """Becomes true on the first item that is out of range of `start_value`. I.e. when using `max` this is a value that is lower than `start_value`"""

        self._transformers: Dict[str, IncrementalTransform] = {}
        self._bound_pipe: SupportsPipe = None
        """Bound pipe"""

    @property
    def primary_key(self) -> Optional[TTableHintTemplate[TColumnNames]]:
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value: str) -> None:
        # set key in incremental and data type transformers
        self._primary_key = value
        if self._transformers:
            for transform in self._transformers.values():
                transform.primary_key = value

    def _make_transforms(self) -> None:
        types = [("arrow", ArrowIncremental), ("json", JsonIncremental)]
        for dt, kls in types:
            self._transformers[dt] = kls(
                self.resource_name,
                self.cursor_path,
                self.initial_value,
                self.start_value,
                self.end_value,
                self.last_value_func,
                self._primary_key,
                set(self._cached_state["unique_hashes"]),
            )

    @classmethod
    def from_existing_state(
        cls, resource_name: str, cursor_path: str
    ) -> "Incremental[TCursorValue]":
        """Create Incremental instance from existing state."""
        state = Incremental._get_state(resource_name, cursor_path)
        i = cls(cursor_path, state["initial_value"])
        i.resource_name = resource_name
        return i

    def merge(self, other: "Incremental[TCursorValue]") -> "Incremental[TCursorValue]":
        """Create a new incremental instance which merges the two instances.
        Only properties which are not `None` from `other` override the current instance properties.

        This supports use cases with partial overrides, such as:
        >>> def my_resource(updated=incremental('updated', initial_value='1970-01-01'))
        >>>     ...
        >>>
        >>> my_resource(updated=incremental(initial_value='2023-01-01', end_value='2023-02-01'))
        """
        # func, resource name and primary key are not part of the dict
        kwargs = dict(self, last_value_func=self.last_value_func, primary_key=self._primary_key)
        for key, value in dict(
            other, last_value_func=other.last_value_func, primary_key=other.primary_key
        ).items():
            if value is not None:
                kwargs[key] = value
        # preserve Generic param information
        if hasattr(self, "__orig_class__"):
            constructor = self.__orig_class__
        else:
            constructor = (
                other.__orig_class__ if hasattr(other, "__orig_class__") else other.__class__
            )
        constructor = extract_inner_type(constructor)
        merged = constructor(**kwargs)
        merged.resource_name = self.resource_name
        if other.resource_name:
            merged.resource_name = other.resource_name
        # also pass if resolved
        merged.__is_resolved__ = other.__is_resolved__
        merged.__exception__ = other.__exception__
        return merged  # type: ignore

    def copy(self) -> "Incremental[TCursorValue]":
        # merge creates a copy
        return self.merge(self)

    def on_resolved(self) -> None:
        compile_path(self.cursor_path)
        if self.end_value is not None and self.initial_value is None:
            raise ConfigurationValueError(
                "Incremental 'end_value' was specified without 'initial_value'. 'initial_value' is"
                " required when using 'end_value'."
            )
        self._cursor_datetime_check(self.initial_value, "initial_value")
        self._cursor_datetime_check(self.initial_value, "end_value")
        # Ensure end value is "higher" than initial value
        if (
            self.end_value is not None
            and self.last_value_func([self.end_value, self.initial_value]) != self.end_value
        ):
            if self.last_value_func in (min, max):
                adject = "higher" if self.last_value_func is max else "lower"
                msg = (
                    f"Incremental 'initial_value' ({self.initial_value}) is {adject} than"
                    f" 'end_value` ({self.end_value}). 'end_value' must be {adject} than"
                    " 'initial_value'"
                )
            else:
                msg = (
                    f"Incremental 'initial_value' ({self.initial_value}) is greater than"
                    f" 'end_value' ({self.end_value}) as determined by the custom"
                    " 'last_value_func'. The result of"
                    f" '{self.last_value_func.__name__}([end_value, initial_value])' must equal"
                    " 'end_value'"
                )
            raise ConfigurationValueError(msg)

    def parse_native_representation(self, native_value: Any) -> None:
        if isinstance(native_value, Incremental):
            if self is self.EMPTY:
                raise ValueError("Trying to resolve EMPTY Incremental")
            if native_value is self.EMPTY:
                raise ValueError(
                    "Do not use EMPTY Incremental as default or explicit values. Pass None to reset"
                    " an incremental."
                )
            merged = self.merge(native_value)
            self.cursor_path = merged.cursor_path
            self.initial_value = merged.initial_value
            self.last_value_func = merged.last_value_func
            self.end_value = merged.end_value
            self.resource_name = merged.resource_name
            self._primary_key = merged._primary_key
            self.allow_external_schedulers = merged.allow_external_schedulers
            self.row_order = merged.row_order
            self.__is_resolved__ = self.__is_resolved__
        else:  # TODO: Maybe check if callable(getattr(native_value, '__lt__', None))
            # Passing bare value `incremental=44` gets parsed as initial_value
            self.initial_value = native_value

    def get_state(self) -> IncrementalColumnState:
        """Returns an Incremental state for a particular cursor column"""
        if self.end_value is not None:
            # End value uses mock state. We don't want to write it.
            return {
                "initial_value": self.initial_value,
                "last_value": self.initial_value,
                "unique_hashes": [],
            }

        if not self.resource_name:
            raise IncrementalUnboundError(self.cursor_path)

        self._cached_state = Incremental._get_state(self.resource_name, self.cursor_path)
        if len(self._cached_state) == 0:
            # set the default like this, setdefault evaluates the default no matter if it is needed or not. and our default is heavy
            self._cached_state.update(
                {
                    "initial_value": self.initial_value,
                    "last_value": self.initial_value,
                    "unique_hashes": [],
                }
            )
        return self._cached_state

    @staticmethod
    def _get_state(resource_name: str, cursor_path: str) -> IncrementalColumnState:
        state: IncrementalColumnState = (
            resource_state(resource_name).setdefault("incremental", {}).setdefault(cursor_path, {})
        )
        # if state params is empty
        return state

    @staticmethod
    def _cursor_datetime_check(value: Any, arg_name: str) -> None:
        if value and isinstance(value, datetime) and value.tzinfo is None:
            logger.warning(
                f"The {arg_name} argument {value} is a datetime without timezone. This may result"
                " in an error when such values  are compared by Incremental class. Note that `dlt`"
                " stores datetimes in timezone-aware types so the UTC timezone will be added by"
                " the destination"
            )

    @property
    def last_value(self) -> Optional[TCursorValue]:
        s = self.get_state()
        return s["last_value"]  # type: ignore

    def _transform_item(
        self, transformer: IncrementalTransform, row: TDataItem
    ) -> Optional[TDataItem]:
        row, self.start_out_of_range, self.end_out_of_range = transformer(row)
        # if we know that rows are ordered we can close the generator automatically
        # mind that closing pipe will not immediately close processing. it only closes the
        # generator so this page will be fully processed
        # TODO: we cannot close partially evaluated transformer gen. to implement that
        # we'd need to pass the source gen along with each yielded item and close this particular gen
        # NOTE: with that implemented we could implement add_limit as a regular transform having access to gen
        if self.can_close() and not self._bound_pipe.has_parent:
            self._bound_pipe.close()
        return row

    def get_incremental_value_type(self) -> Type[Any]:
        """Infers the type of incremental value from a class of an instance if those preserve the Generic arguments information."""
        return get_generic_type_argument_from_instance(self, self.initial_value)

    def _join_external_scheduler(self) -> None:
        """Detects existence of external scheduler from which `start_value` and `end_value` are taken. Detects Airflow and environment variables.
        The logical "start date" coming from external scheduler will set the `initial_value` in incremental. if additionally logical "end date" is
        present then also "end_value" will be set which means that resource state is not used and exactly this range of date will be loaded
        """
        # fit the pendulum into incremental type
        param_type = self.get_incremental_value_type()

        try:
            if param_type is not Any:
                data_type = py_type_to_sc_type(param_type)
        except Exception as ex:
            logger.warning(
                f"Specified Incremental last value type {param_type} is not supported. Please use"
                f" DateTime, Date, float, int or str to join external schedulers.({ex})"
            )
            return

        if param_type is Any:
            logger.warning(
                "Could not find the last value type of Incremental class participating in external"
                " schedule. Please add typing when declaring incremental argument in your resource"
                " or pass initial_value from which the type can be inferred."
            )
            return

        def _ensure_airflow_end_date(
            start_date: pendulum.DateTime, end_date: pendulum.DateTime
        ) -> Optional[pendulum.DateTime]:
            """if end_date is in the future or same as start date (manual run), set it to None so dlt state is used for incremental loading"""
            now = pendulum.now()
            if end_date is None or end_date > now or start_date == end_date:
                return now
            return end_date

        try:
            # we can move it to separate module when we have more of those
            from airflow.operators.python import get_current_context  # noqa

            context = get_current_context()
            start_date = context["data_interval_start"]
            end_date = _ensure_airflow_end_date(start_date, context["data_interval_end"])
            self.initial_value = coerce_from_date_types(data_type, start_date)
            if end_date is not None:
                self.end_value = coerce_from_date_types(data_type, end_date)
            else:
                self.end_value = None
            logger.info(
                f"Found Airflow scheduler: initial value: {self.initial_value} from"
                f" data_interval_start {context['data_interval_start']}, end value:"
                f" {self.end_value} from data_interval_end {context['data_interval_end']}"
            )
            return
        except TypeError as te:
            logger.warning(
                f"Could not coerce Airflow execution dates into the last value type {param_type}."
                f" ({te})"
            )
        except Exception:
            pass

        if start_value := os.environ.get("DLT_START_VALUE"):
            self.initial_value = coerce_value(data_type, "text", start_value)
            if end_value := os.environ.get("DLT_END_VALUE"):
                self.end_value = coerce_value(data_type, "text", end_value)
            else:
                self.end_value = None
            return

    def bind(self, pipe: SupportsPipe) -> "Incremental[TCursorValue]":
        """Called by pipe just before evaluation"""
        # bind the resource/pipe name
        if self.is_partial():
            raise IncrementalCursorPathMissing(pipe.name, None, None)
        self.resource_name = pipe.name
        self._bound_pipe = pipe
        # try to join external scheduler
        if self.allow_external_schedulers:
            self._join_external_scheduler()
        # set initial value from last value, in case of a new state those are equal
        self.start_value = self.last_value
        logger.info(
            f"Bind incremental on {self.resource_name} with initial_value: {self.initial_value},"
            f" start_value: {self.start_value}, end_value: {self.end_value}"
        )
        # cache state
        self._cached_state = self.get_state()
        self._make_transforms()
        return self

    def can_close(self) -> bool:
        """Checks if incremental is out of range and can be closed.

        Returns true only when `row_order` was set and
        1. results are ordered ascending and are above upper bound (end_value)
        2. results are ordered descending and are below or equal lower bound (start_value)
        """
        # ordered ascending, check if we cross upper bound
        return (
            self.row_order == "asc"
            and self.end_out_of_range
            or self.row_order == "desc"
            and self.start_out_of_range
        )

    def __str__(self) -> str:
        return (
            f"Incremental at 0x{id(self):x} for resource {self.resource_name} with cursor path:"
            f" {self.cursor_path} initial {self.initial_value} - {self.end_value} lv_func"
            f" {self.last_value_func}"
        )

    def _get_transformer(self, items: TDataItems) -> IncrementalTransform:
        # Assume list is all of the same type
        for item in items if isinstance(items, list) else [items]:
            if is_arrow_item(item):
                return self._transformers["arrow"]
            elif pandas is not None and isinstance(item, pandas.DataFrame):
                return self._transformers["arrow"]
            return self._transformers["json"]
        return self._transformers["json"]

    def __call__(self, rows: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        if rows is None:
            return rows

        transformer = self._get_transformer(rows)
        if isinstance(rows, list):
            rows = [
                item
                for item in (self._transform_item(transformer, row) for row in rows)
                if item is not None
            ]
        else:
            rows = self._transform_item(transformer, rows)

        # write back state
        self._cached_state["last_value"] = transformer.last_value
        if not transformer.deduplication_disabled:
            # compute hashes for new last rows
            unique_hashes = set(
                transformer.compute_unique_value(row, self.primary_key)
                for row in transformer.last_rows
            )
            # add directly computed hashes
            unique_hashes.update(transformer.unique_hashes)
            self._cached_state["unique_hashes"] = list(unique_hashes)

        return rows


Incremental.EMPTY = Incremental[Any]()
Incremental.EMPTY.__is_resolved__ = True


class IncrementalResourceWrapper(ItemTransform[TDataItem]):
    placement_affinity: ClassVar[float] = 1  # stick to end

    _incremental: Optional[Incremental[Any]] = None
    """Keeps the injectable incremental"""
    _from_hints: bool = False
    """If True, incremental was set explicitly from_hints"""
    _resource_name: str = None

    def __init__(self, primary_key: Optional[TTableHintTemplate[TColumnNames]] = None) -> None:
        """Creates a wrapper over a resource function that accepts Incremental instance in its argument to perform incremental loading.

        The wrapper delays instantiation of the Incremental to the moment of actual execution and is currently used by `dlt.resource` decorator.
        The wrapper explicitly (via `resource_name`) parameter binds the Incremental state to a resource state.
        Note that wrapper implements `FilterItem` transform interface and functions as a processing step in the before-mentioned resource pipe.

        Args:
            primary_key (TTableHintTemplate[TColumnKey], optional): A primary key to be passed to Incremental Instance at execution. Defaults to None.
        """
        self.primary_key = primary_key
        self.incremental_state: IncrementalColumnState = None
        self._allow_external_schedulers: bool = None
        self._bound_pipe: SupportsPipe = None

    @staticmethod
    def should_wrap(sig: inspect.Signature) -> bool:
        return IncrementalResourceWrapper.get_incremental_arg(sig) is not None

    @staticmethod
    def get_incremental_arg(sig: inspect.Signature) -> Optional[inspect.Parameter]:
        incremental_param: Optional[inspect.Parameter] = None
        for p in sig.parameters.values():
            annotation = extract_inner_type(p.annotation)
            if is_subclass(annotation, Incremental) or isinstance(p.default, Incremental):
                incremental_param = p
                break
        return incremental_param

    def wrap(self, sig: inspect.Signature, func: TFun) -> TFun:
        """Wrap the callable to inject an `Incremental` object configured for the resource."""
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
                if explicit_value is Incremental.EMPTY or p.default is Incremental.EMPTY:
                    raise ValueError(
                        "Do not use EMPTY Incremental as default or explicit values. Pass None to"
                        " reset an incremental."
                    )
                elif isinstance(explicit_value, Incremental):
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

            if (not new_incremental or new_incremental.is_partial()) and not self._incremental:
                if is_optional_type(p.annotation):
                    bound_args.arguments[p.name] = None  # Remove partial spec
                    return func(*bound_args.args, **bound_args.kwargs)
                raise ValueError(
                    f"{p.name} Incremental argument has no default. Please wrap its typing in"
                    " Optional[] to allow no incremental"
                )
            # pass Generic information from annotation to new_incremental
            if (
                new_incremental
                and not hasattr(new_incremental, "__orig_class__")
                and p.annotation
                and get_args(p.annotation)
            ):
                new_incremental.__orig_class__ = p.annotation  # type: ignore

            # set the incremental only if not yet set or if it was passed explicitly
            # NOTE: the _incremental may be also set by applying hints to the resource see `set_template` in `DltResource`
            if (new_incremental and p.name in bound_args.arguments) or not self._incremental:
                self.set_incremental(new_incremental)
            if not self._incremental.is_resolved():
                self._incremental.resolve()
            # in case of transformers the bind will be called before this wrapper is set: because transformer is called for a first time late in the pipe
            if self._resource_name:
                # rebind internal _incremental from wrapper that already holds
                # instance of a Pipe
                self.bind(None)
            bound_args.arguments[p.name] = self._incremental
            return func(*bound_args.args, **bound_args.kwargs)

        return _wrap  # type: ignore

    @property
    def incremental(self) -> Optional[Incremental[Any]]:
        return self._incremental

    def set_incremental(
        self, incremental: Optional[Incremental[Any]], from_hints: bool = False
    ) -> None:
        """Sets the incremental. If incremental was set from_hints, it can only be changed in the same manner"""
        if self._from_hints and not from_hints:
            # do not accept incremental if apply hints were used
            return
        self._from_hints = from_hints
        self._incremental = incremental

    @property
    def allow_external_schedulers(self) -> bool:
        """Allows the Incremental instance to get its initial and end values from external schedulers like Airflow"""
        if self._incremental:
            return self._incremental.allow_external_schedulers
        return self._allow_external_schedulers

    @allow_external_schedulers.setter
    def allow_external_schedulers(self, value: bool) -> None:
        self._allow_external_schedulers = value
        if self._incremental:
            self._incremental.allow_external_schedulers = value

    def bind(self, pipe: SupportsPipe) -> "IncrementalResourceWrapper":
        # if pipe is None we are re-binding internal incremental
        pipe = pipe or self._bound_pipe
        self._bound_pipe = pipe
        self._resource_name = pipe.name
        if self._incremental:
            if self._allow_external_schedulers is not None:
                self._incremental.allow_external_schedulers = self._allow_external_schedulers
            self._incremental.bind(pipe)
        return self

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        if not self._incremental:
            return item
        if self._incremental.primary_key is None:
            self._incremental.primary_key = self.primary_key
        elif self.primary_key is None:
            # propagate from incremental
            self.primary_key = self._incremental.primary_key
        return self._incremental(item, meta)


__all__ = [
    "Incremental",
    "IncrementalResourceWrapper",
    "IncrementalColumnState",
    "IncrementalCursorPathMissing",
    "IncrementalPrimaryKeyMissing",
    "IncrementalUnboundError",
    "LastValueFunc",
    "TCursorValue",
]
