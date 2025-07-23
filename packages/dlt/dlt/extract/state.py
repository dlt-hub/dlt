import contextlib
import contextvars
from typing import AsyncIterator, Iterator, List, Optional

from dlt.common.configuration import known_sections
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.exceptions import (
    PipelineStateNotAvailable,
    SourceSectionNotAvailable,
    ResourceNameNotAvailable,
)
from dlt.common.jsonpath import delete_matches, TAnyJsonPath
from dlt.common.pipeline import TPipelineState, pipeline_state
from dlt.common.typing import DictStrAny, REPattern

from dlt.extract.items import SupportsPipe


def _sources_state(pipeline_state_: Optional[TPipelineState] = None, /) -> DictStrAny:
    if pipeline_state_ is None:
        state, _ = pipeline_state(Container())
    else:
        state = pipeline_state_
    if state is None:
        raise PipelineStateNotAvailable()

    sources_state_: DictStrAny = state.setdefault(known_sections.SOURCES, {})  # type: ignore
    return sources_state_


def source_state() -> DictStrAny:
    """Returns a dictionary with the source-scoped state. Source-scoped state may be shared across the resources of a particular source. Please avoid using source scoped state. Check
    the `resource_state` function for resource-scoped state that is visible within particular resource. Dlt state is preserved across pipeline runs and may be used to implement incremental loads.

    #### Note:
    The source state is a python dictionary-like object that is available within the `@dlt.source` and `@dlt.resource` decorated functions and may be read and written to.
    The data within the state is loaded into destination together with any other extracted data and made automatically available to the source/resource extractor functions when they are run next time.
    When using the state:
    * The source state is scoped to a particular source and will be stored under the source name in the pipeline state
    * It is possible to share state across many sources if they share a schema with the same name
    * Any JSON-serializable values can be written and the read from the state. `dlt` dumps and restores instances of Python bytes, DateTime, Date and Decimal types.
    * The state available in the source decorated function is read only and any changes will be discarded.
    * The state available in the resource decorated function is writable and written values will be available on the next pipeline run
    """
    container = Container()

    # get the source name from the section context
    source_state_key: str = None
    with contextlib.suppress(ContextDefaultCannotBeCreated):
        sections_context = container[ConfigSectionContext]
        source_state_key = sections_context.source_state_key

    if not source_state_key:
        raise SourceSectionNotAvailable()

    try:
        state = _sources_state()
    except PipelineStateNotAvailable as e:
        # Reraise with source section
        raise PipelineStateNotAvailable(source_state_key) from e

    return state.setdefault(source_state_key, {})  # type: ignore[no-any-return]


def delete_source_state_keys(
    key: TAnyJsonPath, source_state_: Optional[DictStrAny] = None, /
) -> None:
    """Remove one or more key from the source state.
    The `key` can be any number of keys and/or json paths to be removed.
    """
    state_ = source_state() if source_state_ is None else source_state_
    delete_matches(key, state_)


def resource_state(
    resource_name: str = None, source_state_: Optional[DictStrAny] = None, /
) -> DictStrAny:
    """Returns a dictionary with the resource-scoped state. Resource-scoped state is visible only to resource requesting the access. dlt state is preserved across pipeline runs and may be used to implement incremental loads.

    Note that this function accepts the resource name as optional argument. There are rare cases when `dlt` is not able to resolve resource name due to requesting function
    working in different thread than the main. You'll need to pass the name explicitly when you request resource_state from async functions or functions decorated with @defer.

    Summary:
        The resource state is a python dictionary-like object that is available within the `@dlt.resource` decorated functions and may be read and written to.
        The data within the state is loaded into destination together with any other extracted data and made automatically available to the source/resource extractor functions when they are run next time.
        When using the state:
        * The resource state is scoped to a particular resource requesting it.
        * Any JSON-serializable values can be written and the read from the state. `dlt` dumps and restores instances of Python bytes, DateTime, Date and Decimal types.
        * The state available in the resource decorated function is writable and written values will be available on the next pipeline run

    Example:
        The most typical use case for the state is to implement incremental load.
        >>> @dlt.resource(write_disposition="append")
        >>> def players_games(chess_url, players, start_month=None, end_month=None):
        >>>     checked_archives = dlt.current.resource_state().setdefault("archives", [])
        >>>     archives = players_archives(chess_url, players)
        >>>     for url in archives:
        >>>         if url in checked_archives:
        >>>             print(f"skipping archive {url}")
        >>>             continue
        >>>         else:
        >>>             print(f"getting archive {url}")
        >>>             checked_archives.append(url)
        >>>         # get the filtered archive
        >>>         r = requests.get(url)
        >>>         r.raise_for_status()
        >>>         yield r.json().get("games", [])

    Here we store all the urls with game archives in the state and we skip loading them on next run. The archives are immutable. The state will grow with the coming months (and more players).
    Up to few thousand archives we should be good though.
    Args:
        resource_name (str, optional): forces to use state for a resource with this name. Defaults to None.
        source_state_ (Optional[DictStrAny], optional): Alternative source state. Defaults to None.

    Raises:
        ResourceNameNotAvailable: Raise if used outside of resource context or from a different thread than main

    Returns:
        DictStrAny: State dictionary
    """
    state_ = source_state() if source_state_ is None else source_state_
    # backtrace to find the shallowest resource
    if not resource_name:
        resource_name = get_current_pipe_name()
    return state_.setdefault("resources", {}).setdefault(resource_name, {})  # type: ignore


def reset_resource_state(resource_name: str, source_state_: Optional[DictStrAny] = None, /) -> None:
    """Resets the resource state with name `resource_name` by removing it from `source_state`

    Args:
        resource_name (str): The resource key to reset
        source_state_ (Optional[DictStrAny]): Optional source state dictionary to operate on. Use when working outside source context.
    """
    state_ = source_state() if source_state_ is None else source_state_
    if "resources" in state_ and resource_name in state_["resources"]:
        state_["resources"].pop(resource_name)


def get_matching_sources(
    pattern: REPattern, pipeline_state: Optional[TPipelineState] = None, /
) -> List[str]:
    """Get all source names in state matching the regex pattern"""
    state_ = _sources_state(pipeline_state)
    return [key for key in state_ if pattern.match(key)]


def _get_matching_resources(
    pattern: REPattern, source_state_: Optional[DictStrAny] = None, /
) -> List[str]:
    """Get all resource names in state matching the regex pattern"""
    state_ = source_state() if source_state_ is None else source_state_
    if "resources" not in state_:
        return []
    return [key for key in state_["resources"] if pattern.match(key)]


_CURRENT_PIPE_CONTEXT: contextvars.ContextVar[Optional[SupportsPipe]] = contextvars.ContextVar(
    "current_pipe", default=None
)
"""Currently executing pipe set for all futures and main thread of execution pipe"""


@contextlib.contextmanager
def pipe_context(pipe: SupportsPipe) -> Iterator[None]:
    """Sync context-manager that sets the variable temporarily."""
    token = _CURRENT_PIPE_CONTEXT.set(pipe)
    try:
        yield
    finally:
        _CURRENT_PIPE_CONTEXT.reset(token)


@contextlib.asynccontextmanager
async def async_pipe_context(pipe: SupportsPipe) -> AsyncIterator[None]:
    """Async variant for `async with` blocks."""
    token = _CURRENT_PIPE_CONTEXT.set(pipe)
    try:
        yield
    finally:
        _CURRENT_PIPE_CONTEXT.reset(token)


def get_current_pipe() -> SupportsPipe:
    """When executed from within dlt.resource decorated function, gets execution pipe"""
    pipe = _CURRENT_PIPE_CONTEXT.get()
    if pipe is None:
        raise ResourceNameNotAvailable()
    return pipe


def get_current_pipe_name() -> str:
    """When executed from within dlt.resource decorated function, gets pipe name associated with current resource

    Pipe name is the same as resource name for all currently known cases.
    """
    pipe = _CURRENT_PIPE_CONTEXT.get()
    if pipe is None:
        raise ResourceNameNotAvailable()
    return pipe.name
