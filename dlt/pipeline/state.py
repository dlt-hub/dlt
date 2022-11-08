from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.pipeline import PipelineContext
from dlt.common.typing import DictStrAny

from dlt.pipeline.configuration import StateInjectableContext
from dlt.pipeline.exceptions import PipelineStateNotAvailable
from dlt.pipeline.typing import TSourceState


def state() -> DictStrAny:
    container = Container()
    try:
        state: TSourceState = container[StateInjectableContext].state  # type: ignore
        return state.setdefault("sources", {})
    except ContextDefaultCannotBeCreated:
        # check if there's pipeline context
        proxy = container[PipelineContext]
        raise PipelineStateNotAvailable(proxy.is_active())
