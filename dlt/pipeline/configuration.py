from typing import ClassVar, Optional, TYPE_CHECKING
from typing_extensions import runtime

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration, BaseConfiguration, ContainerInjectableContext
from dlt.common.typing import TSecretValue
from dlt.common.utils import uniq_id

from experiments.pipeline.typing import TPipelineState


@configspec
class PipelineConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    working_dir: Optional[str] = None
    pipeline_secret: Optional[TSecretValue] = None
    runtime: RunConfiguration

    def check_integrity(self) -> None:
        if not self.pipeline_secret:
            self.pipeline_secret = TSecretValue(uniq_id())
        if not self.pipeline_name:
            self.pipeline_name = self.runtime.pipeline_name


@configspec(init=True)
class StateInjectableContext(ContainerInjectableContext):
    state: TPipelineState

    can_create_default: ClassVar[bool] = False

    if TYPE_CHECKING:
        def __init__(self, state: TPipelineState = None) -> None:
            ...
