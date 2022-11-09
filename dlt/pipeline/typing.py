from typing import Any, Dict, Literal, TypedDict, Optional


TPipelineStep = Literal["extract", "normalize", "load"]

class TPipelineState(TypedDict, total=False):
    pipeline_name: str
    dataset_name: str
    default_schema_name: Optional[str]
    destination: Optional[str]


class TSourceState(TPipelineState):
    sources: Dict[str, Dict[str, Any]]
