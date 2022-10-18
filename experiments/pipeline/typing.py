from typing import Literal, TypedDict, Optional


TPipelineStep = Literal["extract", "normalize", "load"]

class TPipelineState(TypedDict):
    pipeline_name: str
    dataset_name: str
    default_schema_name: Optional[str]
    # destination_name: Optional[str]


# TSourceState = NewType("TSourceState", DictStrAny)

# class TPipelineState()
#     sources: Dict[str, TSourceState]
