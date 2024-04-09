from typing import Literal

TPipelineStep = Literal["sync", "extract", "normalize", "load"]

TRefreshMode = Literal["full", "replace"]
