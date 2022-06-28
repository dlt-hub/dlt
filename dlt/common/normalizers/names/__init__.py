from typing import Callable, Sequence

# function signature to normalize names
TNormalizeNameFunc = Callable[[str], str]
# function signature to make paths
TNormalizeMakePath = Callable[..., str]
# function signature to break path into components
TNormalizeBreakPath = Callable[[str], Sequence[str]]
