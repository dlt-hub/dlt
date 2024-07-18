from typing import Callable, Union, List, Dict, Any

ChunkInputT = Union[str, Dict[str, Any], Any]
ChunkOutputT = List[Any]

TSplitter = Callable[[ChunkInputT, Any], ChunkOutputT]
