from collections.abc import Callable
from typing import List, Any

TSplitter = Callable[[str, Any], List[str | Any]]
