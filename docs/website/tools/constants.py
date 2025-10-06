"""
Constants for documentation preprocessing.
"""

# Directory paths
MD_SOURCE_DIR = "docs/"
MD_TARGET_DIR = "docs_processed/"

# File extensions
MOVE_FILES_EXTENSION = [".md", ".mdx", ".py", ".png", ".jpg", ".jpeg"]
DOCS_EXTENSIONS = [".md", ".mdx"]
WATCH_EXTENSIONS = [".md", ".py", ".toml"]

# Timing
DEBOUNCE_INTERVAL_MS = 100

# Snippets
SNIPPETS_FILE_SUFFIX = "-snippets.py"

# Tuba links
NUM_TUBA_LINKS = 10

# Examples settings
EXAMPLES_DESTINATION_DIR = f"./{MD_TARGET_DIR}examples/"
EXAMPLES_SOURCE_DIR = "../examples/"
EXAMPLES_EXCLUSIONS = [".", "_", "archive", "local_cache"]

# Markers
DLT_MARKER = "@@@DLT"
TUBA_MARKER = f"{DLT_MARKER}_TUBA"
SNIPPET_MARKER = f"{DLT_MARKER}_SNIPPET"
SNIPPET_START_MARKER = f"{DLT_MARKER}_SNIPPET_START"
SNIPPET_END_MARKER = f"{DLT_MARKER}_SNIPPET_END"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"

# Strings to search for in check_docs
HTTP_LINK = "](https://dlthub.com/docs"
ABS_LINK = "](/"
ABS_IMG_LINK = "](/img"

