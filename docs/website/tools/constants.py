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

# Snippets
SNIPPETS_FILE_SUFFIX = "-snippets.py"

# Tuba links
NUM_TUBA_LINKS = 10

# Examples settings
EXAMPLES_DESTINATION_DIR = f"./{MD_TARGET_DIR}examples/"
EXAMPLES_SOURCE_DIR = "../examples/"
EXAMPLES_EXCLUSIONS = [".", "_", "archive", "local_cache"]

# Destination capabilities settings
DESTINATION_CAPABILITIES_TARGET_DIR = "./docs_processed/dlt-ecosystem/destinations"
DESTINATION_CAPABILITIES_SOURCE_DIR = "../../dlt/destinations/impl"
DESTINATION_NAME_PATTERN = r"([a-z0-9_-]+?)(?:--|$)"
CAPABILITIES_TABLE_HEADER = "| Feature | Value | More |\n"

SELECTED_CAPABILITIES_ATTRIBUTES = {
    "preferred_loader_file_format", # /docs/dlt-ecosystem/file-formats
    "supported_loader_file_formats", # /docs/dlt-ecosystem/file-formats
    "preferred_staging_file_format", # /docs/dlt-ecosystem/file-formats
    "supported_staging_file_formats", # /docs/devel/dlt-ecosystem/file-formats
    "has_case_sensitive_identifiers", # /docs/general-usage/naming-convention#case-sensitive-and-insensitive-destinations
    "supported_merge_strategies", # /docs/general-usage/merge-loading#merge-strategies
    "supported_replace_strategies", # /docs/general-usage/full-loading#the-truncate-and-insert-strategy
    "supports_tz_aware_datetime", # /docs/general-usage/schema#handling-of-timestamp-and-time-zones
    "supports_naive_datetime", # /docs/general-usage/schema#handling-of-timestamp-and-time-zones
    "sqlglot_dialect", # /docs/general-usage/dataset-access/dataset
    "preferred_table_format", # /docs/dlt-ecosystem/table-formats
    "supported_table_formats", # /docs/dlt-ecosystem/table-formats
}

CAPABILITIES_DOC_LINK_PATTERNS = [
    ("_formats", "../file-formats/"),
    ("_strategies", "../../general-usage/merge-loading#merge-strategies"),
]

CAPABILITIES_DATA_TYPES_DOC_LINK = "[Data types](../../general-usage/schema#data-types)"

# Markers
DLT_MARKER = "@@@DLT"
TUBA_MARKER = f"{DLT_MARKER}_TUBA"
SNIPPET_MARKER = f"{DLT_MARKER}_SNIPPET"
SNIPPET_START_MARKER = f"{DLT_MARKER}_SNIPPET_START"
SNIPPET_END_MARKER = f"{DLT_MARKER}_SNIPPET_END"
CAPABILITIES_MARKER = f"{DLT_MARKER}_DESTINATION_CAPABILITIES"

# Strings to search for in check_docs
HTTP_LINK = "](/docs"
ABS_LINK = "](/"
ABS_IMG_LINK = "](/img"
