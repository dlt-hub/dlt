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
    "preferred_loader_file_format",
    "supported_loader_file_formats",
    "preferred_staging_file_format",
    "supported_staging_file_formats",
    "has_case_sensitive_identifiers",
    "supported_merge_strategies",
    "supported_replace_strategies",
    "supports_tz_aware_datetime",
    "supports_naive_datetime",
    "sqlglot_dialect",
    "preferred_table_format",
    "supported_table_formats",
}

CAPABILITIES_DOC_LINK_PATTERNS = [
    ("table", "../table-formats/"),
    ("file_format", "../file-formats/"),
    ("merge", "../../general-usage/merge-loading#merge-strategies"),
    (
        "replace",
        "../../general-usage/full-loading#choosing-the-correct-replace-strategy-for-your-full-load",
    ),
    ("time", "../../general-usage/schema#handling-of-timestamp-and-time-zones"),
    ("dialect", "../../general-usage/dataset-access/dataset"),
    (
        "identifier",
        "../../general-usage/naming-convention#case-sensitive-and-insensitive-destinations",
    ),
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
