import pyarrow as pa
import unicodedata
from dlt.cli.init_command import (
    _list_verified_sources,
    DEFAULT_VERIFIED_SOURCES_REPO,
)
from dlt.cli.echo import suppress_echo


def format_df(table: pa.Table, info: str = "") -> str:
    # just copy metadata
    df = table.to_pandas().copy(deep=False)

    # Remove non ascii characters from the columns. Those hang claude desktop - server
    # at least on Windows
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda x: unicodedata.normalize("NFKD", str(x))
            .encode("ascii", "ignore")
            .decode("ascii")
            .replace("\n", " ")
            .replace("\r", " ")
        )
    if info:
        info += "csv delimited with | containing header starts in next line:\n"
    return str(info + df.to_csv(index=False, sep="|"))


def get_verified_sources() -> dict[str, str]:
    """List all available verified sources, cloning from dlt-verified-sources"""
    sources = {}
    with suppress_echo():
        for source_name, source_config in _list_verified_sources(
            repo_location=DEFAULT_VERIFIED_SOURCES_REPO
        ).items():
            sources[source_name] = source_config.doc
        return sources
