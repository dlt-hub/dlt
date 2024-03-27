from typing_extensions import get_args
from dlt.cli import echo as fmt
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.schema.typing import TSchemaEvolutionMode, TWriteDisposition


supported_formats = "|".join(get_args(TLoaderFileFormat))
supported_evolution_modes = "|".join(get_args(TSchemaEvolutionMode))
supported_write_disposition = "|".join(get_args(TWriteDisposition))

run_args_help = "".join(
    (
        "Supported arguments passed to pipeline.run are",
        fmt.info_style("\n - destination=string"),
        fmt.info_style("\n - staging=string,"),
        fmt.info_style("\n - credentials=string,"),
        fmt.info_style("\n - table_name=string,"),
        fmt.info_style(f"\n - write_disposition={supported_write_disposition},"),
        fmt.info_style("\n - dataset_name=string,"),
        fmt.info_style("\n - primary_key=string,"),
        fmt.info_style(f"\n - schema_contract={supported_evolution_modes},"),
        fmt.info_style(f"\n - loader_file_format={supported_formats}"),
    )
)
