from typing_extensions import get_args
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.schema.typing import TSchemaEvolutionMode, TWriteDisposition


supported_formats = "|".join(get_args(TLoaderFileFormat))
supported_evolution_modes = "|".join(get_args(TSchemaEvolutionMode))
supported_write_disposition = "|".join(get_args(TWriteDisposition))

run_args_help = (
    "Arguments passed to pipeline.run, allowed options "
    "destination=string,\n"
    "staging=string,\n"
    "credentials=string,\n"
    "table_name=string,\n"
    f"write_disposition={supported_write_disposition},\n"
    "dataset_name=string,\n"
    "primary_key=string,\n"
    f"schema_contract={supported_evolution_modes},\n"
    f"loader_file_format={supported_formats},\n"
)
