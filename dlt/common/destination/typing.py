from dlt.common.schema.typing import _TTableSchemaBase, TWriteDisposition


class PreparedTableSchema(_TTableSchemaBase, total=False):
    """Table schema with all hints prepared to be loaded"""

    write_disposition: TWriteDisposition
    _x_prepared: bool  # needed for the type checker
