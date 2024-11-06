from typing import Any, List
from dlt.common.exceptions import MissingDependencyException

try:
    import ibis  # type: ignore[import-untyped]
    from ibis import Table
    from ibis import memtable
    from ibis import BaseBackend
    from ibis.backends.sql import SQLBackend
except ModuleNotFoundError:
    raise MissingDependencyException("dlt ibis Helpers", ["ibis"])

from dlt.destinations.dataset import ReadableDBAPIDataset


class DltBackend(ReadableDBAPIDataset, SQLBackend):
    
    def list_tables(self) -> List[str]:
        return list(self.schema.tables.keys())
    
    def version(self) -> str:
        pass
    
    def dialect(self) -> str:
        pass
    
    def drop_table(self, table_name: str) -> None:
        pass
    
    def drop_view(self, view_name: str) -> None:
        pass
    
    def create_table(self, table_name: str, table: Table) -> None:
        pass
    
    def create_view(self, view_name: str, view: Table) -> None:
        pass
    
    def disconnect(self) -> None:
        pass
    
    def _get_schema_using_query(self, query: str):
        return None


