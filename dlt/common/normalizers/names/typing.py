from typing import Any, Protocol, Sequence


class NamingConvention(Protocol):
    PATH_SEPARATOR: str

    def normalize_identifier(self, name: str) -> str:
        """Normalizes the identifier according to naming convention represented by this function"""
        ...

    def normalize_path(self, path: str) -> str:
        """Breaks path into identifiers using PATH_SEPARATOR, normalizes components and reconstitutes the path"""
        ...

    def normalize_make_path(self, *identifiers: Any) -> str:
        """Builds path out of path identifiers using PATH_SEPARATOR. Identifiers are not normalized"""
        ...

    def normalize_break_path(self, path: str) -> Sequence[str]:
        """Breaks path into sequence of identifiers"""
        ...

    def normalize_make_dataset_name(self, dataset_name: str, default_schema_name: str, schema_name: str) -> str:
        """Builds full db dataset (dataset) name out of (normalized) default dataset and schema name"""
        pass
