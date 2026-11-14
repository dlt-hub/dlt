from typing import Dict, List, Literal, Optional, Sequence, Tuple

from dlt.common.exceptions import DltException

TLineageFailureReason = Literal[
    "unknown_column",
    "unknown_qualifier",
    "column_not_found",
    "ambiguous_column",
    "unknown_table",
    "unresolved_star",
    "not_select",
    "ambiguous_table",
    "other",
]
TLineageSource = Tuple[str, Optional[List[str]]]
"""Label of a query source, for example table `ds.jobs` (alias `j`), and its columns.
The columns are `None` when unknown."""

MAX_LISTED_NAMES = 50


def join_names(names: Sequence[str]) -> str:
    """Comma-separated `names`, cut at `MAX_LISTED_NAMES`."""
    listed = ", ".join(names[:MAX_LISTED_NAMES])
    if len(names) > MAX_LISTED_NAMES:
        listed += f", ... and {len(names) - MAX_LISTED_NAMES} more"
    return listed


class DatasetException(DltException):
    pass


class RelationHasQueryException(DatasetException):
    def __init__(self, attempted_change: str) -> None:
        msg = (
            "This readable relation was created with a provided sql query. You cannot change"
            f" `{attempted_change}`. Please change the original sql query."
        )
        super().__init__(msg)


class RelationUnknownColumnException(DatasetException):
    def __init__(self, column_name: str) -> None:
        msg = (
            f"The selected column `{column_name}` is not known in the dlt schema for this relation."
        )
        super().__init__(msg)


class LineageFailedException(DltException):
    """Raised when dlt cannot compute the lineage of an SQL query against the dlt schema."""

    def __init__(
        self,
        msg: Optional[str] = None,
        *,
        resource_name: Optional[str] = None,
        reason: TLineageFailureReason = "other",
        column: Optional[str] = None,
        qualifier: Optional[str] = None,
        sources: Optional[Dict[str, TLineageSource]] = None,
        suggestions: Optional[List[str]] = None,
        datasets: Optional[List[str]] = None,
        query: Optional[str] = None,
        dialect: Optional[str] = None,
        dataset_name: Optional[str] = None,
        table_name: Optional[str] = None,
        notes: Optional[List[str]] = None,
    ) -> None:
        super().__init__(msg)
        self.msg = msg
        """Raw sqlglot message or failed check, rendered for `not_select` and `other` reasons"""
        self.reason = reason
        self.column = column
        self.qualifier = qualifier
        """Table alias, CTE or unqualified table name that the failure refers to"""
        self.sources = sources or {}
        """Sources of the query scope that failed, keyed by alias"""
        self.suggestions = suggestions or []
        self.datasets = datasets or []
        self.query = query
        self.dialect = dialect
        self.dataset_name = dataset_name
        """Dataset of the relation, set by `Relation`"""
        self.table_name = table_name
        """Table the resource writes to, set by the extractor"""
        self.resource_name = resource_name
        self.notes = notes or []
        """Facts from the dlt schema that explain the failure, set by `Relation`"""

    def __str__(self) -> str:
        head = "dlt cannot compute the lineage of the SQL query"
        if self.dataset_name:
            head += f" for dataset `{self.dataset_name}`"
        if self.resource_name:
            table = f" (table `{self.table_name}`)" if self.table_name else ""
            head = f"In resource `{self.resource_name}`{table}: {head}"
        lines = [f"{head}. {self._headline()}", *self._known_columns(), *self.notes]
        if self.suggestions:
            lines.append("Did you mean: " + ", ".join(f"`{s}`" for s in self.suggestions) + "?")
        if self.query:
            dialect = f" ({self.dialect})" if self.dialect else ""
            lines.append(f"Query{dialect}: {self.query}")
        return "\n".join(lines)

    def _labels(self, unknown_only: bool = False) -> str:
        return ", ".join(
            label for label, columns in self.sources.values() if not unknown_only or columns is None
        )

    def _headline(self) -> str:
        if self.reason == "unknown_column":
            if self.qualifier in self.sources:
                label = self.sources[self.qualifier][0]
            else:
                label = f"`{self.qualifier}`"
            # labels are lowercase because they also render inside sentences
            return f"{label[:1].upper()}{label[1:]} has no column `{self.column}`."
        if self.reason == "unknown_qualifier":
            return (
                f"`{self.qualifier}.{self.column}` refers to `{self.qualifier}`. The query has no"
                f" table, alias or CTE `{self.qualifier}`. Query sources: {self._labels()}."
            )
        if self.reason == "column_not_found":
            return (
                f"No source of the query has column `{self.column}`. Query sources:"
                f" {self._labels()}."
            )
        if self.reason == "ambiguous_column":
            return (
                f"Column `{self.column}` is ambiguous: it exists in {self._labels()}. Qualify it"
                " with the table alias."
            )
        if self.reason == "unknown_table":
            return (
                f"Column `{self.column}` has no known source: the columns of"
                f" {self._labels(unknown_only=True)} are not known to dlt."
            )
        if self.reason == "unresolved_star":
            return (
                f"dlt cannot expand `*`: the columns of {self._labels(unknown_only=True)} are not"
                " known to dlt. Select the columns explicitly or limit `*` to known tables, for"
                " example `known_table.*`."
            )
        if self.reason == "ambiguous_table":
            datasets = ", ".join(f"`{d}`" for d in self.datasets)
            return (
                f"Table `{self.qualifier}` exists in several datasets: {datasets}. Qualify it with"
                " the dataset name."
            )
        return self.msg or "Unknown error."

    def _known_columns(self) -> List[str]:
        if self.reason == "unknown_column":
            aliases = [self.qualifier]
        elif self.reason in ("column_not_found", "ambiguous_column", "unknown_table"):
            aliases = list(self.sources)
        else:
            return []
        lines = []
        for alias in aliases:
            label, columns = self.sources.get(alias, (None, None))
            if columns:
                lines.append(
                    f"Known columns of {label}, {len(columns)} in total: {join_names(columns)}"
                )
        return lines
